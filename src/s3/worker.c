/*-------------------------------------------------------------------------
 *
 * worker.c
 *		Routines for S3 worker process.
 *
 * Copyright (c) 2024, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/s3/worker.c
 *
 *-------------------------------------------------------------------------
 */
#include "c.h"
#include "postgres.h"

#include "orioledb.h"

#include "btree/io.h"
#include "s3/headers.h"
#include "s3/queue.h"
#include "s3/requests.h"
#include "s3/worker.h"

#include "access/xlog_internal.h"
#include "common/hashfn.h"
#include "common/string.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/bgwriter.h"
#include "storage/bufmgr.h"
#include "storage/condition_variable.h"
#include "storage/fd.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/sinvaladt.h"
#include "transam/undo.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timeout.h"

#include "pgstat.h"
#include "utils/wait_event.h"

#include <sys/fcntl.h>
#include <unistd.h>

typedef struct S3WorkerCtl
{
	pg_atomic_uint32 pgFilesCnt;
	pg_atomic_uint32 pgFilesFlushCnt;

	ConditionVariable pgFilesFlushedCV;

	S3TaskLocation locations[FLEXIBLE_ARRAY_MEMBER];
} S3WorkerCtl;

static volatile sig_atomic_t	shutdown_requested = false;

static S3WorkerCtl *workers_ctl = NULL;
static HTAB		   *pgfiles_hash = NULL;
static StringInfo	pgfiles_crc_buf = NULL;

typedef struct PGDataHashEntry
{
	char		filename[MAXPGPATH];
	uint64		crc;
} PGDataHashEntry;

static HTAB *init_pgfiles_hash(void);
static void flush_pgfiles_hash(int worker_num);
static bool check_pgfile_changed(const char *filename, Pointer new_data,
								 uint64 size);

Size
s3_workers_shmem_needs(void)
{
	Size		size;

	size = CACHELINEALIGN(offsetof(S3WorkerCtl, locations) +
						  sizeof(S3TaskLocation) * s3_num_workers);

	return size;
}

void
s3_workers_init_shmem(Pointer ptr, bool found)
{
	int			i;

	workers_ctl = (S3WorkerCtl *) ptr;

	if (!found)
	{
		pg_atomic_init_u32(&workers_ctl->pgFilesCnt, 0);
		pg_atomic_init_u32(&workers_ctl->pgFilesFlushCnt, 0);

		ConditionVariableInit(&workers_ctl->pgFilesFlushedCV);

		for (i = 0; i < s3_num_workers; i++)
			workers_ctl->locations[i] = InvalidS3TaskLocation;
	}
}

static void
handle_sigterm(SIGNAL_ARGS)
{
	shutdown_requested = true;
	SetLatch(MyLatch);
}

void
register_s3worker(int num)
{
	BackgroundWorker worker;

	/* Set up background worker parameters */
	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_CLASS_SYSTEM;
	worker.bgw_start_time = BgWorkerStart_PostmasterStart;
	worker.bgw_restart_time = 0;
	worker.bgw_main_arg = Int32GetDatum(num);
	strcpy(worker.bgw_library_name, "orioledb");
	strcpy(worker.bgw_function_name, "s3worker_main");
	pg_snprintf(worker.bgw_name, sizeof(worker.bgw_name),
				"orioledb s3 worker %d", num);
	strcpy(worker.bgw_type, "orioledb s3 worker");
	RegisterBackgroundWorker(&worker);
}

/*
 * Wait until all S3 workers flushed their hashes.
 */
static void
s3_workers_wait_for_flush(void)
{
	Assert(workers_ctl != NULL);

	for (;;)
	{
		if (pg_atomic_read_u32(&workers_ctl->pgFilesFlushCnt) == 0)
			break;

		ConditionVariableSleep(&workers_ctl->pgFilesFlushedCV, WAIT_EVENT_BGWRITER_MAIN);
	}

	ConditionVariableCancelSleep();
}

/*
 * Compact all S3 workers PGDATA hash files into one file.
 */
void
s3_workers_compact_hash(void)
{
	int			file;

	s3_workers_wait_for_flush();

	file = BasicOpenFile(PGDATA_CRC_FILENAME, O_CREAT | O_WRONLY | O_TRUNC | PG_BINARY);
	if (file < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", PGDATA_CRC_FILENAME)));

	for (int i = 0; i < s3_num_workers; i++)
	{
		int			worker_file;
		char		worker_filename[MAXPGPATH];
		int			readBytes;
		char		buffer[8192];

		snprintf(worker_filename, sizeof(worker_filename), "%s.%d",
				 PGDATA_CRC_FILENAME, i);

		worker_file = BasicOpenFile(worker_filename, O_RDONLY | PG_BINARY);
		if (worker_file < 0)
		{
			/*
			 * In case if this worker didn't manage to process any
			 * S3TaskTypeWritePGFile just skip it.
			 */
			if (errno == ENOENT)
				continue;

			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open file \"%s\": %m", worker_filename)));
		}

		while ((readBytes = pg_pread(worker_file, buffer, sizeof(buffer), 0)) > 0)
		{
			if (pg_pwrite(file, buffer, sizeof(buffer), 0) != sizeof(buffer))
				ereport(ERROR,
						(errcode_for_file_access(),
						 errmsg("could not write file \"%s\": %m", PGDATA_CRC_FILENAME)));
		}

		if (readBytes < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m", worker_filename)));

		close(worker_file);
	}

	if (pg_fsync(file) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m", PGDATA_CRC_FILENAME)));

	close(file);

	/* The compaction is completed, now we can remove worker files */
	for (int i = 0; i < s3_num_workers; i++)
	{
		char		worker_filename[MAXPGPATH];

		snprintf(worker_filename, sizeof(worker_filename), "%s.%d",
				 PGDATA_CRC_FILENAME, i);

		unlink(worker_filename);
	}
}

/*
 * Process the task at given location.
 */
static void
s3process_task(uint64 taskLocation)
{
	S3Task	   *task = (S3Task *) s3_queue_get_task(taskLocation);
	char	   *objectname;

	Assert(workers_ctl != NULL);

	if (task->type == S3TaskTypeWriteFile)
	{
		char	   *filename = task->typeSpecific.writeFile.filename;
		long		result;

		if (filename[0] == '.' && filename[1] == '/')
			filename += 2;

		objectname = psprintf("data/%u/%s",
							  task->typeSpecific.writeFile.chkpNum,
							  filename);

		elog(DEBUG1, "S3 put %s %s", objectname, filename);

		result = s3_put_file(objectname, filename, false);

		pfree(objectname);

		if ((result == S3_RESPONSE_OK) && task->typeSpecific.writeFile.delete)
			unlink(filename);
	}
	else if (task->type == S3TaskTypeWriteEmptyDir)
	{
		char	   *dirname = task->typeSpecific.writeEmptyDir.dirname;

		if (dirname[0] == '.' && dirname[1] == '/')
			dirname += 2;

		objectname = psprintf("data/%u/%s/",
							  task->typeSpecific.writeFile.chkpNum,
							  dirname);

		elog(DEBUG1, "S3 dir put %s %s", objectname, dirname);

		s3_put_empty_dir(objectname);
		pfree(objectname);
	}
	else if (task->type == S3TaskTypeReadFilePart &&
			 task->typeSpecific.filePart.segNum < 0)
	{
		char	   *filename;
		SeqBufTag	chkp_tag;

		memset(&chkp_tag, 0, sizeof(chkp_tag));
		chkp_tag.datoid = task->typeSpecific.filePart.datoid;
		chkp_tag.relnode = task->typeSpecific.filePart.relnode;
		chkp_tag.num = task->typeSpecific.filePart.chkpNum;
		chkp_tag.type = 'm';

		filename = get_seq_buf_filename(&chkp_tag);

		objectname = psprintf("orioledb_data/%u/%u/%u.map",
							  task->typeSpecific.filePart.chkpNum,
							  task->typeSpecific.filePart.datoid,
							  task->typeSpecific.filePart.relnode);

		elog(DEBUG1, "S3 map get %s %s", objectname, filename);

		s3_get_file(objectname, filename);

		pfree(filename);
		pfree(objectname);
	}
	else if (task->type == S3TaskTypeReadFilePart)
	{
		char	   *filename;
		S3HeaderTag tag;

		filename = btree_filename(task->typeSpecific.filePart.datoid,
								  task->typeSpecific.filePart.relnode,
								  task->typeSpecific.filePart.segNum,
								  task->typeSpecific.filePart.chkpNum);

		objectname = psprintf("orioledb_data/%u/%u/%u.%u.%u",
							  task->typeSpecific.filePart.chkpNum,
							  task->typeSpecific.filePart.datoid,
							  task->typeSpecific.filePart.relnode,
							  task->typeSpecific.filePart.segNum,
							  task->typeSpecific.filePart.partNum);

		elog(DEBUG1, "S3 part get %s %s", objectname, filename);

		s3_get_file_part(objectname, filename, task->typeSpecific.filePart.partNum);

		tag.datoid = task->typeSpecific.filePart.datoid;
		tag.relnode = task->typeSpecific.filePart.relnode;
		tag.checkpointNum = task->typeSpecific.filePart.chkpNum;
		tag.segNum = task->typeSpecific.filePart.segNum;

		s3_header_mark_part_loaded(tag, task->typeSpecific.filePart.partNum);

		pfree(filename);
		pfree(objectname);
	}
	else if (task->type == S3TaskTypeWriteFilePart &&
			 task->typeSpecific.filePart.segNum >= 0)
	{
		char	   *filename;
		S3HeaderTag tag;

		filename = btree_filename(task->typeSpecific.filePart.datoid,
								  task->typeSpecific.filePart.relnode,
								  task->typeSpecific.filePart.segNum,
								  task->typeSpecific.filePart.chkpNum);

		objectname = psprintf("orioledb_data/%u/%u/%u.%u.%u",
							  task->typeSpecific.filePart.chkpNum,
							  task->typeSpecific.filePart.datoid,
							  task->typeSpecific.filePart.relnode,
							  task->typeSpecific.filePart.segNum,
							  task->typeSpecific.filePart.partNum);

		elog(DEBUG1, "S3 part put %s %s", objectname, filename);

		tag.datoid = task->typeSpecific.filePart.datoid;
		tag.relnode = task->typeSpecific.filePart.relnode;
		tag.checkpointNum = task->typeSpecific.filePart.chkpNum;
		tag.segNum = task->typeSpecific.filePart.segNum;

		s3_header_mark_part_writing(tag, task->typeSpecific.filePart.partNum);

		PG_TRY();
		{
			(void) s3_put_file_part(objectname, filename, task->typeSpecific.filePart.partNum);
		}
		PG_CATCH();
		{
			s3_header_mark_part_not_written(tag, task->typeSpecific.filePart.partNum);
			PG_RE_THROW();
		}
		PG_END_TRY();

		s3_header_mark_part_written(tag, task->typeSpecific.filePart.partNum);
		pfree(filename);
		pfree(objectname);
	}
	else if (task->type == S3TaskTypeWriteFilePart &&
			 task->typeSpecific.filePart.segNum < 0)
	{
		char	   *filename;
		SeqBufTag	chkp_tag;

		memset(&chkp_tag, 0, sizeof(chkp_tag));
		chkp_tag.datoid = task->typeSpecific.filePart.datoid;
		chkp_tag.relnode = task->typeSpecific.filePart.relnode;
		chkp_tag.num = task->typeSpecific.filePart.chkpNum;
		chkp_tag.type = 'm';

		filename = get_seq_buf_filename(&chkp_tag);

		objectname = psprintf("orioledb_data/%u/%u/%u.map",
							  task->typeSpecific.filePart.chkpNum,
							  task->typeSpecific.filePart.datoid,
							  task->typeSpecific.filePart.relnode);

		elog(DEBUG1, "S3 map put %s %s", objectname, filename);

		s3_put_file(objectname, filename, false);

		pfree(filename);
		pfree(objectname);
	}
	else if (task->type == S3TaskTypeWriteWALFile)
	{
		char	   *filename;

		filename = psprintf(XLOGDIR "/%s", task->typeSpecific.walFilename);
		objectname = psprintf("wal/%s", task->typeSpecific.walFilename);

		s3_put_file(objectname, filename, false);

		pfree(filename);
		pfree(objectname);
	}
	else if (task->type == S3TaskTypeWriteUndoFile)
	{
		uint64		fileNum = task->typeSpecific.writeUndoFile.fileNum;
		char	   *filename;

		if (task->typeSpecific.writeUndoFile.undoType == UndoLogRegular)
		{
			filename = psprintf(ORIOLEDB_UNDO_DATA_FILENAME_TEMPLATE,
								(uint32) (fileNum >> 32),
								(uint32) fileNum);
			objectname = psprintf("orioledb_undo/%02X%08Xdata",
								  (uint32) (fileNum >> 32),
								  (uint32) fileNum);
		}
		else if (task->typeSpecific.writeUndoFile.undoType == UndoLogSystem)
		{
			filename = psprintf(ORIOLEDB_UNDO_SYSTEM_FILENAME_TEMPLATE,
								(uint32) (fileNum >> 32),
								(uint32) fileNum);
			objectname = psprintf("orioledb_undo/%02X%08Xsystem",
								  (uint32) (fileNum >> 32),
								  (uint32) fileNum);
		}
		else
		{
			Assert(false);
			filename = NULL;
			objectname = NULL;
		}

		s3_put_file(objectname, filename, false);

		pfree(filename);
		pfree(objectname);
	}
	else if (task->type == S3TaskTypeWriteRootFile)
	{
		char	   *filename = task->typeSpecific.writeRootFile.filename;
		long		result;

		if (filename[0] == '.' && filename[1] == '/')
			filename += 2;

		objectname = psprintf("data/%s", filename);

		elog(DEBUG1, "S3 put %s %s", objectname, filename);

		result = s3_put_file(objectname, filename, false);

		pfree(objectname);

		if ((result == S3_RESPONSE_OK) && task->typeSpecific.writeRootFile.delete)
			unlink(filename);
	}
	else if (task->type == S3TaskTypeWritePGFile)
	{
		char	   *filename = task->typeSpecific.writePGFile.filename;
		Pointer		data;
		uint64		size;

		if (filename[0] == '.' && filename[1] == '/')
			filename += 2;

		objectname = psprintf("data/%u/%s",
							  task->typeSpecific.writePGFile.chkpNum,
							  filename);

		elog(DEBUG1, "S3 PG file put %s %s", objectname, filename);

		data = read_file(filename, &size);

		if (data != NULL && check_pgfile_changed(filename, data, size))
		{
			(void) s3_put_object_with_contents(objectname, data, size, false);
			pfree(data);
		}

		pfree(objectname);

		/* Mark this task as processed */
		pg_atomic_fetch_sub_u32(&workers_ctl->pgFilesCnt, 1);
	}

	pfree(task);
	s3_queue_erase_task(taskLocation);
}

/*
 * Schedule a synchronization of given data file to S3.
 */
S3TaskLocation
s3_schedule_file_write(uint32 chkpNum, char *filename, bool delete)
{
	S3Task	   *task;
	int			filenameLen,
				taskLen;
	S3TaskLocation location;

	filenameLen = strlen(filename);
	taskLen = INTALIGN(offsetof(S3Task, typeSpecific.writeFile.filename) + filenameLen + 1);
	task = (S3Task *) palloc0(taskLen);
	task->type = S3TaskTypeWriteFile;
	task->typeSpecific.writeFile.chkpNum = chkpNum;
	task->typeSpecific.writeFile.delete = delete;
	memcpy(task->typeSpecific.writeFile.filename, filename, filenameLen + 1);

	location = s3_queue_put_task((Pointer) task, taskLen);

	elog(DEBUG1, "S3 schedule file write: %s %u %u (%llu)",
		 filename, chkpNum, delete ? 1 : 0, (unsigned long long) location);

	pfree(task);

	return location;
}

/*
 * Schedule a synchronization of given empty directory to S3.
 */
S3TaskLocation
s3_schedule_empty_dir_write(uint32 chkpNum, char *dirname)
{
	S3Task	   *task;
	int			dirnameLen,
				taskLen;
	S3TaskLocation location;

	dirnameLen = strlen(dirname);
	taskLen = INTALIGN(offsetof(S3Task, typeSpecific.writeEmptyDir.dirname) +
					   dirnameLen + 1);
	task = (S3Task *) palloc0(taskLen);
	task->type = S3TaskTypeWriteEmptyDir;
	task->typeSpecific.writeEmptyDir.chkpNum = chkpNum;
	memcpy(task->typeSpecific.writeEmptyDir.dirname, dirname, dirnameLen + 1);

	location = s3_queue_put_task((Pointer) task, taskLen);

	elog(DEBUG1, "S3 schedule empty dir write: %s %u (%llu)",
		 dirname, chkpNum, (unsigned long long) location);

	pfree(task);

	return location;
}

/*
 * Schedule a synchronization of given data file part to S3.
 */
S3TaskLocation
s3_schedule_file_part_write(uint32 chkpNum, Oid datoid, Oid relnode,
							int32 segNum, int32 partNum)
{
	S3Task	   *task;
	S3TaskLocation location;
	S3HeaderTag tag = {.datoid = datoid,.relnode = relnode,.checkpointNum = chkpNum,.segNum = segNum};

	if (partNum >= 0 && !s3_header_mark_part_scheduled_for_write(tag, partNum))
		return (S3TaskLocation) 0;

	task = (S3Task *) palloc0(sizeof(S3Task));
	task->type = S3TaskTypeWriteFilePart;
	task->typeSpecific.filePart.chkpNum = chkpNum;
	task->typeSpecific.filePart.datoid = datoid;
	task->typeSpecific.filePart.relnode = relnode;
	task->typeSpecific.filePart.segNum = segNum;
	task->typeSpecific.filePart.partNum = partNum;

	location = s3_queue_put_task((Pointer) task, sizeof(S3Task));

	elog(DEBUG1, "S3 schedule file part write: %u %u %u %d %d (%llu)",
		 datoid, relnode, chkpNum, segNum, partNum, (unsigned long long) location);

	pfree(task);

	return location;
}

/*
 * Schedule the read of given data file part from S3.
 */
S3TaskLocation
s3_schedule_file_part_read(uint32 chkpNum, Oid datoid, Oid relnode,
						   int32 segNum, int32 partNum)
{
	S3Task	   *task;
	S3TaskLocation location;
	S3PartStatus status;
	S3HeaderTag tag = {.datoid = datoid,.relnode = relnode,.checkpointNum = chkpNum,.segNum = segNum};

	status = s3_header_mark_part_loading(tag, partNum);
	if (status == S3PartStatusLoading)
	{
		/*
		 * The task is already scheduled.  We don't know the location, but we
		 * know it's lower than current insert location.
		 */
		return s3_queue_get_insert_location();
	}
	else if (status == S3PartStatusLoaded)
	{
		return 0;
	}
	Assert(status == S3PartStatusNotLoaded);

	task = (S3Task *) palloc0(sizeof(S3Task));
	task->type = S3TaskTypeReadFilePart;
	task->typeSpecific.filePart.chkpNum = chkpNum;
	task->typeSpecific.filePart.datoid = datoid;
	task->typeSpecific.filePart.relnode = relnode;
	task->typeSpecific.filePart.segNum = segNum;
	task->typeSpecific.filePart.partNum = partNum;

	location = s3_queue_put_task((Pointer) task, sizeof(S3Task));

	elog(DEBUG1, "S3 schedule file part read: %u %u %u %d %d (%llu)",
		 datoid, relnode, chkpNum, segNum, partNum, (unsigned long long) location);

	pfree(task);

	return location;
}

/*
 * Schedule a synchronization of given WAL file to S3.
 */
S3TaskLocation
s3_schedule_wal_file_write(char *filename)
{
	S3Task	   *task;
	int			filenameLen,
				taskLen;
	S3TaskLocation location;

	filenameLen = strlen(filename);
	taskLen = INTALIGN(offsetof(S3Task, typeSpecific.walFilename) + filenameLen + 1);
	task = (S3Task *) palloc0(taskLen);
	task->type = S3TaskTypeWriteWALFile;
	memcpy(task->typeSpecific.walFilename, filename, filenameLen + 1);

	location = s3_queue_put_task((Pointer) task, taskLen);

	elog(DEBUG1, "S3 schedule WAL file write: %s (%llu)",
		 filename, (unsigned long long) location);

	pfree(task);

	return location;
}

/*
 * Schedule a synchronization of given UNDO file to S3.
 */
S3TaskLocation
s3_schedule_undo_file_write(UndoLogType undoType, uint64 fileNum)
{
	S3Task	   *task;
	S3TaskLocation location;

	task = (S3Task *) palloc0(sizeof(S3Task));
	task->type = S3TaskTypeWriteUndoFile;
	task->typeSpecific.writeUndoFile.undoType = undoType;
	task->typeSpecific.writeUndoFile.fileNum = fileNum;

	location = s3_queue_put_task((Pointer) task, sizeof(S3Task));

	elog(DEBUG1, "S3 schedule UNDO file write: %llu (%llu)",
		 (unsigned long long) fileNum, (unsigned long long) location);

	pfree(task);

	return location;
}


/*
 * Schedule the load of given downlink from S3 to local storage.
 */
S3TaskLocation
s3_schedule_downlink_load(BTreeDescr *desc, uint64 downlink)
{
	uint64		offset = DOWNLINK_GET_DISK_OFF(downlink);
	uint16		len = DOWNLINK_GET_DISK_LEN(downlink);
	off_t		byte_offset,
				read_size;
	uint32		chkpNum;
	int32		segNum,
				partNum;
	S3TaskLocation result = 0;

	chkpNum = S3_GET_CHKP_NUM(offset);
	offset &= S3_OFFSET_MASK;

	if (!OCompressIsValid(desc->compress))
	{
		byte_offset = (off_t) offset * (off_t) ORIOLEDB_BLCKSZ;
		read_size = ORIOLEDB_BLCKSZ;
	}
	else
	{
		byte_offset = (off_t) offset * (off_t) ORIOLEDB_COMP_BLCKSZ;
		read_size = len * ORIOLEDB_COMP_BLCKSZ;
	}

	while (true)
	{
		S3TaskLocation location;

		segNum = byte_offset / ORIOLEDB_SEGMENT_SIZE;
		partNum = (byte_offset % ORIOLEDB_SEGMENT_SIZE) / ORIOLEDB_S3_PART_SIZE;
		location = s3_schedule_file_part_read(chkpNum,
											  desc->oids.datoid, desc->oids.relnode,
											  segNum, partNum);
		result = Max(result, location);
		if (byte_offset % ORIOLEDB_S3_PART_SIZE + read_size > ORIOLEDB_S3_PART_SIZE)
		{
			uint64		shift = ORIOLEDB_S3_PART_SIZE - byte_offset % ORIOLEDB_S3_PART_SIZE;

			byte_offset += shift;
			read_size -= shift;
		}
		else
		{
			break;
		}
	}

	return result;
}

/*
 * Schedule a synchronization of given file to S3.
 */
S3TaskLocation
s3_schedule_root_file_write(char *filename, bool delete)
{
	S3Task	   *task;
	int			filenameLen,
				taskLen;
	S3TaskLocation location;

	filenameLen = strlen(filename);
	taskLen = INTALIGN(offsetof(S3Task, typeSpecific.writeRootFile.filename) + filenameLen + 1);
	task = (S3Task *) palloc0(taskLen);
	task->type = S3TaskTypeWriteRootFile;
	task->typeSpecific.writeRootFile.delete = delete;
	memcpy(task->typeSpecific.writeRootFile.filename, filename, filenameLen + 1);

	location = s3_queue_put_task((Pointer) task, taskLen);

	elog(DEBUG1, "S3 schedule root file write: %s %u (%llu)",
		 filename, delete ? 1 : 0, (unsigned long long) location);

	pfree(task);

	return location;
}

/*
 * Schedule a synchronization of given PGDATA file to S3.
 */
S3TaskLocation
s3_schedule_pg_file_write(uint32 chkpNum, char *filename)
{
	S3Task	   *task;
	int			filenameLen,
				taskLen;
	S3TaskLocation location;

	Assert(workers_ctl != NULL);

	filenameLen = strlen(filename);
	taskLen = INTALIGN(offsetof(S3Task, typeSpecific.writePGFile.filename) + filenameLen + 1);
	task = (S3Task *) palloc0(taskLen);
	task->type = S3TaskTypeWritePGFile;
	task->typeSpecific.writePGFile.chkpNum = chkpNum;
	memcpy(task->typeSpecific.writePGFile.filename, filename, filenameLen + 1);

	location = s3_queue_put_task((Pointer) task, taskLen);

	elog(DEBUG1, "S3 schedule PGDATA file write: %s %u (%llu)",
		 filename, chkpNum, (unsigned long long) location);

	pfree(task);

	pg_atomic_fetch_add_u32(&workers_ctl->pgFilesCnt, 1);

	return location;
}

void
s3_load_file_part(uint32 chkpNum, Oid datoid, Oid relnode,
				  int32 segNum, int32 partNum)
{
	S3TaskLocation location;

	location = s3_schedule_file_part_read(chkpNum, datoid, relnode,
										  segNum, partNum);

	s3_queue_wait_for_location(location);
}

void
s3_load_map_file(uint32 chkpNum, Oid datoid, Oid relnode)
{
	S3TaskLocation location;

	location = s3_schedule_file_part_read(chkpNum, datoid, relnode,
										  -1, 0);

	s3_queue_wait_for_location(location);
}

void
s3worker_main(Datum main_arg)
{
	int			rc,
				wake_events = WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT,
				num = Int32GetDatum(main_arg);

	/* enable timeout for relation lock */
	RegisterTimeout(DEADLOCK_TIMEOUT, CheckDeadLockAlert);

	/* enable relation cache invalidation (remove old OTableDescr) */
	RelationCacheInitialize();
	InitCatalogCache();
	SharedInvalBackendInit(false);

	/* show the s3 worker in pg_stat_activity, */
	InitializeSessionUserIdStandalone();
	pgstat_beinit();
	pgstat_bestart();

	SetProcessingMode(NormalProcessing);

	/* catch SIGTERM signal for reason to not interupt background writing */
	pqsignal(SIGTERM, handle_sigterm);
	BackgroundWorkerUnblockSignals();

	elog(LOG, "orioledb s3 worker %d started", num);

	CurTransactionContext = AllocSetContextCreate(TopMemoryContext,
												  "orioledb s3worker current transaction context",
												  ALLOCSET_DEFAULT_SIZES);
	TopTransactionContext = AllocSetContextCreate(TopMemoryContext,
												  "orioledb s3worker top transaction context",
												  ALLOCSET_DEFAULT_SIZES);

	ResetLatch(MyLatch);

	PG_TRY();
	{
		MemoryContextSwitchTo(CurTransactionContext);

		/*
		 * There might be task to process saved into shared memory.  If so,
		 * pick and process it.
		 */
		if (workers_ctl->locations[num] != InvalidS3TaskLocation)
		{
			s3process_task(workers_ctl->locations[num]);
			workers_ctl->locations[num] = InvalidS3TaskLocation;
		}

		while (true)
		{
			uint64		taskLocation;

			if (shutdown_requested)
				break;

			/*
			 * Sleep until we are signaled or it's time to check the queue.
			 */
			rc = WaitLatch(MyLatch, wake_events,
						   BgWriterDelay,
						   WAIT_EVENT_BGWRITER_MAIN);

			if (rc & WL_POSTMASTER_DEATH)
				shutdown_requested = true;

			/*
			 * Task processing loop.  It might happend that error occurs and
			 * worker restarts.  We save the task location to the shared
			 * memory to be able to process it after restart.
			 */
			while ((taskLocation = s3_queue_try_pick_task()) != InvalidS3TaskLocation)
			{
				workers_ctl->locations[num] = taskLocation;
				s3process_task(taskLocation);
				workers_ctl->locations[num] = InvalidS3TaskLocation;
			}

			if (pgfiles_crc_buf != NULL &&
				pg_atomic_read_u32(&workers_ctl->pgFilesCnt) == 0)
			{
				flush_pgfiles_hash(num);

				ConditionVariableBroadcast(&workers_ctl->pgFilesFlushedCV);
			}

			ResetLatch(MyLatch);
		}
		elog(LOG, "orioledb s3 worker %d is shut down", num);
	}
	PG_CATCH();
	{
		LockReleaseSession(DEFAULT_LOCKMETHOD);
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * Initialize a hash table to store checksums of PGDATA files.
 */
static HTAB *
init_pgfiles_hash(void)
{
	int			file;
	StringInfoData buf;
	uint32		datalen;
	int			readBytes;
	HASHCTL		ctl;
	HTAB	   *hash;
	
	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = MAXPGPATH;
	ctl.entrysize = sizeof(PGDataHashEntry);
	ctl.hcxt = CurTransactionContext;

	hash = hash_create("pgdata hash",
					   32,		/* arbitrary initial size */
					   &ctl,
					   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	file = BasicOpenFile(PGDATA_CRC_FILENAME, O_RDONLY | PG_BINARY);
	if (file < 0)
	{
		if (errno == ENOENT)
			return NULL;

		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\": %m", PGDATA_CRC_FILENAME)));
	}

	initStringInfo(&buf);

	while ((readBytes = pg_pread(file, (char *) &datalen, sizeof(datalen), 0)) > 0)
	{
		const char *filename;
		uint64		file_crc;
		PGDataHashEntry *entry;
		bool		found;

		resetStringInfo(&buf);
		readBytes = pg_pread(file, buf.data, datalen, 0);
		if (readBytes != datalen)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m", PGDATA_CRC_FILENAME)));

		/* Put the filename and its checksum into the hash table */
		filename = pq_getmsgrawstring(&buf);
		pq_copymsgbytes(&buf, (char *) &file_crc, sizeof(file_crc));

		entry = (PGDataHashEntry *) hash_search(hash, filename, HASH_ENTER, &found);
		/* Normally we shouldn't have duplicated keys in the file */
		if (found)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("the file name is duplicated in the hash file \"%s\": %s",
							PGDATA_CRC_FILENAME, filename)));

		entry->crc = file_crc;
	}

	if (readBytes < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\": %m", PGDATA_CRC_FILENAME)));

	pfree(buf.data);

	close(file);

	return hash;
}

/*
 * Save current workers' PGDATA files hash into a temporary file.
 */
static void
flush_pgfiles_hash(int worker_num)
{
	char		filename[MAXPGPATH];
	int			file;
	int			rc;

	Assert(pgfiles_crc_buf != NULL);

	snprintf(filename, sizeof(filename), "%s.%d", PGDATA_CRC_FILENAME, worker_num);

	file = BasicOpenFile(filename, O_CREAT | O_WRONLY | O_APPEND | PG_BINARY);
	if (file <= 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", filename)));

	rc = pg_pwrite(file, pgfiles_crc_buf->data, pgfiles_crc_buf->len, 0);
	if (rc < 0 || rc != pgfiles_crc_buf->len)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m", filename)));

	if (pg_fsync(file) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m", filename)));

	close(file);

	pfree(pgfiles_crc_buf->data);
	pfree(pgfiles_crc_buf);
	pgfiles_crc_buf = NULL;

	hash_destroy(pgfiles_hash);
	pgfiles_hash = NULL;

	/* This worker flushed the hash */
	pg_atomic_fetch_sub_u32(&workers_ctl->pgFilesFlushCnt, 1);
}

/*
 * Check if a PGDATA file changed since last checkpoint.  If pgdata_hash wasn't
 * initialize before initialize it.
 *
 * Returns false if the file didn't change.
 */
static bool
check_pgfile_changed(const char *filename, Pointer new_data, uint64 size)
{
	uint64		prev_crc = 0;
	uint64		cur_crc;
	uint32		filenamelen,
				datalen;

	if (pgfiles_hash == NULL)
		pgfiles_hash = init_pgfiles_hash();

	if (pgfiles_hash != NULL)
	{
		char		key[MAXPGPATH];
		PGDataHashEntry *entry;

		MemSet(key, 0, sizeof(key));
		strncpy(key, filename, MAXPGPATH);

		entry = (PGDataHashEntry *) hash_search(pgfiles_hash, key, HASH_FIND, NULL);
		if (entry != NULL)
			prev_crc = entry->crc;
	}

	cur_crc = hash_bytes_extended((const unsigned char *) new_data, size, 0);

	if (pgfiles_crc_buf == NULL)
	{
		pgfiles_crc_buf = makeStringInfo();
		/* Remember this worker for flushing */
		pg_atomic_fetch_add_u32(&workers_ctl->pgFilesFlushCnt, 1);
	}

	/* Prepare the new checksum to be written to the file */
	filenamelen = strlen(filename);
	datalen = filenamelen + 1 /* null-byte */ + sizeof(cur_crc) + 1 /* null-byte */;
	appendBinaryStringInfoNT(pgfiles_crc_buf, &datalen, sizeof(datalen));
	appendBinaryStringInfo(pgfiles_crc_buf, filename, strlen(filename));
	appendBinaryStringInfo(pgfiles_crc_buf, &cur_crc, sizeof(cur_crc));

	return prev_crc != cur_crc;
}

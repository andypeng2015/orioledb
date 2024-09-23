/*-------------------------------------------------------------------------
 *
 * control.h
 *		Declarations for control file.
 *
 * Copyright (c) 2024, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/checkpoint/control.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __CHECKPOINT_CONTROL_H__
#define __CHECKPOINT_CONTROL_H__

#include "postgres.h"

#include "orioledb.h"

typedef struct
{
	uint64		control_identifier;
	uint32		lastCheckpointNumber;
	CommitSeqNo lastCSN;
	OXid		lastXid;
	UndoLocation lastUndoLocation;
	XLogRecPtr	toastConsistentPtr;
	XLogRecPtr	replayStartPtr;
	XLogRecPtr	sysTreesStartPtr;
	uint64		mmapDataLength;
	UndoLocation checkpointRetainStartLocation;
	UndoLocation checkpointRetainEndLocation;
	OXid		checkpointRetainXmin;
	OXid		checkpointRetainXmax;
	uint32		binaryVersion;
	bool		s3Mode;
	pg_crc32c	crc;
} CheckpointControl;

#define CONTROL_FILENAME    ORIOLEDB_DATA_DIR"/control"

/*
 * Physical size of the orioledb_data/control file.  Note that this is considerably
 * bigger than the actually used size (ie, sizeof(CheckpointControl)).
 * The idea is to keep the physical size constant independent of format
 * changes, so that get_checkpoint_control_data will deliver a suitable wrong-version
 * message instead of a read error if it's looking at an incompatible file.
 */
#define CHECKPOINT_CONTROL_FILE_SIZE	8192

extern bool get_checkpoint_control_data(CheckpointControl *control);
extern void check_checkpoint_control(CheckpointControl *control);
extern void write_checkpoint_control(CheckpointControl *control);

#endif							/* __CHECKPOINT_CONTROL_H__ */

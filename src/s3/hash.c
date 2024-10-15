/*-------------------------------------------------------------------------
 *
 * hash.c
 *		Declarations for hashing of S3-specific data.
 *
 * Copyright (c) 2024, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/s3/hash.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"
#include "utils/hsearch.h"

#include "s3/hash.h"

#include "openssl/sha.h"
#include <unistd.h>

static void initS3PGFilesHash(S3HashState *state, const char *filename);

S3HashState *
makeS3HashState(S3FileHash *pgFiles, uint32 pgFilesSize, const char *hashFilename)
{
	S3HashState *res;

	res = (S3HashState *) palloc0(sizeof(S3HashState));
	res->pgFiles = pgFiles;
	res->pgFilesSize = pgFilesSize;

	initS3PGFilesHash(res, hashFilename);

	return res;
}

void
freeS3HashState(S3HashState *state)
{
	if (state == NULL)
		return;

	hash_destroy(state->hashTable);
	pfree(state);
}

/*
 * Initialize a hash table to store checksums of PGDATA files.
 */
static void
initS3PGFilesHash(S3HashState *state, const char *filename)
{
	int			file;
	S3FileHash fileEntry;
	Size		readBytes;
	HASHCTL		ctl;

	Assert(state->hashTable == NULL);

	MemSet(&ctl, 0, sizeof(ctl));
	ctl.keysize = MAXPGPATH;
	ctl.entrysize = sizeof(S3FileHash);
	ctl.hcxt = CurrentMemoryContext;

	file = BasicOpenFile(filename, O_RDONLY | PG_BINARY);
	if (file < 0)
	{
		if (errno == ENOENT)
			return;

		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\": %m", filename)));
	}

	state->hashTable = hash_create("S3PGFiles hash",
								   32,		/* arbitrary initial size */
								   &ctl,
								   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	while ((readBytes = read(file, (char *) &fileEntry, sizeof(S3FileHash))) > 0)
	{
		S3FileHash *newEntry;
		bool		found;

		if (readBytes != sizeof(S3FileHash))
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m", filename)));

		/* Put the filename and its checksum into the hash table */
		newEntry = (S3FileHash *) hash_search(state->hashTable,
											  fileEntry.filename,
											  HASH_ENTER, &found);
		/* Normally we shouldn't have duplicated keys in the file */
		if (found)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("the file name is duplicated in the hash file \"%s\": %s",
							filename, fileEntry.filename)));

		/* filename is already filled in */
		strncpy(newEntry->hash, fileEntry.hash, sizeof(fileEntry.hash));
		newEntry->changed = fileEntry.changed;
	}

	if (readBytes < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not read file \"%s\": %m", filename)));

	close(file);
}

/*
 * Save current workers' PGDATA files hash into a temporary file.
 */
void
flushS3PGFilesHash(S3HashState *state, const char *filename)
{
	int			file;
	Size		pgfilesSize;
	Size		sizeWritten;

	/*
	 * We open the file for append in case if previous flush already created
	 * the file.
	 */
	file = BasicOpenFile(filename, O_CREAT | O_WRONLY | O_APPEND | PG_BINARY);
	if (file <= 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create file \"%s\": %m", filename)));

	pgfilesSize = sizeof(S3FileHash) * state->pgFilesLen;
	sizeWritten = write(file, state->pgFiles, pgfilesSize);
	if (sizeWritten < 0 || sizeWritten != pgfilesSize)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m", filename)));

	if (pg_fsync(file) != 0)
		ereport(FATAL,
				(errcode_for_file_access(),
				 errmsg("could not fsync file \"%s\": %m", filename)));

	close(file);

	state->pgFilesLen = 0;
}

/*
 * Check if a PGDATA file changed since last checkpoint and return PGFileHash.
 */
S3FileHash *
getS3PGFileHash(S3HashState *state, const char *filename,
				Pointer data, uint64 size)
{
	S3FileHash *prevEntry = NULL;
	S3FileHash *newEntry;
	unsigned char hashbuf[PG_SHA256_DIGEST_LENGTH];
	char		hashstringbuf[PG_SHA256_DIGEST_STRING_LENGTH];

	/*
	 * hashTable might not have been initialized before if a hash file hasn't
	 * been found.
	 */
	if (state->hashTable != NULL)
	{
		char		key[MAXPGPATH];

		MemSet(key, 0, sizeof(key));
		strncpy(key, filename, MAXPGPATH);

		prevEntry = (S3FileHash *) hash_search(state->hashTable, key,
												HASH_FIND, NULL);
	}

	(void) SHA256((unsigned char *) data, size, hashbuf);

	hex_encode((char *) hashbuf, sizeof(hashbuf), hashstringbuf);
	hashstringbuf[PG_SHA256_DIGEST_STRING_LENGTH - 1] = '\0';

	newEntry = (S3FileHash *) palloc0(sizeof(S3FileHash));

	strncpy(newEntry->filename, filename, sizeof(newEntry->filename));
	strncpy(newEntry->hash, hashstringbuf, sizeof(newEntry->hash));

	newEntry->changed = (prevEntry == NULL) ||
		(strncmp(prevEntry->hash, newEntry->hash, sizeof(newEntry->hash)) != 0);

	/* Store new entry into the hash state */
	memcpy(state->pgFiles + state->pgFilesLen, newEntry, sizeof(S3FileHash));
	state->pgFilesLen += 1;

	if ((strstr(filename, "5/16384") != NULL) || (strstr(filename, "5/16387") != NULL))
		elog(WARNING, "getS3PGFileHash: %s, %s", filename, newEntry->hash);

	return newEntry;
}

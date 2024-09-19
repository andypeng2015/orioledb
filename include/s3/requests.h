/*-------------------------------------------------------------------------
 *
 * requests.h
 *		Declarations for S3 requests.
 *
 * Copyright (c) 2024, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/s3/requests.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __S3_REQUESTS_H__
#define __S3_REQUESTS_H__

extern bool s3_put_file(char *objectname, char *filename, bool ifNoneMatch);
extern void s3_get_file(char *objectname, char *filename);
extern void s3_put_empty_dir(char *objectname);
extern bool s3_put_file_part(char *objectname, char *filename, int partnum);
extern void s3_get_file_part(char *objectname, char *filename, int partnum);
extern bool s3_get_object(char *objectname, StringInfo str, bool missing_ok);
extern void s3_delete_object(char *objectname);

#endif							/* __S3_REQUESTS_H__ */

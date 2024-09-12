/*-------------------------------------------------------------------------
 *
 * control.h
 *		Declarations for S3 control and lock files.
 *
 * Copyright (c) 2024, OrioleDATA Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/include/s3/control.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef __S3_CONTROL_H__
#define __S3_CONTROL_H__

extern void s3_check_control(void);
extern void s3_put_lock_file(void);
extern void s3_delete_lock_file(void);

#endif							/* __S3_CONTROL_H__ */

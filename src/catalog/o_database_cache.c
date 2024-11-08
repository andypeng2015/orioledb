/*-------------------------------------------------------------------------
 *
 *  o_database_cache.c
 *		Routines for orioledb database cache.
 *
 * database_cache is tree that contains cached metadata from pg_database.
 *
 * Copyright (c) 2021-2024, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/catalog/o_database_cache.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "catalog/o_sys_cache.h"

#include "catalog/pg_collation.h"
#include "catalog/pg_database.h"
#include "utils/syscache.h"
#include "mb/pg_wchar.h"

static OSysCache *database_cache = NULL;

static void o_database_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key,
										Pointer arg);
static void o_database_cache_free_entry(Pointer entry);

O_SYS_CACHE_FUNCS(database_cache, ODatabase, 1);

static OSysCacheFuncs database_cache_funcs =
{
	.free_entry = o_database_cache_free_entry,
	.fill_entry = o_database_cache_fill_entry
};

/*
 * Initializes the database sys cache memory.
 */
O_SYS_CACHE_INIT_FUNC(database_cache)
{
	Oid			keytypes[] = {OIDOID};

	database_cache = o_create_sys_cache(SYS_TREES_DATABASE_CACHE, false,
										DatabaseOidIndexId, DATABASEOID, 1,
										keytypes, 0, fastcache, mcxt,
										&database_cache_funcs);
}

void
o_database_cache_fill_entry(Pointer *entry_ptr, OSysCacheKey *key,
							Pointer arg)
{
	HeapTuple	databasetup;
	Form_pg_database dbform;
	ODatabase  *o_database = (ODatabase *) *entry_ptr;
	MemoryContext prev_context;
	Oid			dboid;

	dboid = DatumGetObjectId(key->keys[0]);

	databasetup = SearchSysCache1(DATABASEOID, key->keys[0]);
	if (!HeapTupleIsValid(databasetup))
		elog(ERROR, "cache lookup failed for database (%u)", dboid);
	dbform = (Form_pg_database) GETSTRUCT(databasetup);

	prev_context = MemoryContextSwitchTo(database_cache->mcxt);
	if (o_database != NULL)		/* Existed o_database updated */
	{
		Assert(false);
	}
	else
	{
		o_database = palloc0(sizeof(ODatabase));
		*entry_ptr = (Pointer) o_database;
	}

	o_database->encoding = dbform->encoding;
	o_database->datlocprovider = dbform->datlocprovider;

	MemoryContextSwitchTo(prev_context);
	ReleaseSysCache(databasetup);
}

void
o_database_cache_free_entry(Pointer entry)
{
	pfree(entry);
}

int32
o_database_cache_get_database_encoding()
{
	XLogRecPtr	cur_lsn;
	ODatabase  *o_database;

	o_sys_cache_set_datoid_lsn(&cur_lsn, NULL);
	o_database = o_database_cache_search(Template1DbOid, Template1DbOid, cur_lsn,
										 database_cache->nkeys);
	return o_database ? o_database->encoding : PG_SQL_ASCII;
}

void
o_database_cache_set_database_encoding()
{
	int32		encoding = o_database_cache_get_database_encoding();

	SetDatabaseEncoding(encoding);
}

#if PG_VERSION_NUM >= 170000
void
o_database_cache_set_default_locale_provider()
{
	XLogRecPtr	cur_lsn;
	ODatabase  *o_database;

	o_sys_cache_set_datoid_lsn(&cur_lsn, NULL);
	o_database = o_database_cache_search(Template1DbOid, Template1DbOid, cur_lsn,
										 database_cache->nkeys);
	if (o_database)
	{
		default_locale.provider = o_database->datlocprovider;
		default_locale.deterministic = true;
	}
	else
	{
		default_locale.provider = COLLPROVIDER_DEFAULT;
		default_locale.deterministic = true;
	}
}
#endif

/*
 * A tuple print function for o_print_btree_pages()
 */
void
o_database_cache_tup_print(BTreeDescr *desc, StringInfo buf,
						   OTuple tup, Pointer arg)
{
	ODatabase  *o_database = (ODatabase *) tup.data;

	appendStringInfo(buf, "(");
	o_sys_cache_key_print(desc, buf, tup, arg);
	appendStringInfo(buf, ", encoding: %d)", o_database->encoding);
}

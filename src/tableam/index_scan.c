/*-------------------------------------------------------------------------
 *
 * index_scan.c
 *		Routines for index scan of orioledb table
 *
 * Copyright (c) 2021-2024, Oriole DB Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/tableam/index_scan.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "orioledb.h"

#include "btree/io.h"
#include "btree/iterator.h"
#include "tableam/index_scan.h"
#include "tableam/descr.h"
#include "tableam/key_range.h"
#include "tableam/tree.h"
#include "tuple/slot.h"

#include "access/nbtree.h"
#include "access/skey.h"
#include "executor/nodeIndexscan.h"
#include "parser/parse_coerce.h"

void
init_index_scan_state(OScanState *ostate, Relation index,
					  ExprContext *econtext)
{
	ScanKey		scanKeys = NULL;
	int			numScanKeys = 0;
	IndexRuntimeKeyInfo *runtimeKeys = NULL;
	int			numRuntimeKeys = 0;
	BTScanOpaque so;
	IndexScanDesc scan;

	ExecIndexBuildScanKeys(NULL, index, ostate->indexQuals, false, &scanKeys,
						   &numScanKeys, &runtimeKeys, &numRuntimeKeys, NULL,
						   NULL);

	scan = btbeginscan(index, numScanKeys, 0);
	ostate->scandesc = *scan;
	pfree(scan);
	scan = &ostate->scandesc;
	so = (BTScanOpaque) scan->opaque;

	scan->parallel_scan = NULL;
	scan->xs_temp_snap = false;

	if (numRuntimeKeys != 0)
		ExecIndexEvalRuntimeKeys(econtext, runtimeKeys, numRuntimeKeys);
	btrescan(scan, scanKeys, numScanKeys, NULL, 0);
	if (so->numArrayKeys && !BTScanPosIsValid(so->currPos))
	{
		/* punt if we have any unsatisfiable array keys */
		if (so->numArrayKeys > 0)
		{
			_bt_start_array_keys(scan, ForwardScanDirection); //
		}
	}
	_bt_preprocess_keys(scan);
}

static bool
row_key_tuple_is_valid(OBtreeRowKeyBound *row_key, OTuple tup, OIndexDescr *id,
					   bool low)
{
	int			rowkeynum;
	bool		valid = true;

	for (rowkeynum = 0; rowkeynum < row_key->nkeys; rowkeynum++)
	{
		OBTreeValueBound *subkey1 = &row_key->keys[rowkeynum];
		uint8		flags = subkey1->flags;
		int			keynum = row_key->keynums[rowkeynum];

		if (!(flags & O_VALUE_BOUND_UNBOUNDED))
		{
			int			attnum;
			bool		isnull;
			Datum		value;
			int			cmp;
			int			valid_cmp;

			attnum = OIndexKeyAttnumToTupleAttnum(BTreeKeyLeafTuple,
												  id, keynum + 1);
			value = o_fastgetattr(tup, attnum, id->leafTupdesc,
								  &id->leafSpec, &isnull);
			cmp = o_idx_cmp_range_key_to_value(subkey1,
											   &id->fields[keynum],
											   value, isnull);
			if (!(flags & O_VALUE_BOUND_NULL) && isnull)
				valid = false;

			valid_cmp = low ? cmp > 0 : cmp < 0;

			if (valid_cmp)
				valid = false;
			else if (!valid_cmp && cmp != 0 && rowkeynum < row_key->nkeys - 1)
				break;
		}
		if (!valid)
			break;
	}

	return valid;
}

static inline bool
o_bound_is_coercible(OBTreeValueBound *bound, OIndexField *field)
{
	return (bound->flags & O_VALUE_BOUND_COERCIBLE) ||
		IsBinaryCoercible(bound->type, field->inputtype);
}

static bool
is_tuple_valid(OTuple tup, OIndexDescr *id, OBTreeKeyRange *range,
			   BTScanOpaque so, int numPrefixExactKeys)
{
	int			i;
	OBTreeKeyBound *low = &range->low;
	OBTreeKeyBound *high = &range->high;
	bool		valid = true;
	int			keynum;
	BTArrayKeyInfo *arrayKeys = so->arrayKeys;

	Assert(low->nkeys == high->nkeys);

	for (i = 0; valid && i < low->nkeys; i++)
	{
		int			attnum = OIndexKeyAttnumToTupleAttnum(BTreeKeyLeafTuple,
														  id, i + 1);
		bool		isnull;
		Datum		value = o_fastgetattr(tup, attnum, id->leafTupdesc,
										  &id->leafSpec, &isnull);

		if (!(low->keys[i].flags & O_VALUE_BOUND_UNBOUNDED))
		{
			if (!(low->keys[i].flags & O_VALUE_BOUND_NULL) && isnull)
				valid = false;
			if (valid &&
				(o_idx_cmp_range_key_to_value(&low->keys[i], &id->fields[i],
											  value, isnull) > 0))
				valid = false;
		}

		if (valid && !(high->keys[i].flags & O_VALUE_BOUND_UNBOUNDED))
		{
			if (!(high->keys[i].flags & O_VALUE_BOUND_NULL) && isnull)
				valid = false;
			if (valid &&
				(o_idx_cmp_range_key_to_value(&high->keys[i], &id->fields[i],
											  value, isnull) < 0))
				valid = false;
		}
	}

	for (keynum = 0; valid && keynum < low->n_row_keys; keynum++)
	{
		if (!row_key_tuple_is_valid(&low->row_keys[keynum],
									tup, id, true))
			valid = false;
	}

	for (keynum = 0; valid && keynum < high->n_row_keys; keynum++)
	{
		if (!row_key_tuple_is_valid(&high->row_keys[keynum],
									tup, id, false))
			valid = false;
	}

	for (i = 0; i < so->numberOfKeys; i++)
	{
		ScanKey		key = so->keyData + i;

		if ((key->sk_flags & SK_SEARCHARRAY) &&
			key->sk_strategy == BTEqualStrategyNumber &&
			arrayKeys && arrayKeys->num_elems > 0)
		{
			if (i >= numPrefixExactKeys)
			{
				int			j;
				bool		isnull;
				Datum		value = o_fastgetattr(tup, key->sk_attno, id->leafTupdesc,
												  &id->leafSpec, &isnull);
				bool		found = false;
				OBTreeValueBound *bound = &low->keys[key->sk_attno - 1];
				OIndexField *field = &id->fields[key->sk_attno - 1];

				for (j = 0; j < arrayKeys->num_elems; j++)
				{
					int		cmp;

					if (o_bound_is_coercible(bound, field))
						cmp = o_call_comparator(field->comparator,
					  		value, arrayKeys->elem_values[j]);
					else
						cmp = o_call_comparator(bound->comparator,
					  		value, arrayKeys->elem_values[j]);
					if (cmp == 0)
					{
						found = true;
						break;
					}
				}

				if (!found)
					valid = false;
			}

			arrayKeys++;
		}
	}

	return valid;
}

#if PG_VERSION_NUM >= 170000
static bool
o_bt_advance_array_keys_increment(OScanState *ostate, ScanDirection dir)
{
	IndexScanDesc scan = &ostate->scandesc;
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	int		i = so->numArrayKeys - 1;
	int		j;

	for (j = ostate->numPrefixExactKeys; j < so->numberOfKeys; j++)
	{
		ScanKey		key = so->keyData + j;

		if ((key->sk_flags & SK_SEARCHARRAY) &&
			 key->sk_strategy == BTEqualStrategyNumber)
			i--;
	}

	/*
	 * We must advance the last array key most quickly, since it will
	 * correspond to the lowest-order index column among the available
	 * qualifications
	 */
	for (; i >= 0; i--)
	{
		BTArrayKeyInfo *curArrayKey = &so->arrayKeys[i];
		ScanKey		skey = &so->keyData[curArrayKey->scan_key];
		int			cur_elem = curArrayKey->cur_elem;
		int			num_elems = curArrayKey->num_elems;
		bool		rolled = false;

		if (ScanDirectionIsForward(dir) && ++cur_elem >= num_elems)
		{
			cur_elem = 0;
			rolled = true;
		}
		else if (ScanDirectionIsBackward(dir) && --cur_elem < 0)
		{
			cur_elem = num_elems - 1;
			rolled = true;
		}

		curArrayKey->cur_elem = cur_elem;
		skey->sk_argument = curArrayKey->elem_values[cur_elem];
		if (!rolled)
			return true;

		/* Need to advance next array key, if any */
	}

	/*
	 * The array keys are now exhausted.  (There isn't actually a distinct
	 * state that represents array exhaustion, since index scans don't always
	 * end after btgettuple returns "false".)
	 *
	 * Restore the array keys to the state they were in immediately before we
	 * were called.  This ensures that the arrays only ever ratchet in the
	 * current scan direction.  Without this, scans would overlook matching
	 * tuples if and when the scan's direction was subsequently reversed.
	 */
	_bt_start_array_keys(scan, -dir);

	return false;
}
#endif

static bool
switch_to_next_range(OIndexDescr *indexDescr, OScanState *ostate,
					 MemoryContext tupleCxt)
{
	OBTreeKeyBound *bound;
	IndexScanDesc scan = &ostate->scandesc;
	BTScanOpaque so = (BTScanOpaque) scan->opaque;
	MemoryContext oldcontext;
	bool		result = true;

#if PG_VERSION_NUM >= 170000

	if (!so->qual_ok)
		return false;

	if(so->numArrayKeys)
	{
		if (ostate->curKeyRangeIsLoaded)
		{
			result = o_bt_advance_array_keys_increment(ostate, ostate->scanDir);
			elog(LOG, "_bt_start_prim_scan, result %u:", result);
			if(result)
			{
//				result = _bt_advance_array_keys_increment(scan, ForwardScanDirection);
				elog(LOG, "_bt_advance_array_keys_increment, result %u:", result);
			}
		}
		else
		{
			_bt_start_array_keys(scan, ostate->scanDir);
			elog(LOG, "_bt_start_array_keys");
			result = true;
		}
	}
	else
	{
		if (ostate->curKeyRangeIsLoaded)
		{
			result = false;
			so->needPrimScan = false;
			so->scanBehind = false;
	//		elog(LOG, "no array keys");
		}
		else
		{
			result = true;
		}
	}
#else
	if (ostate->curKeyRangeIsLoaded)
		result = _bt_advance_array_keys(scan, ForwardScanDirection);
#endif
	ostate->curKeyRangeIsLoaded = true;

	ostate->exact = result;
	if (!result)
		return false;

	if (ostate->iterator != NULL)
	{
		btree_iterator_free(ostate->iterator);
		ostate->iterator = NULL;
	}

	oldcontext = MemoryContextSwitchTo(ostate->cxt);
	ostate->exact = o_key_data_to_key_range(&ostate->curKeyRange,
											so->keyData,
											so->numberOfKeys,
											(so->numArrayKeys > 0) ? so->arrayKeys : NULL,
											ostate->numPrefixExactKeys,
											indexDescr->nonLeafTupdesc->natts,
											indexDescr->fields);

	if (!ostate->exact)
	{
		bound = (ostate->scanDir == ForwardScanDirection
				 ? &ostate->curKeyRange.low
				 : &ostate->curKeyRange.high);
		ostate->iterator = o_btree_iterator_create(&indexDescr->desc, (Pointer) bound,
												   BTreeKeyBound, ostate->csn,
												   ostate->scanDir);
		o_btree_iterator_set_tuple_ctx(ostate->iterator, tupleCxt);
	}

	MemoryContextSwitchTo(oldcontext);

	return true;
}

OTuple
o_iterate_index(OIndexDescr *indexDescr, OScanState *ostate,
				CommitSeqNo *tupleCsn, MemoryContext tupleCxt,
				BTreeLocationHint *hint)
{
	OTuple		tup = {0};
	bool		tup_fetched = false;
	IndexScanDesc scan = &ostate->scandesc;
	BTScanOpaque so = (BTScanOpaque) scan->opaque;

	if (ostate->exact || ostate->curKeyRange.empty)
	{
		if (!switch_to_next_range(indexDescr, ostate, tupleCxt))
		{
			O_TUPLE_SET_NULL(tup);
			return tup;
		}
	}

	do
	{
		OBTreeKeyBound *bound;
		bool		tup_is_valid = true;

		if (ostate->exact)
		{
			if (hint)
				hint->blkno = InvalidBlockNumber;

			tup = o_btree_find_tuple_by_key(&indexDescr->desc,
											&ostate->curKeyRange.low,
											BTreeKeyBound, ostate->csn,
											tupleCsn, tupleCxt, hint);
			if (!O_TUPLE_IS_NULL(tup))
				tup_fetched = true;
		}
		else if (ostate->iterator)
		{
			bound = (ostate->scanDir == ForwardScanDirection
					 ? &ostate->curKeyRange.high : &ostate->curKeyRange.low);

			do
			{
				tup = o_btree_iterator_fetch(ostate->iterator, tupleCsn,
											 bound, BTreeKeyBound,
											 true, hint);

				if (O_TUPLE_IS_NULL(tup))
					tup_is_valid = true;
				else
				{
					tup_is_valid = is_tuple_valid(tup, indexDescr,
												  &ostate->curKeyRange,
												  so,
												  ostate->numPrefixExactKeys);
					if (tup_is_valid)
						tup_fetched = true;
				}
			} while (!tup_is_valid);
		}
		else
		{
			O_TUPLE_SET_NULL(tup);
			tup_fetched = true;
		}

		if (!tup_fetched &&
			!switch_to_next_range(indexDescr, ostate, tupleCxt))
		{
			O_TUPLE_SET_NULL(tup);
			tup_fetched = true;
		}
	} while (!tup_fetched);
	return tup;
}

OTuple
o_index_scan_getnext(OTableDescr *descr, OScanState *ostate,
					 CommitSeqNo *tupleCsn, bool scan_primary,
					 MemoryContext tupleCxt, BTreeLocationHint *hint)
{
	OIndexDescr *id = descr->indices[ostate->ixNum];
	OTuple		tup;

	o_btree_load_shmem(&id->desc);
	while (true)
	{
		tup = o_iterate_index(id, ostate, tupleCsn, tupleCxt,
							  ostate->ixNum == PrimaryIndexNumber ? hint : NULL);

		if (!scan_primary || O_TUPLE_IS_NULL(tup))
			break;

		/*
		 * if we should fetch tuple from primary and the current index is
		 * secondary
		 */
		if (ostate->ixNum != PrimaryIndexNumber)
		{
			OBTreeKeyBound bound;
			OTuple		ptup;
			OIndexDescr *primary = GET_PRIMARY(descr);

			/* fetch primary index key from tuple and search raw tuple */
			o_fill_pindex_tuple_key_bound(&id->desc, tup, &bound);

			if (hint)
			{
				hint->blkno = InvalidBlockNumber;
				hint->pageChangeCount = 0;
			}

			o_btree_load_shmem(&primary->desc);
			ptup = o_btree_find_tuple_by_key(&primary->desc,
											 (Pointer) &bound, BTreeKeyBound,
											 ostate->csn, tupleCsn,
											 tupleCxt, hint);
			pfree(tup.data);
			tup = ptup;

			/*
			 * in concurrent DELETE/UPDATE it might happen, we should to try
			 * fetch next tuple
			 */
			if (O_TUPLE_IS_NULL(tup))
				continue;
		}
		break;
	}
	return tup;
}

/* checks quals for a tuple slot */
bool
o_exec_qual(ExprContext *econtext, ExprState *qual, TupleTableSlot *slot)
{
	if (qual == NULL)
		return true;

	econtext->ecxt_scantuple = slot;
	return ExecQual(qual, econtext);
}

/*
 * executes a project for a slot fetched by o_exec_fetch function if it
 * needed.
 */
TupleTableSlot *
o_exec_project(ProjectionInfo *projInfo, ExprContext *econtext,
			   TupleTableSlot *scanTuple, TupleTableSlot *innerTuple)
{
	if (!projInfo || TupIsNull(scanTuple))
		return scanTuple;

	econtext->ecxt_scantuple = scanTuple;
	econtext->ecxt_innertuple = innerTuple;
	econtext->ecxt_outertuple = NULL;
	return ExecProject(projInfo);
}

/* explain analyze */

/* initialize explain analyze counters */
void
eanalyze_counters_init(OEACallsCounters *eacc, OTableDescr *descr)
{
	memset(eacc, 0, sizeof(*eacc));
	eacc->oids = descr->oids;
	eacc->descr = descr;
	eacc->nindices = descr->nIndices;
	eacc->indices = (OEACallsCounter *) palloc0(sizeof(OEACallsCounter) *
												eacc->nindices);
}

/* adds explain analyze info for particular index */
void
eanalyze_counter_explain(OEACallsCounter *counter, char *label,
						 char *ix_name, ExplainState *es)
{
	StringInfoData explain;
	char	   *fnames[EA_COUNTERS_NUM] = {"read", "lock", "evict",
	"write", "load"};
	uint32		counts[EA_COUNTERS_NUM],
				i;
	bool		is_first,
				is_null;
	char	   *label_upcase = NULL;

	Assert(counter != NULL);

	counts[0] = counter->read;
	counts[1] = counter->lock;
	counts[2] = counter->evict;
	counts[3] = counter->write;
	counts[4] = counter->load;

	is_null = true;
	for (i = 0; i < EA_COUNTERS_NUM; i++)
		if (counts[i] > 0)
			is_null = false;

	/* do not print empty counters */
	if (is_null)
		return;

	switch (es->format)
	{
		case EXPLAIN_FORMAT_TEXT:
			break;
		case EXPLAIN_FORMAT_JSON:
		case EXPLAIN_FORMAT_XML:
		case EXPLAIN_FORMAT_YAML:
			{
				int			i;
				bool		after_space = true;
				int			len = strlen(label);

				label_upcase = pstrdup(label);
				for (i = 0; i < len; i++)
				{
					if (after_space)
						label_upcase[i] = toupper(label_upcase[i]);
					after_space = label_upcase[i] == ' ';
				}

				ExplainOpenGroup(label_upcase, label_upcase, true, es);
				if (ix_name)
					ExplainPropertyText("Index Name", ix_name, es);
			}
			break;
	}

	is_first = true;
	for (i = 0; i < EA_COUNTERS_NUM; i++)
	{
		if (counts[i] > 0)
		{
			switch (es->format)
			{
				case EXPLAIN_FORMAT_TEXT:
					if (!is_first)
						appendStringInfo(&explain, ", ");
					else
						initStringInfo(&explain);
					appendStringInfo(&explain, "%s=%d", fnames[i], counts[i]);
					break;
				case EXPLAIN_FORMAT_JSON:
				case EXPLAIN_FORMAT_XML:
				case EXPLAIN_FORMAT_YAML:
					{
						char	   *fname = pstrdup(fnames[i]);

						fname[0] = toupper(fname[0]);
						ExplainPropertyUInteger(fname, NULL, counts[i], es);
						pfree(fname);
					}
					break;
			}
			is_first = false;
		}
	}

	switch (es->format)
	{
		case EXPLAIN_FORMAT_TEXT:
			if (!is_first)
				ExplainPropertyText(label, explain.data, es);
			break;
		case EXPLAIN_FORMAT_JSON:
		case EXPLAIN_FORMAT_XML:
		case EXPLAIN_FORMAT_YAML:
			ExplainCloseGroup(label_upcase, label_upcase, true, es);
			pfree(label_upcase);
			break;
	}
}

/* adds explain analyze info for particular index */
void
eanalyze_counters_explain(OTableDescr *descr, OEACallsCounters *counters,
						  ExplainState *es)
{
	StringInfoData label;
	int			i;

	initStringInfo(&label);

	eanalyze_counter_explain(&counters->indices[PrimaryIndexNumber],
							 "Primary pages", NULL, es);

	for (i = PrimaryIndexNumber + 1; i < counters->nindices; i++)
	{
		resetStringInfo(&label);
		appendStringInfo(&label, "Secondary index");
		if (es->format == EXPLAIN_FORMAT_TEXT)
		{
			appendStringInfo(&label, " (%s)", descr->indices[i]->name.data);
		}
		appendStringInfo(&label, " pages");

		eanalyze_counter_explain(&counters->indices[i], label.data,
								 descr->indices[i]->name.data, es);
	}

	eanalyze_counter_explain(&counters->toast, "TOAST pages", NULL, es);
	eanalyze_counter_explain(&counters->others, "Other pages", NULL, es);
}

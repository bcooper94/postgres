/*
 * automatviewselect.c
 *
 *  Created on: Dec 13, 2017
 *      Author: brandon
 */

#include "postgres.h"

#include "utils/memutils.h"
#include "nodes/value.h"
#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"
#include "rewrite/automatviewselect.h"

static List *matViewIntoClauses = NIL;
static MemoryContext *AutoMatViewContext = NIL;

void PrintQueryInfo(Query *query)
{
	ListCell *rte_cell;
	ListCell *col_cell;
	ListCell *targetList;
	RangeTblEntry *rte;

	if (query->commandType == CMD_SELECT)
	{
		elog(LOG, "Found select query in pg_plan_queries");

		if (query->rtable && query->rtable->length > 0)
		{
			elog(
					LOG, "Select statement number of RTEs: %d, number of TargetEntries: %d",
					query->rtable->length, query->targetList->length);

			foreach(targetList, query->targetList)
			{
				TargetEntry *te = lfirst_node(TargetEntry, targetList);

				/* junk columns don't get aliases */
				if (te->resjunk)
					continue;
				elog(LOG, "RTE Target entry: %s",
				te->resname);
			}

			foreach(rte_cell, query->rtable)
			{
				//						elog(LOG, "Inspecting RTE for select query");
				//						rte = lfirst_node(RangeTblEntry, rte_cell);
				rte = (RangeTblEntry *) lfirst(rte_cell);

				switch (rte->rtekind)
				{
				case RTE_RELATION:
					if (rte->eref)
					{
						elog(
								LOG, "Select RTE relation alias name with %d columns: %s",
								rte->eref->colnames->length, rte->eref->aliasname);

						foreach(col_cell, rte->eref->colnames)
						{
							elog(LOG, "Select RTE col name: %s",
							strVal(lfirst(col_cell)));
						}
					}
					break;
				case RTE_JOIN:
					elog(LOG, "RTE Join found");
					break;
				}
			}
		}
		else
		{
			elog(LOG, "No RTEs found in select statement");
		}
	}
}

List *
SearchApplicableMatViews(RangeVar *rangeVar)
{
	// TODO: Figure out how we will search for applicable materialized views
	// to use to rewrite future queries
	elog(LOG, "Searching for applicable materialized views given RangeVar...");
	MemoryContext *oldContext;

	if (AutoMatViewContext)
	{
		elog(LOG, "SearcApplicableMatViews switching to AutoMatViewContext");
		oldContext = MemoryContextSwitchTo(AutoMatViewContext);

		elog(LOG, "SearchApplicableMatViews switching to old context");
		MemoryContextSwitchTo(oldContext);
	}
	else
	{
		elog(
				WARNING, "Attempt to search applicable MatViews before any were created");
	}

	return NULL;
}

void AddMatView(IntoClause *into)
{
	MemoryContext oldContext;
	ListCell *cell;
	ListCell *colCell;
	IntoClause *matViewInto;
	IntoClause *intoCopy;
	char *colname;

	if (AutoMatViewContext == NIL)
	{
		elog(LOG, "Creating AutoMatViewContext");
		// Create new top level context to prevent this from being cleared when current
		// 	 ExecutorContext is deleted.
		AutoMatViewContext = AllocSetContextCreate((MemoryContext) NULL,
				"AutoMatViewContext",
				ALLOCSET_DEFAULT_SIZES);
	}

	oldContext = MemoryContextSwitchTo(AutoMatViewContext);
//	elog(LOG, "Old memory context: %s", oldContext->name);

	intoCopy = copyObject(into);
	elog(LOG, "Appending materialized view %s to list of available matviews",
	intoCopy->rel->relname);
	matViewIntoClauses = list_append_unique(matViewIntoClauses, intoCopy);
	elog(LOG, "Materialized views stored length=%d:",
	matViewIntoClauses->length);

	foreach(cell, matViewIntoClauses)
	{
		matViewInto = (IntoClause *) lfirst(cell);
		elog(LOG, "MatView relname=%s", matViewInto->rel->relname);

		if (matViewInto->tableSpaceName)
		{
			elog(
					LOG, "Matview tablespace name: %s", matViewInto->tableSpaceName);
		}

		if (matViewInto->colNames)
		{
			elog(LOG, "Materialized view IntoClause column names length=%d:",
			matViewInto->colNames->length);
			foreach(colCell, matViewInto->colNames)
			{
				if (colCell)
				{
					if (lfirst(colCell))
					{
						colname = strVal(lfirst(colCell));
						elog(LOG, "ColName: %s", colname);
					}
					else
					{
						elog(LOG, "NULL column name in cell");
					}
				}
			}
		}
		else
		{
			elog(LOG, "Found no column names in matview");
		}
	}

//	elog(LOG, "Switching to old memory context: %s",
//	oldContext->name);
	MemoryContextSwitchTo(oldContext);
}

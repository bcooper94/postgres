/*
 * automatviewselect.c
 *
 *  Created on: Dec 13, 2017
 *      Author: brandon
 */

#include "postgres.h"

#include "c.h"
#include "utils/memutils.h"
#include "nodes/value.h"
#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"
#include "parser/parsetree.h"
#include "rewrite/automatviewselect.h"
#include "lib/dshash.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/guc_tables.h"

#define MAX_TABLENAME_SIZE 256
#define MAX_COLNAME_SIZE 256
#define QUERY_BUFFER_SIZE 2048
#define TARGET_BUFFER_SIZE 512

#define STAR_COL "*"
#define RENAMED_STAR_COL "all"

#define left_join_table(joinExpr, rangeTables) \
	(rt_fetch(joinExpr->rtindex - 2, rangeTables))

#define right_join_table(joinExpr, rangeTables) \
	(rt_fetch(joinExpr->rtindex - 1, rangeTables))

#define get_colname(rte, var) \
	(strVal(lfirst(list_nth_cell((rte)->eref->colnames, (var)->varattno - 1))))

typedef struct MatView
{
	char *name;
	char *selectQuery;
	Query *baseQuery; // Query object which this MatView is based on
	List *renamedTargetList; // List (of TargetEntry) with renamed TargetEntries
	List *renamedRtable;
} MatView;

typedef struct QueryPlanStats
{
	Query *query;
	PlannedStmt *plan;
} QueryPlanStats;

typedef struct ColumnStats
{
	char *colName;
	unsigned int selectCounts;
} ColumnStats;

typedef struct SelectQueryStats
{
	unsigned int numColumns;
	List *columnStats; /* List of ColumnStats */

} SelectQueryStats;

typedef struct TableQueryStats
{
	char *tableName;
	unsigned int selectCounts;
	unsigned int updateCounts;
	unsigned int deleteCounts;
	SelectQueryStats *selectStats;
} TableQueryStats;

/** Internal state */
extern MemoryContext AutoMatViewContext = NULL;
static List *matViewIntoClauses = NIL;
//static dshash_table *selectQueryCounts = NULL;
static List *queryStats = NIL;
static List *plannedQueries = NIL;
static List *createdMatViews = NIL;
static List *queryPlanStatsList = NIL;
static bool isCollectingQueries = true;

// Have we loaded the trainingSampleCount from postgresql.conf?
static bool isTrainingSampleCountLoaded = false;
static int trainingSampleCount = 1;

// Internal state functions
static void CheckInitState();

// Query stats functions
static QueryPlanStats *CreateQueryPlanStats(Query *query, PlannedStmt *plan);
static ColumnStats *CreateColumnStats(char *colName);
static ColumnStats *GetColumnStats(SelectQueryStats *stats, char *colName);
static SelectQueryStats *CreateSelectQueryStats(List *targetList);
static void UpdateSelectQueryStats(SelectQueryStats *stats, List *targetList);
static TableQueryStats *CreateTableQueryStats(char *tableName, List *targetList);
static TableQueryStats *FindTableQueryStats(char *tableName);
static void PrintTableQueryStats(TableQueryStats *stats);

// Un-parsing operations
static MatView *UnparseQuery(Query *query);
static List *UnparseTargetList(List *targetList, List *rtable,
				bool renameTargets, char *selectTargetsBuf,
				size_t selectTargetsBufSize);
static TargetEntry *CreateRenamedTargetEntry(TargetEntry *baseTE, char *newName,
				bool flattenExprTree);
static char *UnparseRangeTableEntries(List *rtable);
static void UnparseFromExprRecurs(Query *rootQuery, Node *node,
				int fromClauseIndex, size_t fromClauseLength, char *selectQuery,
				size_t selectQuerySize);
static char *UnparseGroupClause(List *groupClause, List *targetList,
				List *rtable);
static void CreateJoinVarStr(JoinExpr *joinExpr, Var *var, RangeTblEntry *rte,
				List *rangeTables, char *varStrBuf, size_t varStrBufSize);
static char *UnparseQuals(List *quals, RangeTblEntry *leftRte,
				RangeTblEntry *rightRte, JoinExpr *joinExpr, List *rangeTables);
static char *AggrefToString(TargetEntry *aggrefEntry, List *rtable,
				bool renameAggref, char *aggrefStrBuf, size_t aggrefStrBufSize);
static void ExprToString(Expr *expr, JoinExpr *join, RangeTblEntry *rte,
				List *rangeTables, char *targetBuf, size_t targetBufSize);
static char *TargetEntryToString(TargetEntry *targetEntry, List *rtable,
				bool renameTargets, char *outBuf, size_t outBufSize);
static char *VarToString(Var *var, List *rtable, bool renameVar, char *varBuf,
				size_t varBufSize);

// Query rewrite operations
static void RewriteTargetList(Query *query, MatView *matView);
static void RewriteJoinTree(Query *query, MatView *matView);
void RewriteJoinTreeRecurs(Query *rootQuery, MatView *matView, Node *node,
				int fromClauseIndex, size_t fromClauseLength, int *joinCount,
				int *joinsRemoved);
static void CopyRte(RangeTblEntry *destination, RangeTblEntry *copyTarget);
static bool DoesMatViewContainRTE(RangeTblEntry *rte, MatView *matView);

// MatView selection functions
static List *GetMatchingMatViews(Query *query);
static bool DoesQueryMatchMatView(Query *query, MatView *matView);
static bool IsTargetListMatch(List *rtable, List *targetList, MatView *matView);
static bool AreTargetEntriesEqual(TargetEntry *targetEntryOne, List *rtableOne,
				TargetEntry *targetEntryTwo, List *rtableTwo);

// MatView generation operations
int64 CreateMaterializedView(char *viewName, char *selectQuery);
static void CreateRelevantMatViews();
static void PopulateMatViewRenamedRTable(MatView *matView);
static List *GetCostlyQueries(double costCutoffRatio);
static List *GenerateInterestingQueries(List *queryPlanStats);
static List *PruneQueries(List *queries);

// Utilities
static void FreeMatView(MatView *matView);
static void ReplaceChars(char *string, char target, char replacement,
				size_t sizeLimit);
static double GetPlanCost(Plan *plan);
static void ResetVarno(Expr *expr, Index newVarno);
static void SetVarattno(Expr *expr, AttrNumber varattno);
static void SetVarno(Expr *expr, Index varno);
static RangeTblEntry *FindRte(Oid relid, List *rtable);
static void PrintQueryInfo(Query *query);

MemoryContext SwitchToAutoMatViewContext()
{
	return MemoryContextSwitchTo(AutoMatViewContext);
}

void CheckInitState()
{
	if (AutoMatViewContext == NULL)
	{
		AutoMatViewContext = AllocSetContextCreate((MemoryContext) NULL,
													"AutoMatViewContext",
													ALLOCSET_DEFAULT_SIZES
													);
	}
//	if (!isTrainingSampleCountLoaded)
//	{
//		elog(LOG, "Loading training sample count...");
//		struct config_generic **configs = get_guc_variables();
//	}
}

QueryPlanStats *CreateQueryPlanStats(Query *query, PlannedStmt *plan)
{
	QueryPlanStats *stats;

	stats = NULL;

	if (query != NULL && plan != NULL)
	{
		stats = palloc(sizeof(QueryPlanStats));
		stats->query = copyObject(query);
		stats->plan = copyObject(plan);
	}

	return stats;
}

ColumnStats *CreateColumnStats(char *colName)
{
	ColumnStats *stats = (ColumnStats *) palloc(sizeof(ColumnStats));

	if (stats == NULL)
	{
		elog(ERROR, "Failed to allocate memory for ColumnStats");
	}
	else
	{
		stats->colName = pnstrdup(colName, MAX_TABLENAME_SIZE);
		stats->selectCounts = 0;
	}

	return stats;
}

ColumnStats *GetColumnStats(SelectQueryStats *stats, char *colName)
{
	ListCell *colCell;
	ColumnStats *colStats;

	foreach(colCell, stats->columnStats)
	{
		colStats = lfirst_node(ColumnStats, colCell);

		if (strncmp(colName, colStats->colName, MAX_COLNAME_SIZE) == 0)
		{
			elog(LOG, "Found column stats for %s", colName);
			return colStats;
		}
	}

	return NULL;
}

SelectQueryStats *CreateSelectQueryStats(List *targetList)
{
	ListCell *targetEntryCell;
	SelectQueryStats *selectStats;

	elog(LOG, "CreateSelectQueryStats called");

	selectStats = (SelectQueryStats *) palloc(sizeof(SelectQueryStats));
	selectStats->columnStats = NIL;

	if (selectStats == NULL)
	{
		elog(ERROR, "Failed to allocate memory for SelectQueryStats");
	}
	else
	{
		selectStats->numColumns = targetList->length;

		foreach(targetEntryCell, targetList)
		{
			selectStats->columnStats = list_append_unique(
							selectStats->columnStats,
							CreateColumnStats(strVal(lfirst(targetEntryCell))));
		}
	}

	return selectStats;
}

/**
 * Update the column select counts for the given SelectQueryStats.
 * stats: target SelectQueryStats to be updated.
 * targetList: list (of TargetEntry) containing the selected columns.
 */
void UpdateSelectQueryStats(SelectQueryStats *stats, List *targetList)
{
	ListCell *targetEntryCell;
	TargetEntry *targetEntry;
	ColumnStats *colStats;

	if (targetList != NIL)
	{
		foreach(targetEntryCell, targetList)
		{
			targetEntry = lfirst_node(TargetEntry, targetEntryCell);

			if (!targetEntry->resjunk && targetEntry->resname != NULL)
			{
//				colName = strVal(lfirst(col_cell));
				colStats = GetColumnStats(stats, targetEntry->resname);

				if (colStats != NULL)
				{
					colStats->selectCounts++;
				}

				elog(LOG, "Updating select count for RTE col name: %s",
				targetEntry->resname);
			}
		}
	}
}

/**
 * Assume we are already in AutoMatViewContext.
 *
 * tableName: Name of the table to create stats for.
 * targetList: target list (of TargetEntry) for the table.
 */
TableQueryStats *CreateTableQueryStats(char *tableName, List *targetList)
{
	TableQueryStats *stats;

	stats = (TableQueryStats *) palloc(sizeof(TableQueryStats));

	if (stats == NULL)
	{
		elog(ERROR, "Failed to allocate memory for TableQueryStats");
	}
	else
	{
		stats->selectCounts = 0;
		stats->updateCounts = 0;
		stats->deleteCounts = 0;
		stats->selectStats = CreateSelectQueryStats(targetList);
		stats->tableName = pnstrdup(tableName, MAX_TABLENAME_SIZE);
	}

	return stats;
}

TableQueryStats *FindTableQueryStats(char *tableName)
{
	ListCell *statsCell;
	TableQueryStats *curStats;
	TableQueryStats *stats = NULL;

//	elog(LOG, "Finding query stats for table=%s", tableName);

	if (queryStats != NIL)
	{
		foreach(statsCell, queryStats)
		{
			curStats = (TableQueryStats *) lfirst(statsCell);
			if (strcmp(curStats->tableName, tableName) == 0)
			{
//				elog(LOG, "Found stats for %s", curStats->tableName);
				return curStats;
			}
		}
	}

	return stats;
}

void PrintTableQueryStats(TableQueryStats *stats)
{
	ListCell *colStatsCell;
	ColumnStats *colStats;

	elog(
			LOG, "<TableQueryStats table=%s, selectCounts=%d, updateCounts=%d, deleteCounts=%d>",
			stats->tableName, stats->selectCounts, stats->updateCounts, stats->deleteCounts);
}

MatView *UnparseQuery(Query *query)
{
	ListCell *listCell;
	TargetEntry *targetEntry;
	FromExpr *from;
	MatView *matView;
	char *fromClauseStr, *groupClauseStr;
	char targetListBuf[QUERY_BUFFER_SIZE];
	List *renamedTargets;

	memset(targetListBuf, 0, QUERY_BUFFER_SIZE);
	matView = palloc(sizeof(MatView));
	matView->name = palloc(sizeof(char) * 256);
	snprintf(matView->name, 256, "Matview_%d", rand());

	matView->baseQuery = copyObject(query);

	matView->selectQuery = palloc(sizeof(char) * QUERY_BUFFER_SIZE);
	strcpy(matView->selectQuery, "SELECT ");
	matView->renamedTargetList = UnparseTargetList(query->targetList,
													query->rtable, true,
													targetListBuf,
													QUERY_BUFFER_SIZE);
	strncat(matView->selectQuery, targetListBuf, QUERY_BUFFER_SIZE);

	strncat(matView->selectQuery, " FROM ", QUERY_BUFFER_SIZE);

	if (query->jointree != NULL)
	{
		int index = 0;
		from = query->jointree;
//		AnalyzePlanQuals(query->jointree->quals);
		elog(
				LOG, "Unparsing FROM clause. fromlist.length=%d", from->fromlist->length);

		foreach(listCell, from->fromlist)
		{
			// NOTE: Un-parsing of mixed JOIN ON clauses with table cross products will fail
			UnparseFromExprRecurs(query, lfirst(listCell), index,
									from->fromlist->length,
									matView->selectQuery, QUERY_BUFFER_SIZE);
			index++;
		}
	}
	else
	{
		elog(LOG, "No join tree found. Unparsing RTEs into FROM clause...");
		fromClauseStr = UnparseRangeTableEntries(query->rtable);
		strncat(matView->selectQuery, fromClauseStr, QUERY_BUFFER_SIZE);
		pfree(fromClauseStr);
	}
	if (query->groupClause != NIL)
	{
		groupClauseStr = UnparseGroupClause(query->groupClause,
											query->targetList, query->rtable);

		if (groupClauseStr != NULL)
		{
			strncat(matView->selectQuery, groupClauseStr, QUERY_BUFFER_SIZE);
			pfree(groupClauseStr);
		}
	}
	if (query->groupingSets != NIL)
	{
		elog(LOG, "Found grouping sets in query");
	}

	elog(LOG, "Constructed full select query: %s", matView->selectQuery);

	return matView;
}

/**
 * Unparses the SELECT clause, copying it into selectTargetsBuf.
 * Returns a List of TargetEntry with the appropriate renamed entries if renameTargets is true.
 * NOTE: Returned List must be freed.
 */
List *UnparseTargetList(List *targetList, List *rtable, bool renameTargets,
				char *selectTargetsBuf, size_t selectTargetsBufSize)
{
	TargetEntry *targetEntry;
	TargetEntry *renamedTargetEntry;
	RangeTblEntry *rte;
	char targetBuffer[TARGET_BUFFER_SIZE];
	char *renamedTarget;
	ListCell *listCell;
	int index;
	List *renamedTargetEntries;

	renamedTargetEntries = NIL;
	index = 0;

	if (targetList != NIL)
	{
		foreach(listCell, targetList)
		{
			targetEntry = lfirst_node(TargetEntry, listCell);
//			elog(
//					LOG, "TargetEntry %s, TE.expr nodeTag: %d, resorigtable=%d, column attribute number=%d",
//					targetEntry->resname, nodeTag(targetEntry->expr),
//					targetEntry->resorigtbl, targetEntry->resorigcol);
			renamedTarget = TargetEntryToString(targetEntry, rtable,
												renameTargets, targetBuffer,
												TARGET_BUFFER_SIZE);
			if (renameTargets && renamedTarget != NULL)
			{
				renamedTargetEntry = CreateRenamedTargetEntry(targetEntry,
																renamedTarget,
																true);
//				elog(
//				LOG, "Creating renamed TargetEntry for te.resname=%s to %s",
//				renamedTargetEntry->resname, renamedTarget);
				renamedTargetEntries = list_append_unique(renamedTargetEntries,
															renamedTargetEntry);
			}

			if (index < targetList->length - 1)
			{
				strncat(targetBuffer, ", ", TARGET_BUFFER_SIZE);
			}
//			elog(LOG, "Copying %s into selectTargets buffer", targetBuffer);
			strncat(selectTargetsBuf, targetBuffer, selectTargetsBufSize);

			index++;
		}
	}
	// Assume SELECT *
	else
	{
		elog(LOG, "Query.targetList was NIL; assuming SELECT *");
		strcpy(selectTargetsBuf, "*");
	}

	return renamedTargetEntries;
}

/**
 * NOTE: newName must be palloc'd.
 */
TargetEntry *CreateRenamedTargetEntry(TargetEntry *baseTE, char *newName,
				bool flattenExprTree)
{
	TargetEntry *renamedTE = copyObject(baseTE);
	renamedTE->resname = newName;

	// Need to convert Aggrefs to Var
	if (flattenExprTree && nodeTag(renamedTE->expr) != T_Var)
	{
		elog(LOG, "CreateRenamedTargetEntry flattening expression tree to Var");
		Var *flattenedVar = makeNode(Var);
		pfree(renamedTE->expr);
		renamedTE->expr = flattenedVar;
	}
	// Reset all varnos to 1 to reference new MatView rtable, which will have one RTE to represent the MatView
	ResetVarno(renamedTE->expr, 1);
	return renamedTE;
}

/**
 * Unparse a List (of RangeTblEntry) into the FROM clause of a query string.
 * NOTE: Returned pointer must be pfree'd.
 */
char *UnparseRangeTableEntries(List *rtable)
{
	RangeTblEntry *rte;
	char *clauseString;
	char rteBuffer[TARGET_BUFFER_SIZE];
	ListCell *rtCell;
	int index;

	index = 0;
	clauseString = palloc(sizeof(char) * QUERY_BUFFER_SIZE);
	memset(clauseString, 0, QUERY_BUFFER_SIZE);

	foreach(rtCell, rtable)
	{
		rte = lfirst_node(RangeTblEntry, rtCell);

		if (index < rtable->length - 1)
		{
			snprintf(rteBuffer, TARGET_BUFFER_SIZE, "%s, ",
						rte->eref->aliasname);
		}
		else
		{
			strncat(rteBuffer, rte->eref->aliasname, TARGET_BUFFER_SIZE);
		}

		strncat(rteBuffer, rteBuffer, QUERY_BUFFER_SIZE);
		index++;
	}

	return clauseString;
}

void UnparseFromExprRecurs(Query *rootQuery, Node *node, int fromClauseIndex,
				size_t fromClauseLength, char *selectQuery,
				size_t selectQuerySize)
{
	RangeTblEntry *joinRte, *leftRte, *rightRte;
	char *qualStr;
	char joinBuf[QUERY_BUFFER_SIZE];

	memset(joinBuf, 0, QUERY_BUFFER_SIZE);

	if (node != NULL)
	{
		if (IsA(node, JoinExpr))
		{
			char *joinTag;
			JoinExpr *joinExpr = (JoinExpr *) node;
			switch (joinExpr->jointype)
			{
				case JOIN_ANTI:
					joinTag = "ANTI JOIN";
					break;
				case JOIN_UNIQUE_INNER:
					joinTag = "UNIQUE INNER JOIN";
					break;
				case JOIN_UNIQUE_OUTER:
					joinTag = "UNIQUE OUTER JOIN";
					break;
				case JOIN_INNER:
					joinTag = "INNER JOIN";
					break;
				case JOIN_FULL:
					joinTag = "FULL JOIN";
					break;
				case JOIN_LEFT:
					joinTag = "LEFT JOIN";
					break;
				case JOIN_RIGHT:
					joinTag = "RIGHT JOIN";
					break;
				case JOIN_SEMI:
					joinTag = "SEMI JOIN";
					break;
				default:
					joinTag = "??? JOIN";
					elog(WARNING, "Couldn't recognize given JoinType");
			}

			// TODO: Is this correct to assume indices of left and right tables?
			// Left and right RTE indices should be correct.
			// See addRangeTableEntryForJoin in src/backend/parser/parse_relation.c:1858
			// 	for how RTEs and join RTEs are added to the Query's list of RTEs

			// NOTE: Un-parsing of mixed JOIN ON clauses with table cross products will fail
			if (!IsA(joinExpr->larg, RangeTblRef))
			{
				UnparseFromExprRecurs(rootQuery, joinExpr->larg,
										fromClauseIndex, fromClauseLength,
										selectQuery, selectQuerySize);
			}
			if (!IsA(joinExpr->rarg, RangeTblRef))
			{
				UnparseFromExprRecurs(rootQuery, joinExpr->rarg,
										fromClauseIndex, fromClauseLength,
										selectQuery, selectQuerySize);
			}

			if (joinExpr->rtindex != 0)
			{
				joinRte = rt_fetch(joinExpr->rtindex, rootQuery->rtable);
				leftRte = left_join_table(joinExpr, rootQuery->rtable);
				rightRte = right_join_table(joinExpr, rootQuery->rtable);
				elog(LOG, "Unparsed %s %s %s", leftRte->eref->aliasname,
				joinTag, rightRte->eref->aliasname);
				qualStr = UnparseQuals(joinExpr->quals, leftRte, rightRte,
										joinExpr, rootQuery->rtable);
				elog(LOG, "Unparsed qualifiers for %s %s %s",
				leftRte->eref->aliasname,
				joinTag, rightRte->eref->aliasname);

				if (leftRte->rtekind == RTE_JOIN)
				{
					snprintf(joinBuf, QUERY_BUFFER_SIZE, " %s %s ON %s",
								joinTag, rightRte->eref->aliasname, qualStr);
				}
				else
				{
					snprintf(joinBuf, QUERY_BUFFER_SIZE, " %s %s %s ON %s",
								leftRte->eref->aliasname, joinTag,
								rightRte->eref->aliasname, qualStr);
				}

				pfree(qualStr);
				qualStr = NULL;

				strncat(selectQuery, joinBuf, selectQuerySize);
//			AnalyzePlanQuals(joinExpr->quals, leftRte, rightRte);
			}
			else
			{
				elog(LOG, "Found JoinExpr.rtindex == 0. Skipping...");
			}
		}
		else if (IsA(node, RangeTblRef))
		{
			elog(LOG, "UnparseFromExprRecurs: found RangeTblRef in jointree");
			RangeTblRef *rtRef = (RangeTblRef *) node;
			elog(
					LOG, "UnparseFromExprRecurs: RangeTblRef.rtindex=%d", rtRef->rtindex);
			RangeTblEntry *rte = rt_fetch(rtRef->rtindex, rootQuery->rtable);

			if (fromClauseIndex < fromClauseLength - 1)
			{
//				elog(LOG, "UnparseFromExpr: no next entry for fromExprCell");
				snprintf(joinBuf, QUERY_BUFFER_SIZE, "%s, ",
							rte->eref->aliasname);
			}
			else
			{
				strncpy(joinBuf, rte->eref->aliasname, QUERY_BUFFER_SIZE);
//				elog(LOG, "UnparseFromExpr: copied table name to joinBUf");
			}

			strcat(selectQuery, joinBuf);
//			elog(LOG, "UnparseFromExpr: concatenated joinBuf to selectQuery");
		}
		else if (IsA(node, RangeTblEntry))
		{
			elog(WARNING, "Found RangeTblEntry in join analysis");
		}
		else if (IsA(node, FromExpr))
		{
			elog(WARNING, "Found FromExpr in recursive join analysis");
		}
	}
}

/**
 * Unparse a list of SortGroupClauses into the GROUP BY clause of a SQL string.
 * param groupClause: List (of SortGroupClause) from Query
 * param targetList: List (of TargetEntry) from Query
 * param rtable: List (of RangeTblEntry) from Query
 *
 * NOTE: returned pointer must be pfree'd.
 */
char *UnparseGroupClause(List *groupClause, List *targetList, List *rtable)
{
	char *groupClauseStr;
	ListCell *groupCell;
	SortGroupClause *groupStmt;
	TargetEntry *targetEntry;
	char targetBuffer[TARGET_BUFFER_SIZE];
	int index;

	if (groupClause != NIL && groupClause->length > 0)
	{
		index = 0;
		groupClauseStr = palloc(sizeof(char) * TARGET_BUFFER_SIZE);

		strcpy(groupClauseStr, " GROUP BY ");

		foreach(groupCell, groupClause)
		{
			groupStmt = lfirst_node(SortGroupClause, groupCell);
			targetEntry = (TargetEntry *) list_nth(targetList,
													groupStmt->tleSortGroupRef);
			TargetEntryToString(targetEntry, rtable, false, targetBuffer,
			TARGET_BUFFER_SIZE);
			elog(
					LOG, "Found SortGroupClause nodeTag=%d, sortGroupRef=%d, eqop=%d, sortop=%d, targetEntry=%s",
					nodeTag(groupStmt), groupStmt->tleSortGroupRef,
					groupStmt->eqop, groupStmt->sortop,
					targetBuffer);

			strncat(groupClauseStr, targetBuffer, TARGET_BUFFER_SIZE);

			if (index < groupClause->length - 1)
			{
				strncat(groupClauseStr, ", ", TARGET_BUFFER_SIZE);
			}

			index++;
		}
	}
	else
	{
		groupClauseStr = NULL;
	}

	elog(LOG, "Constructed GROUP BY clause: %s", groupClauseStr);

	return groupClauseStr;
}

void CreateJoinVarStr(JoinExpr *joinExpr, Var *var, RangeTblEntry *rte,
				List *rangeTables, char *varStrBuf, size_t varStrBufSize)
{
	RangeTblEntry *targetRte;

	targetRte = rte;

	// Special case for Join RTEs since their aliasname is "unnamed_join"
	if (rte->rtekind == RTE_JOIN)
	{
		RangeTblEntry *varRte = rt_fetch(var->varno, rangeTables);
		elog(LOG, "Var from table=%s: %s", varRte->eref->aliasname,
		get_colname(varRte, var));
		// TODO: Ensure leftRte index is correct.
//		 If rte is an RTE_JOIN, left should be the right table of last join
		RangeTblEntry *leftRte = rt_fetch(joinExpr->rtindex - 3, rangeTables);
		RangeTblEntry *rightRte = right_join_table(joinExpr, rangeTables);
//		elog(LOG, "CreateJoinVarStr found JOIN RTE: rtindex=%d, name=%s, left.name=%s, right.name=%s",
//						joinExpr->rtindex, rte->eref->aliasname,
//						leftRte->eref->aliasname, rightRte->eref->aliasname);

// Var was from left RTE
// TODO: ensure this logic is correct
		if (var->varattno <= leftRte->eref->colnames->length)
		{
			elog(
					LOG, "Renaming tableName in Join RTE to left table: %s", leftRte->eref->aliasname);
			targetRte = leftRte;
		}
		// Var from right RTE
		else
		{
			elog(
					LOG, "Renaming tableName in Join RTE to right table: %s", rightRte->eref->aliasname);
			targetRte = rightRte;
		}
	}

	sprintf(varStrBuf, "%s.%s", targetRte->eref->aliasname,
			get_colname(targetRte, var));

	elog(LOG, "Var str for rtekind=%d, varattno=%d %s",
	rte->rtekind, var->varattno, varStrBuf);

	if (var->varno == INDEX_VAR)
	{
		elog(LOG, "Var is an index var");
	}

}

/**
 * Returns a string representation of the qualifiers.
 * NOTE: returned char * must be pfreed.
 */
char *UnparseQuals(List *quals, RangeTblEntry *leftRte, RangeTblEntry *rightRte,
				JoinExpr *joinExpr, List *rangeTables)
{
	ListCell *qualCell;
	Expr *expr;
	size_t qualBufSize = 512;
	char *qualBuf;

	qualBuf = palloc(sizeof(char) * qualBufSize);
	memset(qualBuf, 0, qualBufSize);

	if (quals != NIL)
	{
		foreach(qualCell, quals)
		{
			expr = (Expr *) lfirst(qualCell);
//			elog(LOG, "Qualifier expression: %d", nodeTag(expr));

			switch (nodeTag(expr))
			{
				case T_OpExpr:
				{
					OpExpr *opExpr = (OpExpr *) expr;
//					elog(LOG, "Found OpExpr opno=%d, number of args=%d",
//					opExpr->opno,
//					opExpr->args != NULL ? opExpr->args->length : 0);

					if (opExpr->args != NULL)
					{
						if (opExpr->args->length == 1)
						{
							elog(
									WARNING, "Unary Operation Expressions are not currently supported");
							// TODO: handle unary op
						}
						else if (opExpr->args->length == 2)
						{
							char leftVarStr[256], rightVarStr[256];
							Expr *leftExpr = (Expr *) linitial(opExpr->args);
							Expr *rightExpr = (Expr *) lsecond(opExpr->args);

							memset(leftVarStr, 0, 256);
							memset(rightVarStr, 0, 256);
							ExprToString(leftExpr, joinExpr, leftRte,
											rangeTables, leftVarStr, 256);
							ExprToString(rightExpr, joinExpr, rightRte,
											rangeTables, rightVarStr, 256);

							if (qualBuf != NULL)
							{
								snprintf(qualBuf, qualBufSize,
											"%s %s %s",
											leftVarStr,
											// TODO: Figure out opno to string mapping
											opExpr->opno == 96 ?
															"=" : "UNKNOWN",
											rightVarStr);
							}
							else
							{
								elog(WARNING, "QualBuf was NULL");
							}
						}
						else
						{
							elog(
									WARNING, "Not currently handling OpExprs with more than 2 args");
						}
					}
					break;
				}
				case T_BoolExpr:
				{
					BoolExpr *boolExpr = (BoolExpr *) expr;
					elog(
							LOG, "Found boolean expression of type %s with %d args",
							boolExpr->boolop == AND_EXPR ? "AND" : boolExpr->boolop == OR_EXPR ? "OR" : "NOT",
							boolExpr->args != NIL ? boolExpr->args->length : 0);
					break;
				}
				default:
					elog(LOG, "Unrecognized qualifier expression");
			}
		}
	}

	return qualBuf;
}

char *AggrefToString(TargetEntry *aggrefEntry, List *rtable, bool renameAggref,
				char *aggrefStrBuf, size_t aggrefStrBufSize)
{
	Aggref *aggref;
	ListCell *argCell;
	TargetEntry *arg;
	char targetStr[TARGET_BUFFER_SIZE];
	char copyBuf[TARGET_BUFFER_SIZE];
	char *renamedBuf = NULL;

	memset(targetStr, 0, TARGET_BUFFER_SIZE);
	aggref = (Aggref *) aggrefEntry->expr;

	if (aggref->aggstar == true)
	{
		strcpy(targetStr, STAR_COL);
	}
	else
	{
		UnparseTargetList(aggref->args, rtable, false, targetStr,
		TARGET_BUFFER_SIZE);
	}

	if (renameAggref == true)
	{
		if (aggref->aggstar == true)
		{
			strcpy(copyBuf, RENAMED_STAR_COL);
		}
		else
		{
			strncpy(copyBuf, targetStr, TARGET_BUFFER_SIZE);
			ReplaceChars(copyBuf, '.', '_', TARGET_BUFFER_SIZE);
		}

		renamedBuf = palloc(sizeof(char) * TARGET_BUFFER_SIZE);
		snprintf(renamedBuf, TARGET_BUFFER_SIZE, "%s_%s", aggrefEntry->resname,
					copyBuf);
		snprintf(aggrefStrBuf, aggrefStrBufSize, "%s(%s) AS %s",
					aggrefEntry->resname, targetStr, renamedBuf);
	}
	else
	{
		snprintf(aggrefStrBuf, aggrefStrBufSize, "%s(%s)", aggrefEntry->resname,
					targetStr);
	}

	elog(LOG, "AggrefToString result: %s", aggrefStrBuf);

	return renamedBuf;
}

void ExprToString(Expr *expr, JoinExpr *join, RangeTblEntry *rte,
				List *rangeTables, char *targetBuf, size_t targetBufSize)
{
//	elog(LOG, "ExprToString Expr tag: %d", nodeTag(expr));

	switch (nodeTag(expr))
	{
		case T_Var:
			VarToString((Var *) expr, rangeTables, false, targetBuf,
						targetBufSize);
			elog(LOG, "ExprToString: converted Var to %s", targetBuf);
			break;
	}

}

char *TargetEntryToString(TargetEntry *targetEntry, List *rtable,
				bool renameTargets, char *outBuf, size_t outBufSize)
{
	char *targetEntryRename = NULL;

	switch (nodeTag(targetEntry->expr))
	{
		case T_Var:
		{
//			elog(LOG, "TargetEntryToString: converting Var to string. expr=%p",
//			targetEntry->expr);
			Var *var = (Var *) targetEntry->expr;
//			elog(LOG, "TargetEntryToString: var.varno=%d, rtable length=%d",
//			var->varno, rtable->length);
			targetEntryRename = VarToString(var, rtable, renameTargets, outBuf,
											outBufSize);
			break;
		}
		case T_Aggref:
//			elog(LOG, "TargetEntryToString: converting Aggref to string...");
			targetEntryRename = AggrefToString(targetEntry, rtable, true,
												outBuf, outBufSize);
			break;
		default:
			elog(WARNING, "Failed to convert TargetEntry to string");
	}

	return targetEntryRename;
}

char *VarToString(Var *var, List *rtable, bool renameVar, char *varBuf,
				size_t varBufSize)
{
	RangeTblEntry *varRte;
	char *tableName, *colName, *renamedColName;
	char *returnedVarRename = NULL;

	varRte = rt_fetch(var->varno, rtable);
	tableName = varRte->eref->aliasname;
//	elog(LOG, "VarToString fetched RTE=%s", varRte != NULL ? varRte->eref->aliasname : "NULL");

	if (var->varattno > 0)
	{
//		elog(
//				LOG, "VarToString getting colName from colnames with length=%d and varattno=%d, colname listcell=%s",
//				varRte->eref->colnames != NIL ? varRte->eref->colnames->length : 0,
//				var->varattno, get_colname(varRte, var));
		colName = renamedColName = get_colname(varRte, var);
//										strVal(lfirst(list_nth_cell(varRte->eref->colnames, var->varattno - 1)));
//		elog(LOG, "VarToString: colname=%s", colName);

//		if (renameVar)
//		{
//			elog(
//					LOG, "VarToString renaming Var. targetEntry->resname=%s", targetEntry->resname);
//		}
	}
	else
	{
		colName = STAR_COL;
		renamedColName = RENAMED_STAR_COL;
	}

	if (renameVar == true)
	{
//		elog(LOG, "VarToString renaming Var");
		returnedVarRename = palloc(sizeof(char) * TARGET_BUFFER_SIZE);
		snprintf(returnedVarRename, TARGET_BUFFER_SIZE, "%s_%s", tableName,
					renamedColName);
		snprintf(varBuf, varBufSize, "%s.%s AS %s", tableName, colName,
					returnedVarRename);
	}
	else
	{
		snprintf(varBuf, varBufSize, "%s.%s", tableName, colName);
	}

//	elog(LOG, "VarToString result: %s", varBuf);
	return returnedVarRename;
}

void AddQuery(Query *query, PlannedStmt *plannedStatement)
{
	MemoryContext oldContext;
	List *matViewQueries;

	CheckInitState();
	oldContext = MemoryContextSwitchTo(AutoMatViewContext);
	queryPlanStatsList = lappend(
					queryPlanStatsList,
					CreateQueryPlanStats(query, plannedStatement));
	MatView *mView = UnparseQuery(query);
	elog(LOG, "Unparsed Query: %s", mView->selectQuery);

	// TODO: Set training threshold from postgres properties file
	if (queryPlanStatsList->length > trainingSampleCount)
	{
		isCollectingQueries = false;
		CreateRelevantMatViews();
	}

	MemoryContextSwitchTo(oldContext);
}

int64 CreateMaterializedView(char *viewName, char *selectQuery)
{
	int ret;
	int64 processed;
	char resultQuery[1000];

	sprintf(resultQuery, "CREATE MATERIALIZED VIEW IF NOT EXISTS %s AS %s;",
			viewName, selectQuery);

	SPI_connect();
	ret = SPI_exec(resultQuery, 0);
	processed = SPI_processed;

	if (ret > 0 && SPI_tuptable != NULL)
	{
		elog(LOG, "Found returned tuples from SPI_tuptable");
	}
	else
	{
		elog(LOG, "No returned tuples in SPI_tuptable");
	}

	SPI_finish();

	return processed;
}

void CreateRelevantMatViews()
{
	List *matViewQueries;
	List *costlyQueryPlanStats;
	ListCell *queryCell;
	Query *query;
	MatView *newMatView;

	costlyQueryPlanStats = GetCostlyQueries(0.1);
	matViewQueries = GenerateInterestingQueries(costlyQueryPlanStats);
	matViewQueries = PruneQueries(matViewQueries);

	foreach(queryCell, matViewQueries)
	{
		query = lfirst_node(Query, queryCell);
		newMatView = UnparseQuery(query);
		PopulateMatViewRenamedRTable(newMatView);
		CreateMaterializedView(newMatView->name, newMatView->selectQuery);
		createdMatViews = list_append_unique(createdMatViews, newMatView);
	}

	elog(LOG, "Created %d materialized views based on given query workload",
	createdMatViews != NIL ? createdMatViews->length : 0);
}

void PopulateMatViewRenamedRTable(MatView *matView)
{
	ListCell *targetEntryCell;
	TargetEntry *targetEntry;
	RangeTblEntry *renamedRte;
	List *newRTable = NIL;
	List *newColnames = NIL;
	int16 varattno = 1;

//	elog(LOG, "PopulatedMatViewRenamedRTable called...");
	// Make an rtable of length 1 from existing one for ease of creation
	matView->renamedRtable = list_make1(
					copyObject(linitial(matView->baseQuery->rtable)));
	renamedRte = linitial(matView->renamedRtable);
	renamedRte->eref->aliasname = pstrdup(matView->name);
//	elog(LOG, "Renaming targetEntries...");

//	elog(
//	LOG, "Renamed targetList length %d", matView->renamedTargetList != NIL ?
//	matView->renamedTargetList->length : 0);
	foreach(targetEntryCell, matView->renamedTargetList)
	{
//		elog(LOG, "Getting first TargetEntry node...");
		targetEntry = lfirst_node(TargetEntry, targetEntryCell);
//		elog(LOG, "Renamed TargetEntry: %s", targetEntry->resname);
		newColnames = list_append_unique(
						newColnames, makeString(pstrdup(targetEntry->resname)));
//		elog(LOG, "Appended new colname: %s", targetEntry->resname);
		SetVarattno(targetEntry->expr, varattno++);
	}

	list_free(renamedRte->eref->colnames);
	renamedRte->eref->colnames = newColnames;

	char targetListBuf[QUERY_BUFFER_SIZE];
//	elog(
//	LOG, "Unparsing new renamed targetList of length=%d and rtable.length=%d",
//	matView->renamedTargetList != NIL ? matView->renamedTargetList->length : 0,
//	matView->renamedRtable != NIL ? matView->renamedRtable->length : 0);
	UnparseTargetList(matView->renamedTargetList, matView->renamedRtable, false,
						targetListBuf, QUERY_BUFFER_SIZE);
	elog(LOG, "Unparsed renamed targetList: %s", targetListBuf);
}

/**
 * Retrieves all QueryPlanStats whose estimated plan costs are higher than the supplied
 * 	ratio of the individual query cost to the total cost of all gathered queries.
 *
 * returns: List (of QueryPlanStats)
 */
List *GetCostlyQueries(double costCutoffRatio)
{
	int index;
	double totalCost;
	double *planCosts;
	double costCutoff;
	ListCell *planCell;
	QueryPlanStats *stats;
	List *costliestQueryPlans;

	costliestQueryPlans = NIL;
	totalCost = 0;
	index = 0;
	planCosts = palloc(sizeof(double) * queryPlanStatsList->length);

	foreach(planCell, queryPlanStatsList)
	{
		stats = (QueryPlanStats *) lfirst(planCell);

		if (stats != NULL)
		{
			planCosts[index] = GetPlanCost(stats->plan->planTree);
			totalCost += planCosts[index];
		}

		index++;
	}

	costCutoff = totalCost * costCutoffRatio;

	for (index = 0; index < queryPlanStatsList->length; index++)
	{
		if (planCosts[index] >= costCutoff)
		{
			stats = (QueryPlanStats *) list_nth(queryPlanStatsList, index);
			costliestQueryPlans = lappend(costliestQueryPlans, stats);
			MatView *view = UnparseQuery(stats->query);
		}
	}

	pfree(planCosts);

	if (costliestQueryPlans != NIL)
	{
		elog(
				LOG, "GetCostlyQueries found %d costly queries", costliestQueryPlans->length);
	}
	else
	{
		elog(LOG, "GetCostlyQueries found no costly queries");
	}

	return costliestQueryPlans;
}

/**
 * Generates a set of interesting Query trees to be used to generate materialized views.
 *
 * param queryPlanStats: List (of QueryPlanStats)
 * returns: List (of Query)
 */
List *GenerateInterestingQueries(List *queryPlanStats)
{
	List *interestingQueries;
	ListCell *queryStatsCell;
	QueryPlanStats *stats;

	interestingQueries = NIL;

	// TODO: generate only interesting MatViews
	foreach(queryStatsCell, queryPlanStats)
	{
		stats = (QueryPlanStats *) lfirst(queryStatsCell);
		interestingQueries = lappend(interestingQueries, stats->query);
	}

	return interestingQueries;
}

/**
 * Prunes less useful queries from a list of queries.
 *
 * param queries: List (of Query)
 * returns: List (of Query)
 */
List *PruneQueries(List *queries)
{
	// TODO: actually prune queries
	// TODO: remove duplicates
	return queries;
}

bool IsCollectingQueries()
{
	return isCollectingQueries;
}

/**
 * Rewrite the given Query to use the given materialized view.
 * returns: SQL query string representing rewritten Query object.
 */
char *RewriteQuery(Query *query, MatView *matView)
{
	char *rewrittenQuery;
	Query *queryCopy = copyObject(query);

	elog(LOG, "RewriteQuery called...");

	if (matView != NULL)
	{
		RewriteTargetList(queryCopy, matView);
		RewriteJoinTree(queryCopy, matView);

		MatView *createdView = UnparseQuery(queryCopy);
		rewrittenQuery = pstrdup(createdView->selectQuery);
		FreeMatView(createdView);
	}
	else
	{
		rewrittenQuery = NULL;
	}

	return rewrittenQuery;
}

/**
 * Rewrite the given Query's targetList to reference the given MatView's Query's targetList.
 * NOTE: Currently won't work for "SELECT *" queries with joins.
 */
void RewriteTargetList(Query *query, MatView *matView)
{
	ListCell *targetEntryCell, *matViewTargetEntryCell;
	TargetEntry *queryTargetEntry, *matViewTargetEntry;
	bool foundMatchingEntry;
	List *newTargetList;
	int matViewTargetListIndex;

	newTargetList = NIL;

	if (query->targetList != NIL)
	{
		// Add MatView's single rtable entry to the end of this Query's rtable so converted references can reference it
		query->rtable = lappend(query->rtable,
								copyObject(linitial(matView->renamedRtable)));

		foreach(targetEntryCell, query->targetList)
		{
			matViewTargetListIndex = 0;
			foundMatchingEntry = false;
			queryTargetEntry = lfirst_node(TargetEntry, targetEntryCell);

			for (matViewTargetEntryCell = list_head(
							matView->baseQuery->targetList);
							!foundMatchingEntry
											&& matViewTargetEntryCell != NULL;
							matViewTargetEntryCell =
											matViewTargetEntryCell->next)
			{
				matViewTargetEntry = lfirst_node(TargetEntry,
													matViewTargetEntryCell);
				foundMatchingEntry = AreTargetEntriesEqual(
								queryTargetEntry, query->rtable,
								matViewTargetEntry, matView->baseQuery->rtable);
				elog(LOG, "Found matching TargetEntry? %s",
				foundMatchingEntry ? "true" : "false");
				matViewTargetListIndex++;
			}

			if (foundMatchingEntry)
			{
				matViewTargetEntry = copyObject(
								list_nth(matView->renamedTargetList,
											matViewTargetListIndex - 1));
				// Set varno of Vars in the replaced expression to reference MatView's rtable entry
				SetVarno(matViewTargetEntry->expr, query->rtable->length);

				if (IsA(matViewTargetEntry->expr, Var))
				{
					Var *var = (Var *) matViewTargetEntry->expr;
					elog(LOG, "Fetching replaced Var's RangeTblEntry...");
					RangeTblEntry *replacedRT = rt_fetch(var->varno,
															query->rtable);
					if (replacedRT != NULL)
					{
						elog(LOG, "Replaced Var references RTE: %s, colname=%s",
						replacedRT->eref->aliasname,
						get_colname(replacedRT, var));
					}
				}
				elog(LOG, "Found matching MatView TargetEntry for %s with: %s",
				queryTargetEntry->resname, matViewTargetEntry->resname);
				newTargetList = lappend(newTargetList, matViewTargetEntry);
			}
			else
			{
				elog(
						LOG, "No matching MatView TargetEntry found. Appending existing TargetEntry");
				newTargetList = lappend(newTargetList,
										copyObject(queryTargetEntry));
			}

		}
	}

	list_free(query->targetList);
	query->targetList = newTargetList;
	elog(
			LOG, "Rewrote targetList. Length=%d", newTargetList != NIL ? newTargetList->length : 0);
	char rewrittenBuf[QUERY_BUFFER_SIZE];
	MatView *mview = UnparseTargetList(query->targetList, query->rtable, false,
										rewrittenBuf, QUERY_BUFFER_SIZE);
	elog(LOG, "Rewritten targetList=%s", rewrittenBuf);
}

/**
 * Rewrite the Query's jointree to utilize the tables contained in the given MatView.
 */
void RewriteJoinTree(Query *query, MatView *matView)
{
	int joinListLength;
	ListCell *fromCell;
	int joinIndex = 0;
	int joinCount = 0;
	int joinsRemoved = 0;

	if (query->jointree != NULL)
	{
		joinListLength = query->jointree->fromlist->length;
		foreach(fromCell, query->jointree->fromlist)
		{
			RewriteJoinTreeRecurs(query, matView, lfirst(fromCell), joinIndex++,
									joinListLength, &joinCount, &joinsRemoved);
		}

		if (joinsRemoved == joinCount)
		{
			elog(
					LOG, "All existing joins were removed. Setting jointree to single RangeTblRef");

			if (query->jointree->quals != NULL)
			{
				pfree(query->jointree->quals);
				query->jointree->quals = NULL;
			}
			if (query->jointree->fromlist != NIL)
			{
				list_free(query->jointree->fromlist);
				query->jointree->fromlist = NIL;
			}

			RangeTblRef *matViewRef = makeNode(RangeTblRef);
			// Set rtindex to query.rtable.length since the MatView RTE was added to the end of query.rtable
			matViewRef->rtindex = query->rtable->length;
			query->jointree->fromlist = list_make1(matViewRef);
		}
		elog(LOG, "Successfully rewrote Query join tree");
	}
}

void RewriteJoinTreeRecurs(Query *rootQuery, MatView *matView, Node *node,
				int fromClauseIndex, size_t fromClauseLength, int *joinCount,
				int *joinsRemoved)
{
	RangeTblEntry *joinRte, *leftRte, *rightRte;
//	List *removedRTEs = NIL;

	if (node != NULL)
	{
		if (IsA(node, JoinExpr))
		{
			char *joinTag;
			JoinExpr *joinExpr = (JoinExpr *) node;
			bool containsLeftRte, containsRightRte;

			(*joinCount)++;

			// NOTE: Un-parsing of mixed JOIN ON clauses with table cross products will fail
			if (IsA(joinExpr->larg, JoinExpr))
			{
				RewriteJoinTreeRecurs(rootQuery, matView, joinExpr->larg,
										fromClauseIndex, fromClauseLength,
										joinCount, joinsRemoved);
			}
			if (IsA(joinExpr->rarg, JoinExpr))
			{
				RewriteJoinTreeRecurs(rootQuery, matView, joinExpr->rarg,
										fromClauseIndex, fromClauseLength,
										joinCount, joinsRemoved);
			}

			joinRte = rt_fetch(joinExpr->rtindex, rootQuery->rtable);
			leftRte = left_join_table(joinExpr, rootQuery->rtable);
			rightRte = right_join_table(joinExpr, rootQuery->rtable);
			containsLeftRte = DoesMatViewContainRTE(leftRte, matView);
			containsRightRte = DoesMatViewContainRTE(rightRte, matView);

			if (containsLeftRte && containsRightRte)
			{
				(*joinsRemoved)++;
				joinExpr->rtindex = 0; // JoinExpr.rtindex of 0 means no join

				if (leftRte->rtekind == RTE_JOIN)
				{
					elog(
							LOG, "Replacing JOIN of leftTable(RTE_JOIN)=%s and rightTable=%s with MatView table=%s",
							rt_fetch(joinExpr->rtindex - 3, rootQuery->rtable)->eref->aliasname,
							rightRte->eref->aliasname,
							((RangeTblEntry *)llast(rootQuery->rtable))->eref->aliasname);
				}
				else
				{
					// TODO: Remove this join
					elog(
							LOG, "Replacing JOIN of leftTable=%s and rightTable=%s with MatView table=%s",
							leftRte->eref->aliasname,
							rightRte->eref->aliasname,
							((RangeTblEntry *)llast(rootQuery->rtable))->eref->aliasname);
				}
//				removedRTEs = list_append_unique_int(removedRTEs,
//														joinExpr->rtindex);
//				removedRTEs = list_append_unique_int(removedRTEs,
//														joinExpr->rtindex - 1);
//				removedRTEs = list_append_unique_int(removedRTEs,
//														joinExpr->rtindex - 2);
			}
			else if (containsLeftRte)
			{
				elog(
						LOG, "Replacing leftTable=%s from Query.rtable with %s from MatView",
						leftRte->eref->aliasname,
						((RangeTblEntry *)llast(rootQuery->rtable))->eref->aliasname);
				CopyRte(leftRte, llast(rootQuery->rtable));
				// TODO: Rewrite joinExpr->quals to reference MatView
			}
			else if (containsRightRte)
			{
				// TODO: replace right RTE with MatView RTE
				elog(
						LOG, "Replacing rightTable=%s from Query.rtable with %s from MatView",
						rightRte->eref->aliasname,
						((RangeTblEntry *)llast(rootQuery->rtable))->eref->aliasname);
				CopyRte(rightRte, llast(rootQuery->rtable));
				// TODO: Rewrite joinExpr->quals to reference MatView
			}

			// TODO: ensure quals are equal
//			qualStr = AnalyzePlanQuals(joinExpr->quals, leftRte, rightRte,
//										joinExpr, rootQuery->rtable);

			if (leftRte->rtekind == RTE_JOIN)
			{
//					snprintf(joinBuf, QUERY_BUFFER_SIZE, " %s %s ON %s", joinTag,
//								rightRte->eref->aliasname, qualStr);
			}
			else
			{
//					snprintf(joinBuf, QUERY_BUFFER_SIZE, " %s %s %s ON %s",
//								leftRte->eref->aliasname, joinTag,
//								rightRte->eref->aliasname, qualStr);
			}

//				pfree(qualStr);
//				qualStr = NULL;
//
//				strncat(selectQuery, joinBuf, selectQuerySize);
			//			AnalyzePlanQuals(joinExpr->quals, leftRte, rightRte);

		}
	}
	else if (IsA(node, RangeTblRef))
	{
		RangeTblRef *rtRef = (RangeTblRef *) node;
		RangeTblEntry *rte = rt_fetch(rtRef->rtindex, rootQuery->rtable);

		if (fromClauseIndex < fromClauseLength - 1)
		{
			//				elog(LOG, "UnparseFromExpr: no next entry for fromExprCell");
//					snprintf(joinBuf, QUERY_BUFFER_SIZE, "%s, ",
//								rte->eref->aliasname);
		}
		else
		{
//					strncpy(joinBuf, rte->eref->aliasname, QUERY_BUFFER_SIZE);
			//				elog(LOG, "UnparseFromExpr: copied table name to joinBUf");
		}

//				strcat(selectQuery, joinBuf);
		//			elog(LOG, "UnparseFromExpr: concatenated joinBuf to selectQuery");
	}
	else if (IsA(node, RangeTblEntry))
	{
		elog(WARNING, "Found RangeTblEntry in join analysis");
	}
	else if (IsA(node, FromExpr))
	{
		elog(WARNING, "Found FromExpr in recursive join analysis");
	}
}

void CopyRte(RangeTblEntry *destination, RangeTblEntry *copyTarget)
{
	if (destination != NULL && copyTarget != NULL)
	{
		destination->rtekind = copyTarget->rtekind;
		destination->relkind = copyTarget->relkind;
		pfree(destination->eref);
		destination->eref = copyObject(copyTarget->eref);
		destination->relid = copyTarget->relid;
		destination->jointype = copyTarget->jointype;

		if (destination->alias != NULL)
		{
			pfree(destination->alias);
			destination->alias = NULL;
		}
		if (copyTarget->alias != NULL)
		{
			destination->alias = copyObject(copyTarget->alias);
		}

		if (destination->functions != NIL)
		{
			pfree(destination->functions);
			destination->functions = NIL;
		}
		if (copyTarget->functions != NIL)
		{
			destination->functions = copyObject(copyTarget->eref);
		}
	}
	else
	{
		elog(WARNING, "CopyRte found NULL destination or copyTarget");
	}
}

bool DoesMatViewContainRTE(RangeTblEntry *rte, MatView *matView)
{
	ListCell *rteCell;
	RangeTblEntry *matViewRte;
	bool containsRte = false;

	for (rteCell = list_head(matView->baseQuery->rtable);
					!containsRte && rteCell != NULL; rteCell = rteCell->next)
	{
		matViewRte = lfirst_node(RangeTblEntry, rteCell);
		containsRte = rte->relid == matViewRte->relid;
	}

	if (containsRte)
	{
		elog(LOG, "MatView contains RTE=%s: %s",
		rte->eref->aliasname, matViewRte->eref->aliasname);
	}
	else
	{
		elog(LOG, "MatView does NOT contain RTE=%s", rte->eref->aliasname);
	}

	return containsRte;
}

//bool DoesMatViewContainJoinNode(Node *queryNode, List *queryRtable,
//				MatView *matView)
//{
//	bool containsNode = false;
//	ListCell *fromCell;
//
//	if (matView->baseQuery->jointree != NULL)
//	{
//		for (fromCell = list_head(matView->baseQuery->jointree->fromlist);
//						!containsNode && fromCell != NULL;
//						fromCell = fromCell->next)
//		{
//
//			containsNode = DoesMatViewContainJoinNodeRecurs(
//							queryNode, queryRtable, lfirst(fromCell),
//							matView->baseQuery->rtable);
//		}
//	}
//
//	return containsNode;
//}
//
//bool DoesMatViewContainJoinNodeRecurs(Node *queryNode, List *queryRtable,
//				Node *matViewNode, List *matViewRtable)
//{
//	RangeTblEntry *joinRte, *leftRte, *rightRte;
//	bool containsNode = false;
//
//	if (queryNode != NULL)
//	{
//		if (IsA(queryNode, JoinExpr))
//		{
//			char *joinTag;
//			JoinExpr *joinExpr = (JoinExpr *) queryNode;
//
//			// TODO: Is this correct to assume indices of left and right tables?
//			// Left and right RTE indices should be correct.
//			// See addRangeTableEntryForJoin in src/backend/parser/parse_relation.c:1858
//			// 	for how RTEs and join RTEs are added to the Query's list of RTEs
//
//			// NOTE: Un-parsing of mixed JOIN ON clauses with table cross products will fail
//			if (!IsA(joinExpr->larg, RangeTblRef))
//			{
//				//					UnparseFromExprRecurs(rootQuery, joinExpr->larg,
//				//											fromClauseIndex, fromClauseLength,
//				//											selectQuery, selectQuerySize);
//			}
//			if (!IsA(joinExpr->rarg, RangeTblRef))
//			{
//				//					UnparseFromExprRecurs(rootQuery, joinExpr->rarg,
//				//											fromClauseIndex, fromClauseLength,
//				//											selectQuery, selectQuerySize);
//			}
//
//			joinRte = rt_fetch(joinExpr->rtindex, rootQuery->rtable);
//			leftRte = left_join_table(joinExpr, rootQuery->rtable);
//			rightRte = right_join_table(joinExpr, rootQuery->rtable);
//			// TODO: ensure quals are equal
//			//			qualStr = AnalyzePlanQuals(joinExpr->quals, leftRte, rightRte,
//			//										joinExpr, rootQuery->rtable);
//
//			if (leftRte->rtekind == RTE_JOIN)
//			{
//				//					snprintf(joinBuf, QUERY_BUFFER_SIZE, " %s %s ON %s", joinTag,
//				//								rightRte->eref->aliasname, qualStr);
//			}
//			else
//			{
//				//					snprintf(joinBuf, QUERY_BUFFER_SIZE, " %s %s %s ON %s",
//				//								leftRte->eref->aliasname, joinTag,
//				//								rightRte->eref->aliasname, qualStr);
//			}
//
//			//				pfree(qualStr);
//			//				qualStr = NULL;
//			//
//			//				strncat(selectQuery, joinBuf, selectQuerySize);
//			//			AnalyzePlanQuals(joinExpr->quals, leftRte, rightRte);
//		}
//		else if (IsA(queryNode, RangeTblRef))
//		{
//			RangeTblRef *rtRef = (RangeTblRef *) queryNode;
//			RangeTblEntry *rte = rt_fetch(rtRef->rtindex, rootQuery->rtable);
//
//			if (fromClauseIndex < fromClauseLength - 1)
//			{
//				//				elog(LOG, "UnparseFromExpr: no next entry for fromExprCell");
//				//					snprintf(joinBuf, QUERY_BUFFER_SIZE, "%s, ",
//				//								rte->eref->aliasname);
//			}
//			else
//			{
//				//					strncpy(joinBuf, rte->eref->aliasname, QUERY_BUFFER_SIZE);
//				//				elog(LOG, "UnparseFromExpr: copied table name to joinBUf");
//			}
//
//			//				strcat(selectQuery, joinBuf);
//			//			elog(LOG, "UnparseFromExpr: concatenated joinBuf to selectQuery");
//		}
//		else if (IsA(queryNode, RangeTblEntry))
//		{
//			elog(WARNING, "Found RangeTblEntry in join analysis");
//		}
//		else if (IsA(queryNode, FromExpr))
//		{
//			elog(WARNING, "Found FromExpr in recursive join analysis");
//		}
//	}
//	return containsNode;
//}

/**
 * Get the best MatView match which will be used to rewrite the given Query.
 * returns: a MatView if there was a match, or NULL otherwise.
 */
MatView *GetBestMatViewMatch(Query *query)
{
	List *matchingMatViews;
	MatView *bestMatch;

	matchingMatViews = GetMatchingMatViews(query);
	bestMatch = NULL;
	// TODO: filter returned MatViews to find best match

	if (matchingMatViews != NIL && matchingMatViews->length > 0)
	{
		// Choose the first match for now
		elog(LOG, "GetBestMatViewMatch choosing first MatView");
		bestMatch = (MatView *) linitial(matchingMatViews);
		elog(LOG, "GetBestMatViewMatch chose first MatView: %p", bestMatch);
	}

	return bestMatch;
}

/**
 * Get a list of matching MatViews given a Query.
 * returns: List (of MatView)
 *
 * NOTE: returned List must be pfree'd.
 */
List *GetMatchingMatViews(Query *query)
{
	List *matchingViews;
	ListCell *viewCell;
	MatView *matView;

	matchingViews = NIL;

	foreach(viewCell, createdMatViews)
	{
		matView = (MatView *) lfirst(viewCell);

		if (DoesQueryMatchMatView(query, matView))
		{
			matchingViews = list_append_unique(matchingViews, matView);
		}
	}

	elog(LOG, "Found %d matching materialized views for Query",
	matchingViews != NIL ? matchingViews->length : 0);

	return matchingViews;
}

bool DoesQueryMatchMatView(Query *query, MatView *matView)
{
	bool isMatch;

	// TODO: validate matching conditions and (maybe) joins
	isMatch = IsTargetListMatch(query->rtable, query->targetList, matView);

	return isMatch;
}

/**
 * Determines whether or not the provided List (of TargetEntry) matches the given MatView.
 */
bool IsTargetListMatch(List *rtable, List *targetList, MatView *matView)
{
	bool isMatch, foundTargetEntryMatch;
	ListCell *targetEntryCell, *viewTargetEntryCell;
	TargetEntry *targetEntry, *viewTargetEntry;

	isMatch = true;
	foundTargetEntryMatch = false;
	targetEntryCell = list_head(targetList);

	for (targetEntryCell = list_head(targetList);
					isMatch && targetEntryCell != ((void *) 0);
					targetEntryCell = targetEntryCell->next)
	{
		targetEntry = lfirst_node(TargetEntry, targetEntryCell);

		for (viewTargetEntryCell = list_head(matView->baseQuery->targetList);
						!foundTargetEntryMatch
										&& viewTargetEntryCell != ((void *) 0);
						viewTargetEntryCell = viewTargetEntryCell->next)
		{
			viewTargetEntry = lfirst_node(TargetEntry, viewTargetEntryCell);
			// TODO: Should we only compare non-join tables?
			foundTargetEntryMatch = AreTargetEntriesEqual(
							targetEntry, rtable, viewTargetEntry,
							matView->baseQuery->rtable);
		}

		isMatch = foundTargetEntryMatch;
	}

	elog(LOG, "Found match for TargetList? %s",
	isMatch ? "true" : "false");

	return isMatch;
}

bool AreTargetEntriesEqual(TargetEntry *targetEntryOne, List *rtableOne,
				TargetEntry *targetEntryTwo, List *rtableTwo)
{
	bool equal = false;

	if (nodeTag(targetEntryOne->expr) == nodeTag(targetEntryTwo->expr))
	{
		switch (nodeTag(targetEntryOne->expr))
		{
			case T_Var:
			{
				Var *varOne = (Var *) targetEntryOne->expr;
				Var *varTwo = (Var *) targetEntryTwo->expr;
				RangeTblEntry *varRteOne = rt_fetch(varOne->varno, rtableOne);
				RangeTblEntry *varRteTwo = rt_fetch(varTwo->varno, rtableTwo);
				elog(
						LOG, "Comparing Vars: tableOne.relid=%d to tableTwo.relid=%d and varOne.varattno=%d to varTwo.varattno=%d",
						varRteOne->relid, varRteTwo->relid,
						varOne->varattno, varTwo->varattno);
				equal = varRteOne->relid == varRteTwo->relid
								&& varOne->varattno == varTwo->varattno;
				break;
			}
			case T_Aggref:
			{
				ListCell *argCellOne, *argCellTwo;
				Aggref *aggrefOne = (Aggref *) targetEntryOne->expr;
				Aggref *aggrefTwo = (Aggref *) targetEntryTwo->expr;
				elog(
						LOG, "Comparing Aggrefs: aggrefOne.aggfnoid=%d, aggrefTwo.aggfnoid=%d, aggrefOne.args.length=%d, aggrefTwo.args.length=%d",
						aggrefOne->aggfnoid, aggrefTwo->aggfnoid,
						aggrefOne->args != NIL ? aggrefOne->args->length : 0,
						aggrefTwo->args != NIL ? aggrefTwo->args->length : 0);
				equal = aggrefOne->aggfnoid == aggrefTwo->aggfnoid
								&& aggrefOne->args != NIL
								&& aggrefTwo->args != NIL
								&& aggrefOne->args->length
												== aggrefTwo->args->length;

				if (equal)
				{
					argCellOne = list_head(aggrefOne->args);
					argCellTwo = list_head(aggrefTwo->args);

					while (equal && argCellOne != NULL && argCellTwo != NULL)
					{
						equal = AreTargetEntriesEqual(
										lfirst_node(TargetEntry, argCellOne),
										rtableOne,
										lfirst_node(TargetEntry, argCellTwo),
										rtableTwo);
						argCellOne = argCellOne->next;
						argCellTwo = argCellTwo->next;
					}
				}
				break;
			}
			default:
				elog(
						LOG, "AreTargetEntriesEqual found unrecognized nodeTag; returning false");
				equal = false;
		}
	}

	return equal;
}

void AddQueryStats(Query *query)
{
	ListCell *rte_cell;
	ListCell *col_cell;
	RangeTblEntry *rte;
	TableQueryStats *stats;
	MemoryContext oldContext;
	Query *queryCopy;

	elog(LOG, "AddQueryStats...");

	CheckInitState();
	oldContext = MemoryContextSwitchTo(AutoMatViewContext);
	elog(LOG, "Copying query object");
	queryCopy = copyObject(query);

	foreach(rte_cell, query->rtable)
	{
		rte = (RangeTblEntry *) lfirst(rte_cell);

		switch (rte->rtekind)
		{
			case RTE_RELATION:
				stats = FindTableQueryStats(rte->eref->aliasname);

				if (stats == NULL)
				{
					elog(
							LOG, "Creating new query stats for %s", rte->eref->aliasname);
					stats = CreateTableQueryStats(rte->eref->aliasname,
					// RangeTblEntry.eref.colnames contains list of all column names every time
													rte->eref->colnames);
					queryStats = list_append_unique(queryStats, stats);
				}

				switch (query->commandType)
				{
					case CMD_SELECT:
						elog(LOG, "Analyzing select query");
						stats->selectCounts++;
						// Query.targetList contains only currently selected columns
						UpdateSelectQueryStats(stats->selectStats,
												query->targetList);
						break;
					case CMD_INSERT:
						elog(LOG, "Analyzing insert query");
						break;
					case CMD_UPDATE:
						stats->updateCounts++;
						elog(LOG, "Analyzing update query");
						break;
					case CMD_DELETE:
						stats->deleteCounts++;
						elog(LOG, "Analyzing delete query");
						break;
					default:
						elog(
						WARNING, "Couldn't recognize given query command type");
				}
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

//				PrintTableQueryStats(stats);
				break;
			case RTE_JOIN:
				elog(LOG, "RTE Join found");
				break;
		}
	}

	MemoryContextSwitchTo(oldContext);
}

void FreeMatView(MatView *matView)
{
	if (matView != NULL)
	{
		pfree(matView->baseQuery);
		pfree(matView->name);
		pfree(matView->selectQuery);
		pfree(matView);
	}
}

/**
 * string MUST be null-terminated.
 */
void ReplaceChars(char *string, char target, char replacement, size_t sizeLimit)
{
	size_t index;

	for (index = 0; index < sizeLimit && string[index] != '\0'; index++)
	{
		if (string[index] == target)
		{
			string[index] = replacement;
		}
	}
}

double GetPlanCost(Plan *plan)
{
	double cost;
	ListCell *targetCell;
	char *planTag;

	cost = 0;

	if (plan != NULL)
	{
//		elog(
//				LOG, "Plan node type=%d, cost=%f, tuples=%f, tupleWidth=%d",
//				nodeTag(plan), plan->total_cost, plan->plan_rows, plan->plan_width);
		switch (nodeTag(plan))
		{
			case T_Result:
//				elog(LOG, "Result plan node found");
				planTag = "Result";
				break;
			case T_SeqScan: // 18
				planTag = "SequenceScan";
				break;
			case T_Join:
			case T_MergeJoin:
			case T_HashJoin:
				planTag = "Join";
//				elog(LOG, "Join plan node found");
				break;
			case T_Hash:
				planTag = "Hash";
				break;
			default:
				planTag = "Unknown";
		}

//		AnalyzePlanQuals(plan->qual);

		foreach(targetCell, plan->targetlist)
		{
			TargetEntry *targetEntry = lfirst_node(TargetEntry, targetCell);
			// Skip junk entries and those with no table
			if (targetEntry->resjunk
							|| targetEntry->resorigtbl
											== 0|| targetEntry->resname == NULL)
			{
				continue;
			}
		}

		cost = plan->total_cost + GetPlanCost(plan->lefttree)
						+ GetPlanCost(plan->righttree);
	}

	return cost;
}

void ResetVarno(Expr *expr, Index newVarno)
{
	switch (nodeTag(expr))
	{
		case T_Var:
		{
			Var *var = (Var *) expr;
			var->varno = newVarno;
//			elog(LOG, "Setting varno of Var to %d", newVarno);
			break;
		}
		case T_Aggref:
		{
			Aggref *aggref = (Aggref *) expr;
			ListCell *argCell;
			TargetEntry *argTE;
			elog(LOG, "Setting varno of Vars in Aggref to %d", newVarno);

			foreach(argCell, aggref->args)
			{
				argTE = lfirst_node(TargetEntry, argCell);
				ResetVarno(argTE->expr, newVarno);
			}
			break;
		}
		default:
			elog(
					WARNING, "Failed to create renamed TargetEntry due to unrecognized nodeTag");
	}
}

void SetVarattno(Expr *expr, AttrNumber varattno)
{
	switch (nodeTag(expr))
	{
		case T_Var:
		{
			elog(LOG, "Setting varattno for Var to %d", varattno);
			Var *var = (Var *) expr;
			var->varattno = varattno;
			break;
		}
		case T_Aggref:
		{
			Aggref *aggref = (Aggref *) expr;
			ListCell *argCell;
			TargetEntry *argTE;

			elog(LOG, "Setting varattno for args in Aggref to %d", varattno);

			foreach(argCell, aggref->args)
			{
				argTE = lfirst_node(TargetEntry, argCell);
				SetVarattno(argTE->expr, varattno);
			}
			break;
		}
		default:
			elog(WARNING, "Failed to set varattno for unrecognized node");
	}
}

void SetVarno(Expr *expr, Index varno)
{
	switch (nodeTag(expr))
	{
		case T_Var:
		{
			elog(LOG, "Setting varno for Var to %d", varno);
			Var *var = (Var *) expr;
			var->varno = varno;
			break;
		}
		case T_Aggref:
		{
			Aggref *aggref = (Aggref *) expr;
			ListCell *argCell;
			TargetEntry *argTE;

			elog(LOG, "Setting varno for args in Aggref to %d", varno);

			foreach(argCell, aggref->args)
			{
				argTE = lfirst_node(TargetEntry, argCell);
				SetVarno(argTE->expr, varno);
			}
			break;
		}
		default:
			elog(WARNING, "Failed to set varno for unrecognized node");
	}
}

/**
 * Find a particular RangeTblEntry by its Oid within a list of RTEs.
 */
RangeTblEntry *FindRte(Oid relid, List *rtable)
{
	ListCell *rteCell;
	RangeTblEntry *rte;

	foreach(rteCell, rtable)
	{
		rte = lfirst_node(RangeTblEntry, rteCell);

		if (rte->relid == relid)
		{
			return rte;
		}
	}

	return NULL;
}

void PrintQueryInfo(Query *query)
{
	ListCell *rte_cell;
	ListCell *col_cell;
	ListCell *targetList;
	RangeTblEntry *rte;

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
			{
				continue;
			}
			elog(LOG, "RTE Target entry: %s",
			te->resname);
		}

		foreach(rte_cell, query->rtable)
		{
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

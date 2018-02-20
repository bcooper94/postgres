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
#include "parser/parsetree.h"
#include "rewrite/automatviewselect.h"
#include "lib/dshash.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/guc_tables.h"

#define MAX_TABLENAME_SIZE 256
#define MAX_COLNAME_SIZE 256
#define QUERY_BUFFER_SIZE 1024
#define TARGET_BUFFER_SIZE 256

#define STAR_COL "*"
#define RENAMED_STAR_COL "all"

#define left_join_table(joinExpr, rangeTables) \
	(rt_fetch(joinExpr->rtindex - 2, rangeTables))

#define right_join_table(joinExpr, rangeTables) \
	(rt_fetch(joinExpr->rtindex - 1, rangeTables))

typedef struct QueryPlanStats
{
	Query *query;
	PlannedStmt *plan;
} QueryPlanStats;

typedef struct MatView
{
	char *name;
	char *selectQuery;
	Query *baseQuery; // Query object which this MatView is based on
} MatView;

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
static MemoryContext AutoMatViewContext = NULL;
static List *matViewIntoClauses = NIL;
//static dshash_table *selectQueryCounts = NULL;
static List *queryStats = NIL;
static List *plannedQueries = NIL;
static List *createdMatViews = NIL;
static List *queryPlanStatsList = NIL;
static bool isCollectingQueries = true;

// Have we loaded the trainingSampleCount from postgresql.conf?
static bool isTrainingSampleCountLoaded = false;
static int trainingSampleCount = 100;

static QueryPlanStats *CreateQueryPlanStats(Query *query, PlannedStmt *plan);
static ColumnStats *CreateColumnStats(char *colName);
static ColumnStats *GetColumnStats(SelectQueryStats *stats, char *colName);
static SelectQueryStats *CreateSelectQueryStats(List *targetList);
static void UpdateSelectQueryStats(SelectQueryStats *stats, List *targetList);
static TableQueryStats *CreateTableQueryStats(char *tableName, List *targetList);
static TableQueryStats *FindTableQueryStats(char *tableName);
static void PrintTableQueryStats(TableQueryStats *stats);

static void CreateJoinVarStr(JoinExpr *joinExpr, Var *var, RangeTblEntry *rte,
				List *rangeTables, char *varStrBuf, size_t varStrBufSize);
static char *AnalyzePlanQuals(List *quals, RangeTblEntry *leftRte,
				RangeTblEntry *rightRte, JoinExpr *joinExpr, List *rangeTables);
static void AggrefToString(TargetEntry *aggrefEntry, List *rtable,
				bool renameAggref, char *targetBuf, size_t targetBufSize);
static void ExprToString(Expr *expr, JoinExpr *join, RangeTblEntry *rte,
				List *rangeTables, char *targetBuf, size_t targetBufSize);
static bool *TargetEntryToString(TargetEntry *targetEntry, List *rtable,
				bool renameTargets, char *outBuf, size_t outBufSize);
static void VarToString(Var *var, List *rtable, bool renameVar, char *varBuf,
				size_t varBufSize);

static void ReplaceChars(char *string, char target, char replacement,
				size_t sizeLimit);

int64 CreateMaterializedView(char *viewName, char *selectQuery);
double GetPlanCost(Plan *plan);

static MatView *GetBestMatViewMatch(Query *query);
static List *GetMatchingMatViews(Query *query);
static bool DoesQueryMatchMatView(Query *query, MatView *matView);

static MatView *UnparseQuery(Query *query);
static RangeTblEntry *FindRte(Oid relid, List *rtable);
static char *UnparseTargetList(List *targetList, List *rtable,
				bool renameTargets);
static char *UnparseRangeTableEntries(List *rtable);
static void UnparseFromExprRecurs(Query *rootQuery, ListCell *fromExprCell,
				char *selectQuery);
static char *UnparseGroupClause(List *groupClause, List *targetList,
				List *rtable);

static void CreateRelevantMatViews();
static List *GetCostlyQueries(double costCutoffRatio);
static List *GenerateInterestingQueries(List *queryPlanStats);
static List *PruneQueries(List *queries);

static void CheckInitState();
static void PrintQueryInfo(Query *query);

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

void CreateJoinVarStr(JoinExpr *joinExpr, Var *var, RangeTblEntry *rte,
				List *rangeTables, char *varStrBuf, size_t varStrBufSize)
{
	ListCell *varCell;
	RangeTblEntry *targetRte;

	targetRte = rte;

	// Special case for Join RTEs since their aliasname is "unnamed_join"
	if (rte->rtekind == RTE_JOIN)
	{
		// TODO: Ensure leftRte index is correct.
		// If rte is an RTE_JOIN, left should be the right table of last join
		RangeTblEntry *leftRte = rt_fetch(joinExpr->rtindex - 3, rangeTables);
		RangeTblEntry *rightRte = right_join_table(joinExpr, rangeTables);
//		elog(LOG, "CreateJoinVarStr found JOIN RTE: rtindex=%d, name=%s, left.name=%s, right.name=%s",
//						joinExpr->rtindex, rte->eref->aliasname,
//						leftRte->eref->aliasname, rightRte->eref->aliasname);

		// Var was from left RTE
		// TODO: ensure this logic is correct
		if (var->varattno <= leftRte->eref->colnames->length)
		{
//			elog(LOG, "Renaming tableName in Join RTE to left table: %s", leftRte->eref->aliasname);
			targetRte = leftRte;
		}
		// Var from right RTE
		else
		{
//			elog(LOG, "Renaming tableName in Join RTE to right table: %s", rightRte->eref->aliasname);
			targetRte = rightRte;
		}
	}

	sprintf(varStrBuf,
			"%s.%s",
			targetRte->eref->aliasname,
			strVal(lfirst(list_nth_cell(rte->eref->colnames, var->varattno - 1))));

//	elog(LOG, "Var str for rtekind=%d %s",rte->rtekind, varStrBuf);

	if (var->varno == INDEX_VAR)
	{
		elog(LOG, "Var is an index var");
	}

}

/**
 * Returns a string representation of the qualifiers.
 * NOTE: returned char * must be pfreed.
 */
char *AnalyzePlanQuals(List *quals, RangeTblEntry *leftRte,
				RangeTblEntry *rightRte, JoinExpr *joinExpr, List *rangeTables)
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

void ExprToStringQuery(Expr *expr, Query *rootQuery, char *targetBuf)
{
	switch (nodeTag(expr))
	{
		case T_Var:
			elog(LOG, "Found Var in Expr");
			break;
		case T_Aggref:
		{
			Aggref *aggref = (Aggref *) expr;
			elog(LOG, "Found Aggref in Expr");
			break;
		}
		default:
			elog(LOG, "Unrecognized Expr tag: %d", nodeTag(expr));
	}
}

void AggrefToString(TargetEntry *aggrefEntry, List *rtable, bool renameAggref,
				char *targetBuf, size_t targetBufSize)
{
	Aggref *aggref;
	ListCell *argCell;
	TargetEntry *arg;
	char *targetStr;
	char copyBuf[TARGET_BUFFER_SIZE];

	aggref = (Aggref *) aggrefEntry->expr;

	if (aggref->aggstar == true)
	{
		targetStr = STAR_COL;
	}
	else
	{
		targetStr = UnparseTargetList(aggref->args, rtable, false);
	}

	if (renameAggref == true)
	{
		if (aggref->aggstar == true)
		{
			elog(LOG, "Copying renamed star column...");
			strcpy(copyBuf, RENAMED_STAR_COL);
			elog(LOG, "Successfully copied renamed star column");
		}
		else
		{
			strncpy(copyBuf, targetStr, TARGET_BUFFER_SIZE);
			ReplaceChars(copyBuf, '.', '_', TARGET_BUFFER_SIZE);
		}

		snprintf(targetBuf, targetBufSize, "%s(%s) AS %s_%s",
					aggrefEntry->resname, targetStr, aggrefEntry->resname,
					copyBuf);
	}
	else
	{
		snprintf(targetBuf, targetBufSize, "%s(%s)", aggrefEntry->resname,
					targetStr);
	}

	if (aggref->aggstar == false)
	{
		pfree(targetStr);
	}

	elog(LOG, "AggrefToString result: %s", targetBuf);
}

void ExprToString(Expr *expr, JoinExpr *join, RangeTblEntry *rte,
				List *rangeTables, char *targetBuf, size_t targetBufSize)
{
	elog(LOG, "ExprToString Expr tag: %d", nodeTag(expr));

	switch (nodeTag(expr))
	{
		case T_Var:
			CreateJoinVarStr(join, (Var *) expr, rte, rangeTables, targetBuf,
								targetBufSize);
			break;
	}

}

bool *TargetEntryToString(TargetEntry *targetEntry, List *rtable,
				bool renameTargets, char *outBuf, size_t outBufSize)
{
	bool success = true;

	switch (nodeTag(targetEntry->expr))
	{
		case T_Var:
		{
			Var *var = (Var *) targetEntry->expr;
			RangeTblEntry *varRte = rt_fetch(var->varno, rtable);
			VarToString(var, rtable, renameTargets, outBuf, outBufSize);
			break;
		}
		case T_Aggref:
			AggrefToString(targetEntry, rtable, true, outBuf, outBufSize);
			break;
		default:
			success = false;
	}

	return success;
}

void VarToString(Var *var, List *rtable, bool renameVar, char *varBuf,
				size_t varBufSize)
{
	RangeTblEntry *varRte;
	char *tableName, *colName, *renamedColName;

	varRte = rt_fetch(var->varno, rtable);
	tableName = varRte->eref->aliasname;

	if (var->varattno > 0)
	{
		colName =
						renamedColName =
										strVal(lfirst(list_nth_cell(varRte->eref->colnames, var->varattno - 1)));
	}
	else
	{
		colName = STAR_COL;
		renamedColName = RENAMED_STAR_COL;
	}

	if (renameVar == true)
	{
		snprintf(varBuf, varBufSize, "%s.%s AS %s_%s", tableName, colName,
					tableName, renamedColName);
	}
	else
	{
		snprintf(varBuf, varBufSize, "%s.%s", tableName, colName);
	}

	elog(LOG, "VarToString result: %s", varBuf);
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

MatView *UnparseQuery(Query *query)
{
	ListCell *listCell;
	TargetEntry *targetEntry;
	FromExpr *from;
	MatView *matView;
	char *targetListStr, *fromClauseStr, *groupClauseStr;

	matView = palloc(sizeof(MatView));
	matView->name = palloc(sizeof(char) * 256);
	snprintf(matView->name, 256, "Matview_%d", rand());

	matView->baseQuery = copyObject(query);

	matView->selectQuery = palloc(sizeof(char) * QUERY_BUFFER_SIZE);
	strcpy(matView->selectQuery, "SELECT ");
	targetListStr = UnparseTargetList(query->targetList, query->rtable, true);
	strncat(matView->selectQuery, targetListStr, QUERY_BUFFER_SIZE);
	pfree(targetListStr);

	strncat(matView->selectQuery, " FROM ", QUERY_BUFFER_SIZE);

	if (query->jointree != NULL)
	{
		from = query->jointree;
//		AnalyzePlanQuals(query->jointree->quals);

		foreach(listCell, from->fromlist)
		{
			UnparseFromExprRecurs(query, listCell, matView->selectQuery);
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
	else
	{
		elog(LOG, "No GROUP clause found");
	}
	if (query->groupingSets != NIL)
	{
		elog(LOG, "Found grouping sets in query");
	}

	elog(LOG, "Constructed full select query: %s", matView->selectQuery);

	return matView;
}

/**
 * Returns the SELECT clause of a select statement.
 * NOTE: Returned pointer must be pfree'd.
 */
char *UnparseTargetList(List *targetList, List *rtable, bool renameTargets)
{
	TargetEntry *targetEntry;
	RangeTblEntry *rte;
	char *selectTargets;
	char targetBuffer[TARGET_BUFFER_SIZE];
	ListCell *listCell;
	int index;

	selectTargets = palloc(sizeof(char) * QUERY_BUFFER_SIZE);
	memset(selectTargets, 0, QUERY_BUFFER_SIZE);
	index = 0;

	if (targetList != NIL)
	{
		foreach(listCell, targetList)
		{
			targetEntry = lfirst_node(TargetEntry, listCell);
			elog(
					LOG, "TargetEntry %s, TE.expr nodeTag: %d, resorigtable=%d, column attribute number=%d",
					targetEntry->resname, nodeTag(targetEntry->expr),
					targetEntry->resorigtbl, targetEntry->resorigcol);
			if (TargetEntryToString(targetEntry, rtable, renameTargets,
									targetBuffer, TARGET_BUFFER_SIZE))
			{
				if (index < targetList->length - 1)
				{
					strncat(targetBuffer, ", ", TARGET_BUFFER_SIZE);
				}
				strncat(selectTargets, targetBuffer, QUERY_BUFFER_SIZE);
			}
			else
			{
				elog(WARNING, "Failed to convert TargetEntry to string");
			}

			index++;
		}
	}
	// Assume SELECT *
	else
	{
		elog(LOG, "Query.targetList was NIL");
		strcpy(selectTargets, "*");
	}

	return selectTargets;
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

void UnparseFromExprRecurs(Query *rootQuery, ListCell *fromExprCell,
				char *selectQuery)
{
	RangeTblEntry *joinRte, *leftRte, *rightRte;
	int selectQueryIndex;
	char *joinBuf, *qualStr;
	Node *node;

	node = lfirst_node(Node, fromExprCell);
	selectQueryIndex = strnlen(selectQuery, QUERY_BUFFER_SIZE);
	joinBuf = palloc(sizeof(char) * QUERY_BUFFER_SIZE);
	memset(joinBuf, 0, QUERY_BUFFER_SIZE);

	elog(LOG, "Unparsing FROM clause nodeTag=%d", nodeTag(node));

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
			UnparseFromExprRecurs(rootQuery, joinExpr->larg, selectQuery);
			UnparseFromExprRecurs(rootQuery, joinExpr->rarg, selectQuery);

			joinRte = rt_fetch(joinExpr->rtindex, rootQuery->rtable);
			leftRte = left_join_table(joinExpr, rootQuery->rtable);
			rightRte = right_join_table(joinExpr, rootQuery->rtable);
			qualStr = AnalyzePlanQuals(joinExpr->quals, leftRte, rightRte,
										joinExpr, rootQuery->rtable);
//			elog(LOG, "Attempting to create join string from Qual string...");

			// TODO: differentiate between first join and following joins
			if (leftRte->rtekind == RTE_JOIN)
			{
				snprintf(joinBuf, QUERY_BUFFER_SIZE, " %s %s ON %s", joinTag,
							rightRte->eref->aliasname, qualStr);
			}
			else
			{
				snprintf(joinBuf, QUERY_BUFFER_SIZE, " %s %s %s ON %s",
							leftRte->eref->aliasname, joinTag,
							rightRte->eref->aliasname, qualStr);
			}

			pfree(qualStr);
			qualStr = NULL;
//			elog(LOG, "Created join string: %s", joinBuf);

			strcat(selectQuery + selectQueryIndex, joinBuf);
//			AnalyzePlanQuals(joinExpr->quals, leftRte, rightRte);
		}
		else if (IsA(node, RangeTblRef))
		{
			RangeTblRef *rtRef = (RangeTblRef *) node;
			RangeTblEntry *rte = rt_fetch(rtRef->rtindex, rootQuery->rtable);

			if (lnext(fromExprCell) != NULL)
			{
				snprintf(joinBuf, QUERY_BUFFER_SIZE, "%s, ",
							rte->eref->aliasname);
			}
			else
			{
				strncpy(joinBuf, rte->eref->aliasname, QUERY_BUFFER_SIZE);
			}

			strcat(selectQuery, joinBuf);
		}
		else if (IsA(node, RangeTblEntry))
		{
			elog(LOG, "Found RangeTblEntry in join analysis");
		}
		else if (IsA(node, FromExpr))
		{
			elog(LOG, "Found FromExpr in recursive join analysis");
		}
	}

	if (joinBuf != NULL)
	{
		pfree(joinBuf);
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
			if (TargetEntryToString(targetEntry, rtable, false, targetBuffer,
									TARGET_BUFFER_SIZE))
			{
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
			}
			else
			{
				elog(
						WARNING, "Failed to convert TargetEntry in GROUP BY clause to string");
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

void AddQuery(Query *query, PlannedStmt *plannedStatement)
{
	MemoryContext oldContext;
	List *matViewQueries;

	CheckInitState();
	oldContext = MemoryContextSwitchTo(AutoMatViewContext);
	queryPlanStatsList = lappend(
					queryPlanStatsList,
					CreateQueryPlanStats(query, plannedStatement));
	UnparseQuery(query);

	// TODO: Set training threshold from postgres properties file
	if (queryPlanStatsList->length > trainingSampleCount)
	{
		isCollectingQueries = false;
		CreateRelevantMatViews();
	}

	MemoryContextSwitchTo(oldContext);
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
		CreateMaterializedView(newMatView->name, newMatView->selectQuery);
		createdMatViews = list_append_unique(createdMatViews, newMatView);
	}

	elog(LOG, "Created %d materialized views based on given query workload",
					createdMatViews != NIL ? createdMatViews->length : 0);
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
	return queries;
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

bool IsCollectingQueries()
{
	return isCollectingQueries;
}

/**
 * Rewrite the given Query to use available materialized views.
 * returns: SQL query string representing rewritten Query object.
 */
char *RewriteQuery(Query *query)
{
	MatView *bestMatViewMatch;

	bestMatViewMatch = GetBestMatViewMatch(query);
	// TODO: rewrite query to use MatView

	return NULL;
}

MatView *GetBestMatViewMatch(Query *query)
{
	List *matchingMatViews;

	matchingMatViews = GetMatchingMatViews(query);
	// TODO: filter returned MatViews to find best match

	return NULL;
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

	return matchingViews;
}

bool DoesQueryMatchMatView(Query *query, MatView *matView)
{
	bool isMatch = false;

	return isMatch;
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

void InspectQuery(Query *query)
{
	MemoryContext oldContext;
	Query *queryCopy;

	CheckInitState();

	oldContext = MemoryContextSwitchTo(AutoMatViewContext);

	elog(LOG, "Copying query object");
	queryCopy = copyObject(query);

	if (queryCopy->commandType == CMD_SELECT)
	{
		elog(LOG, "Found select query in pg_plan_queries");
		PrintQueryInfo(queryCopy);
	}
	else if (queryCopy->commandType == CMD_INSERT
					|| queryCopy->commandType == CMD_UPDATE)
	{
		elog(LOG, "Insert or update found");
	}

	pfree(queryCopy);
	MemoryContextSwitchTo(oldContext);
}

List *
SearchApplicableMatViews(RangeVar *rangeVar)
{
// TODO: Figure out how we will search for applicable materialized views
// to use to rewrite future queries
	MemoryContext oldContext;

	elog(LOG, "Searching for applicable materialized views given RangeVar...");

	if (AutoMatViewContext != NULL)
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

// TODO: Creating materialized views: look into creatas.c's ExecCreateTableAs function
// 		  Given

void AddMatView(IntoClause *into)
{
	MemoryContext oldContext;
	ListCell *cell;
	ListCell *colCell;
	IntoClause *matViewInto;
	IntoClause *intoCopy;
	char *colname;

	CheckInitState();

	oldContext = MemoryContextSwitchTo(AutoMatViewContext);

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

	MemoryContextSwitchTo(oldContext);
}

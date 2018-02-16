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

#define MAX_TABLENAME_SIZE 256
#define MAX_COLNAME_SIZE 256
#define QUERY_BUFFER_SIZE 1024
#define TARGET_BUFFER_SIZE 256

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
static List *queryPlanStatsList = NIL;

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
static void ExprToString(Expr *expr, JoinExpr *join, RangeTblEntry *rte,
				List *rangeTables, char *targetBuf, size_t targetBufSize);

static List *GenerateMatViewPerQuery();
int64 CreateMaterializedView(char *viewName, char *selectQuery);
double GetPlanCost(Plan *plan);

static MatView *UnparseQuery(Query *query);
static RangeTblEntry *FindRte(Oid relid, List *rtable);
static char *UnparseTargetList(Query *query);
void UnparseFromExprRecurs(Query *rootQuery, Node *node, char *selectQuery);

static void CheckInitState();
static void PrintQueryInfo(Query *query);

//static ListCell *FindTableInfo(char *tableName);
//static void AddQuery(Query *query);

void CheckInitState()
{
	if (AutoMatViewContext == NULL)
	{
//		elog(LOG, "Creating AutoMatViewContext");
		// Create new top level context to prevent this from being cleared when current
		// 	 ExecutorContext is deleted.
		AutoMatViewContext = AllocSetContextCreate((MemoryContext) NULL,
													"AutoMatViewContext",
													ALLOCSET_DEFAULT_SIZES
													);
	}
//	if (selectQueryCounts == NULL)
//	{
//		selectQueryCounts = dshash_create();
//	}
//	if (insertQueryCounts == NULL)
//	{
//		insertQueryCounts = dshash_create();
//	}
//	if (updateQueryCounts == NULL)
//	{
//		updateQueryCounts = dshash_create();
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
//			TargetEntry *targetEntry = lfirst_node(TargetEntry,
//					targetEntryCell);
//
//			/* junk columns don't get aliases */
//			if (targetEntry->resjunk)
//			{
//				continue;
//			}
//			elog(LOG, "RTE Target entry: %s", targetEntry->resname);
//			selectStats->columnStats = list_append_unique(
//					selectStats->columnStats,
//					CreateColumnStats(targetEntry->resname));
			selectStats->columnStats = list_append_unique(
							selectStats->columnStats,
							CreateColumnStats(strVal(lfirst(targetEntryCell))));
		}

//		elog(
//				LOG, "Created SelectQueryStats with %d columns", selectStats->columnStats->length);
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
//		elog(LOG, "Copying table name into TableQueryStats...");
		stats->tableName = pnstrdup(tableName, MAX_TABLENAME_SIZE);
//		elog(LOG, "Finished copying table name into TableQueryStats");
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

//	foreach(colStatsCell, stats->selectStats->columnStats)
//	{
//		colStats = lfirst_node(ColumnStats, colStatsCell);
//		elog(LOG, "<ColumnStats colName=%s selectCount=%d>",
//		colStats->colName, colStats->selectCounts);
//	}
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

	elog(LOG, "Var str for rtekind=%d %s",rte->rtekind, varStrBuf);

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

void ExprToString(Expr *expr, JoinExpr *join, RangeTblEntry *rte,
				List *rangeTables, char *targetBuf, size_t targetBufSize)
{
	switch (nodeTag(expr))
	{
		case T_Var:
			CreateJoinVarStr(join, (Var *) expr, rte, rangeTables, targetBuf,
								targetBufSize);
			break;
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
		elog(
				LOG, "Plan node type=%d, cost=%f, tuples=%f, tupleWidth=%d",
				nodeTag(plan), plan->total_cost, plan->plan_rows, plan->plan_width);
		switch (nodeTag(plan))
		{
			case T_Result:
				elog(LOG, "Result plan node found");
				planTag = "Result";
				break;
			case T_SeqScan: // 18
				planTag = "SequenceScan";
				break;
			case T_Join:
			case T_MergeJoin:
			case T_HashJoin:
				planTag = "Join";
				elog(LOG, "Join plan node found");
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

			elog(LOG, "Plan tag=%s targetEntry: %s from tableId=%u",
			// Use targetEntry->resorigtbl to get joined tables
			// TODO: how to figure out the join conditions?
					planTag, targetEntry->resname, targetEntry->resorigtbl);
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

	matView = palloc(sizeof(MatView));
	matView->name = palloc(sizeof(char) * 256);

	matView->selectQuery = UnparseTargetList(query);
	if (query->jointree != NULL)
	{
		from = query->jointree;
//		AnalyzePlanQuals(query->jointree->quals);
		strncat(matView->selectQuery, "FROM", QUERY_BUFFER_SIZE);

		foreach(listCell, from->fromlist)
		{
			UnparseFromExprRecurs(query, lfirst(listCell), matView->selectQuery);
		}

		elog(LOG, "Constructed full select query from Join tree: %s", matView->selectQuery);
	}

	return matView;
}

/**
 * Returns the SELECT clause of a select statement.
 * NOTE: Returned pointer must be pfree'd.
 */
char *UnparseTargetList(Query *query)
{
	TargetEntry *targetEntry;
	RangeTblEntry *rte;
	char *selectTargets;
	char targetBuffer[TARGET_BUFFER_SIZE];
	ListCell *listCell;
	int index;

	selectTargets = palloc(sizeof(char) * QUERY_BUFFER_SIZE);
	strcpy(selectTargets, "SELECT ");
	index = 0;

	if (query->targetList != NIL)
	{
		foreach(listCell, query->targetList)
		{
			targetEntry = lfirst_node(TargetEntry, listCell);
			rte = FindRte(targetEntry->resorigtbl, query->rtable);

			if (targetEntry->resname != NULL && rte != NULL && rte->eref->aliasname != NULL)
			{
				if (index < query->targetList->length - 1)
				{
					snprintf(targetBuffer, TARGET_BUFFER_SIZE, "%s.%s, ",
							 rte->eref->aliasname, targetEntry->resname);
				}
				else
				{
					snprintf(targetBuffer, TARGET_BUFFER_SIZE, "%s.%s ",
							 rte->eref->aliasname, targetEntry->resname);
				}

				strncat(selectTargets, targetBuffer, QUERY_BUFFER_SIZE);
			}

			index++;
		}
	}
	// Assume SELECT *
	else
	{
		strcpy(selectTargets, "* ");
	}

	return selectTargets;
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

void UnparseFromExprRecurs(Query *rootQuery, Node *node, char *selectQuery)
{
	RangeTblEntry *joinRte, *leftRte, *rightRte;
	int selectQueryIndex;
	char *joinBuf, *qualStr;

	selectQueryIndex = strnlen(selectQuery, QUERY_BUFFER_SIZE);
	joinBuf = palloc(sizeof(char) * QUERY_BUFFER_SIZE);
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
			UnparseFromExprRecurs(rootQuery, joinExpr->larg, selectQuery);
			UnparseFromExprRecurs(rootQuery, joinExpr->rarg, selectQuery);

			joinRte = rt_fetch(joinExpr->rtindex, rootQuery->rtable);
			leftRte = left_join_table(joinExpr, rootQuery->rtable);
			rightRte = right_join_table(joinExpr, rootQuery->rtable);
			qualStr = AnalyzePlanQuals(joinExpr->quals, leftRte, rightRte,
										joinExpr, rootQuery->rtable);
			elog(LOG, "Attempting to create join string from Qual string...");

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
			elog(LOG, "Created join string: %s", joinBuf);

			strcat(selectQuery + selectQueryIndex, joinBuf);
//			AnalyzePlanQuals(joinExpr->quals, leftRte, rightRte);
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

void AddQuery(Query *query, PlannedStmt *plannedStatement)
{
//	ListCell *rteCell;
//	ListCell *colCell;
//	ListCell *joinVarCell;
//	RangeTblEntry *rte;
	MemoryContext oldContext;
//	Query *queryCopy;
//	PlannedStmt *statementCopy;
//	TableQueryStats *stats;

	CheckInitState();
	oldContext = MemoryContextSwitchTo(AutoMatViewContext);
//	queryCopy = copyObject(query);
//	statementCopy = copyObject(plannedStatement);
//	plannedQueries = lappend(plannedQueries, statementCopy);
	queryPlanStatsList = lappend(
					queryPlanStatsList,
					CreateQueryPlanStats(query, plannedStatement));
	UnparseQuery(query);

	// TODO: Set training threshold from postgres properties file
	if (queryPlanStatsList->length > 5)
	{

	}

//	AnalyzeJoins(queryCopy);
//	Plan p;

	/*
	 //	elog(LOG, "%d total query plans stored", plannedQueries->length);

	 if (statementCopy->planTree != NULL)
	 {
	 elog(
	 LOG, "Total plan cost: %f", GetPlanCost(plannedStatement->planTree));
	 //		elog(
	 //				LOG, "Total cost of query plan selecting estimated %f rows: %f; plan type: %s",
	 //				statementCopy->planTree->plan_rows,
	 //				statementCopy->planTree->total_cost,
	 //				nodeToString(statementCopy->planTree));
	 }
	 else
	 {
	 elog(WARNING, "No planTree found from query plan");
	 }

	 foreach(rteCell, statementCopy->rtable)
	 {
	 rte = (RangeTblEntry *) lfirst(rteCell);
	 //		if (rte->eref)
	 //		{
	 //			elog(LOG, "Select RTE relation alias name with %d columns: %s",
	 //				 rte->eref->colnames->length, rte->eref->aliasname);
	 //
	 //			foreach(colCell, rte->eref->colnames)
	 //			{
	 //				elog(LOG, "Select RTE col name: %s",
	 //				strVal(lfirst(colCell)));
	 //			}
	 //		}

	 switch (rte->rtekind)
	 {
	 case RTE_RELATION:
	 stats = FindTableQueryStats(rte->eref->aliasname);

	 if (stats == NULL)
	 {
	 //				elog(
	 //				LOG, "Creating new query stats for %s", rte->eref->aliasname);
	 stats = CreateTableQueryStats(rte->eref->aliasname,
	 // RangeTblEntry.eref.colnames contains list of all column names every time
	 rte->eref->colnames);
	 queryStats = list_append_unique(queryStats, stats);
	 }

	 switch (statementCopy->commandType)
	 {
	 case CMD_SELECT:
	 //				elog(LOG, "Analyzing select query");
	 stats->selectCounts++;
	 // TODO: Can we view the currently selected columns from a PlannedStmt?
	 // Query.targetList contains only currently selected columns
	 //				UpdateSelectQueryStats(stats->selectStats, query->targetList);
	 break;
	 case CMD_INSERT:
	 //				elog(LOG, "Analyzing insert query");
	 break;
	 case CMD_UPDATE:
	 stats->updateCounts++;
	 //				elog(LOG, "Analyzing update query");
	 break;
	 case CMD_DELETE:
	 stats->deleteCounts++;
	 //				elog(LOG, "Analyzing delete query");
	 break;
	 default:
	 elog(
	 WARNING, "Couldn't recognize given query command type");
	 }

	 //				PrintTableQueryStats(stats);
	 break;
	 case RTE_JOIN:
	 //				if (rte->jointype != NULL)
	 //				{
	 //					switch (rte->jointype)
	 //					{
	 //						case JOIN_ANTI:
	 //							elog(LOG, "Found anti join");
	 //							break;
	 //						case JOIN_UNIQUE_INNER:
	 //							elog(LOG, "Found unique inner join");
	 //							break;
	 //						case JOIN_UNIQUE_OUTER:
	 //							elog(LOG, "Found unique outer join");
	 //							break;
	 //						case JOIN_INNER:
	 //							elog(LOG, "Found inner join");
	 //							break;
	 //						case JOIN_FULL:
	 //							elog(LOG, "Found full join");
	 //							break;
	 //						case JOIN_LEFT:
	 //							elog(LOG, "Found left join");
	 //							break;
	 //						case JOIN_RIGHT:
	 //							elog(LOG, "Found right join");
	 //							break;
	 //						case JOIN_SEMI:
	 //							elog(LOG, "Found semi join");
	 //							break;
	 //						default:
	 //							elog(WARNING, "Couldn't recognize given JoinType");
	 //					}
	 //				}
	 //				else
	 //				{
	 //					// Was RTE_JOIN, but jointype == NULL. Assume inner join
	 //					elog(
	 //							LOG, "RTE_JOIN found, but jointype was NULL; assuming inner join");
	 //				}
	 //
	 //				if (rte->joinaliasvars != NIL)
	 //				{
	 //					elog(LOG, "Number of joinaliasvars: %d", rte->joinaliasvars->length);
	 //					foreach(joinVarCell, rte->joinaliasvars)
	 //					{
	 //						if (IsA(joinVarCell, Var))
	 //						{
	 //							elog(LOG, "Found Var type in joinaliasvars");
	 //						}
	 //					}
	 //				}
	 //				else
	 //				{
	 //					elog(LOG, "joinaliasvars was NIL");
	 //				}

	 break;
	 }
	 }
	 */

	MemoryContextSwitchTo(oldContext);
}

/**
 * Generates a MatView per recorded query.
 *
 * returns: List (of MatView)
 */
List *GenerateMatViewPerQuery()
{
	ListCell *queryPlanCell;
	QueryPlanStats *queryPlan;
	Query *query;
	PlannedStmt *plannedStmt;
	char selectStmtBuf[1000], viewNameBuf[256];

	foreach(queryPlanCell, queryPlanStatsList)
	{
		queryPlan = lfirst_node(QueryPlanStats, queryPlanCell);
		query = queryPlan->query;
	}

	return NIL;
}

int64 CreateMaterializedView(char *viewName, char *selectQuery)
{
	int ret;
	int64 processed;
	char resultQuery[1000];

	sprintf(resultQuery, "CREATE MATERIALIZED VIEW %s AS %s", viewName,
			selectQuery);
	elog(LOG, "CreateMaterializedView executing: %s", resultQuery);

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

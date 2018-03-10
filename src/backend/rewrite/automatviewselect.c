/*
 * automatviewselect.c
 *
 *  Created on: Dec 13, 2017
 *      Author: Brandon Cooper
 */

#include "rewrite/automatviewselect.h"

#include "rewrite/automatviewselect_match.h"
#include "rewrite/automatviewselect_unparse.h"
#include "rewrite/automatviewselect_rewrite.h"

#include "c.h"
#include "catalog/pg_type.h"
#include "utils/memutils.h"
#include "nodes/value.h"
#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"
#include "parser/parsetree.h"
#include "lib/dshash.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/guc_tables.h"

//typedef struct MatView
//{
//    char *name;
//    char *selectQuery;
//    Query *baseQuery; // Query object which this MatView is based on
//    List *renamedTargetList; // List (of TargetEntry) with renamed TargetEntries
//    List *renamedRtable;
//} MatView;

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

// MatView selection functions
static List *GetMatchingMatViews(Query *query);

// MatView generation operations
int64 CreateMaterializedView(char *viewName, char *selectQuery);
static void CreateRelevantMatViews();
static void PopulateMatViewRenamedRTable(MatView *matView);
static List *GetCostlyQueries(double costCutoffRatio);
static List *GenerateInterestingQueries(List *queryPlanStats);
static List *PruneQueries(List *queries);

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
            ALLOCSET_DEFAULT_SIZES);
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

void AddQuery(Query *query, PlannedStmt *plannedStatement)
{
    MemoryContext oldContext;
    List *matViewQueries;

    CheckInitState();

    if (CanQueryBeOptimized(query))
    {
        oldContext = MemoryContextSwitchTo(AutoMatViewContext);
        queryPlanStatsList = lappend(queryPlanStatsList,
            CreateQueryPlanStats(query, plannedStatement));

        // TODO: Set training threshold from postgres properties file
        if (queryPlanStatsList->length > trainingSampleCount)
        {
            isCollectingQueries = false;
            CreateRelevantMatViews();
        }

        MemoryContextSwitchTo(oldContext);
    }
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
        // We don't want the WHERE clause included in created materialized views
        newMatView = UnparseQuery(query, false);
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

    matView->renamedRtable = list_make1(
        copyObject(linitial(matView->baseQuery->rtable)));
    renamedRte = linitial(matView->renamedRtable);
    renamedRte->eref->aliasname = pstrdup(matView->name);
    renamedRte->relid = 0;

    foreach(targetEntryCell, matView->renamedTargetList)
    {
        targetEntry = lfirst_node(TargetEntry, targetEntryCell);
        newColnames = list_append_unique(newColnames,
            makeString(pstrdup(targetEntry->resname)));
        SetVarattno(targetEntry->expr, varattno++);
    }

    list_free(renamedRte->eref->colnames);
    renamedRte->eref->colnames = newColnames;

    char targetListBuf[QUERY_BUFFER_SIZE];
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
 * Get the best MatView match which will be used to rewrite the given Query.
 * returns: a MatView if there was a match, or NULL otherwise.
 */
MatView *GetBestMatViewMatch(Query *query)
{
    List *matchingMatViews;
    MatView *bestMatch = NULL;

    if (CanQueryBeOptimized(query))
    {
        elog(LOG, "GetBestMatViewMatch: finding best matching view");
        matchingMatViews = GetMatchingMatViews(query);
        // TODO: filter returned MatViews to find best match

        if (matchingMatViews != NIL && matchingMatViews->length > 0)
        {
            // Choose the first match for now
            elog(LOG, "GetBestMatViewMatch choosing first MatView");
            bestMatch = (MatView *) linitial(matchingMatViews);
            elog(LOG, "GetBestMatViewMatch chose first MatView: %p", bestMatch);
        }
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

        if (DoesQueryMatchMatView(query, matView->baseQuery))
        {
            matchingViews = list_append_unique(matchingViews, matView);
        }
    }

    elog(LOG, "Found %d matching materialized views for Query",
    matchingViews != NIL ? matchingViews->length : 0);

    return matchingViews;
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


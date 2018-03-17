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
#include "access/xact.h"

#define AUTOMATVIEW_SCHEMA "automatview"
#define STORED_QUERIES_TABLE "stored_queries"
#define MATVIEWS_TABLE "matviews"

#define INVALID_ID 0
#define MAX_QUERY_RETRY_COUNT 3

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC
;
#endif

//typedef struct MatView
//{
//    char *name;
//    char *selectQuery;
//    Query *baseQuery; // Query object which this MatView is based on
//    List *renamedTargetList; // List (of TargetEntry) with renamed TargetEntries
//    List *renamedRtable;
//} MatView;

typedef struct OutstandingQuery
{
    StringInfo query;
    int tupleCount; /* Limit on number of returned tuples. 0 for unlimited */
    int retryCount; /* How many times have we tried to execute this query? */
    bool readOnly; /* execute the query in read-only mode? */
    void (*callback)(SPITupleTable *, int64);
} OutstandingQuery;

typedef struct QueryPlanStats
{
    unsigned long storedQueryId;
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
static List *outstandingQueries = NIL;

//static dshash_table *selectQueryCounts = NULL;
static List *queryStats = NIL;
static List *plannedQueries = NIL;
static List *createdMatViews = NIL;
static List *queryPlanStatsList = NIL;
static unsigned int storedQueryCount = 0;
static bool isCollectingQueries = true;

static bool isAutomatviewsReady = false;
static bool isTransactionStarted = false;

// Have we loaded the trainingSampleCount from postgresql.conf?
static bool isTrainingSampleCountLoaded = false;
static int trainingSampleCount = 1;

// Internal state initialization functions
static void PopulateUserTables();
static void HandlePopulateUserTables(SPITupleTable *tuptable,
    int64 processedCount);
static void PopulateStoredQueryCount();
static void HandlePopulateStoredQueryCount(SPITupleTable *tuptable,
    int64 processedCount);
static void PersistQuery(char *query);
static void PopulateStoredQueries();
static void HandlePopulateStoredQueries(SPITupleTable *tuptable,
    int64 processedCount);
static void PersistMatView(MatView *matView);
static void PopulateMatViews();
static void HandlePopulateMatViews(SPITupleTable *tuptable,
    int64 processedCount);

// Query execution functions
static OutstandingQuery *CreateOutstandingQuery(char *query, int tupleCount,
    bool readOnly, void (*callback)(SPITupleTable *, int64));
static void AddOutstandingQuery(OutstandingQuery *query);

// Query stats functions
static QueryPlanStats *CreateQueryPlanStats(Query *query, PlannedStmt *plan);
static void FreeQueryPlanStats(QueryPlanStats *queryStats);
static QueryPlanStats *QueryStringToQueryPlanStats(char *query);
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
static void StartAMVTransaction();
static void FinishAMVTransaction();
static int64 ExecuteOutstandingQuery(OutstandingQuery *query);
static int64 CreateMaterializedView(char *viewName, char *selectQuery);
static void CreateRelevantMatViews();
static void PopulateMatViewRenamedRTable(MatView *matView);
static List *GetCostlyQueries(double costCutoffRatio);
static List *GenerateInterestingQueries(List *queryPlanStats);
static List *PruneQueries(List *queries);

void InitializeAutomatviewModule()
{
    if (AutoMatViewContext == NULL)
    {
        elog(LOG, "Initializing Automatview module...");
        if (AutoMatViewContext == NULL)
        {
            AutoMatViewContext = AllocSetContextCreate((MemoryContext) NULL,
                "AutoMatViewContext",
                ALLOCSET_DEFAULT_SIZES);
        }

        MemoryContext oldContext = SwitchToAutoMatViewContext();
        PopulateUserTables();
        PopulateMatViews();
        elog(LOG, "InitializeAutomatviewModule: %d outstanding queries", list_length(outstandingQueries));
        MemoryContextSwitchTo(oldContext);
    }
}

void PopulateUserTables()
{
    static char *getTablesQuery =
        "SELECT n.nspname, c.relname, cast(c.oid as varchar(32)) "
            "FROM pg_catalog.pg_class c "
            "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
            "WHERE c.relkind IN ('r','p','') "
            "AND n.nspname <> 'pg_catalog' "
            "AND n.nspname <> 'information_schema' "
            "AND n.nspname !~ '^pg_toast' "
            "AND pg_catalog.pg_table_is_visible(c.oid) "
            "ORDER BY 1,2;";
    AddOutstandingQuery(
        CreateOutstandingQuery(getTablesQuery, 0, true,
            &HandlePopulateUserTables));
}

void HandlePopulateUserTables(SPITupleTable *tuptable, int64 processedCount)
{
    if (tuptable != NULL)
    {
        TupleDesc tupDesc = tuptable->tupdesc;
        char tableNameBuf[8192];
        char schemaBuf[8192];
        char relidBuf[1024];
        uint64 tupleIndex;
        MemoryContext oldContext = SwitchToAutoMatViewContext();
        ClearUserTables();

        for (tupleIndex = 0; tupleIndex < processedCount; tupleIndex++)
        {
            HeapTuple tuple = tuptable->vals[tupleIndex];
            char *schemaValue = SPI_getvalue(tuple, tupDesc, 1);
            char *tableNameValue = SPI_getvalue(tuple, tupDesc, 2);
            char *relidValue = SPI_getvalue(tuple, tupDesc, 3);
            if (schemaValue != NULL)
            {
                strcpy(schemaBuf, schemaValue);
                pfree(schemaValue);
            }
            else
            {
                elog(ERROR, "HandlePopulateUserTables received NULL "
                "schema column");
            }
            if (tableNameValue != NULL)
            {
                strcpy(tableNameBuf, tableNameValue);
                pfree(tableNameValue);
            }
            else
            {
                elog(ERROR, "HandlePopulateUserTables received NULL "
                "table name column");
            }
            if (relidValue != NULL)
            {
                strcpy(relidBuf, relidValue);
                pfree(relidValue);
            }
            else
            {
                elog(ERROR, "HandlePopulateUserTables received NULL "
                "relid column");
            }
            if (schemaValue != NULL && tableNameValue != NULL
                && relidValue != NULL)
            {
                MemoryContext spiContext = SwitchToAutoMatViewContext();
                AddUserTable(strtol(relidBuf, relidBuf + strlen(relidBuf), 10),
                    schemaBuf, tableNameBuf);
                MemoryContextSwitchTo(spiContext);
            }
        }

        elog(LOG, "Automatviews is now ready to collect queries");
        MemoryContextSwitchTo(oldContext);
    }
}

void PopulateStoredQueryCount()
{
    static char *query = "SELECT count(*) from %s.%s;";
    char queryBuf[QUERY_BUFFER_SIZE];
    snprintf(queryBuf, QUERY_BUFFER_SIZE, query,
    AUTOMATVIEW_SCHEMA, STORED_QUERIES_TABLE);
    elog(LOG, "PopulateStoredQueryCount: adding query=%s", queryBuf);
    AddOutstandingQuery(
        CreateOutstandingQuery(queryBuf, 0, true,
            &HandlePopulateStoredQueryCount));
}

void HandlePopulateStoredQueryCount(SPITupleTable *tuptable,
    int64 processedCount)
{
    if (tuptable != NULL)
    {
        TupleDesc tupDesc = tuptable->tupdesc;

        if (processedCount == 1)
        {
            char *queryCountValue = SPI_getvalue(tuptable->vals[0], tupDesc, 1);

            if (queryCountValue != NULL)
            {
                elog(
                LOG, "HandlePopulateStoredQueryCount: current MemoryContext=%s",
                CurrentMemoryContext->name);
                storedQueryCount = strtol(queryCountValue,
                    queryCountValue + strlen(queryCountValue), 10);
                pfree(queryCountValue);
                elog(
                LOG, "HandlePopulateStoredQueryCount: found %d stored queries",
                storedQueryCount);
            }
            else
            {
                elog(ERROR, "HandlePopulateStoredQueryCount retrieved "
                "NULL stored query count value");
            }
            if (storedQueryCount >= trainingSampleCount)
            {
                MemoryContext oldContext = SwitchToAutoMatViewContext();
                PopulateStoredQueries();
                MemoryContextSwitchTo(oldContext);
            }

            isAutomatviewsReady = true;
        }
        else
        {
            elog(
                ERROR, "HandlePopulateStoredQueryCount expected only one returned tuple");
        }
    }
}

void PersistQuery(char *query)
{
    static char *insertQuery = "INSERT INTO %s.%s(query_string) VALUES ('%s');";
    char queryBuf[QUERY_BUFFER_SIZE];
    snprintf(queryBuf, QUERY_BUFFER_SIZE, insertQuery,
    AUTOMATVIEW_SCHEMA, STORED_QUERIES_TABLE, query);
    AddOutstandingQuery(CreateOutstandingQuery(queryBuf, 0, false, NULL));
    elog(LOG, "PersistQuery: added outstanding query %s", queryBuf);
    storedQueryCount++;
}

void PopulateStoredQueries()
{
    static char *query =
        "SELECT id, query_string from %s.%s WHERE id NOT IN (SELECT stored_query_id from %s.%s);";
    char queryBuf[QUERY_BUFFER_SIZE];
    snprintf(queryBuf, QUERY_BUFFER_SIZE, query, AUTOMATVIEW_SCHEMA,
    STORED_QUERIES_TABLE, AUTOMATVIEW_SCHEMA, MATVIEWS_TABLE);
    elog(LOG, "PopulateStoredQueries: executing %s", queryBuf);
    AddOutstandingQuery(
        CreateOutstandingQuery(queryBuf, 0, true,
            &HandlePopulateStoredQueries));
}

void HandlePopulateStoredQueries(SPITupleTable *tuptable, int64 processedCount)
{
    elog(
        LOG, "HandlePopulateStoredQueries called with processedCount=%d", processedCount);
    if (tuptable != NULL)
    {
        MatView *matView;
        HeapTuple tuple;
        TupleDesc tupDesc = tuptable->tupdesc;
        MemoryContext oldContext = SwitchToAutoMatViewContext();

        for (uint64 tupleIndex = 0; tupleIndex < processedCount; tupleIndex++)
        {
            tuple = tuptable->vals[tupleIndex];
            char *storedQueryIdValue = SPI_getvalue(tuple, tupDesc, 1);
            char *queryValue = SPI_getvalue(tuple, tupDesc, 2);

            if (queryValue != NULL)
            {
                elog(
                    LOG, "HandlePopulateStoredQueries: retrieved query=%s", queryValue);
                QueryPlanStats *queryStats = QueryStringToQueryPlanStats(
                    queryValue);
                elog(
                    LOG, "HandlePopulateStoredQueries: created QueryPlanStats with query.rtable.length=%d",
                    list_length(queryStats->query->rtable));
                pfree(queryValue);

                if (queryStats != NULL && storedQueryIdValue != NULL)
                {
                    elog(
                        LOG, "HandlePopulateStoredQueries: setting storedQueryId...");
                    queryStats->storedQueryId = strtol(storedQueryIdValue,
                        storedQueryIdValue + strlen(storedQueryIdValue), 10);
                    elog(
                        LOG, "HandlePopulateStoredQueries: appending QueryPlanStats to list...");
                    queryPlanStatsList = lappend(queryPlanStatsList,
                        queryStats);
                    pfree(storedQueryIdValue);
                    elog(
                        LOG, "HandlePopulateStoredQueries: finished adding new QueryPlanStats");
                }
                else
                {
                    elog(
                        ERROR, "HandlePopulateStoredQueries: returned QueryPlanStats was NULL "
                        "or retrieved automatview.stored_query.id was NULL");
                }
            }
        }
        elog(
        LOG, "QueryPlanStatsList has %d entries. Now in MemContext=%s",
        list_length(queryPlanStatsList), CurrentMemoryContext->name);

        isCollectingQueries = false;
        CreateRelevantMatViews();
        MemoryContextSwitchTo(oldContext);
    }
}

void PersistMatView(MatView *matView)
{
    static char *query =
        "INSERT INTO %s.%s(name, stored_query_id) VALUES ('%s', %d);";

    elog(LOG, "PersistMatView called...");

    if (matView != NULL && matView->name != NULL
        && matView->storedQueryId != INVALID_ID)
    {
        char queryBuf[QUERY_BUFFER_SIZE];
        snprintf(queryBuf, QUERY_BUFFER_SIZE, query, AUTOMATVIEW_SCHEMA,
        MATVIEWS_TABLE, matView->name, matView->storedQueryId);
        elog(LOG, "PersistMatView: adding query=%s", queryBuf);
        AddOutstandingQuery(CreateOutstandingQuery(queryBuf, 0, false, NULL));
    }
    else
    {
        elog(ERROR, "PersistMatView received invalid MatView");
    }
}

void PopulateMatViews()
{
    static char *query = "SELECT name, query_string from %s.%s JOIN %s.%s ON "
        "%s.%s.stored_query_id = %s.%s.id";
    char queryBuf[QUERY_BUFFER_SIZE];
    snprintf(queryBuf, QUERY_BUFFER_SIZE, query, AUTOMATVIEW_SCHEMA,
    MATVIEWS_TABLE, AUTOMATVIEW_SCHEMA, STORED_QUERIES_TABLE,
    AUTOMATVIEW_SCHEMA, MATVIEWS_TABLE, AUTOMATVIEW_SCHEMA,
    STORED_QUERIES_TABLE);
    elog(LOG, "PopulateMatViews: executing %s", queryBuf);
    AddOutstandingQuery(
        CreateOutstandingQuery(queryBuf, 0, true, &HandlePopulateMatViews));
}

void HandlePopulateMatViews(SPITupleTable *tuptable, int64 processedCount)
{
    elog(LOG, "HandlePopulateMatViews called...");
    if (tuptable != NULL)
    {
        HeapTuple tuple;
        TupleDesc tupDesc = tuptable->tupdesc;
        char matViewNameBuf[MAX_TABLENAME_SIZE];
        char queryBuf[QUERY_BUFFER_SIZE];
        char *nameValue, *queryValue;
        MemoryContext oldContext = SwitchToAutoMatViewContext();

        if (processedCount > 0)
        {
            if (createdMatViews != NIL)
            {
                list_free(createdMatViews);
            }

            createdMatViews = NIL;

            for (uint64 tupleIndex = 0; tupleIndex < processedCount;
                tupleIndex++)
            {
                tuple = tuptable->vals[tupleIndex];
                nameValue = SPI_getvalue(tuple, tupDesc, 1);
                queryValue = SPI_getvalue(tuple, tupDesc, 2);

                if (nameValue != NULL)
                {
                    strncpy(matViewNameBuf, nameValue,
                    MAX_TABLENAME_SIZE);
                    pfree(nameValue);
                }
                else
                {
                    elog(
                    ERROR, "HandlePopulateMatViews retrieved NULL name column");
                }
                if (queryValue != NULL)
                {
                    strncpy(queryBuf, queryValue,
                    QUERY_BUFFER_SIZE);
                    pfree(queryValue);
                }
                else
                {
                    elog(
                        ERROR, "HandlePopulateMatViews retrieved NULL query_string column");
                }
                if (nameValue != NULL && queryValue != NULL)
                {
                    QueryPlanStats *queryStats = QueryStringToQueryPlanStats(
                        queryBuf);

                    if (queryStats != NULL)
                    {
                        elog(
                            LOG, "HandlePopulateMatViews: unparsing query from MemoryContext=%s",
                            CurrentMemoryContext->name);
                        MemoryContext spiContext = SwitchToAutoMatViewContext();
                        MatView *matView = UnparseQuery(queryStats->query,
                        false);

                        if (matView != NULL && matView->name != NULL)
                        {
                            strncpy(matView->name, matViewNameBuf,
                            MAX_TABLENAME_SIZE);
                            createdMatViews = lappend(createdMatViews, matView);
                        }
                        FreeQueryPlanStats(queryStats);
                        MemoryContextSwitchTo(spiContext);
                    }
                    else
                    {
                        elog(
                            ERROR, "HandlePopulateMatViews: returned QueryPlanStats "
                            "was NULL");
                    }
                }
            }

            isCollectingQueries = false;
            isAutomatviewsReady = true;
            elog(LOG, "HandlePopulateMatViews: finished populated matViews "
            "list with %d MatViews", processedCount);
        }
        else
        {
            elog(LOG, "HandlePopulateMatViews: found no existing MatViews. "
            "Populating storedQueryCount...");
            PopulateStoredQueryCount();
        }

        MemoryContextSwitchTo(oldContext);
    }
    else
    {
        elog(ERROR, "HandlePopulateMatViews found NULL SPITupleTable");
    }
}

OutstandingQuery *CreateOutstandingQuery(char *query, int tupleCount,
    bool readOnly, void (*callback)(SPITupleTable *, int64))
{
    OutstandingQuery *outstandingQuery = palloc(sizeof(OutstandingQuery));
    outstandingQuery->query = makeStringInfo();
    appendStringInfo(outstandingQuery->query, query);
    outstandingQuery->tupleCount = tupleCount;
    outstandingQuery->retryCount = 0;
    outstandingQuery->readOnly = readOnly;
    outstandingQuery->callback = callback;

    return outstandingQuery;
}

/**
 * Add an outstanding query to be executed later.
 *
 * NOTE: caller is responsible for palloc'ing query.
 */
void AddOutstandingQuery(OutstandingQuery *query)
{
    outstandingQueries = lappend(outstandingQueries, query);
}

/**
 * Execute the first outstanding query.
 * returns: true if a query was executed, false otherwise.
 */
bool ExecuteFirstOutstandingQuery()
{
    ListCell *outstandingQueryCell;
    OutstandingQuery *query;
    bool executedQuery = false;
    MemoryContext oldContext = SwitchToAutoMatViewContext();

    elog(
        LOG, "ExecuteFirstOutstandingQuery called with %d queries", list_length(outstandingQueries));
    if (list_length(outstandingQueries) > 0)
    {
        foreach(outstandingQueryCell, outstandingQueries)
        {
            query = (OutstandingQuery *) lfirst(outstandingQueryCell);

            if (query != NULL && query->retryCount < MAX_QUERY_RETRY_COUNT)
            {
                elog(
                    LOG, "ExecuteFirstOustandingQuery: executing %s", query->query->data);
                query->retryCount++;
                executedQuery = ExecuteOutstandingQuery(query) >= 0;

                if (executedQuery || query->retryCount >= MAX_QUERY_RETRY_COUNT)
                {
                    outstandingQueries = list_delete_first(outstandingQueries);
                }
                if (executedQuery && !isAutomatviewsReady)
                {
                    elog(LOG, "Automatview is ready to collect queries");
                }
            }
        }
    }

    MemoryContextSwitchTo(oldContext);

    return executedQuery;
}

MemoryContext SwitchToAutoMatViewContext()
{
    MemoryContextStats(AutoMatViewContext);
    return MemoryContextSwitchTo(AutoMatViewContext);
}

QueryPlanStats *QueryStringToQueryPlanStats(char *query)
{
    List *querytree_list;
    ListCell *parsetree_item, *queryList;
    QueryPlanStats *queryStats = NULL;
    MemoryContext oldContext = MemoryContextSwitchTo(MessageContext);
    List *parsetree_list = pg_parse_query(query);

    foreach(parsetree_item, parsetree_list)
    {
        RawStmt *parsetree = lfirst_node(RawStmt, parsetree_item);
        querytree_list = pg_analyze_and_rewrite(parsetree, query,
        NULL, 0, NULL);

        if (list_length(querytree_list) == 1)
        {
            Query *query = (Query *) linitial(querytree_list);
            PlannedStmt *plan = pg_plan_query(query,
            CURSOR_OPT_PARALLEL_OK, NULL);
            queryStats = CreateQueryPlanStats(query, plan);
            pfree(query);
            pfree(plan);
        }
        else
        {
            elog(ERROR, "QueryStringToQueryPlanStats expected only one query");
        }
    }

    MemoryContextSwitchTo(oldContext);
    return queryStats;
}

/**
 * NOTE: This will always be allocated in AutoMatViewContext.
 */
QueryPlanStats *CreateQueryPlanStats(Query *query, PlannedStmt *plan)
{
    QueryPlanStats *stats;
    MemoryContext oldContext = SwitchToAutoMatViewContext();

    stats = NULL;

    if (query != NULL)
    {
        stats = palloc(sizeof(QueryPlanStats));
        stats->query = copyObject(query);
//        stats->planCost = planCost;
        stats->plan = copyObject(plan);
    }

    MemoryContextSwitchTo(oldContext);

    return stats;
}

void FreeQueryPlanStats(QueryPlanStats *queryStats)
{
    elog(LOG, "FreeQueryPlanStats called...");
    if (queryStats != NULL)
    {
        if (queryStats->query != NULL)
        {
            pfree(queryStats->query);
        }
        if (queryStats->plan != NULL)
        {
            pfree(queryStats->plan);
        }
        pfree(queryStats);
    }
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

    if (queryStats != NIL)
    {
        foreach(statsCell, queryStats)
        {
            curStats = (TableQueryStats *) lfirst(statsCell);
            if (strcmp(curStats->tableName, tableName) == 0)
            {
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

// TODO: Once queryString is persisted, don't need PlannedStmt anymore
void AddQuery(char *queryString, Query *query, PlannedStmt *plannedStatement)
{
    MemoryContext oldContext;
    List *matViewQueries;

    elog(LOG, "AddQuery called...");
    oldContext = MemoryContextSwitchTo(AutoMatViewContext);

    if (IsQueryForUserTables(query) && CanQueryBeOptimized(query))
    {
        if (plannedStatement != NULL && plannedStatement->planTree != NULL)
        {
            PersistQuery(queryString);
        }
        else
        {
            elog(
            ERROR, "AddQuery: failed to add query to list of stored queries");
        }

        // TODO: Set training threshold from postgres properties file
        if (storedQueryCount >= trainingSampleCount)
        {
            PopulateStoredQueries();
        }
        elog(LOG, "AddQuery: finished adding query");
    }

    MemoryContextSwitchTo(oldContext);
}

void StartAMVTransaction()
{
    if (!isTransactionStarted)
    {
        StartTransactionCommand();
        isTransactionStarted = true;
    }
}

void FinishAMVTransaction()
{
    if (isTransactionStarted)
    {
        CommitTransactionCommand();
        isTransactionStarted = false;
    }
}

int64 ExecuteOutstandingQuery(OutstandingQuery *query)
{
    int64 proc;
    int ret = -1;

//    StartAMVTransaction();
    elog(LOG, "ExecuteQuery: executing %s", query->query->data);
    int connectSuccess = SPI_connect();
    elog(
    LOG, "ExecuteSPIQuery: SPI_connect returned %d", connectSuccess);

    if (connectSuccess == SPI_OK_CONNECT)
    {
        elog(LOG, "ExecuteSPIQuery: successfully connected. Executing query...");
        ret = SPI_execute(query->query->data, query->readOnly,
            query->tupleCount);
        elog(LOG, "ExecuteSPIQuery: finished SPI_exec... query.callback=%p",
        query->callback);

        if (query->callback != NULL)
        {
            elog(
                LOG, "ExecuteOutstandingQuery: calling OutstandingQuery's callback...");
            query->callback(SPI_tuptable, SPI_processed);
        }
    }
    else
    {
        elog(ERROR, "ExecuteSPIQuery failed to connect to SPI");
    }

    SPI_finish();
//    FinishAMVTransaction();

    return ret;
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
    QueryPlanStats *queryStats;
    MatView *newMatView;
    elog(LOG, "CreateRelevantMatViews called from MemoryContext=%s...",
    CurrentMemoryContext->name);

    costlyQueryPlanStats = GetCostlyQueries(0.1);
    matViewQueries = GenerateInterestingQueries(costlyQueryPlanStats);
    matViewQueries = PruneQueries(matViewQueries);

    if (list_length(matViewQueries) > 0)
    {
        foreach(queryCell, matViewQueries)
        {
            queryStats = lfirst_node(QueryPlanStats, queryCell);
            elog(
                LOG, "CreateRelevantMatViews: current QueryPlanStats from MemContext=%s",
                GetMemoryChunkContext(queryStats)->name);
            // We don't want the WHERE clause included in created materialized views
            newMatView = UnparseQuery(queryStats->query, false);
            elog(LOG, "CreateRelevantMatViews: Query.rtable.length=%d",
            list_length(newMatView->baseQuery->rtable));

            if (newMatView != NULL && newMatView->baseQuery != NULL
                && newMatView->baseQuery->rtable != NIL)
            {
                newMatView->storedQueryId = queryStats->storedQueryId;
                PopulateMatViewRenamedRTable(newMatView);
                CreateMaterializedView(newMatView->name,
                    newMatView->selectQuery);
                PersistMatView(newMatView);
                createdMatViews = list_append_unique(createdMatViews,
                    newMatView);
            }
            else
            {
                elog(ERROR, "CreateRelevantMatViews failed to create a MatView");
            }
        }

        elog(LOG, "Created %d materialized views based on given query workload",
        createdMatViews != NIL ? createdMatViews->length : 0);
    }
}

void PopulateMatViewRenamedRTable(MatView *matView)
{
    ListCell *targetEntryCell;
    TargetEntry *targetEntry;
    RangeTblEntry *renamedRte;
    List *newRTable = NIL;
    List *newColnames = NIL;
    int16 varattno = 1;

    MemoryContextStats(CurrentMemoryContext);

    // TODO: remove this when done testing
    if (matView != NULL && matView->baseQuery != NULL
        && matView->baseQuery->rtable != NIL)
    {
        elog(LOG, "PopulateMatViewRenamedRTable: matView.query.rtable=%p",
        matView->baseQuery->rtable);
//        MemoryContext matViewContext = GetMemoryChunkContext(
//            matView->baseQuery->rtable);
//        elog(
//            LOG, "PopulateMatViewRenamedRTable: MatView.query belongs to context=%s",
//            matViewContext->name);
        elog(
            LOG, "PopulateMatViewRenamedRTable called from MemoryContext=%s, query.rtable.length=%d",
            CurrentMemoryContext->name, list_length(matView->baseQuery->rtable));
    }
    else
    {
        elog(ERROR, "PopulateMatViewRenamedRTable received invalid MatView");
        return;
    }

    if (list_length(matView->baseQuery->rtable) > 0)
    {
        matView->renamedRtable = list_make1(
            copyObject(linitial(matView->baseQuery->rtable)));
        elog(LOG, "PopulateMatViewRenamedRTable: finished creating new rtable");

        if (matView->renamedRtable != NIL)
        {
            renamedRte = linitial(matView->renamedRtable);
            renamedRte->eref->aliasname = pstrdup(matView->name);
            renamedRte->relid = 0;
            elog(
            LOG, "PopulateMatViewRenamedRTable: finished creating new rtable");

            if (list_length(matView->renamedTargetList) > 0)
            {
                foreach(targetEntryCell, matView->renamedTargetList)
                {
                    targetEntry = lfirst_node(TargetEntry, targetEntryCell);
                    newColnames = list_append_unique(newColnames,
                        makeString(pstrdup(targetEntry->resname)));
                    SetVarattno(targetEntry->expr, varattno++);
                }

                if (list_length(newColnames) > 0)
                {
                    list_free(renamedRte->eref->colnames);
                    renamedRte->eref->colnames = newColnames;

                    // TODO: remove below when done testing
                    char targetListBuf[QUERY_BUFFER_SIZE];
                    UnparseTargetList(matView->renamedTargetList,
                        matView->renamedRtable,
                        false, targetListBuf, QUERY_BUFFER_SIZE);
                    elog(LOG, "Unparsed renamed targetList: %s", targetListBuf);
                }
                else
                {
                    elog(ERROR, "PopulateMatViewRenamedRTable failed to "
                    "create list of new column names");
                }
            }
            else
            {
                elog(
                    ERROR, "PopulateMatViewRenamedRTable found matView->renamedTargetList "
                    "with no TargetEntries");
            }
        }
        else
        {
            elog(
            ERROR, "PopulateMatViewRenamedRTable failed to create new rtable");
        }
    }
    else
    {
        elog(ERROR, "PopulateMatViewRenamedRTable received MatView whose Query "
        "has an empty rtable");
    }
}

/**
 * Retrieves all QueryPlanStats whose estimated plan costs are higher than the supplied
 * 	ratio of the individual query cost to the total cost of all gathered queries.
 *
 * returns: List (of QueryPlanStats)
 */
List *GetCostlyQueries(double costCutoffRatio)
{
    double *planCosts;
    double costCutoff;
    ListCell *planCell;
    QueryPlanStats *stats;
    List *costliestQueryPlans = NIL;
    double totalCost = 0;
    int index = 0;

    if (list_length(queryPlanStatsList) > 0)
    {
        planCosts = palloc(sizeof(double) * list_length(queryPlanStatsList));

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

        for (index = 0; index < list_length(queryPlanStatsList); index++)
        {
            if (planCosts[index] >= costCutoff)
            {
                stats = (QueryPlanStats *) list_nth(queryPlanStatsList, index);
                costliestQueryPlans = lappend(costliestQueryPlans, stats);
            }
        }

        pfree(planCosts);
    }

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
    List *interestingQueryStats;
    ListCell *queryStatsCell;
    QueryPlanStats *stats;

    interestingQueryStats = NIL;

// TODO: generate only interesting MatViews
    foreach(queryStatsCell, queryPlanStats)
    {
        stats = (QueryPlanStats *) lfirst(queryStatsCell);
        interestingQueryStats = lappend(interestingQueryStats, stats);
    }

    return interestingQueryStats;
}

/**
 * Prunes less useful queries from a list of queries.
 *
 * param queries: List (of QueryPlanStats)
 * returns: List (of QueryPlanStats)
 */
List *PruneQueries(List *queries)
{
// TODO: actually prune queries
// TODO: remove duplicates
    return queries;
}

bool IsAutomatviewReady()
{
    return isAutomatviewsReady;
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

    if (query->commandType == CMD_SELECT && IsQueryForUserTables(query)
        && CanQueryBeOptimized(query))
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


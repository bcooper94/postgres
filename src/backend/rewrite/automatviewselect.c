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
#include "utils/guc.h"
#include "utils/guc_tables.h"

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC
;
#endif

#define MAX_QUERY_RETRY_COUNT 3
#define TRAINING_SAMPLE_COUNT_CONFIG "automatview.training_sample_count"
#define DEFAULT_TRAINING_SAMPLE_COUNT 10

typedef struct OutstandingQuery
{
    StringInfo query;
    int tupleCount; /* Limit on number of returned tuples. 0 for unlimited */
    int retryCount; /* How many times have we tried to execute this query? */
    void (*callback)(SPITupleTable *, int64);
} OutstandingQuery;

typedef struct QueryPlanStats
{
    Query *query;
    PlannedStmt *plan;
} QueryPlanStats;

/** Internal state */
extern MemoryContext AutoMatViewContext = NULL;
static List *outstandingQueries = NIL;

static List *createdMatViews = NIL;
static List *queryPlanStatsList = NIL;
static bool isCollectingQueries = true;

static bool isAutomatviewsReady = false;

static int trainingSampleCount;

// Internal state initialization functions
static void PopulateUserTables();
static void HandlePopulateUserTables(SPITupleTable *tuptable,
    int64 processedCount);

// Query execution functions
static OutstandingQuery *CreateOutstandingQuery(char *query, int tupleCount,
    void (*callback)(SPITupleTable *, int64));
static void AddOutstandingQuery(OutstandingQuery *query);

// Query stats functions
static QueryPlanStats *CreateQueryPlanStats(Query *query, PlannedStmt *plan);

// MatView selection functions
static List *GetMatchingMatViews(Query *query);

// MatView generation operations
static int64 ExecuteOutstandingQuery(OutstandingQuery *query);
static int64 CreateMaterializedView(char *viewName, char *selectQuery);
static void CreateRelevantMatViews();
static void PopulateMatViewRenamedRTable(MatView *matView);
static List *GetCostlyQueries(double costCutoffRatio);
static List *GenerateInterestingQueries(List *queryPlanStats);
static List *PruneQueries(List *queryPlans);

void InitializeAutomatviewModule()
{
    if (!isAutomatviewsReady)
    {
        elog(LOG, "Initializing Automatview module...");
        if (AutoMatViewContext == NULL)
        {
            AutoMatViewContext = AllocSetContextCreate((MemoryContext) NULL,
                "AutoMatViewContext",
                ALLOCSET_DEFAULT_SIZES);
        }

        MemoryContext oldContext = SwitchToAutoMatViewContext();
        char *trainingSampleCountVal = GetConfigOptionByName(
            TRAINING_SAMPLE_COUNT_CONFIG, NULL, true);

        if (trainingSampleCountVal != NULL)
        {
            trainingSampleCount = strtol(trainingSampleCountVal,
                trainingSampleCountVal + strlen(trainingSampleCountVal), 10);
            pfree(trainingSampleCountVal);
        }
        else
        {
            trainingSampleCount = DEFAULT_TRAINING_SAMPLE_COUNT;
        }

        elog(
        LOG, "InitializeAutomatviewModule: set trainingSampleCount to %d",
        trainingSampleCount);
        PopulateUserTables();
        MemoryContextSwitchTo(oldContext);
    }
}

/*
 * Populate a list of user tables. Queries will only be used to generate
 *  materialized views if they only select from these tables.
 */
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
        CreateOutstandingQuery(getTablesQuery, 0, &HandlePopulateUserTables));
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
            strcpy(schemaBuf, SPI_getvalue(tuple, tupDesc, 1));
            strcpy(tableNameBuf, SPI_getvalue(tuple, tupDesc, 2));
            strcpy(relidBuf, SPI_getvalue(tuple, tupDesc, 3));
            AddUserTable(strtol(relidBuf, relidBuf + 32, 10), schemaBuf,
                tableNameBuf);
        }

        isAutomatviewsReady = true;
        elog(LOG, "Automatviews is now ready to collect queries");
        MemoryContextSwitchTo(oldContext);
    }
}

OutstandingQuery *CreateOutstandingQuery(char *query, int tupleCount,
    void (*callback)(SPITupleTable *, int64))
{
    OutstandingQuery *outstandingQuery = palloc(sizeof(OutstandingQuery));
    outstandingQuery->query = makeStringInfo();
    appendStringInfo(outstandingQuery->query, query);
    outstandingQuery->tupleCount = tupleCount;
    outstandingQuery->retryCount = 0;
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
    bool executedQuery = false;

    elog(
    LOG, "ExecuteFirstOustandingQuery: checking for outstanding queries...");
    if (list_length(outstandingQueries) > 0)
    {
        OutstandingQuery *query = (OutstandingQuery *) linitial(
            outstandingQueries);
        elog(
            LOG, "ExecuteFirstOustandingQuery: found outstanding query %p...", query);

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
                // First outstanding query is initialization
                isAutomatviewsReady = true;
                elog(LOG, "Automatview is ready to collect queries");
            }
        }
    }

    return executedQuery;
}

MemoryContext SwitchToAutoMatViewContext()
{
    return MemoryContextSwitchTo(AutoMatViewContext);
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

void AddQuery(Query *query, PlannedStmt *plannedStatement)
{
    MemoryContext oldContext;
    List *matViewQueries;

    oldContext = MemoryContextSwitchTo(AutoMatViewContext);

    if (IsQueryForUserTables(query) && CanQueryBeOptimized(query))
    {
        elog(LOG, "Adding query to stored queries list");
        queryPlanStatsList = lappend(queryPlanStatsList,
            CreateQueryPlanStats(query, plannedStatement));

        if (queryPlanStatsList->length >= trainingSampleCount)
        {
            isCollectingQueries = false;
            CreateRelevantMatViews();
        }
    }

    MemoryContextSwitchTo(oldContext);
}

int64 ExecuteOutstandingQuery(OutstandingQuery *query)
{
    int64 proc;
    int ret = -1;

    elog(LOG, "ExecuteQuery: executing %s", query);
    int connectSuccess = SPI_connect();
    elog(
    LOG, "ExecuteSPIQuery: SPI_connect returned %d", connectSuccess);

    if (connectSuccess == SPI_OK_CONNECT)
    {
        elog(LOG, "ExecuteSPIQuery: successfully connected. Executing query...");
        ret = SPI_execute(query->query->data, false, query->tupleCount);
        elog(LOG, "ExecuteSPIQuery: finished SPI_exec...");

        if (query->callback != NULL)
        {
            query->callback(SPI_tuptable, SPI_processed);
        }
    }
    else
    {
        elog(ERROR, "ExecuteSPIQuery failed to connect to SPI");
    }

    SPI_finish();

    return ret;
}

int64 CreateMaterializedView(char *viewName, char *selectQuery)
{
    int ret;
    int64 processed;
    char resultQuery[1000];

    sprintf(resultQuery, "CREATE MATERIALIZED VIEW IF NOT EXISTS %s AS %s;",
        viewName, selectQuery);
    elog(LOG, "CreateMaterializedView: %s", resultQuery);

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
    List *matViewQueries, *prunedQueries;
    List *costlyQueryPlanStats;
    ListCell *queryCell;
    Query *query;
    MatView *newMatView;

    costlyQueryPlanStats = GetCostlyQueries(0.1);
    matViewQueries = GenerateInterestingQueries(costlyQueryPlanStats);

    if (list_length(matViewQueries) > 0)
    {
        prunedQueries = PruneQueries(matViewQueries);
        elog(INFO, "Pruned to %d MatViews", list_length(prunedQueries));

        if (list_length(prunedQueries) > 0)
        {
            foreach(queryCell, prunedQueries)
            {
                query = lfirst_node(Query, queryCell);
                // We don't want the WHERE clause included in created materialized views
                newMatView = UnparseQuery(query, false);
                PopulateMatViewRenamedRTable(newMatView);
                CreateMaterializedView(newMatView->name,
                    newMatView->selectQuery);
                createdMatViews = list_append_unique(createdMatViews,
                    newMatView);
            }

            list_free(prunedQueries);
        }
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
 * returns: List (of QueryPlanStats)
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
 * param queryPlans: List (of Query)
 * returns: List (of Query)
 */
List *PruneQueries(List *queryPlans) // TODO: Create test scripts for this
{
    ListCell *targetQueryCell, *otherQueryCell;
    Query *targetQuery, *otherQuery;
    bool doAddQuery;
    bool doDeleteTargetQuery;
    int queriesPruned;
    List *prunedQueries = NIL;

    if (list_length(queryPlans) > 1)
    {
        do
        {
            queriesPruned = 0;
            targetQueryCell = list_head(queryPlans);

            while (targetQueryCell != NULL)
            {
                doDeleteTargetQuery = false;
                targetQuery = (Query *) lfirst(targetQueryCell);

                for (otherQueryCell = list_head(queryPlans);
                     otherQueryCell != NULL;
                     otherQueryCell = otherQueryCell->next)
                {
                    otherQuery = (Query *) lfirst(otherQueryCell);

                    if (targetQuery != otherQuery && IsQuerySubsetOfOtherQuery(targetQuery,
                        otherQuery, false))
                    {
                        MatView *toDelete = UnparseQuery(targetQuery, false);
                        elog(LOG, "PruneQueries: breaking out of inner loop to prune query=%s",
                            toDelete->selectQuery);
                        FreeMatView(toDelete);
                        doDeleteTargetQuery = true;
                        queriesPruned++;
                        break;
                    }
                }

                targetQueryCell = lnext(targetQueryCell);
                if (doDeleteTargetQuery)
                {
                    elog(LOG, "PruneQueries: pruning target Query %p",
                        targetQuery);
                    list_delete(queryPlans, targetQuery);
                }
            }

            elog(LOG, "PruneQueries: pruned %d queries in one iteration",
                queriesPruned);
        } while (queriesPruned > 0);
    }

    elog(LOG, "PruneQueries: pruned down to %d queries",
        list_length(queryPlans));

    return queryPlans;
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

    if (IsQueryForUserTables(query) && CanQueryBeOptimized(query))
    {
        elog(LOG, "GetBestMatViewMatch: finding best matching view");
        matchingMatViews = GetMatchingMatViews(query);
        // TODO: filter returned MatViews to find best match

        if (list_length(matchingMatViews) > 0)
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

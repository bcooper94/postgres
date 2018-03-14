/*
 * automatviewselect.h
 *
 *  Created on: Dec 13, 2017
 *      Author: brandon
 */

#ifndef AUTOMATVIEWSELECT_H
#define AUTOMATVIEWSELECT_H

#include "postgres.h"

#include "rewrite/automatviewselect_utils.h"

#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "nodes/plannodes.h"

extern void InitializeAutomatviewModule();

extern bool ExecuteFirstOutstandingQuery();

extern MemoryContext SwitchToAutoMatViewContext();

extern bool IsAutomatviewReady();

extern bool IsCollectingQueries();

extern void AddQueryStats(Query *query);

extern void AddQuery(Query *query, PlannedStmt *plannedStatement);

extern void InspectQuery(Query *query);

extern List *SearchApplicableMatViews(RangeVar *rangeVar);

extern void AddMatView(IntoClause *into);

extern MatView *GetBestMatViewMatch(Query *query);

#endif

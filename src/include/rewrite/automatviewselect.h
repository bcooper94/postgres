/*
 * automatviewselect.h
 *
 *  Created on: Dec 13, 2017
 *      Author: brandon
 */

#ifndef AUTOMATVIEWSELECT_H
#define AUTOMATVIEWSELECT_H

#include "postgres.h"

#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "nodes/plannodes.h"


typedef struct MatView
{
	char *name;
	char *selectQuery;
	Query *baseQuery; // Query object which this MatView is based on
	List *renamedTargetList; // List (of TargetEntry) with renamed TargetEntries
	List *renamedRtable;
} MatView;


extern MemoryContext SwitchToAutoMatViewContext();

extern bool IsCollectingQueries();

extern void AddQueryStats(Query *query);

extern void AddQuery(Query *query, PlannedStmt *plannedStatement);

extern void InspectQuery(Query *query);

extern List *SearchApplicableMatViews(RangeVar *rangeVar);

extern void AddMatView(IntoClause *into);

extern MatView *GetBestMatViewMatch(Query *query);

extern char *RewriteQuery(Query *query, MatView *matView);

#endif

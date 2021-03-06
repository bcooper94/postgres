/*
 * AutoMatViewSelect module's primary external API.
 *
 *  Created on: Dec 13, 2017
 *      Author: Brandon Cooper
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

extern int GetTrainingSampleCount();

extern bool ExecuteFirstOutstandingQuery();

extern MemoryContext SwitchToAutoMatViewContext();

extern bool IsAutomatviewReady();

extern bool IsCollectingQueries();

extern void AddQuery(Query *query, PlannedStmt *plannedStatement);

extern MatView *GetBestMatViewMatch(Query *query);

#endif

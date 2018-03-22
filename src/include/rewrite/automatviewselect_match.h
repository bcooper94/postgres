/*
 * automatviewselect module's API for matching incoming queries to generated MatViews.
 *
 * NOTE: All functions defined here assume the current
 *  MemoryContext is the AutoMatViewContext.
 *
 *  Created on: Mar 9, 2018
 *      Author: Brandon Cooper
 */

#ifndef AUTOMATVIEWSELECT_MATCH_H
#define AUTOMATVIEWSELECT_MATCH_H

#include "postgres.h"

#include "rewrite/automatviewselect_utils.h"

#include "nodes/pg_list.h"

extern void ClearUserTables();

extern void AddUserTable(Oid relid, char *schema, char *tableName);

extern bool IsQueryForUserTables(Query *query);

extern bool DoesQueryMatchMatView(Query *query, Query *matViewQuery);

extern bool IsQuerySubsetOfOtherQuery(Query *targetQuery, Query *otherQuery,
    bool includeWhereClause);

extern bool CanQueryBeOptimized(Query *query);

extern bool AreExprsEqual(Expr *exprOne, List *rtableOne, Expr *exprTwo,
    List *rtableTwo);

#endif /* AUTOMATVIEWSELECT_MATCH_H */

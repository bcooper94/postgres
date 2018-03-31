/*
 * AutoMatViewSelect module for rewriting incoming queries
 *  to use existing MatViews.
 *
 * NOTE: All functions defined here assume the current
 *  MemoryContext is the AutoMatViewContext.
 *
 *  Created on: Mar 9, 2018
 *      Author: Brandon Cooper
 */

#ifndef AUTOMATVIEWSELECT_REWRITE_H
#define AUTOMATVIEWSELECT_REWRITE_H

#include "postgres.h"

#include "rewrite/automatviewselect_utils.h"

extern char *RewriteQuery(Query *query, MatView *matView);

extern void RewriteTargetList(Query *query, MatView *matView);

extern void RewriteJoinTree(Query *query, MatView *matView);

extern void RewriteJoinTreeRecurs(Query *rootQuery, MatView *matView,
                                  Node *node, int fromClauseIndex, size_t fromClauseLength, int *joinCount,
                                  int *joinsRemoved);

extern void RewriteQuals(List *queryRtable, List *quals, Index targetVarno,
                         List *matViewRtable, List *matViewTargetList);

extern void RewriteVarReferences(List *queryRtable, Expr *target,
                                 Index targetVarno, List *matViewRtable, List *matViewTargetList);

extern void RewriteGroupByClause(Query *query, MatView *matView);

extern void CopyRte(RangeTblEntry *destination, RangeTblEntry *copyTarget);

extern bool DoesMatViewContainRTE(RangeTblEntry *rte, MatView *matView);

#endif /* AUTOMATVIEWSELECT_REWRITE_H */

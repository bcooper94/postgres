/*
 * automatviewselect_rewrite.c
 *
 *  Created on: Mar 9, 2018
 *      Author: Brandon Cooper
 */

#include "rewrite/automatviewselect_rewrite.h"

#include "rewrite/automatviewselect_utils.h"

#include "parser/parsetree.h"
#include "nodes/nodes.h"

/**
 * Rewrite the given Query to use the given materialized view.
 *
 * NOTE: query will be modified.
 *
 * returns: SQL query string representing rewritten Query object.
 */
char *RewriteQuery(Query *query, MatView *matView)
{
    char *rewrittenQuery;

    if (matView != NULL)
    {
        RewriteTargetList(query, matView);
        RewriteJoinTree(query, matView);
        RewriteGroupByClause(query, matView);

        MatView *createdView = UnparseQuery(query, true);
        rewrittenQuery = pstrdup(createdView->selectQuery);
        FreeMatView(createdView);
    }
    else
    {
        rewrittenQuery = NULL;
    }

    return rewrittenQuery;
}

/**
 * Rewrite the given Query's targetList to reference the given MatView's Query's targetList.
 * NOTE: Currently won't work for "SELECT *" queries with joins.
 */
void RewriteTargetList(Query *query, MatView *matView)
{
    ListCell *targetEntryCell, *matViewTargetEntryCell;
    TargetEntry *queryTargetEntry, *matViewTargetEntry;
    bool foundMatchingEntry;
    List *newTargetList;
    int matViewTargetListIndex;

    newTargetList = NIL;

    if (query->targetList != NIL)
    {
        /*
         * Add MatView's single rtable entry to the end of this
         *  Query's rtable so converted references can reference it
         */
        query->rtable = lappend(query->rtable,
                                copyObject(linitial(matView->renamedRtable)));

        foreach (targetEntryCell, query->targetList)
        {
            matViewTargetListIndex = 0;
            foundMatchingEntry = false;
            queryTargetEntry = lfirst_node(TargetEntry, targetEntryCell);

            for (matViewTargetEntryCell = list_head(
                     matView->baseQuery->targetList);
                 !foundMatchingEntry && matViewTargetEntryCell != NULL;
                 matViewTargetEntryCell = matViewTargetEntryCell->next)
            {
                matViewTargetEntry = lfirst_node(TargetEntry,
                                                 matViewTargetEntryCell);
                foundMatchingEntry = AreExprsEqual(queryTargetEntry->expr,
                                                   query->rtable, matViewTargetEntry->expr,
                                                   matView->baseQuery->rtable);
                matViewTargetListIndex++;
            }

            if (foundMatchingEntry)
            {
                matViewTargetEntry = copyObject(
                    list_nth(matView->renamedTargetList,
                             matViewTargetListIndex - 1));
                /*
                 * Set varno of Vars in the replaced expression to
                 *  reference MatView's rtable entry
                 */
                SetVarno(matViewTargetEntry->expr, query->rtable->length);

                if (IsA(matViewTargetEntry->expr, Var))
                {
                    Var *var = (Var *)matViewTargetEntry->expr;
                    RangeTblEntry *replacedRT = rt_fetch(var->varno,
                                                         query->rtable);
                }

                newTargetList = lappend(newTargetList, matViewTargetEntry);
            }
            else
            {
                newTargetList = lappend(newTargetList,
                                        copyObject(queryTargetEntry));
            }
        }
    }

    list_free(query->targetList);
    query->targetList = newTargetList;
}

/**
 * Rewrite the Query's jointree to utilize the tables contained in the given MatView.
 */
void RewriteJoinTree(Query *query, MatView *matView)
{
    int joinListLength;
    ListCell *fromCell;
    int joinIndex = 0;
    int joinCount = 0;
    int joinsRemoved = 0;

    if (query->jointree != NULL)
    {
        joinListLength = query->jointree->fromlist->length;
        foreach (fromCell, query->jointree->fromlist)
        {
            RewriteJoinTreeRecurs(query, matView, lfirst(fromCell), joinIndex++,
                                  joinListLength, &joinCount, &joinsRemoved);
        }

        if (joinsRemoved == joinCount)
        {
            if (query->jointree->quals != NULL)
            {
                pfree(query->jointree->quals);
                query->jointree->quals = NULL;
            }
            if (query->jointree->fromlist != NIL)
            {
                list_free(query->jointree->fromlist);
                query->jointree->fromlist = NIL;
            }

            RangeTblRef *matViewRef = makeNode(RangeTblRef);
            /*
             * Set rtindex to query.rtable.length since the MatView RTE
             *  was added to the end of query.rtable
             */
            matViewRef->rtindex = query->rtable->length;
            query->jointree->fromlist = list_make1(matViewRef);
        }
    }
}

void RewriteJoinTreeRecurs(Query *rootQuery, MatView *matView, Node *node,
                           int fromClauseIndex, size_t fromClauseLength, int *joinCount,
                           int *joinsRemoved)
{
    RangeTblEntry *joinRte, *leftRte, *rightRte;

    if (node != NULL)
    {
        if (IsA(node, JoinExpr))
        {
            char *joinTag;
            JoinExpr *joinExpr = (JoinExpr *)node;
            bool containsLeftRte, containsRightRte;

            (*joinCount)++;

            /* NOTE: Mixed JOIN ON clauses with table cross products will fail */
            if (IsA(joinExpr->larg, JoinExpr))
            {
                RewriteJoinTreeRecurs(rootQuery, matView, joinExpr->larg,
                                      fromClauseIndex, fromClauseLength,
                                      joinCount, joinsRemoved);
            }
            if (IsA(joinExpr->rarg, JoinExpr))
            {
                RewriteJoinTreeRecurs(rootQuery, matView, joinExpr->rarg,
                                      fromClauseIndex, fromClauseLength,
                                      joinCount, joinsRemoved);
            }

            joinRte = rt_fetch(joinExpr->rtindex, rootQuery->rtable);
            leftRte = left_join_table(joinExpr, rootQuery->rtable);
            rightRte = right_join_table(joinExpr, rootQuery->rtable);
            containsLeftRte = DoesMatViewContainRTE(leftRte, matView);
            containsRightRte = DoesMatViewContainRTE(rightRte, matView);

            if (containsLeftRte && containsRightRte)
            {
                (*joinsRemoved)++;
                /* Get rid of this join, and have dependent joins consider this a relation */
                joinExpr->rtindex = 0; /* JoinExpr.rtindex of 0 means no join */
            }
            else if (containsLeftRte)
            {
                /*
                 * Preserve the old RTEKind so we don't rewrite "TABLE_NAME_1 JOIN TABLE_NAME_2"
                 *  after the first JOIN ON statement when un-parsing the rewritten query
                 */
                RTEKind oldRTEKind = leftRte->rtekind;
                RewriteQuals(rootQuery->rtable, joinExpr->quals,
                             list_length(rootQuery->rtable), matView->baseQuery->rtable,
                             matView->baseQuery->targetList);
                CopyRte(leftRte, llast(rootQuery->rtable));
                leftRte->rtekind = oldRTEKind;
            }
            /*  e.g. it is one JoinExpr's leftJoin and the next JoinExpr's rightJoin */
            else if (containsRightRte)
            {
                /*
                 * Preserve the old RTEKind so we don't rewrite "TABLE_NAME_1 JOIN TABLE_NAME_2"
                 *  after the first JOIN ON statement when un-parsing the rewritten query
                 */
                RTEKind oldRTEKind = rightRte->rtekind;
                RewriteQuals(rootQuery->rtable, joinExpr->quals,
                             list_length(rootQuery->rtable), matView->baseQuery->rtable,
                             matView->baseQuery->targetList);
                CopyRte(rightRte, llast(rootQuery->rtable));
                rightRte->rtekind = oldRTEKind;
            }
        }
    }
    else if (IsA(node, RangeTblEntry))
    {
        elog(WARNING, "Found RangeTblEntry in join analysis");
    }
    else if (IsA(node, FromExpr))
    {
        elog(WARNING, "Found FromExpr in recursive join analysis");
    }
}

void RewriteQuals(List *queryRtable, List *quals, Index targetVarno,
                  List *matViewRtable, List *matViewTargetList)
{
    ListCell *qualCell;
    Expr *expr;

    if (quals != NIL)
    {
        foreach (qualCell, quals)
        {
            RewriteVarReferences(queryRtable, lfirst_node(Expr, qualCell),
                                 targetVarno, matViewRtable, matViewTargetList);
        }
    }
}

/**
 * Rewrite Var references within the target Expr to reference
 */
void RewriteVarReferences(List *queryRtable, Expr *target, Index targetVarno,
                          List *matViewRtable, List *matViewTargetList)
{
    switch (nodeTag(target))
    {
        case T_OpExpr:
        {
            OpExpr *opExpr = (OpExpr *)target;
            ListCell *argCell;

            if (opExpr->args != NULL)
            {
                foreach (argCell, opExpr->args)
                {
                    RewriteVarReferences(queryRtable,
                                        lfirst_node(Expr, argCell), targetVarno, matViewRtable,
                                        matViewTargetList);
                }
            }
            break;
        }
        case T_BoolExpr:
        {
            BoolExpr *boolExpr = (BoolExpr *)target;
            elog(WARNING, "RewriteVarReferences does not currently handle BoolOps");
            break;
        }
        case T_Var:
        {
            ListCell *matViewTECell;
            TargetEntry *matViewTE;
            Var *var = (Var *)target;
            AttrNumber targetEntryIndex = 1;

            RangeTblEntry *matViewRte = rt_fetch(targetVarno, queryRtable);

            /* Rewrite the Var to reference the MatView if it is in the MatView's targetList */
            foreach (matViewTECell, matViewTargetList)
            {
                matViewTE = lfirst_node(TargetEntry, matViewTECell);
                if (AreExprsEqual(target, queryRtable, matViewTE->expr,
                                matViewRtable))
                {
                    SetVarattno(var, targetEntryIndex);
                    SetVarno(var, targetVarno);
                }
                targetEntryIndex++;
            }
            break;
        }
        case T_Aggref:
        {
            Aggref *aggref = (Aggref *)target;
            ListCell *argCell;

            if (aggref->args != NIL)
            {
                foreach (argCell, aggref->args)
                {
                    RewriteVarReferences(queryRtable,
                                        lfirst_node(Expr, argCell),
                                        targetVarno,
                                        matViewRtable,
                                        matViewTargetList);
                }
            }
            break;
        }
        default:
            elog(WARNING, "RewriteVarReferences: Unrecognized Expr type");
    }
}

/**
 * Assuming query has been matched with matView, removes query's
 *  GROUP BY clause if it has one.
 */
void RewriteGroupByClause(Query *query, MatView *matView)
{
    /*
     * If query.groupClause is not NIL, then it has been matched to
     *  matView.baseQuery.groupClause, i.e. both GROUP clauses are the same,
     *  so we can remove query's GROUP clause.
     */
    if (query->groupClause != NIL && matView->baseQuery != NIL)
    {
        query->groupClause = NIL;
    }
}

void CopyRte(RangeTblEntry *destination, RangeTblEntry *copyTarget)
{
    if (destination != NULL && copyTarget != NULL)
    {
        destination->rtekind = copyTarget->rtekind;
        destination->relkind = copyTarget->relkind;
        pfree(destination->eref);
        destination->eref = copyObject(copyTarget->eref);
        destination->relid = copyTarget->relid;
        destination->jointype = copyTarget->jointype;

        if (destination->alias != NULL)
        {
            pfree(destination->alias);
            destination->alias = NULL;
        }
        if (copyTarget->alias != NULL)
        {
            destination->alias = copyObject(copyTarget->alias);
        }

        if (destination->functions != NIL)
        {
            pfree(destination->functions);
            destination->functions = NIL;
        }
        if (copyTarget->functions != NIL)
        {
            destination->functions = copyObject(copyTarget->eref);
        }
    }
    else
    {
        elog(WARNING, "CopyRte found NULL destination or copyTarget");
    }
}

bool DoesMatViewContainRTE(RangeTblEntry *rte, MatView *matView)
{
    ListCell *rteCell;
    RangeTblEntry *matViewRte;
    bool containsRte = false;

    for (rteCell = list_head(matView->baseQuery->rtable);
         !containsRte && rteCell != NULL; rteCell = rteCell->next)
    {
        matViewRte = lfirst_node(RangeTblEntry, rteCell);
        containsRte = rte->relid == matViewRte->relid;
    }

    return containsRte;
}

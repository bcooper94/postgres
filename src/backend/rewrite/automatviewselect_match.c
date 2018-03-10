/*
 * automatviewselect module for matching incoming queries to existing MatViews.
 *
 *  Created on: Mar 9, 2018
 *      Author: Brandon Cooper
 */

#include "rewrite/automatviewselect_match.h"

#include "rewrite/automatviewselect_utils.h"

static bool IsTargetListMatch(List *rtable, List *targetList,
    List *matViewTargetList, List *matViewRtable);

static bool IsExprRTEInMatView(Expr *expr, List *queryRtable,
    List *matViewRtable);

static bool IsExprMatch(Expr *expr, List *queryRtable, List *matViewTargetList,
    List *matViewRtable);

static bool IsFromClauseMatch(Query *query, List *matViewTargetList,
    List *matViewRtable, FromExpr *matViewJoinTree);

static bool IsFromClauseMatchRecurs(Query *rootQuery, Node *queryNode,
    List *matViewTargetList, List *matViewRtable);

static bool AreQualsMatch(List *matViewTargetList, List *matViewRtable,
    List *quals, List *queryRtable);

static bool IsGroupByClauseMatch(List *queryGroupClause, List *queryTargetList,
    List *queryRtable, List *matViewGroupClause, List *matViewTargetList,
    List *matViewRtable);

/**
 * Determine if the given query can be rewritten to use the given materialized view.
 *
 * query: target Query to match to the materialized view
 * matViewQuery: Query object for the materialized view to match query to.
 *
 * return: true if query can be rewritten to use the materialized view
 *  to which matViewQuery belongs.
 */
bool DoesQueryMatchMatView(Query *query, Query *matViewQuery)
{
    return IsTargetListMatch(query->rtable, query->targetList,
        matViewQuery->targetList, matViewQuery->rtable)
        && IsFromClauseMatch(query, matViewQuery->targetList,
            matViewQuery->rtable, matViewQuery->jointree)
        && IsGroupByClauseMatch(query->groupClause, query->targetList,
            query->rtable, matViewQuery->groupClause, matViewQuery->targetList,
            matViewQuery->rtable);
}

/**
 * Determine whether or not a Query should be rewritten to use any materialized views,
 *  or if it should be used to create a new materialized view. e.g. if there are no joins,
 *  group by clauses or aggregation functions, we will not improve performance of
 *  the query's execution.
 */
bool CanQueryBeOptimized(Query *query)
{
    ListCell *joinCell;
    bool hasJoins = false;

    if (query->jointree != NULL && list_length(query->jointree->fromlist) > 0)
    {
        for (joinCell = list_head(query->jointree->fromlist);
            joinCell != NULL && !hasJoins; joinCell = joinCell->next)
        {
            hasJoins = IsA(lfirst(joinCell), JoinExpr);
        }
    }

    return query->hasAggs || query->groupClause != NIL || hasJoins;
}

/**
 * Determines whether or not the provided List (of TargetEntry) matches the given MatView.
 */
bool IsTargetListMatch(List *rtable, List *targetList, List *matViewTargetList,
    List *matViewRtable)
{
    bool isMatch;
    ListCell *targetEntryCell;
    TargetEntry *targetEntry;

    isMatch = true;

    for (targetEntryCell = list_head(targetList);
        isMatch && targetEntryCell != NULL;
        targetEntryCell = targetEntryCell->next)
    {
        targetEntry = lfirst_node(TargetEntry, targetEntryCell);
        /**
         * A TargetEntry is a match for a MatView if either the TargetEntry's
         *  originating RTE is not present in the MatView's Query, or if both the
         *  TargetEntry's originating RTE are present in the MatView's Query and the
         *  TargetEntry is present in the MatView's targetList.
         */
//        elog(
//        LOG, "IsTargetListMatch: Finding RTE and checking if Expr is match...");
        isMatch = FindRte(targetEntry->resorigtbl, matViewRtable) == NULL
            || IsExprMatch(targetEntry->expr, rtable, matViewTargetList,
                matViewRtable);
    }

    elog(LOG, "Found match for TargetList? %s",
    isMatch ? "true" : "false");

    return isMatch;
}

bool IsExprRTEInMatView(Expr *expr, List *queryRtable, List *matViewRtable)
{
    bool isInMatView = true;

    switch (nodeTag(expr))
    {
        case T_Var:
        {
            Var *var = (Var *) expr;
            RangeTblEntry *varRte = rt_fetch(var->varno, queryRtable);
            isInMatView = FindRte(varRte->relid,
//                matView->baseQuery->rtable) != NULL;
                matViewRtable) != NULL;
//            elog(LOG, "IsExprRTEInMatView: Found Var=%s.%s. RTE in MatView? %s",
//            varRte->eref->aliasname, get_colname(varRte, var),
//            isInMatView ? "true" : "false");
            break;
        }
        case T_Aggref:
        {
            ListCell *argCell;
            Aggref *aggref = (Aggref *) expr;

            if (list_length(aggref->args) > 0)
            {
                for (argCell = list_head(aggref->args);
                    argCell != NULL && isInMatView; argCell = argCell->next)
                {
                    isInMatView = IsExprRTEInMatView(lfirst_node(Expr, argCell),
                        queryRtable, matViewRtable);
                }
            }
            else
            {
                elog(ERROR, "IsExprRTEInMatView found Aggref with no args");
                isInMatView = false;
            }
            break;
        }
        default:
            elog(WARNING, "IsExprRTEInMatView found unsupported Expr type");
            isInMatView = false;
    }

    return isInMatView;
}

/**
 * Determine if the given Expr is in the MatView's targetList.
 */
bool IsExprMatch(Expr *expr, List *queryRtable, List *matViewTargetList,
    List *matViewRtable)
{
    ListCell *viewTargetEntryCell;
    TargetEntry *viewTargetEntry;
    bool isExprMatch = false;

//    elog(LOG, "IsExprMatch called...");
    for (viewTargetEntryCell = list_head(matViewTargetList);
        !isExprMatch && viewTargetEntryCell != ((void *) 0);
        viewTargetEntryCell = viewTargetEntryCell->next)
    {
        viewTargetEntry = lfirst_node(TargetEntry, viewTargetEntryCell);
        // TODO: Should we only compare non-join tables?

        isExprMatch = AreExprsEqual(expr, queryRtable, viewTargetEntry->expr,
            matViewRtable);
    }

    return isExprMatch;
}

/**
 * Ensure the MatView's FROM clause is a match for the Query's FROM clause.
 */
bool IsFromClauseMatch(Query *query, List *matViewTargetList,
    List *matViewRtable, FromExpr *matViewJoinTree)
{
    FromExpr *from;
    ListCell *fromCell;
    bool isMatch = true;

    if (query->jointree != NULL)
    {
        from = query->jointree;
//        elog(LOG, "IsFromClauseMatch called. fromlist.length=%d",
//        list_length(from->fromlist));

//        foreach(fromCell, from->fromlist)
        for (fromCell = list_head(from->fromlist); fromCell != NULL && isMatch;
            fromCell = fromCell->next)
        {
            // NOTE: Mixed JOIN ON clauses with table cross products will cause this to fail
            isMatch = IsFromClauseMatchRecurs(query, lfirst(fromCell),
                matViewTargetList, matViewRtable);
        }
    }
    else
    {
        isMatch = matViewJoinTree == NULL;
    }

    if (isMatch)
    {
        elog(
        LOG, "IsFromClauseMatch: found match for MatView");
    }
    else
    {
        elog(
        LOG, "IsFromClauseMatch: did NOT find match for MatView");
    }

    return isMatch;
}

bool IsFromClauseMatchRecurs(Query *rootQuery, Node *queryNode,
    List *matViewTargetList, List *matViewRtable)
{
    RangeTblEntry *joinRte, *leftRte, *rightRte;
    bool isMatch = true;

    if (queryNode != NULL)
    {
        if (IsA(queryNode, JoinExpr))
        {
            char *joinTag;
            JoinExpr *joinExpr = (JoinExpr *) queryNode;
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

            // Left and right RTE indices should be correct.
            // See addRangeTableEntryForJoin in src/backend/parser/parse_relation.c:1858
            //  for how RTEs and join RTEs are added to the Query's list of RTEs

            // NOTE: Un-parsing of mixed JOIN ON clauses with table cross products will fail
            if (!IsA(joinExpr->larg, RangeTblRef))
            {
                isMatch = isMatch
                    && IsFromClauseMatchRecurs(rootQuery, joinExpr->larg,
                        matViewTargetList, matViewRtable);
            }
            if (!IsA(joinExpr->rarg, RangeTblRef))
            {
                isMatch = isMatch
                    && IsFromClauseMatchRecurs(rootQuery, joinExpr->rarg,
                        matViewTargetList, matViewRtable);
            }

            if (joinExpr->rtindex != 0)
            {
                joinRte = rt_fetch(joinExpr->rtindex, rootQuery->rtable);
                leftRte = left_join_table(joinExpr, rootQuery->rtable);
                rightRte = right_join_table(joinExpr, rootQuery->rtable);
                isMatch = isMatch
                    && AreQualsMatch(matViewTargetList, matViewRtable,
                        joinExpr->quals, rootQuery->rtable);
                if (isMatch)
                {
                    elog(
                        LOG, "IsFromClauseMatchRecurs: found match for %s %s %s",
                        leftRte->eref->aliasname, joinTag, rightRte->eref->aliasname);
                }
                else
                {
                    elog(
                        LOG, "IsFromClauseMatchRecurs: no match found for %s %s %s",
                        leftRte->eref->aliasname, joinTag, rightRte->eref->aliasname);
                }
            }
//            else
//            {
//                elog(LOG, "Found JoinExpr.rtindex == 0. Skipping...");
//            }
        }
        else if (IsA(queryNode, RangeTblRef))
        {
//            elog(LOG, "IsFromClauseMatchRecurs: found RangeTblRef in jointree");
            RangeTblRef *rtRef = (RangeTblRef *) queryNode;
//            elog(LOG, "UnparseFromExprRecurs: RangeTblRef.rtindex=%d",
//            rtRef->rtindex);
            RangeTblEntry *rte = rt_fetch(rtRef->rtindex, rootQuery->rtable);
        }
        else if (IsA(queryNode, RangeTblEntry))
        {
            elog(
                WARNING, "IsFromClauseMatchRecurs: Found RangeTblEntry in join analysis");
        }
        else if (IsA(queryNode, FromExpr))
        {
            elog(
                WARNING, "IsFromClauseMatchRecurs: Found FromExpr in recursive join analysis");
        }
    }

    return isMatch;
}

bool AreQualsMatch(List *matViewTargetList, List *matViewRtable, List *quals,
    List *queryRtable)
{
    ListCell *qualCell;
    Expr *expr;
    bool isMatch = true;

//    elog(LOG, "AreQualsMatch called...");

    if (quals != NIL)
    {
        for (qualCell = list_head(quals); qualCell != NULL && isMatch;
            qualCell = qualCell->next)
        {
            expr = (Expr *) lfirst(qualCell);

            switch (nodeTag(expr))
            {
                case T_OpExpr:
                {
                    ListCell *argCell;
                    Expr *arg;
                    OpExpr *opExpr = (OpExpr *) expr;

                    if (list_length(opExpr) > 0)
                    {
                        for (argCell = list_head(opExpr->args);
                            argCell != NULL && isMatch; argCell = argCell->next)
                        {
                            arg = lfirst_node(Expr, argCell);
                            isMatch = !IsExprRTEInMatView(arg, queryRtable,
                                matViewRtable)
                                || IsExprMatch(arg, queryRtable,
                                    matViewTargetList, matViewRtable);
                        }
                    }
                    else
                    {
                        elog(
                        ERROR, "AreQualsMatch found OpExpr with no arguments");
                    }
                    break;
                }
                default:
                    elog(WARNING, "AreQualsMatch found unsupported Expr type");
                    isMatch = false;
            }
        }
    }

    return isMatch;
}

bool IsGroupByClauseMatch(List *queryGroupClause, List *queryTargetList,
    List *queryRtable, List *matViewGroupClause, List *matViewTargetList,
    List *matViewRtable)
{
    ListCell *queryGroupCell, *matViewGroupCell;
    SortGroupClause *queryGroupStmt, *matViewGroupStmt;
    TargetEntry *queryTargetEntry, *matViewTargetEntry;
    bool isMatch = list_length(queryGroupClause)
        == list_length(matViewGroupClause);

//    elog(LOG, "IsGroupByClauseMatch called...");

    if (isMatch && list_length(queryGroupClause) > 0)
    {
        for (queryGroupCell = list_head(queryGroupClause), matViewGroupCell =
            list_head(matViewGroupClause);
            queryGroupCell != NULL && matViewGroupCell != NULL && isMatch;
            queryGroupCell = queryGroupCell->next, matViewGroupCell =
                matViewGroupCell->next)
        {
            queryGroupStmt = lfirst_node(SortGroupClause, queryGroupCell);
            queryTargetEntry = (TargetEntry *) list_nth(queryTargetList,
                queryGroupStmt->tleSortGroupRef);
            matViewGroupStmt = lfirst_node(SortGroupClause, matViewGroupCell);
            matViewTargetEntry = (TargetEntry *) list_nth(matViewTargetList,
                matViewGroupStmt->tleSortGroupRef);
            isMatch = AreExprsEqual(queryTargetEntry->expr, queryRtable,
                matViewTargetEntry->expr, matViewRtable);
        }
    }
//    else if (isMatch)
//    {
//        elog(LOG, "IsGroupByClauseMatch: no group clauses "
//        "found in either Query or MatView. Considering match");
//    }

    if (isMatch)
    {
        elog(LOG, "Group clause matches MatView");
    }

    return isMatch;
}

bool AreExprsEqual(Expr *exprOne, List *rtableOne, Expr *exprTwo,
    List *rtableTwo)
{
    bool equal = false;

    if (nodeTag(exprOne) == nodeTag(exprTwo))
    {
        switch (nodeTag(exprOne))
        {
            case T_Var:
            {
                Var *varOne = (Var *) exprOne;
                Var *varTwo = (Var *) exprTwo;
                RangeTblEntry *varRteOne = rt_fetch(varOne->varno, rtableOne);
                RangeTblEntry *varRteTwo = rt_fetch(varTwo->varno, rtableTwo);
//                elog(
//                    LOG, "Comparing Vars: varOne.relid=%d, varTwo.relid=%d tableOne.relid=%d (%s) to tableTwo.relid=%d (%s) and varOne.varattno=%d to varTwo.varattno=%d",
//                    varRteOne->relid, varRteTwo->relid,
//                    varRteOne->relid, varRteOne->eref->aliasname,
//                    varRteTwo->relid, varRteTwo->eref->aliasname,
//                    varOne->varattno, varTwo->varattno);
                equal = varRteOne->relid == varRteTwo->relid
                    && varOne->varattno == varTwo->varattno;
                break;
            }
            case T_Aggref:
            {
                ListCell *argCellOne, *argCellTwo;
                Aggref *aggrefOne = (Aggref *) exprOne;
                Aggref *aggrefTwo = (Aggref *) exprTwo;
//                elog(
//                    LOG, "Comparing Aggrefs: aggrefOne.aggfnoid=%d, aggrefTwo.aggfnoid=%d, aggrefOne.args.length=%d, aggrefTwo.args.length=%d",
//                    aggrefOne->aggfnoid, aggrefTwo->aggfnoid,
//                    aggrefOne->args != NIL ? aggrefOne->args->length : 0,
//                    aggrefTwo->args != NIL ? aggrefTwo->args->length : 0);
                equal = aggrefOne->aggfnoid == aggrefTwo->aggfnoid
                    && aggrefOne->args != NIL && aggrefTwo->args != NIL
                    && aggrefOne->args->length == aggrefTwo->args->length;

                if (equal)
                {
                    argCellOne = list_head(aggrefOne->args);
                    argCellTwo = list_head(aggrefTwo->args);

                    while (equal && argCellOne != NULL && argCellTwo != NULL)
                    {
                        equal = AreExprsEqual(
                        lfirst_node(TargetEntry, argCellOne)->expr, rtableOne,
                        lfirst_node(TargetEntry, argCellTwo)->expr, rtableTwo);
                        argCellOne = argCellOne->next;
                        argCellTwo = argCellTwo->next;
                    }
                }
                break;
            }
            default:
                elog(
                    LOG, "AreTargetEntriesEqual found unrecognized nodeTag; returning false");
                equal = false;
        }
    }

    return equal;
}

/*
 * automatviewselect_utils.h
 *
 *  Created on: Mar 9, 2018
 *      Author: brandon
 */

#include "rewrite/automatviewselect_utils.h"

#include "nodes/primnodes.h"
#include "nodes/pg_list.h"

void FreeMatView(MatView *matView)
{
    if (matView != NULL)
    {
        pfree(matView->baseQuery);
        pfree(matView->name);
        pfree(matView->selectQuery);
        pfree(matView);
    }
}

/**
 * string MUST be null-terminated.
 */
void ReplaceChars(char *string, char target, char replacement, size_t sizeLimit)
{
    size_t index;

    for (index = 0; index < sizeLimit && string[index] != '\0'; index++)
    {
        if (string[index] == target)
        {
            string[index] = replacement;
        }
    }
}

double GetPlanCost(Plan *plan)
{
    double cost;
    ListCell *targetCell;

    cost = 0;

    if (plan != NULL && plan->targetlist != NIL)
    {
        cost = plan->total_cost + GetPlanCost(plan->lefttree)
            + GetPlanCost(plan->righttree);
    }

    return cost;
}

void SetVarattno(Expr *expr, AttrNumber varattno)
{
    switch (nodeTag(expr))
    {
        case T_Var:
        {
            Var *var = (Var *) expr;
            var->varattno = varattno;
            break;
        }
        case T_Aggref:
        {
            Aggref *aggref = (Aggref *) expr;
            ListCell *argCell;
            TargetEntry *argTE;

            foreach(argCell, aggref->args)
            {
                argTE = lfirst_node(TargetEntry, argCell);
                SetVarattno(argTE->expr, varattno);
            }
            break;
        }
        default:
            elog(WARNING, "Failed to set varattno for unrecognized node");
    }
}

void SetVarno(Expr *expr, Index varno)
{
    switch (nodeTag(expr))
    {
        case T_Var:
        {
            Var *var = (Var *) expr;
            var->varno = varno;
            break;
        }
        case T_Aggref:
        {
            Aggref *aggref = (Aggref *) expr;
            ListCell *argCell;
            TargetEntry *argTE;

            foreach(argCell, aggref->args)
            {
                argTE = lfirst_node(TargetEntry, argCell);
                SetVarno(argTE->expr, varno);
            }
            break;
        }
        default:
            elog(WARNING, "Failed to set varno for unrecognized node");
    }
}

/**
 * Find a particular RangeTblEntry by its Oid within a list of RTEs.
 */
RangeTblEntry *FindRte(Oid relid, List *rtable)
{
    ListCell *rteCell;
    RangeTblEntry *rte;

    foreach(rteCell, rtable)
    {
        rte = lfirst_node(RangeTblEntry, rteCell);

        if (rte->relid == relid)
        {
            return rte;
        }
    }

    return NULL;
}

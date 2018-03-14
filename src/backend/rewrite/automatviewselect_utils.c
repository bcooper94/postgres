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
//          elog(LOG, "Setting varattno for Var to %d", varattno);
            Var *var = (Var *) expr;
            var->varattno = varattno;
            break;
        }
        case T_Aggref:
        {
            Aggref *aggref = (Aggref *) expr;
            ListCell *argCell;
            TargetEntry *argTE;

//          elog(LOG, "Setting varattno for args in Aggref to %d", varattno);

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
//            elog(LOG, "Setting varno for Var to %d", varno);
            Var *var = (Var *) expr;
            var->varno = varno;
            break;
        }
        case T_Aggref:
        {
            Aggref *aggref = (Aggref *) expr;
            ListCell *argCell;
            TargetEntry *argTE;

//            elog(LOG, "Setting varno for args in Aggref to %d", varno);

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

void PrintQueryInfo(Query *query)
{
    ListCell *rte_cell;
    ListCell *col_cell;
    ListCell *targetList;
    RangeTblEntry *rte;

    if (query->rtable && query->rtable->length > 0)
    {
        elog(
        LOG, "Select statement number of RTEs: %d, number of TargetEntries: %d",
        query->rtable->length, query->targetList->length);

        foreach(targetList, query->targetList)
        {
            TargetEntry *te = lfirst_node(TargetEntry, targetList);

            /* junk columns don't get aliases */
            if (te->resjunk)
            {
                continue;
            }
            elog(LOG, "RTE Target entry: %s",
            te->resname);
        }

        foreach(rte_cell, query->rtable)
        {
            rte = (RangeTblEntry *) lfirst(rte_cell);

            switch (rte->rtekind)
            {
                case RTE_RELATION:
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
                    break;
                case RTE_JOIN:
                    elog(LOG, "RTE Join found");
                    break;
            }
        }
    }
    else
    {
        elog(LOG, "No RTEs found in select statement");
    }
}

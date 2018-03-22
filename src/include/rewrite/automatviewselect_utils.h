/*
 * automatviewselect_utils.h
 *
 * NOTE: All functions defined here assume the current
 *  MemoryContext is the AutoMatViewContext.
 *
 *  Created on: Mar 9, 2018
 *      Author: brandon
 */

#ifndef AUTOMATVIEWSELECT_UTILS_H
#define AUTOMATVIEWSELECT_UTILS_H

#include "postgres.h"

#include "parser/parsetree.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"

#define MAX_TABLENAME_SIZE 256
#define MAX_COLNAME_SIZE 256
#define QUERY_BUFFER_SIZE 2048
#define TARGET_BUFFER_SIZE 512

#define EQ_OID 96

#define left_join_table(joinExpr, rangeTables) \
    (rt_fetch(joinExpr->rtindex - 2, rangeTables))

#define right_join_table(joinExpr, rangeTables) \
    (rt_fetch(joinExpr->rtindex - 1, rangeTables))

#define get_colname(rte, var) \
    (strVal(lfirst(list_nth_cell((rte)->eref->colnames, (var)->varattno - 1))))

typedef struct MatView
{
    char *name;
    char *selectQuery;
    Query *baseQuery; // Query object which this MatView is based on
    List *renamedTargetList; // List (of TargetEntry) with renamed TargetEntries
    List *renamedRtable;
} MatView;

extern void FreeMatView(MatView *matView);

extern void ReplaceChars(char *string, char target, char replacement,
    size_t sizeLimit);

extern double GetPlanCost(Plan *plan);

extern void SetVarattno(Expr *expr, AttrNumber varattno);

extern void SetVarno(Expr *expr, Index varno);

extern RangeTblEntry *FindRte(Oid relid, List *rtable);

#endif

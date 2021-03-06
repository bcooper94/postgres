/*
 * AutoMatViewSelect module for unparsing incoming Query objects.
 *
 * NOTE: All functions defined here assume the current
 *  MemoryContext is the AutoMatViewContext.
 *
 *  Created on: Mar 9, 2018
 *      Author: Brandon Cooper
 */

#ifndef AUTOMATVIEWSELECT_UNPARSE_H
#define AUTOMATVIEWSELECT_UNPARSE_H

#include "postgres.h"

#include "rewrite/automatviewselect_utils.h"

#include "lib/stringinfo.h"

extern MatView *UnparseQuery(Query *query, bool includeWhereClause);

extern List *UnparseTargetList(List *targetList, List *rtable,
                               bool renameTargets, char *selectTargetsBuf,
                               size_t selectTargetsBufSize);

extern TargetEntry *CreateRenamedTargetEntry(TargetEntry *baseTE, char *newName,
                                             bool flattenExprTree);

extern char *UnparseRangeTableEntries(List *rtable);

extern void UnparseFromExprRecurs(Query *rootQuery, Node *node, int fromClauseIndex,
                                  size_t fromClauseLength, StringInfo fromClause,
                                  size_t selectQuerySize);

extern char *UnparseGroupClause(List *groupClause, List *targetList,
                                List *rtable);

extern TargetEntry *FindTargetEntryForGroupStatement(SortGroupClause *sortGroupClause,
                                                     List *targetEntries);

extern void CreateJoinVarStr(JoinExpr *joinExpr, Var *var, RangeTblEntry *rte,
                             List *rangeTables, char *varStrBuf, size_t varStrBufSize);

extern char *UnparseQuals(List *quals, List *rangeTables);

extern char *AggrefToString(TargetEntry *aggrefEntry, List *rtable,
                            bool renameAggref, char *aggrefStrBuf, size_t aggrefStrBufSize);

extern void ExprToString(Expr *expr, List *rangeTables, char *targetBuf,
                         size_t targetBufSize);

extern char *TargetEntryToString(TargetEntry *targetEntry, List *rtable,
                                 bool renameTargets, char *outBuf, size_t outBufSize);

extern void ConstToString(Const *constant, char *outputBuf, Size outputBufSize);

extern char *VarToString(Var *var, List *rtable, bool renameVar, char *varBuf,
                         size_t varBufSize);

#endif /* AUTOMATVIEWSELECT_UNPARSE_H */

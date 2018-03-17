/*
 * automatviewselect_unparse.c
 *
 *  Created on: Mar 9, 2018
 *      Author: Brandon Cooper
 */

#include "rewrite/automatviewselect_unparse.h"

#include "rewrite/automatviewselect_utils.h"
#include "rewrite/automatviewselect.h"

#include "catalog/pg_type.h"
#include "parser/parsetree.h"
#include "utils/memutils.h"

#define STAR_COL "*"
#define RENAMED_STAR_COL "all"

MatView *UnparseQuery(Query *query, bool includeWhereClause)
{
    ListCell *listCell;
    TargetEntry *targetEntry;
    FromExpr *from;
    MatView *matView;
    char *fromClauseStr, *groupClauseStr;
    char *whereClauseStr = NULL;
    char targetListBuf[QUERY_BUFFER_SIZE];
    List *renamedTargets;

    MemoryContextStats(CurrentMemoryContext);
    targetListBuf[0] = '\0';
    matView = palloc(sizeof(MatView));
    matView->name = palloc(sizeof(char) * MAX_TABLENAME_SIZE);
    snprintf(matView->name, MAX_TABLENAME_SIZE, "Matview_%d", rand());

//    elog(LOG, "UnparseQuery: current MemContext=%s", CurrentMemoryContext->name);
    // TODO: this is returning garbage
//    matView->baseQuery = copyObject(query);
    matView->baseQuery = query;
//    elog(LOG, "UnparseQuery: baseQuery from MemContext=%s",
//        GetMemoryChunkContext(matView->baseQuery)->name);

    matView->selectQuery = palloc(sizeof(char) * QUERY_BUFFER_SIZE);
    strcpy(matView->selectQuery, "SELECT ");
    matView->renamedTargetList = UnparseTargetList(query->targetList,
        query->rtable, true, targetListBuf,
        QUERY_BUFFER_SIZE);
    strncat(matView->selectQuery, targetListBuf, QUERY_BUFFER_SIZE);

    strncat(matView->selectQuery, " FROM ", QUERY_BUFFER_SIZE);

    if (query->jointree != NULL)
    {
        int index = 0;
        from = query->jointree;
        elog(
            LOG, "Unparsing FROM clause. fromlist.length=%d", from->fromlist->length);

        foreach(listCell, from->fromlist)
        {
            // NOTE: Un-parsing of mixed JOIN ON clauses with table cross products will fail
            UnparseFromExprRecurs(query, lfirst(listCell), index,
                from->fromlist->length, matView->selectQuery,
                QUERY_BUFFER_SIZE);
            index++;
        }

        if (includeWhereClause)
        {
            whereClauseStr = UnparseQuals(from->quals, query->rtable);
            elog(
                LOG, "UnparseQuery: unparsing WHERE clause. query.jointree.quals=%s",
                whereClauseStr);
        }
    }
    else
    {
        elog(LOG, "No join tree found. Unparsing RTEs into FROM clause...");
        fromClauseStr = UnparseRangeTableEntries(query->rtable);
        strncat(matView->selectQuery, fromClauseStr, QUERY_BUFFER_SIZE);
        pfree(fromClauseStr);
    }
    if (whereClauseStr != NULL)
    {
        strncat(matView->selectQuery, " WHERE ", QUERY_BUFFER_SIZE);
        strncat(matView->selectQuery, whereClauseStr, QUERY_BUFFER_SIZE);
        pfree(whereClauseStr);
    }
    if (query->groupClause != NIL)
    {
        groupClauseStr = UnparseGroupClause(query->groupClause,
            query->targetList, query->rtable);

        if (groupClauseStr != NULL)
        {
            strncat(matView->selectQuery, groupClauseStr, QUERY_BUFFER_SIZE);
            pfree(groupClauseStr);
        }
    }
    if (query->groupingSets != NIL)
    {
        elog(LOG, "Found grouping sets in query");
    }

    elog(LOG, "Constructed full select query: %s", matView->selectQuery);
    MemoryContextStats(CurrentMemoryContext);

    return matView;
}

/**
 * Unparses the SELECT clause, copying it into selectTargetsBuf.
 * Returns a List of TargetEntry with the appropriate renamed entries if renameTargets is true.
 * NOTE: Returned List must be freed.
 */
List *UnparseTargetList(List *targetList, List *rtable, bool renameTargets,
    char *selectTargetsBuf, size_t selectTargetsBufSize)
{
    TargetEntry *targetEntry;
    TargetEntry *renamedTargetEntry;
    RangeTblEntry *rte;
    char targetBuffer[TARGET_BUFFER_SIZE];
    char *renamedTarget;
    ListCell *listCell;
    int index;
    List *renamedTargetEntries;

    renamedTargetEntries = NIL;
    index = 0;

    if (targetList != NIL)
    {
        foreach(listCell, targetList)
        {
            targetEntry = lfirst_node(TargetEntry, listCell);
//            elog(
//                    LOG, "TargetEntry %s, TE.expr nodeTag: %d, resorigtable=%d, column attribute number=%d",
//                    targetEntry->resname, nodeTag(targetEntry->expr),
//                    targetEntry->resorigtbl, targetEntry->resorigcol);
            renamedTarget = TargetEntryToString(targetEntry, rtable,
                renameTargets, targetBuffer,
                TARGET_BUFFER_SIZE);
            if (renameTargets && renamedTarget != NULL)
            {
                renamedTargetEntry = CreateRenamedTargetEntry(targetEntry,
                    renamedTarget,
                    true);
                if (nodeTag(targetEntry->expr) == T_Var)
                {
                    Var *v = (Var *) targetEntry->expr;
//                    elog(LOG, "Var.varno after rename=%d", v->varno);
                }
                renamedTargetEntries = list_append_unique(renamedTargetEntries,
                    renamedTargetEntry);
            }
            if (index < targetList->length - 1)
            {
                strncat(targetBuffer, ", ", TARGET_BUFFER_SIZE);
            }

            strncat(selectTargetsBuf, targetBuffer, selectTargetsBufSize);
            index++;
        }
    }
    // Assume SELECT *
    else
    {
        elog(LOG, "Query.targetList was NIL; assuming SELECT *");
        strcpy(selectTargetsBuf, "*");
    }

    return renamedTargetEntries;
}

/**
 * NOTE: newName must be palloc'd.
 */
TargetEntry *CreateRenamedTargetEntry(TargetEntry *baseTE, char *newName,
    bool flattenExprTree)
{
    TargetEntry *renamedTE = copyObject(baseTE);
    renamedTE->resname = newName;

    // Need to convert Aggrefs to Var
    if (flattenExprTree && nodeTag(renamedTE->expr) != T_Var)
    {
        elog(LOG, "CreateRenamedTargetEntry flattening expression tree to Var");
        // TODO: Preserve old varno and varattno using Var.varnoold and Var.varoattno
        Var *flattenedVar = makeNode(Var);
        pfree(renamedTE->expr);
        renamedTE->expr = flattenedVar;
    }
    // Reset all varnos to 1 to reference new MatView rtable, which will have one RTE to represent the MatView
    SetVarno(renamedTE->expr, 1);
    return renamedTE;
}

/**
 * Unparse a List (of RangeTblEntry) into the FROM clause of a query string.
 * NOTE: Returned pointer must be pfree'd.
 */
char *UnparseRangeTableEntries(List *rtable)
{
    RangeTblEntry *rte;
    char *clauseString;
    char rteBuffer[TARGET_BUFFER_SIZE];
    ListCell *rtCell;
    int index;

    index = 0;
    clauseString = palloc(sizeof(char) * QUERY_BUFFER_SIZE);
    clauseString[0] = '\0';

    foreach(rtCell, rtable)
    {
        rte = lfirst_node(RangeTblEntry, rtCell);

        if (index < rtable->length - 1)
        {
            snprintf(rteBuffer, TARGET_BUFFER_SIZE, "%s, ",
                rte->eref->aliasname);
        }
        else
        {
            strncat(rteBuffer, rte->eref->aliasname, TARGET_BUFFER_SIZE);
        }

        strncat(rteBuffer, rteBuffer, QUERY_BUFFER_SIZE);
        index++;
    }

    return clauseString;
}

void UnparseFromExprRecurs(Query *rootQuery, Node *node, int fromClauseIndex,
    size_t fromClauseLength, char *selectQuery, size_t selectQuerySize)
{
    RangeTblEntry *joinRte, *leftRte, *rightRte;
    char *qualStr;
    char joinBuf[QUERY_BUFFER_SIZE];

    joinBuf[0] = '\0';

    if (node != NULL)
    {
        if (IsA(node, JoinExpr))
        {
            char *joinTag;
            JoinExpr *joinExpr = (JoinExpr *) node;
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
            //    for how RTEs and join RTEs are added to the Query's list of RTEs

            // NOTE: Un-parsing of mixed JOIN ON clauses with table cross products will fail
            if (!IsA(joinExpr->larg, RangeTblRef))
            {
                UnparseFromExprRecurs(rootQuery, joinExpr->larg,
                    fromClauseIndex, fromClauseLength, selectQuery,
                    selectQuerySize);
            }
            if (!IsA(joinExpr->rarg, RangeTblRef))
            {
                UnparseFromExprRecurs(rootQuery, joinExpr->rarg,
                    fromClauseIndex, fromClauseLength, selectQuery,
                    selectQuerySize);
            }

            if (joinExpr->rtindex != 0)
            {
                joinRte = rt_fetch(joinExpr->rtindex, rootQuery->rtable);
                leftRte = left_join_table(joinExpr, rootQuery->rtable);
                rightRte = right_join_table(joinExpr, rootQuery->rtable);
                elog(LOG, "Unparsed %s %s %s", leftRte->eref->aliasname,
                joinTag, rightRte->eref->aliasname);
                qualStr = UnparseQuals(joinExpr->quals, rootQuery->rtable);
                elog(LOG, "Unparsed qualifiers for %s %s %s",
                leftRte->eref->aliasname,
                joinTag, rightRte->eref->aliasname);

                if (leftRte->rtekind == RTE_JOIN)
                {
                    snprintf(joinBuf, QUERY_BUFFER_SIZE, " %s %s ON %s",
                        joinTag, rightRte->eref->aliasname, qualStr);
                }
                else
                {
                    snprintf(joinBuf, QUERY_BUFFER_SIZE, " %s %s %s ON %s",
                        leftRte->eref->aliasname, joinTag,
                        rightRte->eref->aliasname, qualStr);
                }

                pfree(qualStr);
                qualStr = NULL;

                strncat(selectQuery, joinBuf, selectQuerySize);
            }
            else
            {
                elog(LOG, "Found JoinExpr.rtindex == 0. Skipping...");
            }
        }
        else if (IsA(node, RangeTblRef))
        {
            RangeTblRef *rtRef = (RangeTblRef *) node;
            elog(
                LOG, "UnparseFromExprRecurs: RangeTblRef.rtindex=%d", rtRef->rtindex);
            RangeTblEntry *rte = rt_fetch(rtRef->rtindex, rootQuery->rtable);

            if (fromClauseIndex < fromClauseLength - 1)
            {
                snprintf(joinBuf, QUERY_BUFFER_SIZE, "%s, ",
                    rte->eref->aliasname);
            }
            else
            {
                strncpy(joinBuf, rte->eref->aliasname, QUERY_BUFFER_SIZE);
            }

            strcat(selectQuery, joinBuf);
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
}

/**
 * Unparse a list of SortGroupClauses into the GROUP BY clause of a SQL string.
 * param groupClause: List (of SortGroupClause) from Query
 * param targetList: List (of TargetEntry) from Query
 * param rtable: List (of RangeTblEntry) from Query
 *
 * NOTE: returned pointer must be pfree'd.
 */
char *UnparseGroupClause(List *groupClause, List *targetList, List *rtable)
{
    char *groupClauseStr;
    ListCell *groupCell;
    SortGroupClause *groupStmt;
    TargetEntry *targetEntry;
    char targetBuffer[TARGET_BUFFER_SIZE];
    int index;

    if (list_length(groupClause) > 0)
    {
        index = 0;
        groupClauseStr = palloc(sizeof(char) * TARGET_BUFFER_SIZE);

        strcpy(groupClauseStr, " GROUP BY ");

        foreach(groupCell, groupClause)
        {
            groupStmt = lfirst_node(SortGroupClause, groupCell);
            targetEntry = (TargetEntry *) list_nth(targetList,
                groupStmt->tleSortGroupRef - 1);
            TargetEntryToString(targetEntry, rtable, false, targetBuffer,
            TARGET_BUFFER_SIZE);
            elog(
                LOG, "Found SortGroupClause nodeTag=%d, sortGroupRef=%d, eqop=%d, sortop=%d, targetEntry=%s",
                nodeTag(groupStmt), groupStmt->tleSortGroupRef,
                groupStmt->eqop, groupStmt->sortop,
                targetBuffer);

            strncat(groupClauseStr, targetBuffer, TARGET_BUFFER_SIZE);

            if (index < groupClause->length - 1)
            {
                strncat(groupClauseStr, ", ", TARGET_BUFFER_SIZE);
            }

            index++;
        }
    }
    else
    {
        groupClauseStr = NULL;
    }

    elog(LOG, "Constructed GROUP BY clause: %s", groupClauseStr);

    return groupClauseStr;
}

void CreateJoinVarStr(JoinExpr *joinExpr, Var *var, RangeTblEntry *rte,
    List *rangeTables, char *varStrBuf, size_t varStrBufSize)
{
    RangeTblEntry *targetRte = rte;

    // Special case for Join RTEs since their aliasname is "unnamed_join"
    if (rte->rtekind == RTE_JOIN)
    {
        RangeTblEntry *varRte = rt_fetch(var->varno, rangeTables);
//        elog(LOG, "Var from table=%s: %s", varRte->eref->aliasname,
//        get_colname(varRte, var));

        // If rte is an RTE_JOIN, left should be the right table of last join
        RangeTblEntry *leftRte = rt_fetch(joinExpr->rtindex - 3, rangeTables);
        RangeTblEntry *rightRte = right_join_table(joinExpr, rangeTables);
//        elog(LOG, "CreateJoinVarStr found JOIN RTE: rtindex=%d, name=%s, left.name=%s, right.name=%s",
//                        joinExpr->rtindex, rte->eref->aliasname,
//                        leftRte->eref->aliasname, rightRte->eref->aliasname);

        // Var was from left RTE
        if (var->varattno <= leftRte->eref->colnames->length)
        {
//            elog(
//                LOG, "Renaming tableName in Join RTE to left table: %s", leftRte->eref->aliasname);
            targetRte = leftRte;
        }
        // Var from right RTE
        else
        {
//            elog(
//                LOG, "Renaming tableName in Join RTE to right table: %s", rightRte->eref->aliasname);
            targetRte = rightRte;
        }
    }

    sprintf(varStrBuf, "%s.%s", targetRte->eref->aliasname,
        get_colname(targetRte, var));

//    elog(LOG, "Var str for rtekind=%d, varattno=%d %s",
//    rte->rtekind, var->varattno, varStrBuf);

    if (var->varno == INDEX_VAR)
    {
        elog(LOG, "Var is an index var");
    }
}

/**
 * Returns a string representation of the qualifiers.
 * NOTE: returned char * must be pfreed.
 */
char *UnparseQuals(List *quals, List *rangeTables)
{
    ListCell *qualCell;
    Expr *expr;
    size_t qualBufSize = TARGET_BUFFER_SIZE;
    char *qualBuf = NULL;

    elog(LOG, "UnparseQuals called...");

    if (quals != NIL)
    {
        qualBuf = palloc(sizeof(char) * qualBufSize);

        if (qualBuf != NULL)
        {
            qualBuf[0] = '\0';
            foreach(qualCell, quals)
            {
                expr = (Expr *) lfirst(qualCell);
                if (qualBuf != NULL)
                {
                    ExprToString(expr, rangeTables, qualBuf, qualBufSize);
                }
            }
        }
        else
        {
            elog(ERROR, "UnparseQuals: failed to allocate qualifiers buffer");
        }
    }

    return qualBuf;
}

char *AggrefToString(TargetEntry *aggrefEntry, List *rtable, bool renameAggref,
    char *aggrefStrBuf, size_t aggrefStrBufSize)
{
    Aggref *aggref;
    ListCell *argCell;
    TargetEntry *arg;
    char targetStr[TARGET_BUFFER_SIZE];
    char copyBuf[TARGET_BUFFER_SIZE];
    char *renamedBuf = NULL;

    targetStr[0] = '\0';
    aggref = (Aggref *) aggrefEntry->expr;

    if (aggref->aggstar == true)
    {
        strcpy(targetStr, STAR_COL);
    }
    else
    {
        UnparseTargetList(aggref->args, rtable, false, targetStr,
        TARGET_BUFFER_SIZE);
    }

    if (renameAggref == true)
    {
        if (aggref->aggstar == true)
        {
            strcpy(copyBuf, RENAMED_STAR_COL);
        }
        else
        {
            strncpy(copyBuf, targetStr, TARGET_BUFFER_SIZE);
            ReplaceChars(copyBuf, '.', '_', TARGET_BUFFER_SIZE);
        }

        renamedBuf = palloc(sizeof(char) * TARGET_BUFFER_SIZE);
        snprintf(renamedBuf, TARGET_BUFFER_SIZE, "%s_%s", aggrefEntry->resname,
            copyBuf);
        snprintf(aggrefStrBuf, aggrefStrBufSize, "%s(%s) AS %s",
            aggrefEntry->resname, targetStr, renamedBuf);
    }
    else
    {
        snprintf(aggrefStrBuf, aggrefStrBufSize, "%s(%s)", aggrefEntry->resname,
            targetStr);
    }

    elog(LOG, "AggrefToString result: %s", aggrefStrBuf);

    return renamedBuf;
}

void ExprToString(Expr *expr, List *rangeTables, char *targetBuf,
    size_t targetBufSize)
{
//    elog(LOG, "ExprToString Expr tag: %d", nodeTag(expr));

    switch (nodeTag(expr))
    {
        case T_Const:
            ConstToString((Const *) expr, targetBuf, targetBufSize);
            break;
        case T_Var:
        {
            Var *var = (Var *) expr;
//            elog(
//                LOG, "ExprToString: Found Var with varno=%d, varattno=%d. rtable.length=%d",
//                var->varno, var->varattno, list_length(rangeTables));

            char *colName = VarToString((Var *) expr, rangeTables, false,
                targetBuf, targetBufSize);
            if (colName != NULL)
            {
                pfree(colName);
            }
//            elog(LOG, "ExprToString: converted Var to %s", targetBuf);
            break;
        }
        case T_OpExpr:
        {
            OpExpr *opExpr = (OpExpr *) expr;
//            elog(LOG, "Found OpExpr opno=%d, number of args=%d",
//            opExpr->opno,
//            opExpr->args != NULL ? opExpr->args->length : 0);

            if (opExpr->args != NULL)
            {
                if (opExpr->args->length == 1)
                {
                    elog(
                        WARNING, "Unary Operation Expressions are not currently supported");
                    // TODO: handle unary op
                }
                else if (opExpr->args->length == 2)
                {
                    char leftVarStr[256], rightVarStr[256];
                    Expr *leftExpr = (Expr *) linitial(opExpr->args);
                    Expr *rightExpr = (Expr *) lsecond(opExpr->args);

                    leftVarStr[0] = '\0';
                    rightVarStr[0] = '\0';
                    ExprToString(leftExpr, rangeTables, leftVarStr, 256);
                    ExprToString(rightExpr, rangeTables, rightVarStr, 256);

                    snprintf(targetBuf, targetBufSize, "%s %s %s", leftVarStr,
                    // TODO: Figure out opno to string mapping
                        opExpr->opno == 96 ? "=" : "UNKNOWN", rightVarStr);
                }
                else
                {
                    elog(ERROR, "Found OpExpr with more than 2 args");
                }
            }
            break;
        }
        case T_BoolExpr:
        {
            BoolExpr *boolExpr = (BoolExpr *) expr;
            elog(
                WARNING, "Found boolean expression of type %s with %d args",
                boolExpr->boolop == AND_EXPR ? "AND" : boolExpr->boolop == OR_EXPR ? "OR" : "NOT",
                boolExpr->args != NIL ? boolExpr->args->length : 0);
            break;
        }
    }

}

char *TargetEntryToString(TargetEntry *targetEntry, List *rtable,
    bool renameTargets, char *outBuf, size_t outBufSize)
{
    char *targetEntryRename = NULL;

    switch (nodeTag(targetEntry->expr))
    {
        case T_Var:
        {
//            elog(LOG, "TargetEntryToString: converting Var to string. expr=%p",
//            targetEntry->expr);
            Var *var = (Var *) targetEntry->expr;
//            elog(LOG, "TargetEntryToString: var.varno=%d, rtable length=%d",
//            var->varno, rtable->length);
            targetEntryRename = VarToString(var, rtable, renameTargets, outBuf,
                outBufSize);
            break;
        }
        case T_Aggref:
//            elog(LOG, "TargetEntryToString: converting Aggref to string...");
            targetEntryRename = AggrefToString(targetEntry, rtable, true,
                outBuf, outBufSize);
            break;
        default:
            elog(WARNING, "Failed to convert TargetEntry to string");
    }

    return targetEntryRename;
}

char *VarToString(Var *var, List *rtable, bool renameVar, char *varBuf,
    size_t varBufSize)
{
    RangeTblEntry *varRte;
    char *tableName, *colName, *renamedColName;
    char *returnedVarName = NULL;

    varRte = rt_fetch(var->varno, rtable);
    tableName = varRte->eref->aliasname;
//    elog(
//        LOG, "VarToString fetched RTE=%s", varRte != NULL ? varRte->eref->aliasname : "NULL");

    if (var->varattno > 0)
    {
//        elog(
//            LOG, "VarToString getting colName from colnames with length=%d and varattno=%d",
//            varRte->eref->colnames != NIL ? varRte->eref->colnames->length : 0,
//            var->varattno);
        colName = renamedColName = get_colname(varRte, var);
//                                        strVal(lfirst(list_nth_cell(varRte->eref->colnames, var->varattno - 1)));
//        elog(LOG, "VarToString: colname=%s", colName);

//        if (renameVar)
//        {
//            elog(
//                    LOG, "VarToString renaming Var. targetEntry->resname=%s", targetEntry->resname);
//        }
    }
    else
    {
        colName = STAR_COL;
        renamedColName = RENAMED_STAR_COL;
    }

    returnedVarName = palloc(sizeof(char) * TARGET_BUFFER_SIZE);
    if (renameVar == true)
    {
//        elog(LOG, "VarToString renaming Var");
        snprintf(returnedVarName, TARGET_BUFFER_SIZE, "%s_%s", tableName,
            renamedColName);
        snprintf(varBuf, varBufSize, "%s.%s AS %s", tableName, colName,
            returnedVarName);
    }
    else
    {
        snprintf(returnedVarName, TARGET_BUFFER_SIZE, "%s", colName);
        snprintf(varBuf, varBufSize, "%s.%s", tableName, colName);
    }

//    elog(LOG, "VarToString result: %s", varBuf);
    return returnedVarName;
}

void ConstToString(Const *constant, char *outputBuf, Size outputBufSize)
{
    elog(
    LOG, "ConstToString: consttype=%d byvalue=%s", constant->consttype,
    constant->constbyval ? "true" : "false");

    if (constant->constisnull)
    {
        strncpy(outputBuf, "null", outputBufSize);
    }
    else
    {
        switch (constant->consttype)
        {
            case INT4OID:
                snprintf(outputBuf, outputBufSize, "%d",
                    DatumGetInt32(constant->constvalue));
                break;
            case INT8OID:
                snprintf(outputBuf, outputBufSize, "%ll",
                    DatumGetInt64(constant->constvalue));
                break;
            case BITOID:
            case NUMERICOID:
            case TEXTOID:
            case UNKNOWNOID:
                // TODO: figure out how to get string Consts
                strncpy(outputBuf, DatumGetCString(constant->constvalue),
                    outputBufSize);
                break;
            default:
                elog(ERROR, "ConstToString failed to parse Const");
        }
    }
}

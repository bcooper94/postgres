/*
 * automatviewselect.h
 *
 *  Created on: Dec 13, 2017
 *      Author: brandon
 */

#ifndef AUTOMATVIEWSELECT_H
#define AUTOMATVIEWSELECT_H

#include "postgres.h"

#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"

void PrintQueryInfo(Query *query);

List *SearchApplicableMatViews(RangeVar *rangeVar);

void AddMatView(IntoClause *into);

#endif

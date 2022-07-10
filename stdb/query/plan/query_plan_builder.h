/*!
 * \file query_plan_builder.h
 */
#ifndef STDB_QUERY_PLAN_QUERY_PLAN_BUILDER_H_
#define STDB_QUERY_PLAN_QUERY_PLAN_BUILDER_H_

#include "stdb/query/plan/query_plan.h"

namespace stdb {
namespace qp {

struct QueryPlanBuilder {
  static std::tuple<common::Status, std::unique_ptr<IQueryPlan> > create(const ReshapeRequest& req);
};

}  // namespace qp
}  // namespaces stdb

#endif  // STDB_QUERY_PLAN_QUERY_PLAN_BUILDER_H_

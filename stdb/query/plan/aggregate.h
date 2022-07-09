/*!
 * \file aggregate.h
 */
#ifndef STDB_QUERY_PLAN_AGGREGATE_H_
#define STDB_QUERY_PLAN_AGGREGATE_H_

#include "stdb/query/plan/materialization_step.h"

namespace stdb {
namespace qp {

/**
 * Aggregate materializer.
 * Accepts the list of ids and the list of aggregate operators.
 * Maps each id to the corresponding operators 1-1.
 * All ids should be different.
 */
struct Aggregate : MaterializationStep {
  std::vector<ParamId> ids_;
  std::vector<AggregationFunction> fn_;
  std::unique_ptr<ColumnMaterializer> mat_;

  template<class IdVec, class FuncVec>
  Aggregate(IdVec&& vec, FuncVec&& fn) :
      ids_(std::forward<IdVec>(vec)),
      fn_(std::forward<FuncVec>(fn)) { }

  boost::property_tree::ptree debug_info() const override {
    boost::property_tree::ptree tree;
    tree.add("name", "Aggregate");
    return tree;
  }

  common::Status apply(ProcessingPrelude *prelude) {
    std::vector<std::unique_ptr<AggregateOperator>> iters;
    auto status = prelude->extract_result(&iters);
    if (!status.IsOk()) {
      return status;
    }
    mat_.reset(new AggregateMaterializer(std::move(ids_), std::move(iters), std::move(fn_)));
    return common::Status::Ok();
  }

  common::Status extract_result(std::unique_ptr<ColumnMaterializer> *dest) {
    if (!mat_) {
      return common::Status::NoData();
    }
    *dest = std::move(mat_);
    return common::Status::Ok();
  }
};

}  // namespace qp
}  // namespace stdb

#endif  // STDB_QUERY_PLAN_AGGREGATE_H_

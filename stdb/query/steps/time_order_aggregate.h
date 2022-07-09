/*!
 * \file time_order_aggregate.h
 */
#ifndef STDB_QUERY_STEPS_TIME_ORDER_AGGREGATER_H_
#define STDB_QUERY_STEPS_TIME_ORDER_AGGREGATER_H_

#include "stdb/query/steps/processing_prelude.h"

namespace stdb {
namespace qp {

struct TimeOrderAggregate : MaterializationStep {
  std::vector<ParamId> ids_;
  std::vector<AggregationFunction> fn_;
  std::unique_ptr<ColumnMaterializer> mat_;

  template<class IdVec, class FnVec>
  TimeOrderAggregate(IdVec&& vec, FnVec&& fn) :
      ids_(std::forward<IdVec>(vec)),
      fn_(std::forward<FnVec>(fn)) { }

  boost::property_tree::ptree debug_info() const {
    boost::property_tree::ptree tree;
    tree.add("name", "TimeOrderAggregate");
    return tree;
  }

  common::Status apply(ProcessingPrelude *prelude) {
    std::vector<std::unique_ptr<AggregateOperator>> iters;
    auto status = prelude->extract_result(&iters);
    if (status != common::Status::Ok()) {
      return status;
    }
    mat_.reset(new TimeOrderAggregateMaterializer(ids_, iters, fn_));
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

#endif  // STDB_QUERY_STEPS_TIME_ORDER_AGGREGATER_H_

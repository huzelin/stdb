/*!
 * \file series_order_aggregate.h
 */
#ifndef STDB_QUERY_STEPS_SERIES_ORDER_AGGREGATER_H_
#define STDB_QUERY_STEPS_SERIES_ORDER_AGGREGATER_H_

#include "stdb/query/steps/materialization_step.h"

namespace stdb {
namespace qp {

/**
 * Merges several group-aggregate operators by chaining
 */
struct SeriesOrderAggregate : MaterializationStep {
  std::vector<ParamId> ids_;
  std::vector<AggregationFunction> fn_;
  std::unique_ptr<ColumnMaterializer> mat_;

  template <class IdVec, class FnVec>
  SeriesOrderAggregate(IdVec&& vec, FnVec&& fn) :
      ids_(std::forward<IdVec>(vec)),
      fn_(std::forward<FnVec>(fn)) { }

  boost::property_tree::ptree debug_info() const override {
    boost::property_tree::ptree tree;
    tree.add("name", "SeriesOrderAggregate");
    return tree;
  }

  common::Status apply(ProcessingPrelude *prelude) {
    std::vector<std::unique_ptr<AggregateOperator>> iters;
    auto status = prelude->extract_result(&iters);
    if (status != common::Status::Ok()) {
      return status;
    }
    mat_.reset(new SeriesOrderAggregateMaterializer(std::move(ids_), std::move(iters), fn_));
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

#endif  // STDB_QUERY_STEPS_SERIES_ORDER_AGGREGATER_H_

/*!
 * \file aggregate_processing_step.h
 */
#ifndef STDB_QUERY_PLAN_AGGREGATE_PROCESSING_STEP_H_
#define STDB_QUERY_PLAN_AGGREGATE_PROCESSING_STEP_H_

#include "stdb/query/plan/processing_prelude.h"

namespace stdb {
namespace qp {

struct AggregateProcessingStep : ProcessingPrelude {
  std::vector<std::unique_ptr<AggregateOperator>> agglist_;
  Timestamp begin_;
  Timestamp end_;
  std::vector<ParamId> ids_;

  template<class T>
  AggregateProcessingStep(Timestamp begin, Timestamp end, T&& t) :
      begin_(begin),
      end_(end),
      ids_(std::forward<T>(t)) { }

  boost::property_tree::ptree debug_info() const override {
    boost::property_tree::ptree tree;
    tree.add("name", "AggregateProcessingStep");
    return tree;
  }

  virtual common::Status apply(const ColumnStore& cstore) {
    return cstore.aggregate(ids_, begin_, end_, &agglist_);
  }

  virtual common::Status extract_result(std::vector<std::unique_ptr<RealValuedOperator>>* dest) {
    return common::Status::NoData();
  }

  virtual common::Status extract_result(std::vector<std::unique_ptr<AggregateOperator>>* dest) {
    if (agglist_.empty()) {
      return common::Status::NoData();
    }
    *dest = std::move(agglist_);
    return common::Status::Ok();
  }

  virtual common::Status extract_result(std::vector<std::unique_ptr<BinaryDataOperator>>* dest) {
    return common::Status::NoData();
  }
};

}  // namespace qp
}  // namespace stdb

#endif  // STDB_QUERY_PLAN_AGGREGATE_PROCESSING_STEP_H_

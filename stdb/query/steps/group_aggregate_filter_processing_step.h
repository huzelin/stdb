/*!
 * \file group_aggregate_filter_processing_step.h
 */
#ifndef STDB_QUERY_STEPS_GROUP_AGGREGATE_FILTER_PROCESSING_STEP_H_
#define STDB_QUERY_STEPS_GROUP_AGGREGATE_FILTER_PROCESSING_STEP_H_

#include "stdb/query/steps/group_aggregate_processing_step.h"

namespace stdb {
namespace qp {

struct GroupAggregateFilterProcessingStep : ProcessingPrelude {
  std::vector<std::unique_ptr<AggregateOperator>> agglist_;
  Timestamp begin_;
  Timestamp end_;
  Timestamp step_;
  std::vector<ParamId> ids_;
  std::map<ParamId, AggregateFilter> filters_;
  AggregationFunction fn_;

  template<class T>
  GroupAggregateFilterProcessingStep(Timestamp begin,
                                     Timestamp end,
                                     Timestamp step,
                                     const std::vector<AggregateFilter>& flt,
                                     T&& t,
                                     AggregationFunction fn = AggregationFunction::FIRST) :
      begin_(begin),
      end_(end),
      step_(step),
      ids_(std::forward<T>(t)),
      fn_(fn) {
    for (size_t ix = 0; ix < ids_.size(); ix++) {
      ParamId id = ids_[ix];
      const auto& filter = flt[ix];
      filters_.insert(std::make_pair(id, filter));
    }
  }

  boost::property_tree::ptree debug_info() const override {
    boost::property_tree::ptree tree;
    tree.add("name", "GroupAggregateFilterProcessingStep");
    return tree;
  }

  virtual common::Status apply(const ColumnStore& cstore) {
    return cstore.group_aggfilter(ids_, begin_, end_, step_, filters_, &agglist_);
  }

  virtual common::Status extract_result(std::vector<std::unique_ptr<RealValuedOperator>>* dest) {
    if (agglist_.empty()) {
      return common::Status::NoData();
    }
    dest->clear();
    for (auto&& it: agglist_) {
      std::unique_ptr<RealValuedOperator> op;
      op.reset(new GroupAggregateConverter(fn_, std::move(it)));
      dest->push_back(std::move(op));
    }
    agglist_.clear();
    return common::Status::Ok();
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

#endif  // STDB_QUERY_STEPS_GROUP_AGGREGATE_FILTER_PROCESSING_STEP_H_

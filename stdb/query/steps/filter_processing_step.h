/*!
 * \file filter_processing_step.h
 */
#ifndef STDB_QUERY_STEPS_FILTER_PROCESSING_STEP_H_
#define STDB_QUERY_STEPS_FILTER_PROCESSING_STEP_H_

#include "stdb/query/steps/processing_prelude.h"

namespace stdb {
namespace qp {

struct FilterProcessingStep : ProcessingPrelude {
  std::vector<std::unique_ptr<RealValuedOperator>> scanlist_;
  Timestamp begin_;
  Timestamp end_;
  std::map<ParamId, ValueFilter> filters_;
  std::vector<ParamId> ids_;

  template<class T>
  FilterProcessingStep(Timestamp begin,
                       Timestamp end,
                       const std::vector<ValueFilter>& flt,
                       T&& t) :
      begin_(begin),
      end_(end),
      filters_(),
      ids_(std::forward<T>(t)) {
    for (size_t ix = 0; ix < ids_.size(); ix++) {
      ParamId id = ids_[ix];
      const ValueFilter& filter = flt[ix];
      filters_.insert(std::make_pair(id, filter));
    }
  }

  boost::property_tree::ptree debug_info() const override {
    boost::property_tree::ptree tree;
    tree.add("name", "FilterProcessingStep");
    tree.add("begin", begin_);
    tree.add("end", end_);

    boost::property_tree::ptree array;
    for (auto id : ids_) {
      boost::property_tree::ptree elem;
      elem.add("id", id);
      auto iter = filters_.find(id);
      elem.add("filter", iter->second.debug_string());
      array.push_back(boost::property_tree::ptree::value_type("", elem));
    }
    tree.add_child("ids", array);
    return tree;
  }

  virtual common::Status apply(const ColumnStore& cstore) {
    return cstore.filter(ids_, begin_, end_, filters_, &scanlist_);
  }

  virtual common::Status extract_result(std::vector<std::unique_ptr<RealValuedOperator>>* dest) {
    if (scanlist_.empty()) {
      return common::Status::NoData();
    }
    *dest = std::move(scanlist_);
    return common::Status::Ok();
  }

  virtual common::Status extract_result(std::vector<std::unique_ptr<AggregateOperator>>* dest) {
    return common::Status::NoData();
  }

  virtual common::Status extract_result(std::vector<std::unique_ptr<BinaryDataOperator>>* dest) {
    return common::Status::NoData();
  }
};

}  // namespace qp
}  // namespace stdb

#endif  // STDB_QUERY_STEPS_FILTER_PROCESSING_STEP_H_

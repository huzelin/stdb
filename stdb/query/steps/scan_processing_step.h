/*!
 * \file scan_processing_step.h
 */
#ifndef STDB_QUERY_STEPS_SCAN_PROCESSING_STEP_H_
#define STDB_QUERY_STEPS_SCAN_PROCESSING_STEP_H_

#include "stdb/query/steps/processing_prelude.h"

namespace stdb {
namespace qp {

struct ScanProcessingStep : ProcessingPrelude {
  std::vector<std::unique_ptr<RealValuedOperator>> scanlist_;
  Timestamp begin_;
  Timestamp end_;
  std::vector<ParamId> ids_;

  template<class T>
  ScanProcessingStep(Timestamp begin, Timestamp end, T&& t) :
      begin_(begin),
      end_(end),
      ids_(std::forward<T>(t)) { }

  boost::property_tree::ptree debug_info() const override {
    boost::property_tree::ptree tree;
    tree.add("name", "ScanProcessingStep");
    boost::property_tree::ptree array;
    for (auto id : ids_) {
      boost::property_tree::ptree elem;
      elem.add("id", id);
      array.push_back(boost::property_tree::ptree::value_type("", elem));
    }
    tree.add_child("ids", array);
    tree.add("begin", std::to_string(begin_));
    tree.add("end", std::to_string(end_));
    return tree;
  }

  virtual common::Status apply(const ColumnStore& cstore) {
    return cstore.scan(ids_, begin_, end_, &scanlist_);
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

#endif  // STDB_QUERY_STEPS_SCAN_PROCESSING_STEP_H_

/*!
 * \file scan_events_processing_step.h
 */
#ifndef STDB_QUERY_PLAN_SCAN_EVENTS_PROCESSING_STEP_H_
#define STDB_QUERY_PLAN_SCAN_EVENTS_PROCESSING_STEP_H_

#include "stdb/query/plan/processing_prelude.h"

namespace stdb {
namespace qp {

struct ScanEventsProcessingStep : ProcessingPrelude {
  std::vector<std::unique_ptr<BinaryDataOperator>> scanlist_;
  Timestamp begin_;
  Timestamp end_;
  std::vector<ParamId> ids_;
  std::string regex_;

  //! C-tor (1), create scan without filter
  template<class T>
  ScanEventsProcessingStep(Timestamp begin, Timestamp end, T&& t) :
      begin_(begin),
      end_(end),
      ids_(std::forward<T>(t)) { }

  //! C-tor (2), create scan with filter
  template<class T>
  ScanEventsProcessingStep(Timestamp begin, Timestamp end, const std::string& exp, T&& t) :
      begin_(begin),
      end_(end),
      ids_(std::forward<T>(t)),
      regex_(exp) { }

  boost::property_tree::ptree debug_info() const override {
    boost::property_tree::ptree tree;
    tree.add("name", "ScanEventsProcessingStep");
    return tree;
  }

  virtual common::Status apply(const ColumnStore& cstore) {
    if (!regex_.empty()) {
      return cstore.filter_events(ids_, begin_, end_, regex_, &scanlist_);
    }
    return cstore.scan_events(ids_, begin_, end_, &scanlist_);
  }

  virtual common::Status extract_result(std::vector<std::unique_ptr<BinaryDataOperator>>* dest) {
    if (scanlist_.empty()) {
      return common::Status::NoData();
    }
    *dest = std::move(scanlist_);
    return common::Status::Ok();
  }

  virtual common::Status extract_result(std::vector<std::unique_ptr<RealValuedOperator>>* dest) {
    return common::Status::NoData();
  }

  virtual common::Status extract_result(std::vector<std::unique_ptr<AggregateOperator>>* dest) {
    return common::Status::NoData();
  }
};

}  // namespace qp
}  // namespace stdb

#endif  // STDB_QUERY_PLAN_SCAN_EVENTS_PROCESSING_STEP_H_

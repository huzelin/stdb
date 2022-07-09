/*!
 * \file group_aggregate_processing_step.h
 */
#ifndef STDB_QUERY_STEPS_GROUP_AGGREGATE_PROCESSING_STEP_H_
#define STDB_QUERY_STEPS_GROUP_AGGREGATE_PROCESSING_STEP_H_

#include "stdb/query/steps/processing_prelude.h"

namespace stdb {
namespace qp {

// Convert AggregateOperator into RealValuedOperator
class GroupAggregateConverter : public RealValuedOperator {
  std::unique_ptr<AggregateOperator> op_;
  AggregationFunction func_;
 
 public:
  GroupAggregateConverter(AggregationFunction func, std::unique_ptr<AggregateOperator> op) :
      op_(std::move(op)), func_(func) { }

  std::tuple<common::Status, size_t> read(Timestamp *destts, double *destval, size_t size) override {
    size_t pos = 0;
    while (pos < size) {
      common::Status status;
      size_t ressz;
      Timestamp ts;
      AggregationResult xs;
      std::tie(status, ressz) = op_->read(&ts, &xs, 1);
      if (ressz == 1) {
        destts[pos] = ts;
        destval[pos] = TupleOutputUtils::get(xs, func_);
        pos++;
      } else if (status.IsOk() || status.Code() == common::Status::kNoData) {
        return std::make_tuple(status, pos);
      } else {
        return std::make_tuple(status, 0);
      }
    }
    return std::make_tuple(common::Status::Ok(), pos);
  }

  Direction get_direction() override {
    return AggregateOperator::Direction::FORWARD == op_->get_direction()
        ? RealValuedOperator::Direction::FORWARD
        : RealValuedOperator::Direction::BACKWARD;
  }
};

struct GroupAggregateProcessingStep : ProcessingPrelude {
  std::vector<std::unique_ptr<AggregateOperator>> agglist_;
  Timestamp begin_;
  Timestamp end_;
  Timestamp step_;
  std::vector<ParamId> ids_;
  AggregationFunction fn_;

  template<class T>
  GroupAggregateProcessingStep(Timestamp begin, Timestamp end, Timestamp step, T&& t, AggregationFunction fn = AggregationFunction::FIRST) :
      begin_(begin),
      end_(end),
      step_(step),
      ids_(std::forward<T>(t)),
      fn_(fn) { }

  boost::property_tree::ptree debug_info() const override {
    boost::property_tree::ptree tree;
    tree.add("name", "GroupAggregateProcessingStep");
    return tree;
  }

  virtual common::Status apply(const ColumnStore& cstore) {
    return cstore.group_aggregate(ids_, begin_, end_, step_, &agglist_);
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

#endif  // STDB_QUERY_STEPS_GROUP_AGGREGATE_PROCESSING_STEP_H_

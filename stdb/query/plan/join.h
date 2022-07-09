/*!
 * \file join.h
 */
#ifndef STDB_QUERY_PLAN_JOIN_H_
#define STDB_QUERY_PLAN_JOIN_H_

#include "stdb/query/plan/materialization_step.h"

namespace stdb {
namespace qp {

/**
 * Joins several operators into one.
 * Number of joined operators is defined by the cardinality.
 * Number of ids should be `cardinality` times smaller than number
 * of operators because every `cardinality` operators are joined into
 * one.
 */
struct Join : MaterializationStep {
  std::vector<ParamId> ids_;
  int cardinality_;
  OrderBy order_;
  Timestamp begin_;
  Timestamp end_;
  std::unique_ptr<ColumnMaterializer> mat_;

  template<class IdVec>
  Join(IdVec&& vec, int cardinality, OrderBy order, Timestamp begin, Timestamp end) :
      ids_(std::forward<IdVec>(vec)),
      cardinality_(cardinality),
      order_(order),
      begin_(begin),
      end_(end) { }

  boost::property_tree::ptree debug_info() const override {
    boost::property_tree::ptree tree;
    tree.add("name", "Join");
    return tree;
  }

  common::Status apply(ProcessingPrelude *prelude) {
    int inc = cardinality_;
    std::vector<std::unique_ptr<RealValuedOperator>> scanlist;
    auto status = prelude->extract_result(&scanlist);
    if (status != common::Status::Ok()) {
      return status;
    }
    std::vector<std::unique_ptr<ColumnMaterializer>> iters;
    for (size_t i = 0; i < ids_.size(); i++) {
      // ids_ contain ids of the joined series that corresponds
      // to the names in the series matcher
      std::vector<std::unique_ptr<RealValuedOperator>> joined;
      std::vector<ParamId> ids;
      for (int j = 0; j < inc; j++) {
        // `inc` number of storage level operators correspond to one
        // materializer
        auto ix = i*inc + j;
        joined.push_back(std::move(scanlist.at(ix)));
        ids.push_back(ix);
      }
      std::unique_ptr<ColumnMaterializer> it;
      it.reset(new JoinMaterializer(std::move(ids), std::move(joined), ids_.at(i)));
      iters.push_back(std::move(it));
    }
    if (order_ == OrderBy::SERIES) {
      mat_.reset(new JoinConcatMaterializer(std::move(iters)));
    } else {
      bool forward = begin_ < end_;
      typedef MergeJoinMaterializer<MergeJoinUtil::OrderByTimestamp> Materializer;
      mat_.reset(new Materializer(std::move(iters), forward));
    }
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

#endif  // STDB_QUERY_PLAN_JOIN_H_

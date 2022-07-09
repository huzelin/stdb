/*!
 * \file aggregate_combiner.h
 */
#ifndef STDB_QUERY_STEPS_AGGREGATE_COMBINER_H_
#define STDB_QUERY_STEPS_AGGREGATE_COMBINER_H_

#include "stdb/query/steps/materialization_step.h"

namespace stdb {
namespace qp {

/**
 * Combines the aggregate operators.
 * Accepts list of ids (shouldn't be different) and list of aggregate
 * operators. Maps each id to operator and then combines operators
 * with the same id (used to implement aggregate + group-by).
 */
struct AggregateCombiner : MaterializationStep {
  std::vector<ParamId> ids_;
  std::vector<AggregationFunction> fn_;
  std::unique_ptr<ColumnMaterializer> mat_;

  template <class IdVec, class FuncVec>
  AggregateCombiner(IdVec&& vec, FuncVec&& fn) :
      ids_(std::forward<IdVec>(vec)),
      fn_(std::forward<FuncVec>(fn)) { }

  boost::property_tree::ptree debug_info() const override {
    boost::property_tree::ptree tree;
    tree.add("name", "AggregateCombiner");
    return tree;
  }

  common::Status apply(ProcessingPrelude *prelude) {
    std::vector<std::unique_ptr<AggregateOperator>> iters;
    auto status = prelude->extract_result(&iters);
    if (status != common::Status::Ok()) {
      return status;
    }
    std::vector<std::unique_ptr<AggregateOperator>> agglist;
    std::map<ParamId, std::vector<std::unique_ptr<AggregateOperator>>> groupings;
    std::map<ParamId, AggregationFunction> functions;
    for (size_t i = 0; i < ids_.size(); i++) {
      auto id = ids_.at(i);
      auto it = std::move(iters.at(i));
      groupings[id].push_back(std::move(it));
      functions[id] = fn_.at(i);
    }
    std::vector<ParamId> ids;
    std::vector<AggregationFunction> fns;
    for (auto& kv: groupings) {
      auto& vec = kv.second;
      ids.push_back(kv.first);
      std::unique_ptr<CombineAggregateOperator> it(new CombineAggregateOperator(std::move(vec)));
      agglist.push_back(std::move(it));
      fns.push_back(functions[kv.first]);
    }
    mat_.reset(new AggregateMaterializer(std::move(ids),
                                         std::move(agglist),
                                         std::move(fns)));
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

#endif  // STDB_QUERY_STEPS_AGGREGATE_COMBINER_H_

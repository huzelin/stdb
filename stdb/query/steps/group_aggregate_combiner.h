/*!
 * \file group_aggregate_combiner.h
 */
#ifndef STDB_QUERY_STEPS_GROUP_AGGREGATER_COMBINER_H_
#define STDB_QUERY_STEPS_GROUP_AGGREGATER_COMBINER_H_

#include "stdb/query/steps/materialization_step.h"

namespace stdb {
namespace qp {

template <OrderBy order>
struct GroupAggregateCombiner_Initializer;

template <>
struct GroupAggregateCombiner_Initializer<OrderBy::SERIES> {
  static std::unique_ptr<ColumnMaterializer> make_materializer(
      std::vector<ParamId>&& ids,
      std::vector<std::unique_ptr<AggregateOperator>>&& agglist,
      const std::vector<AggregationFunction>& fn) {
    std::unique_ptr<ColumnMaterializer> mat;
    mat.reset(new SeriesOrderAggregateMaterializer(std::move(ids), std::move(agglist), fn));
    return mat;
  }
};

template <>
struct GroupAggregateCombiner_Initializer<OrderBy::TIME> {
  static std::unique_ptr<ColumnMaterializer> make_materializer(
      std::vector<ParamId>&& ids,
      std::vector<std::unique_ptr<AggregateOperator>>&& agglist,
      const std::vector<AggregationFunction>& fn) {
    std::vector<ParamId> tmpids(ids);
    std::vector<std::unique_ptr<AggregateOperator>> tmpiters(std::move(agglist));
    std::unique_ptr<ColumnMaterializer> mat;
    mat.reset(new TimeOrderAggregateMaterializer(tmpids, tmpiters, fn));
    return mat;
  }
};

/**
 * Combines the group-aggregate operators.
 * Accepts list of ids (shouldn't be different) and list of aggregate
 * operators. Maps each id to operator and then combines operators
 * with the same id (to implement group-aggregate + group/pivot-by-tag).
 */
template <OrderBy order>
struct GroupAggregateCombiner : MaterializationStep {
  std::vector<ParamId> ids_;
  std::vector<AggregationFunction> fn_;
  std::unique_ptr<ColumnMaterializer> mat_;

  template<class IdVec, class FuncVec>
  GroupAggregateCombiner(IdVec&& vec, FuncVec&& fn) :
      ids_(std::forward<IdVec>(vec)),
      fn_(std::forward<FuncVec>(fn)) { }

  boost::property_tree::ptree debug_info() const override {
    boost::property_tree::ptree tree;
    tree.add("name", "GroupAggregateCombiner");
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
    for (size_t i = 0; i < ids_.size(); i++) {
      auto id = ids_.at(i);
      auto it = std::move(iters.at(i));
      groupings[id].push_back(std::move(it));
    }
    std::vector<ParamId> ids;
    for (auto& kv: groupings) {
      auto& vec = kv.second;
      ids.push_back(kv.first);
      std::unique_ptr<FanInAggregateOperator> it(new FanInAggregateOperator(std::move(vec)));
      agglist.push_back(std::move(it));
    }
    mat_ = GroupAggregateCombiner_Initializer<order>::make_materializer(std::move(ids),
                                                                        std::move(agglist),
                                                                        fn_);
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

#endif  // STDB_QUERY_STEPS_GROUP_AGGREGATER_COMBINER_H_

/*!
 * \file mergeby.h
 */
#ifndef STDB_QUERY_PLAN_MERGEBY_H_
#define STDB_QUERY_PLAN_MERGEBY_H_

#include "stdb/query/plan/materialization_step.h"

namespace stdb {
namespace qp {

namespace detail {

template <OrderBy order, class OperatorT>
struct MergeMaterializerTraits;

template <>
struct MergeMaterializerTraits<OrderBy::SERIES, RealValuedOperator> {
  typedef MergeMaterializer<SeriesOrder> Materializer;
};

template <>
struct MergeMaterializerTraits<OrderBy::TIME, RealValuedOperator> {
  typedef MergeMaterializer<TimeOrder> Materializer;
};

template <>
struct MergeMaterializerTraits<OrderBy::SERIES, BinaryDataOperator> {
  typedef MergeEventMaterializer<EventSeriesOrder> Materializer;
};

template <>
struct MergeMaterializerTraits<OrderBy::TIME, BinaryDataOperator> {
  typedef MergeEventMaterializer<EventTimeOrder> Materializer;
};

}  // namespace detail

/**
 * Merge several series (order by series).
 * Used in scan query.
 */
template <OrderBy order, class OperatorT = RealValuedOperator>
struct MergeBy : MaterializationStep {
  std::vector<ParamId> ids_;
  std::unique_ptr<ColumnMaterializer> mat_;

  template<class IdVec>
  MergeBy(IdVec&& ids) : ids_(std::forward<IdVec>(ids)) { }

  boost::property_tree::ptree debug_info() const override {
    boost::property_tree::ptree tree;
    tree.add("name", "MergeBy");
    boost::property_tree::ptree array;
    for (auto id : ids_) {
      boost::property_tree::ptree elem;
      elem.add("id", id);
      array.push_back(boost::property_tree::ptree::value_type("", elem));
    }
    tree.add_child("ids", array);
    switch (order) {
      case OrderBy::SERIES: {
        tree.add("order", "SERIES");
      } break;
      case OrderBy::TIME: {
        tree.add("orderby", "TIME");
      } break;
    }
    return tree;
  }

  common::Status apply(ProcessingPrelude* prelude) {
    std::vector<std::unique_ptr<OperatorT>> iters;
    auto status = prelude->extract_result(&iters);
    if (!status.IsOk()) {
      return status;
    }
    typedef typename detail::MergeMaterializerTraits<order, OperatorT>::Materializer Merger;
    mat_.reset(new Merger(std::move(ids_), std::move(iters)));
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

#endif  // STDB_QUERY_PLAN_MERGEBY_H_

/*!
 * \file chain.h
 */
#ifndef STDB_QUERY_STEPS_CHAIN_H_
#define STDB_QUERY_STEPS_CHAIN_H_

#include "stdb/query/steps/materialization_step.h"

namespace stdb {
namespace qp {

namespace detail {

template <class OperatorT>
struct ChainMaterializerTraits;

template <>
struct ChainMaterializerTraits<RealValuedOperator> {
  typedef ChainMaterializer Materializer;
};

template <>
struct ChainMaterializerTraits<BinaryDataOperator> {
  typedef EventChainMaterializer Materializer;
};

}  // namespace detail

template <class OperatorT = RealValuedOperator>
struct Chain : MaterializationStep {
  std::vector<ParamId> ids_;
  std::unique_ptr<ColumnMaterializer> mat_;

  template<class IdVec>
  Chain(IdVec&& vec) : ids_(std::forward<IdVec>(vec)) { }

  boost::property_tree::ptree debug_info() const override {
    boost::property_tree::ptree tree;
    tree.add("name", "Chain");
    return tree;
  }

  common::Status apply(ProcessingPrelude *prelude) {
    std::vector<std::unique_ptr<OperatorT>> iters;
    auto status = prelude->extract_result(&iters);
    if (!status.IsOk()) {
      return status;
    }
    typedef typename detail::ChainMaterializerTraits<OperatorT>::Materializer Materializer;
    mat_.reset(new Materializer(std::move(ids_), std::move(iters)));
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

#endif  // STDB_QUERY_STEPS_CHAIN_H_

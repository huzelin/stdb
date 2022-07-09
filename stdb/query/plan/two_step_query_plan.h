/*!
 * \file two_step_query_plan.h
 */
#ifndef STDB_QUERY_PLAN_TWO_STEP_QUERY_PLAN_H_
#define STDB_QUERY_PLAN_TWO_STEP_QUERY_PLAN_H_

#include "stdb/query/plan/query_plan.h"
#include "stdb/query/steps/processing_prelude.h"
#include "stdb/query/steps/materialization_step.h"

namespace stdb {
namespace qp {

struct TwoStepQueryPlan : IQueryPlan {
  std::unique_ptr<ProcessingPrelude> prelude_;
  std::unique_ptr<MaterializationStep> mater_;
  std::unique_ptr<ColumnMaterializer> column_;

  template<class T1, class T2>
  TwoStepQueryPlan(T1&& t1, T2&& t2)
        : prelude_(std::forward<T1>(t1))
        , mater_(std::forward<T2>(t2)) { }

  boost::property_tree::ptree debug_info() const override {
    boost::property_tree::ptree tree;
    tree.add("name", "TwoStepQueryPlan");
    tree.add_child("processing_prelude", prelude_->debug_info());
    tree.add_child("materialization_step", mater_->debug_info());
    return tree;
  }

  common::Status execute(const ColumnStore &cstore) {
    auto status = prelude_->apply(cstore);
    if (status != common::Status::Ok()) {
      return status;
    }
    status = mater_->apply(prelude_.get());
    if (status != common::Status::Ok()) {
      return status;
    }
    return mater_->extract_result(&column_);
  }

  std::tuple<common::Status, size_t> read(u8 *dest, size_t size) {
    if (!column_) {
      LOG(FATAL) << "Successful execute step required";
    }
    return column_->read(dest, size);
  }
};

}  // namespace qp
}  // namespaces stdb

#endif  // STDB_QUERY_PLAN_TWO_STEP_QUERY_PLAN_H_

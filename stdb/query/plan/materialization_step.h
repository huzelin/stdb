/*!
 * \file materialization_step.h
 */
#ifndef STDB_QUERY_PLAN_MATERIALIZATION_STEP_H_
#define STDB_QUERY_PLAN_MATERIALIZATION_STEP_H_

#include "stdb/query/plan/processing_prelude.h"

namespace stdb {
namespace qp {

/**
 * Tier-N operator (materializer)
 */
struct MaterializationStep {
  virtual ~MaterializationStep() = default;

  //! Compute processing step result (list of low level operators)
  virtual common::Status apply(ProcessingPrelude* prelude) = 0;

  /**
   * Get result of the processing step, this method should add cardinality() elements
   * to the `dest` array.
   */
  virtual common::Status extract_result(std::unique_ptr<ColumnMaterializer>* dest) = 0;

  /**
   * Get the debug info of MaterializationStep
   */
  virtual boost::property_tree::ptree debug_info() const = 0;
};

}  // namespace qp
}  // namespace stdb

#endif  // STDB_QUERY_PLAN_MATERIALIZATION_STEP_H_

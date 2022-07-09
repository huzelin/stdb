/*!
 * \file processing_prelude.h
 */
#ifndef STDB_QUERY_STEPS_PROCESSING_PRELUDE_H_
#define STDB_QUERY_STEPS_PROCESSING_PRELUDE_H_

#include "stdb/storage/nbtree.h"
#include "stdb/storage/column_store.h"
#include "stdb/storage/operators/operator.h"
#include "stdb/storage/operators/scan.h"
#include "stdb/storage/operators/merge.h"
#include "stdb/storage/operators/aggregate.h"
#include "stdb/storage/operators/join.h"

namespace stdb {
namespace qp {

using namespace storage;

/**
 * Tier-1 operator
 */
struct ProcessingPrelude {
  virtual ~ProcessingPrelude() = default;
  //! Compute processing step result (list of low level operators)
  virtual common::Status apply(const ColumnStore& cstore) = 0;
  //! Get result of the processing step
  virtual common::Status extract_result(std::vector<std::unique_ptr<RealValuedOperator>>* dest) = 0;
  //! Get result of the processing step
  virtual common::Status extract_result(std::vector<std::unique_ptr<AggregateOperator>>* dest) = 0;
  //! Get result of the processing step
  virtual common::Status extract_result(std::vector<std::unique_ptr<BinaryDataOperator>>* dest) = 0;
  //! Get debug info of the processing step
  virtual boost::property_tree::ptree debug_info() const = 0;
};

}  // namespace qp
}  // namespace stdb

#endif  // STDB_QUERY_STEPS_PROCESSING_PRELUDE_H_

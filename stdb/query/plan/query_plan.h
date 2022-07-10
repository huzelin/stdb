/*!
 * \file query_plan.h
 */
#ifndef STDB_QUERY_PLAN_QUERY_PLAN_H_
#define STDB_QUERY_PLAN_QUERY_PLAN_H_

#include <memory>
#include <vector>

#include "stdb/index/seriesparser.h"
#include "stdb/query/queryprocessor_framework.h"
#include "stdb/storage/column_store.h"

namespace stdb {
namespace qp {

/**
 * Query plan interface
 */
struct IQueryPlan {
  virtual ~IQueryPlan() = default;

  virtual boost::property_tree::ptree debug_info() const = 0;

  /**
   * Execute query plan.
   * Data can be fetched after successful execute call.
   */
  virtual common::Status execute(const storage::ColumnStore& cstore) = 0;

  /** Read samples in batch.
   * Samples can be of variable size.
   * @param dest is a pointer to buffer that will receive series of Sample values
   * @param size is a size of the buffer in bytes
   * @return status of the operation (success or error code) and number of written bytes
   */
  virtual std::tuple<common::Status, size_t> read(u8 *dest, size_t size) = 0;
};

struct QueryPlanExecutor {
  void execute(const storage::ColumnStore& cstore, std::unique_ptr<IQueryPlan>&& iter, IStreamProcessor& qproc);
};

}  // namespace qp
}  // namespaces stdb

#endif  // STDB_QUERY_PLAN_QUERY_PLAN_H_

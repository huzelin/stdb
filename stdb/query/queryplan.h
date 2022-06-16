/*!
 * \file queryplan.h
 */
#ifndef FASTSTDB_QUERY_QUERY_PLAN_H_
#define FASTSTDB_QUERY_QUERY_PLAN_H_

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

  virtual std::string debug_string() const = 0;

  /**
   * Execute query plan.
   * Data can be fetched after successful execute call.
   */
  virtual common::Status execute(const storage::ColumnStore& cstore) = 0;

  /** Read samples in batch.
   * Samples can be of variable size.
   * @param dest is a pointer to buffer that will receive series of aku_Sample values
   * @param size is a size of the buffer in bytes
   * @return status of the operation (success or error code) and number of written bytes
   */
  virtual std::tuple<common::Status, size_t> read(u8 *dest, size_t size) = 0;
};

struct QueryPlanBuilder {
  static std::tuple<common::Status, std::unique_ptr<IQueryPlan> > create(const ReshapeRequest& req);
};

struct QueryPlanExecutor {
  void execute(const storage::ColumnStore& cstore, std::unique_ptr<qp::IQueryPlan>&& iter, qp::IStreamProcessor& qproc);
};

}  // namespace qp
}  // namespaces stdb

#endif  // FASTSTDB_QUERY_QUERY_PLAN_H_

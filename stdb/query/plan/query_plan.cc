/*!
 * \file query_plan.cc
 */
#include "stdb/query/plan/query_plan.h"

namespace stdb {
namespace qp {

void QueryPlanExecutor::execute(
    const storage::ColumnStore& cstore,
    std::unique_ptr<qp::IQueryPlan>&& iter,
    qp::IStreamProcessor& qproc) {
  common::Status status = iter->execute(cstore);
  if (status != common::Status::Ok()) {
    LOG(ERROR) << "Query plan error " << status.ToString();
    qproc.set_error(status);
    return;
  }
  const size_t dest_size = 0x1000;
  std::vector<u8> dest;
  dest.resize(dest_size);
  while (status == common::Status::Ok()) {
    size_t size;
    // This is OK because normal query (aggregate or select) will write fixed size samples with size = sizeof(Sample).
    //
    std::tie(status, size) = iter->read(reinterpret_cast<u8*>(dest.data()), dest_size);
    if (status != common::Status::Ok() &&
        (status != common::Status::NoData() && status != common::Status::Unavailable())) {
      LOG(ERROR) << "Iteration error " << status.ToString();
      qproc.set_error(status);
      return;
    }

    size_t pos = 0;
    while (pos < size) {
      Sample const* sample = reinterpret_cast<Sample const*>(dest.data() + pos);
      if (!qproc.put(*sample)) {
        LOG(INFO) << "Iteration stopped by client";
        return;
      }
      pos += sample->payload.size;
    }
  }
}

}  // namespace qp
}  // namespace stdb

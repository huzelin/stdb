/*!
 * \file recovery_vistor.h
 */
#ifndef STDB_CORE_RECOVERY_VISITOR_H_
#define STDB_CORE_RECOVERY_VISITOR_H_

#include "stdb/common/basic.h"
#include "stdb/storage/input_log.h"

namespace stdb {

class Database;

// The recovery visitor.
struct RecoveryVistor : boost::static_vistor<bool> {
  ServerDatabase* database;
  ParamId curr_id;
  std::unordered_map<ParamId, std::vector<storage::LogicAddr>>* mapping;
  storage::LogicAddr top_addr;
  std::vector<ParamId>* restored_ids;

  WorkerDatabase* worker_database;
  Sample sample;
  std::unordered_set<ParamId> updated_ids;
  u64 nsamples = 0;
  u64 nslot = 0;

  /** Should be called for each input-log record
   * before using as a visitor
   */
  void reset(ParamId id) {
    curr_id = id;

    sample = { };
    sample.paramid = id;
  }

  bool operator()(const storage::InputLogDataPoint&);
  bool operator()(const storage::InputLogSeriesName& sname);
  bool operator()(const storage::InputLogRecoveryInfo& rinfo);
};

}  // namespace stdb

#endif  // STDB_CORE_RECOVERY_VISITOR_H_

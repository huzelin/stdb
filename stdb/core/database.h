/*!
 * \file database.h
 */
#ifndef STDB_CORE_DATABASE_H_
#define STDB_CORE_DATABASE_H_

#include <memory>
#include <future>

#include "stdb/common/basic.h"
#include "stdb/core/database_session.h"
#include "stdb/core/sync_waiter.h"
#include "stdb/query/queryparser.h"
#include "stdb/index/seriesparser.h"
#include "stdb/storage/input_log.h"
#include "stdb/storage/nbtree.h"
#include "stdb/storage/volume.h"

namespace stdb {

// InputLog visitor will call database's callback
class ServerRecoveryVisitor;
class WorkerRecoveryVisitor;

class Database {
 protected:
  bool is_moving_ = false;
  std::shared_ptr<storage::ShardedInputLog> inputlog_;
  std::string input_log_path_;

  // Whether wal recover is enabled.
  bool wal_recovery_is_enabled(const FineTuneParams &params, int* ccr);

 public:
  explicit Database(bool is_moving) : is_moving_(is_moving) { }

  // Get current input log
  storage::InputLog* get_input_log();

  // set input log
  void set_input_log(std::shared_ptr<storage::ShardedInputLog> inputlog, const std::string& input_log_path);

  // Return inputlog related.
  std::shared_ptr<storage::ShardedInputLog> inputlog() const { return inputlog_; }
  const std::string& input_log_path() const { return input_log_path_; }

  bool is_moving() const { return is_moving_; }

  // Initialize database, run recovery and initializa inputlog
  virtual void initialize(const FineTuneParams& params);
  
  // Close operation
  virtual void close() { }
  // Sync operation
  virtual void sync() { }

  // Create database session
  virtual std::shared_ptr<DatabaseSession> create_session() {
    LOG(FATAL) << "not implement create session";
    return nullptr;
  }

  friend class ServerRecoveryVisitor;
  friend class WorkerRecoveryVisitor;

 protected:
  // -- Recovery interface.
  // Return series matcher
  virtual SeriesMatcher* global_matcher() {
    LOG(FATAL) << "Not imeplemented global_matcher interface";
    return nullptr;
  }

  // Create new column store.
  virtual void recovery_create_new_column(ParamId id) { }

  // Update rescue points
  virtual void recovery_update_rescue_points(ParamId id, const std::vector<storage::LogicAddr>& addrs) { }

  // Recovery write.
  virtual storage::NBTreeAppendResult recovery_write(Sample const& sample, bool allow_duplicates) {
    return storage::NBTreeAppendResult::FAIL_BAD_VALUE;
  }

  std::tuple<common::Status, std::string> parse_query(
      boost::property_tree::ptree const& ptree,
      qp::ReshapeRequest*                req);
};

}  // namespace stdb

#endif  // STDB_CORE_DATABASE_H_

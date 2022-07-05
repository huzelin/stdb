/*!
 * \file database.h
 */
#ifndef STDB_CORE_DATABASE_H_
#define STDB_CORE_DATABASE_H_

#include "stdb/common/basic.h"
#include "stdb/storage/input_log.h"

namespace stdb {

struct SessionWaiter {
  std::vector<std::promise<void>> sessions_await_list;
  std::mutex session_lock;

  void add_sync_barrier(std::promise<void>&& barrier) {
    std::lock_guard<std::mutex> lock(session_lock);
    sessions_wait_list.push_back(std::move(barrier));
  }

  void notify_all() {
    std::lock_guard<std::mutex> lck(session_lock);
    for (auto& it : session_await_list) {
      it.set_value();
    }
    session_await_list.clear();
  }
};

class Database {
 protected:
  std::shared_ptr<storage::ShardedInputLog> inputlog_;
  std::string input_log_path_;

  // Initialize input log
  void initialize_input_log(const FineTuneParams& params);

  // Whether run recover is enabled.
  bool run_recovery_is_enabled(const FineTuneParams &params);

  // Get current input log
  storage::InputLog* get_input_log();

  // Create new column store.
  virtual void recovery_create_new_column(ParamId id) { }

  // Update rescue points
  virtual void recovery_update_rescue_points(ParamId id, const std::vector<storage::LogicAddr>& addrs) { }

  // Recovery write.
  virtual storage::NBTreeAppendResult recovery_write(Sample const& sample, bool allow_duplicates) { }
};

}  // namespace stdb

#endif  // STDB_CORE_DATABASE_H_

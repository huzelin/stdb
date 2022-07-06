/*!
 * \file database_manager.h
 */
#ifndef STDB_CORE_CONTROLLER_H_
#define STDB_CORE_CONTROLLER_H_

#include <atomic>
#include <unordered_map>
#include <vector>

#include "stdb/common/basic.h"
#include "stdb/common/singleton.h"
#include "stdb/core/database.h"
#include "stdb/metastorage/synchronization.h"

#include <boost/thread.hpp>

namespace stdb {

class Controller : public common::Singleton<Controller> {
 public:
  Controller();

  common::Status register_database(const std::string& db_name, std::shared_ptr<Database> database);
  std::shared_ptr<Database> get_database(const std::string& db_name);
  
  std::shared_ptr<Synchronization> synchronization() { return synchronization_; }
  std::shared_ptr<SessionWaiter> session_waiter() { return session_waiter_; }

  void close();

 protected:
  void start_sync_worker();
  void start_sync_worker_loop();
  void sync_action();
  std::vector<std::shared_ptr<Database>> get_alldatabases();

  std::atomic<int> done_;
  boost::barrier close_barrier_;
  std::mutex mutex_;
  std::vector<std::shared_ptr<Database>> database_;
  std::unordered_map<std::string, u32> name2index_;
  std::shared_ptr<Synchronization> synchronization_;
  std::shared_ptr<SessionWaiter> session_waiter_;
};

}  // namespace stdb

#endif  // STDB_CORE_CONTROLLER_H_

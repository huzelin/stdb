/*!
 * \file database_manager.h
 */
#ifndef STDB_CORE_CONTROLLER_H_
#define STDB_CORE_CONTROLLER_H_

#include <atomic>
#include <unordered_map>
#include <vector>

#include "stdb/common/apr_utils.h"
#include "stdb/common/basic.h"
#include "stdb/common/singleton.h"
#include "stdb/core/database.h"
#include "stdb/core/role.h"
#include "stdb/metastorage/synchronization.h"

#include <boost/thread.hpp>

namespace stdb {

class Controller : public common::Singleton<Controller> {
 public:
  Controller();
  void init(const char* config_path);

  void set_role(Role role) { role_ = role; }
  Role role() const { return role_; }

  std::shared_ptr<Database> open_standalone_database(const char* dbname);
  void close_database(const char* dbname);
  common::Status new_standalone_database(
      bool ismoving,
      const char* dbname,
      const char* metadata_path,
      const char* volumes_path,
      i32 num_volumes,
      u64 volume_size,
      bool allocate);
  
  void close();

 protected:
  friend class Database;

  // Internal use
  std::shared_ptr<Synchronization> synchronization() { return synchronization_; }
  std::shared_ptr<SyncWaiter> sync_waiter() { return sync_waiter_; }

  common::Status register_database(const std::string& db_name, std::shared_ptr<Database> database);
  void unregister_database(const std::string& db_name);
  std::shared_ptr<Database> get_database(const std::string& db_name);

  void start_sync_worker();
  void start_sync_worker_loop();
  void sync_action();
  std::vector<std::shared_ptr<Database>> get_alldatabases();

  bool set_config_database(bool ismoving, const char* dbname, const char* meta_path);
  bool get_config_database(const char* dbname, std::string* meta_path, bool* ismoving);

  void create_tables();
  
  void begin_transaction();
  void end_transaction();

  /** Execute query that doesn't return anything.
   * @throw std::runtime_error in a case of error
   * @return number of rows changed
   */
  int execute_query(std::string query);

  typedef std::vector<std::string> UntypedTuple;

  /** Execute select query and return untyped results.
   * @throw std::runtime_error in a case of error
   * @return bunch of strings with results
   */
  std::vector<UntypedTuple> select_query(const char* query) const;

  // Typedefs
  typedef std::unique_ptr<apr_pool_t, decltype(&delete_apr_pool)> PoolT;
  typedef const apr_dbd_driver_t* DriverT;
  typedef std::unique_ptr<apr_dbd_t, AprHandleDeleter> HandleT;
  typedef apr_dbd_prepared_t* PreparedT;

  // Members
  PoolT           pool_;
  DriverT         driver_;
  HandleT         handle_;

  std::atomic<int> done_;
  boost::barrier close_barrier_;
  std::mutex mutex_;
  Role role_;
  std::vector<std::shared_ptr<Database>> database_;
  std::unordered_map<std::string, u32> name2index_;
  std::shared_ptr<Synchronization> synchronization_;
  std::shared_ptr<SyncWaiter> sync_waiter_;
};

}  // namespace stdb

#endif  // STDB_CORE_CONTROLLER_H_

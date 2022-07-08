/*!
 * \file server_database.h
 */
#include "stdb/core/database.h"
#include "stdb/metastorage/server_meta_storage.h"
#include "stdb/query/queryprocessor_framework.h"

namespace stdb {

class ServerDatabase : public Database {
 protected:
  std::mutex lock_;
  SeriesMatcher global_matcher_;
  std::shared_ptr<ServerMetaStorage> metadata_;

 public:
  // Create empty in-memmory meta database.
  explicit ServerDatabase(bool is_moving);

  // Open meta database.
  ServerDatabase(const char* path, const FineTuneParams &params, bool is_moving);

  // Init series id, if a new id created return true else return false.
  // @param begin The series string's begin
  // @param end The series string's end
  // @param sid The returned id
  // @param local_matcher local matcher
  // @param ilog The ilog
  // @param sync_waiter sync waiter
  bool init_series_id(const char* begin,
                      const char* end,
                      u64* sid,
                      PlainSeriesMatcher* local_matcher,
                      storage::InputLog* ilog,
                      SyncWaiter* sync_waiter);

  // Init series id, if a new id created return true else return false.
  // @param begin The series string's begin
  // @param end The series string's end
  // @param location The series static location
  // @param sid The returned id
  // @param local_matcher local matcher
  // @param ilog The ilog
  // @param sync_waiter sync waiter 
  bool init_series_id(const char* begin,
                      const char* end,
                      const Location& location,
                      u64* sid,
                      PlainSeriesMatcher* local_matcher,
                      storage::InputLog* ilog,
                      SyncWaiter* sync_waiter);

  // Get series name
  int get_series_name(ParamId id, char* buffer, size_t buffer_size, PlainSeriesMatcher *local_matcher);

  // trigger meta sync
  void trigger_meta_sync();

  // Return global matcher
  SeriesMatcher* global_matcher() override { return &global_matcher_; }

  // Close.
  void close() override;
  // Sync.
  void sync() override;

  // Create empty database from scratch.
  // @param is_moving If moving database
  // @param base_file_name is database name (excl suffix)
  // @param metadata_path is a path to metadata storage
  // @param bstore_type is the bstore type.
  // @return operation status
  static common::Status new_database(bool is_moving, const char* base_file_name, const char* metadata_path, const char* bstore_type);

  // Remove existing database
  // @param file_name is a database name
  // @param wal_path wal path 
  // @return SUCCESS on success or EACCESS if database there is not enough priveleges to
  // delete the files
  static common::Status remove_database(const char* file_name, const char* wal_path);

  // run recovery
  void run_recovery(const FineTuneParams &params, Database* database);

 protected:
  // write wal
  void write_wal(storage::InputLog* ilog, ParamId id, const char* begin, u32 size, SyncWaiter* sync_waiter);

  void run_inputlog_metadata_recovery(
      storage::ShardedInputLog* ilog,
      std::vector<ParamId>* restored_ids,
      Database* database);
};

}  // namespace stdb

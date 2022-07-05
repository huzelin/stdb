/*!
 * \file server_database.h
 */
#include "stdb/core/database.h"
#include "stdb/core/server_meta_storage.h"
#include "stdb/index/seriesparser.h"
#include "stdb/query/queryprocessor_framework.h"

namespace stdb {

class ServerDatabase : public Database {
 protected:
  std::mutex lock_;
  SeriesMatcher global_matcher_;
  std::shared_ptr<ServerMetaStorage> metadata_;

 public:
  // Create empty in-memmory meta database.
  ServerDatabase();

  // Open meta database.
  ServerStorage(const char* path, const FineTuneParams &params);

  // Init series id, if return true, a new id created, else return false.
  bool init_series_id(const char* begin, const char* end, Sample* sample, PlainSeriesMatcher* local_matcher,
                      storage::InputLog* ilog, SessionWaiter* session_waiter);

  // Get series name
  int get_series_name(ParamId id, char* buffer, size_t buffer_size, PlainSeriesMatcher *local_matcher);

  // trigger meta sync
  void trigger_meta_sync();

  // Return global matcher
  SeriesMatcher& global_matcher() { return global_matcher_; }

 protected:
  // write wal
  void write_wal(storage::InputLog* ilog, ParamId id, const char* begin, u32 size, SessionWaiter* session_waiter);

  // run recovery
  void run_recovery(const FineTuneParams &params);
};

}  // namespace stdb

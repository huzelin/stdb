/*!
 * \file standalone_database_session.h
 */
#ifndef STDB_CORE_STANDALONE_DATABASE_SESSION_H_
#define STDB_CORE_STANDALONE_DATABASE_SESSION_H_

#include <memory>

#include "stdb/core/sync_waiter.h"
#include "stdb/core/database_session.h"
#include "stdb/index/seriesparser.h"
#include "stdb/storage/column_store.h"
#include "stdb/storage/input_log.h"

namespace stdb {

class StandaloneDatabase;

class StandaloneDatabaseSession : public DatabaseSession {
 protected:
  PlainSeriesMatcher local_matcher_;

  std::shared_ptr<StandaloneDatabase> database_;
  std::shared_ptr<storage::CStoreSession> session_;
  std::shared_ptr<SyncWaiter> sync_waiter_;
  storage::InputLog* ilog_;

 public:
  StandaloneDatabaseSession(std::shared_ptr<StandaloneDatabase> database,
                            std::shared_ptr<storage::CStoreSession> session,
                            std::shared_ptr<SyncWaiter> sync_waiter);
  virtual ~StandaloneDatabaseSession();

  // Main functions.
  /*!
   * Match series name. If series with such name doesn't exists - create it.
   * This method should be called for each sample to init its `paramid` field.
   */
  common::Status init_series_id(const char* begin, const char* end, Sample* sample) override;

  /*!
   * get series name according to param id.
   */
  common::Status get_series_name(ParamId id, char* buffer, size_t buffer_size) override;
  
  /*!
   * write sample.
   */
  common::Status write(const Sample& sample) override;

  void query(InternalCursor* cursor, const char* query) override;

  /**
   * @brief suggest query implementation
   * @param cursor is a pointer to internal cursor
   * @param query is a string that contains query
   */
  void suggest(InternalCursor* cursor, const char* query) override;

  /**
   * @brief search query implementation
   * @param cursor is a pointer to internal cursor
   * @param query is a string that contains query
   */
  void search(InternalCursor* cursor, const char* query) override;

 protected:
  void init_ilog();
};

}  // namespace stdb

#endif  // STDB_CORE_STANDALONE_DATABASE_SESSION_H_

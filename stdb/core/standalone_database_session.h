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
   * Init static location's IOT device.
   * @param begin series's begin
   * @param end series's end
   * @param location series's location
   * @param id The allocated series's id
   * @return operation status
   */
  common::Status init_series_id(const char* begin, const char* end, const Location& location, u64* id) override;

  /*!
   * Init moving IOT device
   * @param begin series's begin
   * @param end series's end
   * @param id The allocated series's id
   * @return operation status
   */
  common::Status init_series_id(const char* begin, const char* end, u64* id) override;
  
  /*!
   * get series name according to param id.
   * @param id The series's id
   * @param buffer The buffer address
   * @param buffer_size The buffer size
   */
  int get_series_name(ParamId id, char* buffer, size_t buffer_size) override;

  /*!
   * get series name and location according to param id
   * @param id The series's id
   * @buffer The buffer address for series name
   * @buffer_size The buffer size
   * @param location The series location
   */
  int get_series_name_and_location(ParamId id, char* buffer, size_t buffer_size, Location* location) override;
  
  /*!
   * write sample.
   * @param sample The sample to write
   * @return write status
   */
  common::Status write(const Sample& sample) override;

  /*!
   * @brief query implementation
   * @param cursor is a pointer to internal cursor
   * @param query is a string that contains query
   */
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

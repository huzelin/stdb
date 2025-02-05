/*!
 * \file database_session.h
 */
#ifndef STDB_CORE_DATABASE_SESSION_H_
#define STDB_CORE_DATABASE_SESSION_H_

#include "stdb/common/basic.h"

#include "stdb/index/seriesparser.h"
#include "stdb/query/internal_cursor.h"

namespace stdb {

class DatabaseSession {
 public:
  virtual ~DatabaseSession() { }

  /* Init static location's IOT device.
   * @param begin series's begin
   * @param end series's end
   * @param location series's location
   * @param id The allocated series's id
   * @return operation status
   */
  virtual common::Status init_series_id(const char* begin, const char* end, const Location& location, u64* id) = 0;

  /* Init moving IOT device
   * @param begin series's begin
   * @param end series's end
   * @param id The allocated series's id
   * @return operation status
   */
  virtual common::Status init_series_id(const char* begin, const char* end, u64* id) = 0;

  /* Get series name according to param id.
   * @param id The series's id
   * @param buffer The buffer address
   * @param buffer_size The buffer size
   * @return Return the series's length of name
   */
  virtual int get_series_name(ParamId id, char* buffer, size_t buffer_size) = 0;

  /*!
   * get series name and location
   * @param id The series' id
   * @param buffer The buffer address
   * @param buffer_size The buffer size
   * @param location The series's location
   */
  virtual int get_series_name_and_location(ParamId id, char* buffer, size_t buffer_size, Location* location) = 0;
  
  /*!
   * write sample.
   * @param sample The sample
   * @return The write status
   */
  virtual common::Status write(const Sample& sample) = 0;

  /*!
   * spatial and temporal query
   * @cursor The query result's cursor
   * @query The query string 
   */
  virtual void query(InternalCursor* cursor, const char* query) = 0;

  /**
   * @brief suggest query implementation
   * @param cursor is a pointer to internal cursor
   * @param query is a string that contains query
   */
  virtual void suggest(InternalCursor* cursor, const char* query) = 0;

  /**
   * @brief search query implementation
   * @param cursor is a pointer to internal cursor
   * @param query is a string that contains query
   */
  virtual void search(InternalCursor* cursor, const char* query) = 0;

  /**
   * @brief set matcher substitute
   * @param matcher_substitute matcher substitute
   */
  void set_matcher_substitute(std::shared_ptr<PlainSeriesMatcher> matcher_substitute) {
    matcher_substitute_ = matcher_substitute;
  }

  /**
   * @brief clear matcher substitute.
   */
  void clear_matcher_substitute() {
    matcher_substitute_ = nullptr;
  }

 protected:
  mutable std::shared_ptr<PlainSeriesMatcher> matcher_substitute_;
};

}  // namespace stdb

#endif  // STDB_CORE_DATABASE_SESSION_H_

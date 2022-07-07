/*!
 * \file database_session.h
 */
#ifndef STDB_CORE_DATABASE_SESSION_H_
#define STDB_CORE_DATABASE_SESSION_H_

#include "stdb/common/basic.h"

#include "stdb/query/internal_cursor.h"

namespace stdb {

class DatabaseSession {
 public:
  virtual ~DatabaseSession() { }

  /*!
   * Match series name. If series with such name doesn't exists - create it.
   * This method should be called for each sample to init its `paramid` field.
   */
  virtual common::Status init_series_id(const char* begin, const char* end, Sample* sample) = 0;

  /*!
   * get series name according to param id.
   */
  virtual common::Status get_series_name(ParamId id, char* buffer, size_t buffer_size) = 0;
  
  /*!
   * write sample.
   */
  virtual common::Status write(const Sample& sample) = 0;

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
};

}  // namespace stdb

#endif  // STDB_CORE_DATABASE_SESSION_H_

/**
 * \file storage_api.h
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef STDB_CORE_STORAGE_API_H_
#define STDB_CORE_STORAGE_API_H_

#include <atomic>
#include <functional>
#include <memory>
#include <string>

#include "stdb/common/basic.h"
#include "stdb/common/status.h"

namespace stdb {

//! Abstraction layer above Cursor
struct DbCursor {
  virtual ~DbCursor() = default;
  //! Read data from cursor
  virtual size_t read(void* dest, size_t dest_size) = 0;

  //! Check is cursor is done reading
  virtual bool is_done() = 0;

  //! Check for error condition
  virtual bool is_error(common::Status* out_error_code_or_null) = 0;

  //! Check for error condition
  virtual bool is_error(const char** error_message, common::Status* out_error_code) = 0;

  //! Close cursor
  virtual void close() = 0;
};

//! Database session, maps to Session directly
struct DbSession {
  virtual ~DbSession() = default;

  //! Write value to DB
  virtual common::Status write(const Sample& sample) = 0;

  //! Execute database query
  virtual std::shared_ptr<DbCursor> query(const std::string& query) = 0;

  //! Execute suggest query
  virtual std::shared_ptr<DbCursor> suggest(const std::string& query) = 0;

  //! Execute search query
  virtual std::shared_ptr<DbCursor> search(const std::string& query) = 0;

  //! Convert paramid to series name
  virtual int param_id_to_series(ParamId id, char* buffer, size_t buffer_size) = 0;

  virtual common::Status series_to_param_id(const char* name, size_t size, Sample* sample) = 0;

  virtual int name_to_param_id_list(const char* begin, const char* end, ParamId* ids, u32 cap) = 0;
};

//! Abstraction layer above Database
struct DbConnection {

  virtual ~DbConnection() = default;

  virtual std::string get_all_stats() = 0;

  virtual std::shared_ptr<DbSession> create_session() = 0;
};

class StorageSession;

class STDBSession : public DbSession {
  std::shared_ptr<StorageSession> storage_session_;
 
 public:
  STDBSession(std::shared_ptr<StorageSession> storage_session);
  virtual ~STDBSession();

  virtual common::Status write(const Sample &sample) override;
  virtual std::shared_ptr<DbCursor> query(const std::string& query) override;
  virtual std::shared_ptr<DbCursor> suggest(const std::string& query) override;
  virtual std::shared_ptr<DbCursor> search(const std::string& query) override;
  virtual int param_id_to_series(ParamId id, char *buffer, size_t buffer_size) override;
  virtual common::Status series_to_param_id(const char *name, size_t size, Sample *sample) override;
  virtual int name_to_param_id_list(const char* begin, const char* end, ParamId* ids, u32 cap) override;
};

class Storage;

//! Object of this class writes everything to the database
class STDBConnection : public DbConnection {
  std::string   dbpath_;
  std::shared_ptr<Storage> storage_;

 public:
  STDBConnection(const char* path, const FineTuneParams &params);

  virtual ~STDBConnection() override;

  virtual std::string get_all_stats() override;

  virtual std::shared_ptr<DbSession> create_session() override;

  static common::Status create_database(const char* base_file_name, const char* metadata_path,
                                        const char* volumes_path, i32 num_volumes, bool allocate);

  static common::Status create_database_ex(const char* base_file_name, const char* metadata_path,
                                           const char* volumes_path, i32 num_volumes,
                                           u64 volume_size, bool allocate);
  static common::Status delete_database(const char* file_name, const char* wal_path, bool force);
};

void initialize();

struct Utils {
  // parse timestamp.
  static common::Status parse_timestamp(const char* iso_str, Sample* sample);
};

}  // namespace stdb

#endif  // STDB_CORE_STORAGE_API_H_

/*!
 * \file storage_api.cc
 */
#include "stdb/core/storage_api.h"

#include <thread>

#include <boost/exception/all.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/json_parser.hpp>

#include "stdb/common/datetime.h"
#include "stdb/core/cursor.h"
#include "stdb/core/storage.h"

#include <apr_dbd.h>

namespace stdb {

struct BaseCursor : public DbCursor {
  std::unique_ptr<ExternalCursor> cursor_;
  std::string query_;

  BaseCursor(std::shared_ptr<StorageSession> storage_session, const std::string& query) : query_(query) { }

  size_t read(void *dest, size_t dest_size) override {
    return cursor_->read(dest, dest_size);
  }

  bool is_done() override {
    return cursor_->is_done();
  }

  bool is_error(common::Status *out_error_code_or_null) override {
    return cursor_->is_error(out_error_code_or_null);
  }

  bool is_error(const char** error_message, common::Status *out_error_code_or_null) override {
    return cursor_->is_error(error_message, out_error_code_or_null);
  }

  void close() override {
    cursor_->close();
  }
};

struct QueryCursor : public BaseCursor {
  QueryCursor(std::shared_ptr<StorageSession> storage_session, const std::string& query) : BaseCursor(storage_session, query) {
    cursor_ = ConcurrentCursor::make(&StorageSession::query, storage_session, query_.data());
  }
}; 

struct SuggestCursor : public BaseCursor {
  SuggestCursor(std::shared_ptr<StorageSession> storage_session, const std::string& query) : BaseCursor(storage_session, query) {
    cursor_ = ConcurrentCursor::make(&StorageSession::suggest, storage_session, query_.data());
  }
};

struct SearchCursor : public BaseCursor {
  SearchCursor(std::shared_ptr<StorageSession> storage_session, const std::string& query) : BaseCursor(storage_session, query) {
    cursor_ = ConcurrentCursor::make(&StorageSession::search, storage_session, query_.data());
  }
};

// STDBSession implementation
STDBSession::STDBSession(std::shared_ptr<StorageSession> storage_session) : storage_session_(storage_session) { }

STDBSession::~STDBSession() { }

common::Status STDBSession::write(const Sample &sample) {
  return storage_session_->write(sample);
}

std::shared_ptr<DbCursor> STDBSession::query(const std::string& query) {
  std::shared_ptr<DbCursor> ret;
  ret.reset(new QueryCursor(storage_session_, query));
  return ret;
}

std::shared_ptr<DbCursor> STDBSession::suggest(const std::string& query) {
  std::shared_ptr<DbCursor> ret;
  ret.reset(new SuggestCursor(storage_session_, query));
  return ret;
}

std::shared_ptr<DbCursor> STDBSession::search(const std::string& query) {
  std::shared_ptr<DbCursor> ret;
  ret.reset(new SearchCursor(storage_session_, query));
  return ret;
}

int STDBSession::param_id_to_series(ParamId id, char *buffer, size_t buffer_size) {
  return storage_session_->get_series_name(id, buffer, buffer_size);
}

common::Status STDBSession::series_to_param_id(const char *name, size_t size, Sample *sample) {
  return storage_session_->init_series_id(name, name + size, sample);
}

int STDBSession::name_to_param_id_list(const char* begin, const char* end, ParamId* ids, u32 cap) {
  return storage_session_->get_series_ids(begin, end, ids, cap);
}

// Connection implementation
STDBConnection::STDBConnection(const char *path, const FineTuneParams& params) : dbpath_(path) {
  if (path == std::string(":memory:")) {
    storage_ = std::make_shared<Storage>();
  } else {
    storage_ = std::make_shared<Storage>(path, params);
    storage_->initialize_input_log(params);
  }
}

STDBConnection::~STDBConnection() {
  LOG(INFO) << "Close database at " << dbpath_;
  storage_->close();
}

std::string STDBConnection::get_all_stats() {
  auto ptree_stats = storage_->get_stats();
  std::stringstream out;
  boost::property_tree::json_parser::write_json(out, ptree_stats, true);
  return out.str();
}

std::shared_ptr<DbSession> STDBConnection::create_session() {
  auto disp = storage_->create_write_session();
  std::shared_ptr<DbSession> result;
  result.reset(new STDBSession(disp));
  return result;
}

common::Status STDBConnection::create_database(
    const char* base_file_name, const char* metadata_path,
    const char* volumes_path, i32 num_volumes, bool allocate) {
  static const u64 vol_size = 4096ull * 1024 * 1024; // pages (4GB total)
  return create_database_ex(base_file_name, metadata_path, volumes_path, num_volumes, vol_size, allocate);
}

common::Status STDBConnection::create_database_ex(
    const char* base_file_name, const char* metadata_path,
    const char* volumes_path, i32 num_volumes,
    u64 volume_size, bool allocate) {
  return Storage::new_database(base_file_name, metadata_path, volumes_path, num_volumes, volume_size, allocate);
}

common::Status STDBConnection::delete_database(const char* file_name, const char* wal_path, bool force) {
  return Storage::remove_storage(file_name, wal_path, force);
}

common::Status Utility::parse_timestamp(const char* iso_str, Sample* sample) {
  try {
    sample->timestamp = DateTimeUtil::from_iso_string(iso_str);
  } catch (...) {
    return common::Status::BadArg();
  }
  return common::Status::Ok();
}

common::Status Utility::debug_report_dump(const char* path2db, const char* outfile) {
  return Storage::generate_report(path2db, outfile);
}

common::Status Utility::debug_recovery_report_dump(const char* path2db, const char* outfile) {
  return Storage::generate_recovery_report(path2db, outfile);
}

}  // namespace stdb

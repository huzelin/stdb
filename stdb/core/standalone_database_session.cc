/*!
 * \file standalone_database_session.cc
 */
#include "stdb/core/standalone_database_session.h"

#include "stdb/core/standalone_database.h"

namespace stdb {

StandaloneDatabaseSession::StandaloneDatabaseSession(
    std::shared_ptr<StandaloneDatabase> database,
    std::shared_ptr<storage::CStoreSession> session,
    storage::ShardedInputLog* log) :
    database_(database),
    session_(session),
    slog_(log),
    ilog_(nullptr) {
}

StandaloneDatabaseSession::~StandaloneDatabaseSession() {
  LOG(INFO) << "StandaloneDatabaseSession is being closed";
  if (ilog_) {
    std::vector<u64> staleids;
    auto res = ilog_->flush(&staleids);
    if (res.Code() == common::Status::kOverflow) {
      LOG(INFO) << "StorageSession input log overflow, "
          << staleids.size() << " stale ids is about to be closed";
      database_->worker_database()->close_specific_columns(staleids);
    }
  }
}

common::Status StandaloneDatabaseSession::init_series_id(const char* begin, const char* end, Sample* sample) {
  return common::Status::Ok();
}

common::Status StandaloneDatabaseSession::get_series_name(ParamId id, char* buffer, size_t buffer_size) {
  return common::Status::Ok();
}

common::Status StandaloneDatabaseSession::write(const Sample& sample) {
  return common::Status::Ok();
}

void StandaloneDatabaseSession::query(InternalCursor* cursor, const char* query) {

}

void StandaloneDatabaseSession::suggest(InternalCursor* cursor, const char* query) {

}

void StandaloneDatabaseSession::search(InternalCursor* cursor, const char* query) {

}

}  // namespace stdb

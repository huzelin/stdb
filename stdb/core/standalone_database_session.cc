/*!
 * \file standalone_database_session.cc
 */
#include "stdb/core/standalone_database_session.h"

#include "stdb/core/standalone_database.h"

namespace stdb {

StandaloneDatabaseSession::StandaloneDatabaseSession(
    std::shared_ptr<StandaloneDatabase> database,
    std::shared_ptr<storage::CStoreSession> session,
    std::shared_ptr<SyncWaiter> sync_waiter) :
    database_(database),
    session_(session),
    sync_waiter_(sync_waiter),
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

void StandaloneDatabaseSession::init_ilog() {
  if (ilog_ == nullptr) {
    ilog_ = database_->get_input_log();
  }
}

common::Status StandaloneDatabaseSession::init_series_id(const char* begin, const char* end, const Location& location, u64* id) {
  const char* ksbegin = nullptr;
  const char* ksend = nullptr;
  char buf[LIMITS_MAX_SNAME];
  char* ob = static_cast<char*>(buf);
  char* oe = static_cast<char*>(buf) + LIMITS_MAX_SNAME;
  auto status = SeriesParser::to_canonical_form(begin, end, ob, oe, &ksbegin, &ksend);
  if (!status.IsOk()) {
    return status;
  }
  *id = local_matcher_.match(ob, ksend);
  if (*id) {
    return status;
  }
  init_ilog();

  auto server_database = database_->server_database();
  auto create_new = server_database->init_series_id(begin, end, location, id, &local_matcher_, ilog_, sync_waiter_.get());
  if (create_new) {
    auto worker_database = database_->worker_database();
    status = worker_database->cstore()->create_new_column(*id);
  }
  return status;
}

common::Status StandaloneDatabaseSession::init_series_id(const char* begin, const char* end, u64* id) {
  const char* ksbegin = nullptr;
  const char* ksend = nullptr;
  char buf[LIMITS_MAX_SNAME];
  char* ob = static_cast<char*>(buf);
  char* oe = static_cast<char*>(buf) + LIMITS_MAX_SNAME;
  auto status = SeriesParser::to_canonical_form(begin, end, ob, oe, &ksbegin, &ksend);
  if (!status.IsOk()) {
    return status;
  }
  *id = local_matcher_.match(ob, ksend);
  if (*id) {
    return status;
  }
  init_ilog();

  auto server_database = database_->server_database();
  auto create_new = server_database->init_series_id(begin, end, id, &local_matcher_, ilog_, sync_waiter_.get());
  if (create_new) {
    auto worker_database = database_->worker_database();
    status = worker_database->cstore()->create_new_column(*id);
  }
  return status;
}

int StandaloneDatabaseSession::get_series_name(ParamId id, char* buffer, size_t buffer_size) {
  auto name = local_matcher_.id2str(id);
  if (name.first == nullptr) {
    auto server_database = database_->server_database();
    return server_database->get_series_name(id, buffer, buffer_size, &local_matcher_);
  }
  memcpy(buffer, name.first, static_cast<size_t>(name.second));
  return static_cast<int>(name.second);
}

int StandaloneDatabaseSession::get_series_name_and_location(
    ParamId id, char* buffer, size_t buffer_size, Location* location) {

}

common::Status StandaloneDatabaseSession::write(const Sample& sample) {
  init_ilog();

  auto worker_database = database_->worker_database();
  std::vector<u64> rpoints;
  auto status = session_->write(sample, &rpoints);
  switch (status) {
    case storage::NBTreeAppendResult::OK:
      break;
    case storage::NBTreeAppendResult::OK_FLUSH_NEEDED: {
      if (ilog_) {
        auto rpoints_copy = rpoints;
        worker_database->update_rescue_point(sample.paramid, std::move(rpoints_copy));
      } else {
        worker_database->update_rescue_point(sample.paramid, std::move(rpoints));
      }
    } break;
    case storage::NBTreeAppendResult::FAIL_BAD_ID: {
      LOG(ERROR) << "Invalid session cache, id = " << sample.paramid;
      return common::Status::NotFound();
    } break;
    case storage::NBTreeAppendResult::FAIL_LATE_WRITE:
      return common::Status::LateWrite();
    case storage::NBTreeAppendResult::FAIL_BAD_VALUE:
      return common::Status::BadArg();
  }
  if (ilog_ == nullptr) {
    return common::Status::Ok();
  }

  std::vector<u64> staleids;
  auto res = ilog_->append(sample.paramid, sample.timestamp, sample.payload.float64, &staleids);
  if (res.Code() == common::Status::kOverflow) {
    if (!staleids.empty()) {
      std::promise<void> barrier;
      std::future<void> future = barrier.get_future();
      sync_waiter_->add_sync_barrier(std::move(barrier));
      worker_database->close_specific_columns(staleids);
      staleids.clear();
      future.wait();
    }
    ilog_->rotate();
  }
  if (status == storage::NBTreeAppendResult::OK_FLUSH_NEEDED) {
    auto res = ilog_->append(sample.paramid, rpoints.data(), static_cast<u32>(rpoints.size()), &staleids);
    if (res.Code() == common::Status::kOverflow) {
      if (!staleids.empty()) {
        std::promise<void> barrier;
        std::future<void> future = barrier.get_future();
        sync_waiter_->add_sync_barrier(std::move(barrier));
        worker_database->close_specific_columns(staleids);
        future.wait();
      }
      ilog_->rotate();
    }
  }
}

void StandaloneDatabaseSession::query(InternalCursor* cursor, const char* query) {
  database_->query(this, cursor, query);
}

void StandaloneDatabaseSession::suggest(InternalCursor* cursor, const char* query) {
  database_->query(this, cursor, query);
}

void StandaloneDatabaseSession::search(InternalCursor* cursor, const char* query) {
  database_->query(this, cursor, query);
}

}  // namespace stdb

/*!
 * \file server_database.cc
 */
#include "stdb/core/server_database.h"

namespace stdb {

ServerDatabase::ServerDatabase() {
  metadata_.reset(new ServerMetaStorage(":memory"));
}

ServerDatabase::ServerDatabase(const char* path, const FineTuneParams &params) {
  metadata_.reset(new ServerMetaStorage(path));

  // Update series matcher
  auto baseline = metadata_->get_prev_largest_id();
  if (baseline) {
    global_matcher_.series_id = baseline.get() + 1;
  }
  auto status = metadata_->load_matcher_data(global_matcher_);
  if (!status.IsOk()) {
    LOG(FATAL) << "Cann't read series names";
  }
  run_recovery(params);
}

bool ServerDatabase::init_series_id(const char* begin, const char* end, Sample* sample, PlainSeriesMatcher* local_matcher,
                                    storage::InputLog* ilog, SessionWaiter* session_waiter) {
  u64 id = 0;
  bool create_new = false;
  {
    std::lock_guard<std::mutex> guard(lock_);
    id = static_cast<u64>(global_matcher_.match(begin, end));
    if (id == 0) {
      // create new series
      id = static_cast<u64>(global_matcher_.add(begin, end));
      create_new = true;
      // write wal for server meta storage.
      write_wal(ilog, begin, end - begin, session_waiter);
    }
  }
  sample->paramid = id;
  local_matcher->_add(begin, end, id);
  return create_new;
}

int ServerDatabase::get_series_name(ParamId id, char* buffer, size_t buffer_size, PlainSeriesMatcher *local_matcher) {
  auto str = global_matcher_.id2str(static_cast<i64>(id));
  if (str.first == nullptr) {
    return 0;
  }
  // copy value to local matcher
  local_matcher->_add(str.first, str.first + str.second, static_cast<i64>(id));
  // copy the string to out buffer
  if (str.second > buffer_size) {
    return -1 * static_cast<int>(str.second);
  }
  memcpy(buffer, str.first, static_cast<size_t>(str.second));
  return static_cast<int>(str.second);
}

void ServerDatabase::trigger_meta_sync() {
  auto get_names = [this](std::vector<PlainSeriesMatcher::SeriesNameT>* names,
                          std::vector<Location>* locations) {
    std::lock_guard<std::mutex> guard(lock_);
    global_matcher_.pull_new_series(names, locations);
  }
  metadata_->sync_with_metadata_storage(get_names);
}

void ServerDatabase::write_wal(storage::InputLog* ilog, ParamId id, const char* begin, u32 size, SessionWaiter* session_waiter) {
  if (ilog == nullptr) return;

  std::vector<ParamId> staleids;
  auto status = ilog->append(id, begin, size, &staleids);
  if (status.Code() == common::Status::kOverflow) {
    if (!staleids.empty()) {
      // before roate(delete) the oldest wal file, we must wait meta data is
      // backedup.
      std::promise<void> barrier;
      std::future<void> future = barrier.get_future();
      session_waiter->add_sync_barrier(std::move(barrier));
      future.wait();
    }
    ilog->rotate();
  }
}

void ServerDatabase::run_recovery(const FineTuneParams &params) {
  if (!run_recovery_is_enabled(params)) {
    return;
  }
  
  std::vector<ParamId> new_ids;
  auto ilog = std::make_shared<storage::ShardedInputLog>(ccr, params.input_log_path);
  run_inputlog_metadata_recovery(ilog.get(), &new_ids);
}

}  // namespace stdb

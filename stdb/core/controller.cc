/*
 * \file controller.cc
 */
#include "stdb/core/controller.h"

namespace stdb {

void Controller::start_sync_worker() {
  std::thread sync_worker_thread([this]() { this->start_sync_worker_loop(); });
  sync_worker_thread.detach();
}

void Controller::start_sync_worker_loop() {
  enum {
    SYNC_REQUEST_TIMEOUT = 10000,
  };
  LOG(INFO) << "start sync worker loop....";
  while (done_.load() == 0) {
    auto status = synchronization_->wait_for(SYNC_REQUEST_TIMEOUT);
    if (!status.IsOk()) {
      continue;
    }
    sync_action();
  }
  close_barrier_.wait();
}

void Controller::sync_action() {
  std::vector<std::shared_ptr<Database>> all_databases = get_alldatabases();
  for (auto database : all_databases) {
    database->sync();
  }
  session_waiter_->notify_all();
}

void Controller::close() {
  done_.store(1);
  sync_action();
  close_barrier_.wait();

  std::vector<std::shared_ptr<Database>> all_databases = get_alldatabases();
  for (auto database : all_databases) {
    database->close();
  }
}

std::vector<std::shared_ptr<Database>> Controller::get_alldatabases() {
  std::lock_guard<std::mutex> lck(mutex_);
  return database_;
}

common::Status Controller::register_database(const std::string& db_name, std::shared_ptr<Database> database) {
  std::lock_guard<std::mutex> lck(mutex_);
  name2index_[db_name] = database_.size();
  database_.emplace_back(database);
  return common::Status::Ok();
}

std::shared_ptr<Database> Controller::get_database(const std::string& db_name) {
  std::lock_guard<std::mutex> lck(mutex_);
  auto iter = name2index_.find(db_name);
  if (iter == name2index_.end()) {
    static std::shared_ptr<Database> kNull;
    return kNull;
  } else {
    return database_[iter->second];
  }
}

Controller::Controller() : done_(0), close_barrier_(2) {
  synchronization_.reset(new Synchronization());
  session_waiter_.reset(new SessionWaiter());
  start_sync_worker();
}

}  // namespace stdb

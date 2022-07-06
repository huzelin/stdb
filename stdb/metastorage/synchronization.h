/*!
 * \file synchronization.h
 */
#ifndef STDB_METASTORAGE_SYNCHRONIZATION_H_
#define STDB_METASTORAGE_SYNCHRONIZATION_H_

#include <functional>
#include <mutex>
#include <condition_variable>

#include "stdb/common/status.h"

namespace stdb {

class Synchronization {
 public:
  // wait-for timeout usec
  common::Status wait_for(int timeout_us) {
    std::unique_lock<std::mutex> lock(sync_lock_);
    auto res = sync_cvar_.wait_for(lock, std::chrono::microseconds(timeout_us));
    if (res == std::cv_status::timeout) {
      return common::Status::Timeout();
    }
    return common::Status::Ok();
  }
  
  // Signal with action.
  // @func action function
  void signal_action(std::function<void()> func) {
    std::lock_guard<std::mutex> guard(sync_lock_);
    func();
    sync_cvar_.notify_one();
  }
  
  // Lock with action
  // @func action function
  void lock_action(std::function<void()> func) {
    std::lock_guard<std::mutex> guard(sync_lock_);
    func();
  }

 protected:
  mutable std::mutex                                sync_lock_;
  std::condition_variable                           sync_cvar_;
};

}  // namespace stdb

#endif  // STDB_METASTORAGE_SYNCHRONIZATION_H_

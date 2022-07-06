/*!
 * \file session_waiter.h
 */
#ifndef STDB_CORE_SESSION_WAITER_H_
#define STDB_CORE_SESSION_WAITER_H_

#include <future>
#include <memory>
#include <mutex>
#include <vector>

namespace stdb {

struct SessionWaiter {
  std::vector<std::promise<void>> sessions_wait_list;
  std::mutex session_lock;

  void add_sync_barrier(std::promise<void>&& barrier) {
    std::lock_guard<std::mutex> lock(session_lock);
    sessions_wait_list.push_back(std::move(barrier));
  }

  void notify_all() {
    std::lock_guard<std::mutex> lck(session_lock);
    for (auto& it : sessions_wait_list) {
      it.set_value();
    }
    sessions_wait_list.clear();
  }
};

}  // namespace stdb

#endif  // STDB_CORE_SESSION_WAITER_H_

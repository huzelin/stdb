/*!
 * \file signal_handler.h
 */
#ifndef STDB_COMMON_SIGNAL_HANDLER_H_
#define STDB_COMMON_SIGNAL_HANDLER_H_

#include <functional>
#include <memory>
#include <vector>

namespace stdb {

struct SignalHandler {
  typedef std::function<void()> Func;

  std::vector<std::pair<Func, int>> handlers_;

  SignalHandler();

  void add_handler(Func cb, int id);

  std::vector<int> wait();
};

}  // namespace stdb

#endif  // STDB_COMMON_SIGNAL_HANDLER_H_

/*!
 * \file signal_handler.cc
 */
#include "stdb/common/signal_handler.h"

#include <iostream>
#include <signal.h>

#include "stdb/common/logging.h"

namespace stdb {

SignalHandler::SignalHandler() { }

void SignalHandler::add_handler(std::function<void()> fn, int id) {
  handlers_.push_back(std::make_pair(fn, id));
}

static void sig_handler(int signo) {
  if (signo == SIGINT) {
    LOG(INFO) << "SIGINT handler called";
  } else if (signo == SIGTERM) {
    LOG(INFO) << "SIGTERM handler called";
  }
}

std::vector<int> SignalHandler::wait() {
  if (signal(SIGINT, &sig_handler) == SIG_ERR) {
    LOG(ERROR) << "Signal handler error, signal returned SIG_ERR";
    std::runtime_error error("`signal` error");
    throw error;
  }
  if (signal(SIGTERM, &sig_handler) == SIG_ERR) {
    LOG(ERROR) << "Signal handler error, signal returned SIG_ERR";
    std::runtime_error error("`signal` error");
    throw error;
  }
  LOG(INFO) << "Waiting for the signals";
  pause();
  LOG(INFO) << "Start calling signal handlers";

  std::vector<int> ids;
  for (auto pair: handlers_) {
    LOG(INFO) << "Calling signal handler " << pair.second;
    pair.first();
    ids.push_back(pair.second);
  }
  handlers_.clear();
  return ids;
}

}  // namespace stdb


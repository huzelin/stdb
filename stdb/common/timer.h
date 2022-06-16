/*!
 * \file timer.h
 */
#ifndef STDB_COMMON_TIMER_H_
#define STDB_COMMON_TIMER_H_

#include <sys/time.h>

namespace stdb {
namespace common {

class Timer {
 public:
  Timer() { gettimeofday(&start_time_, nullptr); }
  void restart() { gettimeofday(&start_time_, nullptr); }
  double elapsed() const {
    timeval curr;
    gettimeofday(&curr, nullptr);
    return double(curr.tv_sec - start_time_.tv_sec) +
        double(curr.tv_usec - start_time_.tv_usec) / 1000000.0;
  }

 private:
  timeval start_time_;
};

}  // namespace common
}  // namespace stdb

#endif  // STDB_COMMON_TIMER_H_

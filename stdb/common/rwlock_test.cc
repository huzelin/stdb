/*!
 * \file rwlock_test.cc
 */
#include "stdb/common/rwlock.h"

#include "gtest/gtest.h"

namespace stdb {
namespace common {

TEST(TestRWLock, Test) {
  RWLock rwlock;

  rwlock.rdlock();
  rwlock.unlock();
}

}  // namespace common
}  // namespace stdb

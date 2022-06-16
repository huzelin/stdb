/*!
 * \file status_test.cc
 */
#include "stdb/common/status.h"

#include "gtest/gtest.h"

namespace stdb {
namespace common {

TEST(TestStatus, IsOk) {
  Status status;
  EXPECT_TRUE(status.IsOk());
}

}  // namespace common
}  // namespace stdb

/*!
 * \file controller_test.cc
 */
#include "stdb/core/controller.h"

#include "gtest/gtest.h"

namespace stdb {

TEST(TestController, Test1) {
  auto controller = Controller::Get();
  controller->close();
}

}  // namespace stdb

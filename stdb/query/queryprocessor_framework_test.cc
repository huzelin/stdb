/*!
 * \file queryprocessor_framework_test.cc
 */
#include "stdb/query/queryprocessor_framework.h"

#include "gtest/gtest.h"

namespace stdb {
namespace qp {

TEST(TestQuery, Test_1) {
  auto items = list_query_registry();
  for (auto& item : items) {
    LOG(INFO) << "item=" << item;
  }
}

}  // namespace qp
}  // namespace stdb

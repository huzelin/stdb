/*!
 * \file strutil_test.cc
 */
#include "stdb/common/strutil.h"

#include "gtest/gtest.h"

#include "stdb/common/logging.h"

namespace stdb {
namespace common {

TEST(TestStrUtil, split_1) {
  const char* str = "\"series key=abc\" 1200 23.45";
  
  std::vector<std::tuple<const char*, uint32_t>> rets;
  auto success = StrUtil::split(str, strlen(str), rets);
  EXPECT_TRUE(success);

  auto series = std::string(std::get<0>(rets[0]), std::get<1>(rets[0]));
  auto timestamp = std::string(std::get<0>(rets[1]), std::get<1>(rets[1]));
  auto value = std::string(std::get<0>(rets[2]), std::get<1>(rets[2]));

  EXPECT_STREQ("series key=abc", series.c_str());
  EXPECT_STREQ("1200", timestamp.c_str());
  EXPECT_STREQ("23.45", value.c_str());
}

}  // namespace common
}  // namespace stdb

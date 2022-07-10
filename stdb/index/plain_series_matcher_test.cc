/*!
 * \file plain_series_matcher_test.cc
 */
#include "stdb/index/plain_series_matcher.h"

#include "gtest/gtest.h"

namespace stdb {

TEST(TestPlainSeriesMatcher, Test1) {
  PlainSeriesMatcher plain_series_matcher;
  SeriesMatcherBase* base = &plain_series_matcher;
  std::string key = "cpu key=127";
  u64 id = 32;
  base->_add(key, id);
}

}  // namespace stdb

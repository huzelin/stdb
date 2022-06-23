/*!
 * \file roaring64map_test.cc
 */
#include "roaring/roaring64map.hh"

#include "gtest/gtest.h"

#include "stdb/common/logging.h"

namespace stdb {
namespace storage {

TEST(TestInputLog, Roaring64Map) {
  roaring::Roaring64Map map[2];
  map[0].add(12UL);
  map[0].add(64UL);

  map[1].add(56UL);
  map[1].add(13UL);
  map[1].add(12UL);

  auto res = map[1] - map[0];
  auto iter = res.begin();
  for (; iter != res.end(); ++iter) {
    LOG(INFO) << *iter;
  }
}

}  // namespace storage
}  // nemespace stdb

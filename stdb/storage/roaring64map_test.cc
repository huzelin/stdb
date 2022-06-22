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
  EXPECT_EQ(56UL, *iter++);
  EXPECT_EQ(12UL, *iter);

  std::vector<const roaring::Roaring64Map*> vecs;
  vecs.push_back(&map[0]);
  vecs.push_back(&map[1]);

  res = roaring::Roaring64Map::fastunion(vecs.size(), vecs.data());
  iter = res.begin();
  EXPECT_EQ(12UL, *iter++);
  EXPECT_EQ(13UL, *iter++);
  EXPECT_EQ(56UL, *iter++);
  EXPECT_EQ(64UL, *iter);
}

}  // namespace storage
}  // nemespace stdb

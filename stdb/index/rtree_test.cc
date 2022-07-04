/*!
 * \file rtree_test.cc
 */
#include "stdb/index/rtree.h"

#include "gtest/gtest.h"

#include "stdb/common/logging.h"

namespace stdb {
namespace rtree {

TEST(TestRTree, Insert) {
  RTree<float, 2, 96> rtree;
  i64 payload = 1;
  RTree<float, 2, 96>::Point point;
  {
    point.data[0] = 120.21;
    point.data[1] = 32.12; 
    rtree.Insert(point, payload++);
  }
  {
    point.data[0] = 120.22;
    point.data[1] = 32.13;
    rtree.Insert(point, payload++);
  }
  {
    point.data[0] = 120.212;
    point.data[1] = 32.123;
    rtree.Insert(point, payload++);
  }
  {
    point.data[0] = 120.21;
    point.data[1] = 32.33;
    rtree.Insert(point, payload++);
  }
  {
    point.data[0] = 120.219;
    point.data[1] = 32.30;
    rtree.Insert(point, payload++);
  }
  {
    point.data[0] = 120.22;
    point.data[1] = 32.30;
    rtree.Insert(point, payload++);
  }
  {
    point.data[0] = 120.30;
    point.data[1] = 32.34;
    rtree.Insert(point, payload++);
  }
  {
    point.data[0] = 120.31;
    point.data[1] = 32.35;
    rtree.Insert(point, payload++);
  }
  {
    point.data[0] = 120.32;
    point.data[1] = 32.36;
    rtree.Insert(point, payload++);
  }
  {
    point.data[0] = 120.321;
    point.data[1] = 32.361;
    rtree.Insert(point, payload++);
  }
  {
    point.data[0] = 120.322;
    point.data[1] = 32.362;
    rtree.Insert(point, payload++);
  }
  {
    point.data[0] = 120.323;
    point.data[1] = 32.363;
    rtree.Insert(point, payload++);
  }
  LOG(INFO) << rtree.DebugString();

  {
    // Range query
    RTree<float, 2, 96>::Rect rect;
    rect.min.data[0] = 120.323;
    rect.min.data[1] = 32.363;
    rect.max.data[0] = 120.323;
    rect.max.data[1] = 32.363;
    std::vector<i64> results;
    rtree.RangeQuery(rect, results);
    EXPECT_EQ(1, results.size());
    EXPECT_EQ(payload - 1, results[0]);
  }

  {
    // KNN query
    RTree<float, 2, 96>::Point point;
    point.data[0] = 120.323;
    point.data[1] = 32.363;
    std::vector<i64> results;
    rtree.KnnQuery(point, 1, results);
    EXPECT_EQ(1, results.size());
    EXPECT_EQ(payload - 1, results[0]);
  }
}

}  // namespace rtree
}  // namespace stdb

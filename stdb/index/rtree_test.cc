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
  RTree<float, 2, 4096>::Point point;
  {
    point.data[0] = 120.21;
    point.data[1] = 32.12; 
    rtree.Insert(point, payload);
  }
  {
    point.data[0] = 120.22;
    point.data[1] = 32.13;
    rtree.Insert(point, payload);
  }
  {
    point.data[0] = 120.212;
    point.data[1] = 32.123;
    rtree.Insert(point, payload);
  }
  {
    point.data[0] = 120.21;
    point.data[1] = 32.33;
    rtree.Insert(point, payload);
  }
  {
    point.data[0] = 120.219;
    point.data[1] = 32.30;
    rtree.Insert(point, payload);
  }
  {
    point.data[0] = 120.22;
    point.data[1] = 32.30;
    rtree.Insert(point, payload);
  }





  LOG(INFO) << rtree.DebugString();
}

}  // namespace rtree
}  // namespace stdb

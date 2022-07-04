/*!
 * \file perf_rtee.cc
 */
#include "stdb/common/timer.h"
#include "stdb/index/rtree.h"

using namespace stdb;
using namespace stdb::rtree;

RTree<float, 2, 4096> tree;
RTree<float, 2, 4096>::Point point;

void init() {
  i64 payload = 1;
  common::Timer timer;
  timer.restart();
  for (u32 i = 0; i < 1000; i++) {
    for (u32 j = 0; j < 1000; ++j) {
      auto lon = 120.00 + i * 0.001;
      auto lat = 30.00 + j * 0.001;
      point.data[0] = lon;
      point.data[1] = lat;
      tree.Insert(point, payload++);
    }
  }
  LOG(INFO) << "insert time:" << timer.elapsed() << "(sec)";
}

void range_query() {

}

void knn_query() {

}

int main(int argc, char** argv) {
  init();
  range_query();
  knn_query();
  return 0;
}

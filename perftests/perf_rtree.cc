/*!
 * \file perf_rtee.cc
 */
#include "stdb/common/timer.h"
#include "stdb/index/rtree.h"

using namespace stdb;
using namespace stdb::rtree;

#define BLOCK_SIZE 2048

RTree<float, 2, BLOCK_SIZE> tree;
RTree<float, 2, BLOCK_SIZE>::Point point;
common::Timer timer;

void init() {
  i64 payload = 1;
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
  LOG(INFO) << "insert time:" << timer.elapsed() << "(us)";
  LOG(INFO) << "tree height:" << tree.height();
  
  tree.CheckValid();
}

void range_query() {
  RTree<float, 2, BLOCK_SIZE>::Rect rect;
  rect.min.data[0] = 120.00 + 100 * 0.001;
  rect.min.data[1] = 30.00 + 100 * 0.001;
  rect.max.data[0] = 120.00 + 120 * 0.001;
  rect.max.data[1] = 30.00 + 120 * 0.001;
  std::vector<i64> results;

  timer.restart();
  RTree<float, 2, BLOCK_SIZE>::QueryStat query_stat;
  tree.RangeQuery(rect, results, query_stat);
  LOG(INFO) << "range query results.size()=" << results.size();
  LOG(INFO) << "range query time:" << timer.elapsed() * 1000UL * 1000UL << "(us)";
  // LOG(INFO) << query_stat.touch_counts[0];
}

void knn_query() {
  RTree<float, 2, BLOCK_SIZE>::Point point;
  point.data[0] = 120.00;
  point.data[1] = 30.00;
  std::vector<i64> results;

  timer.restart();
  tree.KnnQuery(point, 1, results);
  LOG(INFO) << "knn query results.size()=" << results.size();
  LOG(INFO) << "payload=" << results[0];
  LOG(INFO) << "knn query time:" << timer.elapsed() * 1000UL * 1000UL << "(us)";
}

int main(int argc, char** argv) {
  init();
  range_query();
  knn_query();
  return 0;
}

/*!
 * \file nbtree_test.cc
 */
#include <iostream>

#include <apr.h>
#include <queue>
#include <fstream>
#include <stdlib.h>

#include "stdb/common/basic.h"
#include "stdb/storage/block_store.h"
#include "stdb/storage/volume.h"
#include "stdb/storage/nbtree.h"

#include "gtest/gtest.h"

namespace stdb {
namespace storage {

std::string to_isostring(Timestamp ts) {
  if (ts == STDB_MAX_TIMESTAMP) {
    return "MAX";
  } else if (ts == STDB_MIN_TIMESTAMP) {
    return "MIN";
  }
  return std::to_string(ts);
}

struct Initializer {
  Initializer() {
    apr_initialize();
  }
};

static Initializer initializer;

enum class ScanDir {
  FWD, BWD
};

void test_nbtree_roots_collection(u32 N, u32 begin, u32 end) {
  ScanDir dir = begin < end ? ScanDir::FWD : ScanDir::BWD;
  std::shared_ptr<BlockStore> bstore = BlockStoreBuilder::create_memstore();
  std::vector<LogicAddr> addrlist;  // should be empty at first
  auto collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
  collection->force_init();
  for (u32 i = 0; i < N; i++) {
    collection->append(i, i);
  }

  // Read data back
  std::unique_ptr<RealValuedOperator> it = collection->search(begin, end);

  common::Status status;
  size_t sz;
  size_t outsz = dir == ScanDir::FWD ? end - begin : begin - end;
  std::vector<Timestamp> ts(outsz, 0xF0F0F0F0);
  std::vector<double> xs(outsz, -1);
  std::tie(status, sz) = it->read(ts.data(), xs.data(), outsz);

  EXPECT_EQ(sz, outsz);
  EXPECT_EQ(status, common::Status::Ok());

  if (dir == ScanDir::FWD) {
    for (u32 i = 0; i < outsz; i++) {
      const auto curr = i + begin;
      if (ts[i] != curr) {
        LOG(FATAL) << "Invalid timestamp at " << i << ", expected: " << curr << ", actual: " << ts[i];
      }
      if (!same_value(xs[i], curr)) {
        LOG(FATAL) << "Invalid value at " << i << ", expected: " << curr << ", actual: " << xs[i];
      }
    }
  } else {
    for (u32 i = 0; i < outsz; i++) {
      const auto curr = begin - i;
      if (ts[i] != curr) {
        LOG(FATAL) << "Invalid timestamp at " << i << ", expected: " << curr << ", actual: " << ts[i];
      }
      if (!same_value(xs[i], curr)) {
        LOG(FATAL) << "Invalid value at " << i << ", expected: " << curr << ", actual: " << xs[i];
      }
    }
  }
}

TEST(TestNBTree, Test_nbtree_rc_append_1) {
  test_nbtree_roots_collection(100, 0, 100);
}

TEST(TestNBTree, Test_nbtree_rc_append_2) {
  test_nbtree_roots_collection(2000, 0, 2000);
}

TEST(TestNBTree, Test_nbtree_rc_append_3) {
  test_nbtree_roots_collection(200000, 0, 200000);
}

TEST(TestNBTree, Test_nbtree_rc_append_4) {
  test_nbtree_roots_collection(100, 99, 0);
}

TEST(TestNBTree, Test_nbtree_rc_append_5) {
  test_nbtree_roots_collection(2000, 1999, 0);
}

TEST(TestNBTree, Test_nbtree_rc_append_6) {
  test_nbtree_roots_collection(200000, 199999, 0);
}

TEST(TestNBTree, Test_nbtree_rc_append_rand_read) {
  for (int i = 0; i < 100; i++) {
    auto N = static_cast<u32>(rand()) % 200000u;
    auto from = static_cast<u32>(rand()) % N;
    auto to = static_cast<u32>(rand()) % N;
    test_nbtree_roots_collection(N, from, to);
  }
}

void test_nbtree_chunked_read(u32 N, u32 begin, u32 end, u32 chunk_size) {
  ScanDir dir = begin < end ? ScanDir::FWD : ScanDir::BWD;
  std::shared_ptr<BlockStore> bstore = BlockStoreBuilder::create_memstore();
  std::vector<LogicAddr> addrlist;  // should be empty at first
  auto collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
  collection->force_init();

  for (u32 i = 0; i < N; i++) {
    collection->append(i, i);
  }

  // Read data back
  std::unique_ptr<RealValuedOperator> it = collection->search(begin, end);

  common::Status status;
  size_t sz;
  std::vector<Timestamp> ts(chunk_size, 0xF0F0F0F0);
  std::vector<double> xs(chunk_size, -1);

  u32 total_size = 0u;
  Timestamp ts_seen = begin;
  while (true) {
    std::tie(status, sz) = it->read(ts.data(), xs.data(), chunk_size);

    if (sz == 0 && status == common::Status::Ok()) {
      LOG(FATAL) << "Invalid iterator output, sz=0, status=" << status.ToString();
    }
    total_size += sz;

    EXPECT_TRUE(status == common::Status::Ok() || status == common::Status::NoData());

    if (dir == ScanDir::FWD) {
      for (u32 i = 0; i < sz; i++) {
        const auto curr = ts_seen;
        if (ts[i] != curr) {
          LOG(FATAL) << "Invalid timestamp at " << i << ", expected: " << curr << ", actual: " << ts[i];
        }
        if (!same_value(xs[i], curr)) {
          LOG(FATAL) << "Invalid value at " << i << ", expected: " << curr << ", actual: " << xs[i];
        }
        ts_seen = ts[i] + 1;
      }
    } else {
      for (u32 i = 0; i < sz; i++) {
        const auto curr = ts_seen;
        if (ts[i] != curr) {
          LOG(FATAL) << "Invalid timestamp at " << i << ", expected: " << curr << ", actual: " << ts[i];
        }
        if (!same_value(xs[i], curr)) {
          LOG(FATAL) << "Invalid value at " << i << ", expected: " << curr << ", actual: " << xs[i];
        }
        ts_seen = ts[i] - 1;
      }
    }

    if (status == common::Status::NoData() || ts_seen == end) {
      break;
    }
  }
  if (ts_seen != end) {
    LOG(FATAL) << "Bad range, expected: " << end << ", actual: " << ts_seen <<
        " dir: " << (dir == ScanDir::FWD ? "forward" : "backward");
  }
  size_t outsz = dir == ScanDir::FWD ? end - begin : begin - end;
  EXPECT_EQ(total_size, outsz);
}

TEST(TestNBtree, Test_nbtree_chunked_read) {
  for (u32 i = 0; i < 100; i++) {
    auto N = static_cast<u32>(rand() % 200000);
    auto from = static_cast<u32>(rand()) % N;
    auto to = static_cast<u32>(rand()) % N;
    auto chunk = static_cast<u32>(rand()) % N;
    test_nbtree_chunked_read(N, from, to, chunk);
  }
}

void check_tree_consistency(std::shared_ptr<BlockStore> bstore, size_t level, NBTreeExtent const* extent) {
  NBTreeExtent::check_extent(extent, bstore, level);
}

void test_reopen_storage(i32 Npages, i32 Nitems) {
  LogicAddr last_one = EMPTY_ADDR;
  std::shared_ptr<BlockStore> bstore =
      BlockStoreBuilder::create_memstore([&last_one](LogicAddr addr) { last_one = addr; });
  std::vector<LogicAddr> addrlist;  // should be empty at first
  auto collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
  collection->force_init();

  u32 nleafs = 0;
  u32 nitems = 0;
  for (u32 i = 0; true; i++) {
    if (collection->append(i, i) == NBTreeAppendResult::OK_FLUSH_NEEDED) {
      // addrlist changed
      auto newroots = collection->get_roots();
      if (newroots == addrlist) {
        LOG(FATAL) << "Roots collection must change";
      }
      std::swap(newroots, addrlist);
      nleafs++;
      if (static_cast<i32>(nleafs) == Npages) {
        nitems = i;
        break;
      }
    }
    if (static_cast<i32>(i) == Nitems) {
      nitems = i;
      break;
    }
  }

  addrlist = collection->close();

  EXPECT_EQ(addrlist.back(), last_one);

  // TODO: check attempt to open tree using wrong id!
  collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
  collection->force_init();

  auto extents = collection->get_extents();
  for (size_t i = 0; i < extents.size(); i++) {
    auto extent = extents[i];
    check_tree_consistency(bstore, i, extent);
  }

  std::unique_ptr<RealValuedOperator> it = collection->search(0, nitems);
  std::vector<Timestamp> ts(nitems, 0);
  std::vector<double> xs(nitems, 0);
  common::Status status = common::Status::Ok();
  size_t sz = 0;
  std::tie(status, sz) = it->read(ts.data(), xs.data(), nitems);
  EXPECT_EQ(sz, nitems);
  EXPECT_EQ(status, common::Status::Ok());
  for (u32 i = 0; i < nitems; i++) {
    if (ts[i] != i) {
      LOG(FATAL) << "Invalid timestamp at " << i;
    }
    if (!same_value(xs[i], static_cast<double>(i))) {
      LOG(FATAL) << "Invalid timestamp at " << i;
    }
  }
}

TEST(TestNBTree, Test_nbtree_reopen_1) {
  test_reopen_storage(-1, 1);
}

TEST(TestNBTree, Test_nbtree_reopen_2) {
  test_reopen_storage(1, -1);
}

TEST(TestNBTree, Test_nbtree_reopen_3) {
  test_reopen_storage(2, -1);
}

TEST(TestNBtree, Test_nbtree_reopen_4) {
  test_reopen_storage(32, -1);
}

TEST(TestNBTree, Test_nbtree_reopen_5) {
  test_reopen_storage(33, -1);
}

TEST(TestNBTree, Test_nbtree_reopen_6) {
  test_reopen_storage(32*32, -1);
}

//! Reopen storage that has been closed without final commit.
void test_storage_recovery_status(u32 N, u32 N_values) {
  LogicAddr last_block = EMPTY_ADDR;
  auto cb = [&last_block] (LogicAddr addr) {
    last_block = addr;
  };
  std::shared_ptr<BlockStore> bstore = BlockStoreBuilder::create_memstore(cb);
  std::vector<LogicAddr> addrlist;  // should be empty at first
  auto collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
  collection->force_init();

  u32 nleafs = 0;
  u32 nitems = 0;
  for (u32 i = 0; true; i++) {
    if (collection->append(i, i) == NBTreeAppendResult::OK_FLUSH_NEEDED) {
      // addrlist changed
      auto newroots = collection->get_roots();
      if (newroots == addrlist) {
        LOG(FATAL) << "Roots collection must change";
      }
      std::swap(newroots, addrlist);
      auto status = NBTreeExtentsList::repair_status(addrlist);
      EXPECT_EQ(status, NBTreeExtentsList::RepairStatus::REPAIR);
      nleafs++;
      if (nleafs == N) {
        nitems = i;
        break;
      }
    }
    if (i == N_values) {
      nitems = i;
      break;
    }
  }
  addrlist = collection->close();
  auto status = NBTreeExtentsList::repair_status(addrlist);
  EXPECT_EQ(status, NBTreeExtentsList::RepairStatus::OK);
  EXPECT_EQ(addrlist.back(), last_block);
  UNUSED(nitems);
}

TEST(TestNBTree, Test_nbtree_recovery_status_1) {
  test_storage_recovery_status(~0u, 32);
}

TEST(TestNBTree, Test_nbtree_recovery_status_2) {
  test_storage_recovery_status(2, ~0u);
}

TEST(TestNBtree, Test_nbtree_recovery_status_3) {
  test_storage_recovery_status(32, ~0u);
}

TEST(TestNBTree, Test_nbtree_recovery_status_4) {
  test_storage_recovery_status(32 * 32, ~0u);
}

//! Reopen storage that has been closed without final commit.
void test_storage_recovery(u32 N_blocks, u32 N_values) {
  LogicAddr last_block = EMPTY_ADDR;
  auto cb = [&last_block] (LogicAddr addr) {
    last_block = addr;
  };
  std::shared_ptr<BlockStore> bstore = BlockStoreBuilder::create_memstore(cb);
  std::vector<LogicAddr> addrlist;  // should be empty at first
  auto collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
  collection->force_init();

  u32 nleafs = 0;
  u32 nitems = 0;
  for (u32 i = 0; true; i++) {
    if (collection->append(i, i) == NBTreeAppendResult::OK_FLUSH_NEEDED) {
      // addrlist changed
      auto newroots = collection->get_roots();
      if (newroots == addrlist) {
        LOG(FATAL) << "Roots collection must change";
      }
      std::swap(newroots, addrlist);
      auto status = NBTreeExtentsList::repair_status(addrlist);
      EXPECT_EQ(status, NBTreeExtentsList::RepairStatus::REPAIR);
      nleafs++;
      if (nleafs == N_blocks) {
        nitems = i;
        break;
      }
    }
    if (i == N_values) {
      nitems = i;
      break;
    }
  }

  addrlist = collection->get_roots();
  // delete roots collection
  collection.reset();

  // TODO: check attempt to open tree using wrong id!
  collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
  collection->force_init();

  auto extents = collection->get_extents();
  for (size_t i = 0; i < extents.size(); i++) {
    auto extent = extents[i];
    check_tree_consistency(bstore, i, extent);
  }

  // Scan entire tree
  std::unique_ptr<RealValuedOperator> it = collection->search(0, nitems);
  std::vector<Timestamp> ts(nitems, 0);
  std::vector<double> xs(nitems, 0);
  size_t sz = 0;
  common::Status status;
  std::tie(status, sz) = it->read(ts.data(), xs.data(), nitems);
  LOG(INFO) << "sz=" << sz << " nitems=" << nitems;
  if (addrlist.empty()) {
    // Expect zero, data was stored in single leaf-node.
    EXPECT_EQ(0, sz);
  } else {
    if (nleafs == N_blocks) {
      // new leaf was empty before 'crash'
      EXPECT_EQ(nitems, sz);
    } else {
      // some data can be lost!
      EXPECT_TRUE(sz <= nitems);
    }
  }
  // Note: `status` should be equal to common::Status::Ok() if size of the destination
  // is equal to array's length. Otherwise iterator should return common::Status::NoData()
  // as an indication that all data-elements have ben read.
  EXPECT_TRUE(status == common::Status::NoData() || status == common::Status::Ok());
  for (u32 i = 0; i < sz; i++) {
    if (ts[i] != i) {
      LOG(FATAL) << "Invalid timestamp at " << i;
    }
    if (!same_value(xs[i], static_cast<double>(i))) {
      LOG(FATAL) << "Invalid timestamp at " << i;
    }
  }

  if (sz) {
    // Expected aggregates (calculated by hand)
    AggregationResult exp_agg = INIT_AGGRES;
    exp_agg.do_the_math(ts.data(), xs.data(), sz, false);

    // Single leaf node will be lost and aggregates will be empty anyway
    auto agg_iter = collection->aggregate(0, nitems);
    Timestamp new_agg_ts;
    AggregationResult new_agg_result = INIT_AGGRES;
    size_t agg_size;
    std::tie(status, agg_size) = agg_iter->read(&new_agg_ts, &new_agg_result, 1);
    if (status != common::Status::Ok()) {
      LOG(FATAL) << "Can't aggregate after recovery " << status.ToString();
    }

    // Check that results are correct and match the one that was calculated by hand
    EXPECT_EQ(new_agg_result.cnt, exp_agg.cnt);
    EXPECT_EQ(new_agg_result.first, exp_agg.first);
    EXPECT_EQ(new_agg_result.last, exp_agg.last);
    EXPECT_EQ(new_agg_result.max, exp_agg.max);
    EXPECT_EQ(new_agg_result.maxts, exp_agg.maxts);
    EXPECT_EQ(new_agg_result.min, exp_agg.min);
    EXPECT_EQ(new_agg_result.mints, exp_agg.mints);
    EXPECT_EQ(new_agg_result.sum, exp_agg.sum);
  }
}

TEST(TestNBTree, Test_nbtree_recovery_1) {
  test_storage_recovery(~0u, 10);
}

TEST(TestNBtree, Test_nbtree_recovery_2) {
  test_storage_recovery(1, ~0u);
}

TEST(TestNBtree, Test_nbtree_recovery_3) {
  test_storage_recovery(31, ~0u);
}

TEST(TestNBtree, Test_nbtree_recovery_4) {
  test_storage_recovery(32, ~0u);
}

TEST(TestNBtree, Test_nbtree_recovery_5) {
  test_storage_recovery(33, ~0u);
}

TEST(TestNBtree, Test_nbtree_recovery_6) {
  test_storage_recovery(33 * 33, ~0u);
}

//! Reopen storage that has been closed without final commit.
void test_storage_recovery_2(u32 N_blocks) {
  LogicAddr last_block = EMPTY_ADDR;
  auto cb = [&last_block] (LogicAddr addr) {
    last_block = addr;
  };
  std::shared_ptr<BlockStore> bstore = BlockStoreBuilder::create_memstore(cb);
  std::vector<LogicAddr> addrlist;  // should be empty at first
  auto collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
  collection->force_init();

  u32 nleafs = 0;

  auto try_to_recover = [&](std::vector<LogicAddr>&& addrlist, u32 N) {
    auto col = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
    col->force_init();
    // scan
    auto it = col->search(0, N);
    std::vector<Timestamp> ts(N, 0);
    std::vector<double> xs(N, 0);
    common::Status status = common::Status::Ok();
    size_t sz = 0;
    std::tie(status, sz) = it->read(ts.data(), xs.data(), N+1);
    EXPECT_EQ(sz, N);
    EXPECT_TRUE(status == common::Status::NoData() || status == common::Status::Ok());
    if (sz > 0) {
      EXPECT_EQ(ts[0], 0);
      EXPECT_EQ(ts[sz - 1], sz - 1);
    }

    if (sz) {
      auto agg_iter = col->aggregate(0, N + 1);
      Timestamp agg_ts;
      AggregationResult act_agg = INIT_AGGRES;
      size_t agg_size;
      std::tie(status, agg_size) = agg_iter->read(&agg_ts, &act_agg, 1);
      if (status != common::Status::Ok()) {
        LOG(FATAL) << "Can't aggregate after recovery " << status.ToString();
      }

      // Check that results are correct and match the one that was calculated by hand
      double exp_sum = (static_cast<double>(N - 1) * N) / 2.0;  // sum of the arithmetic progression [0:N-1]
      EXPECT_EQ(act_agg.cnt, N);
      EXPECT_EQ(act_agg.first, 0);
      EXPECT_EQ(act_agg.last, N - 1);
      EXPECT_EQ(act_agg.max, N - 1);
      EXPECT_EQ(act_agg.maxts, N - 1);
      EXPECT_EQ(act_agg.min, 0);
      EXPECT_EQ(act_agg.mints, 0);
      EXPECT_EQ(act_agg.sum, exp_sum);
    }
  };

  for (u32 i = 0; true; i++) {
    if (collection->append(i, i) == NBTreeAppendResult::OK_FLUSH_NEEDED) {
      // addrlist changed
      if (nleafs % 10 == 0) {
        try_to_recover(collection->get_roots(), i);
      }
      nleafs++;
      if (nleafs == N_blocks) {
        break;
      }
    }
  }
}

TEST(TestNBtree, Test_nbtree_recovery_7) {
  test_storage_recovery_2(32 * 32);
}

// Test iteration
void test_nbtree_leaf_iteration(Timestamp begin, Timestamp end) {
  IOVecLeaf leaf(42, EMPTY_ADDR, 0);
  Timestamp last_successfull = 100;
  Timestamp first_timestamp = 100;
  for (size_t ix = first_timestamp; true; ix++) {
    common::Status status = leaf.append(ix, static_cast<double>(ix));
    if (status == common::Status::Overflow()) {
      break;
    }
    if (status == common::Status::Ok()) {
      last_successfull = ix;
      continue;
    }
    LOG(FATAL) << status.ToString();
  }
  // Everytithing should work before commit
  auto iter = leaf.range(begin, end);
  // Calculate output size
  size_t sz = 0;
  auto min = std::min(begin, end);
  min = std::max(min, first_timestamp);
  auto max = std::max(begin, end);
  max = std::min(max, last_successfull);
  sz = max - min;
  // Perform read using iterator
  std::vector<Timestamp> tss(sz, 0);
  std::vector<double> xss(sz, 0);
  common::Status status;
  size_t outsz;
  std::tie(status, outsz) = iter->read(tss.data(), xss.data(), sz);
  // Check results
  EXPECT_EQ(outsz, sz);
  if(status != common::Status::Ok()) {
    LOG(FATAL) << status.ToString();
  }
  if (end < begin) {
    std::reverse(tss.begin(), tss.end());
    std::reverse(xss.begin(), xss.end());
    min++;
  }
  for(size_t ix = 0; ix < sz; ix++) {
    // iter from min to max
    EXPECT_EQ(tss.at(ix), min);
    EXPECT_TRUE(same_value(xss.at(ix), static_cast<double>(min)));
    min++;
  }
}

TEST(TestNBTree, Test_nbtree_leaf_iteration_1) {
  test_nbtree_leaf_iteration(0, 100000000);
}

TEST(TestNBTree, Test_nbtree_leaf_iteration_2) {
  test_nbtree_leaf_iteration(100000000, 0);
}

TEST(TestNBTree, Test_nbtree_leaf_iteration_3) {
  test_nbtree_leaf_iteration(200, 100000000);
}

TEST(TestNBTree, Test_nbtree_leaf_iteration_4) {
  test_nbtree_leaf_iteration(100000000, 200);
}

TEST(TestNBTree, Test_nbtree_leaf_iteration_5) {
  test_nbtree_leaf_iteration(0, 500);
}

TEST(TestNBTree, Test_nbtree_leaf_iteration_6) {
  test_nbtree_leaf_iteration(500, 0);
}

TEST(TestNBTree, Test_nbtree_leaf_iteration_7) {
  test_nbtree_leaf_iteration(200, 500);
}

TEST(TestNBTree, Test_nbtree_leaf_iteration_8) {
  test_nbtree_leaf_iteration(500, 200);
}

// Test aggregation

//! Generate time-series from random walk
struct RandomWalk {
  std::random_device                  randdev;
  std::mt19937                        generator;
  std::normal_distribution<double>    distribution;
  double                              value;

  RandomWalk(double start, double mean, double stddev)
      : generator(randdev())
        , distribution(mean, stddev)
        , value(start)
  {
  }

  double next() {
    value += rand() / double(RAND_MAX);    // distribution(generator);
    return value;
  }
};

AggregationResult calculate_expected_value(std::vector<double> const& xss) {
  AggregationResult expected = INIT_AGGRES;
  expected.sum = std::accumulate(xss.begin(), xss.end(), 0.0, [](double a, double b) { return a + b; });
  expected.max = std::accumulate(xss.begin(), xss.end(), std::numeric_limits<double>::min(),
                                 [](double a, double b) {
                                 return std::max(a, b);
                                 });
  expected.min = std::accumulate(xss.begin(), xss.end(), std::numeric_limits<double>::max(),
                                 [](double a, double b) {
                                 return std::min(a, b);
                                 });
  expected.cnt = xss.size();
  return expected;
}

void test_nbtree_leaf_aggregation(Timestamp begin, Timestamp end) {
  IOVecLeaf leaf(42, EMPTY_ADDR, 0);
  Timestamp first_timestamp = 100;
  std::vector<double> xss;
  RandomWalk rwalk(0.0, 1.0, 1.0);
  for (size_t ix = first_timestamp; true; ix++) {
    double val = rwalk.next();
    common::Status status = leaf.append(ix, val);
    if (status == common::Status::Overflow()) {
      break;
    }
    if (status.IsOk()) {
      if (begin < end) {
        if (ix >= begin && ix < end) {
          xss.push_back(val);
        }
      } else {
        if (ix <= begin && ix > end) {
          xss.push_back(val);
        }
      }
      continue;
    }
    LOG(FATAL) << status.ToString();
  }
  double first = xss.front();
  double last = xss.back();
  if (end < begin) {
    // we should reverse xss, otherwise expected and actual values wouldn't match exactly because
    // floating point arithmetics is not commutative
    std::reverse(xss.begin(), xss.end());
  }

  // Compute expected value
  auto expected = calculate_expected_value(xss);

  // Compare expected and actual
  auto it = leaf.aggregate(begin, end);
  common::Status status;
  size_t size = 100;
  std::vector<Timestamp> destts(size, 0);
  std::vector<AggregationResult> destxs(size, INIT_AGGRES);
  size_t outsz;
  std::tie(status, outsz) = it->read(destts.data(), destxs.data(), size);
  EXPECT_EQ(status, common::Status::Ok());
  EXPECT_EQ(outsz, 1);

  auto actual = destxs.at(0);
  EXPECT_TRUE(fabs(actual.cnt - expected.cnt) <= 10e-5);
  EXPECT_TRUE(fabs(actual.sum - expected.sum) <= 10e-5);
  EXPECT_TRUE(fabs(actual.min - expected.min) <= 10e-5);
  EXPECT_TRUE(fabs(actual.max - expected.max) <= 10e-5);
  EXPECT_TRUE(fabs(actual.first - first) <= 10e-5);
  EXPECT_TRUE(fabs(actual.last - last) <= 10e-5);

  // Subsequent call to `it->read` should fail
  std::tie(status, outsz) = it->read(destts.data(), destxs.data(), size);
  EXPECT_EQ(status, common::Status::NoData());
  EXPECT_EQ(outsz, 0);
}

TEST(TestNBTree, Test_nbtree_leaf_aggregation) {
  std::vector<std::pair<Timestamp, Timestamp>> params = {
    // Fwd
    {   0,   10000000 },
    { 200,   10000000 },
    {   0,        300 },
    { 200,        400 },
    // Bwd
    { 10000000,     0 },
    { 10000000,   200 },
    {      300,     0 },
    {      400,   200 },
  };
  for (auto cp: params) {
    test_nbtree_leaf_aggregation(cp.first, cp.second);
  }
}

void test_nbtree_superblock_iter(Timestamp begin, Timestamp end) {
  // Build this tree structure.
  Timestamp gen = 1000;
  size_t ncommits = 0;
  auto commit_counter = [&ncommits](LogicAddr) {
    ncommits++;
  };
  std::vector<double> expected;
  auto bstore = BlockStoreBuilder::create_memstore(commit_counter);
  std::vector<LogicAddr> empty;
  std::shared_ptr<NBTreeExtentsList> extents(new NBTreeExtentsList(42, empty, bstore));
  extents->force_init();
  RandomWalk rwalk(1.0, 0.1, 0.1);
  while (ncommits < NBTREE_FANOUT * NBTREE_FANOUT) {  // we should build three levels
    double value = rwalk.next();
    Timestamp ts = gen++;
    extents->append(ts, value);
    if (begin < end) {
      if (ts >= begin && ts < end) {
        expected.push_back(value);
      }
    } else {
      if (ts <= begin && ts > end) {
        expected.push_back(value);
      }
    }
  }
  if (begin > end) {
    std::reverse(expected.begin(), expected.end());
  }
  // Check actual output
  auto it = extents->search(begin, end);
  size_t chunk_size = 1000;
  std::vector<double> destxs(chunk_size, 0);
  std::vector<Timestamp> destts(chunk_size, 0);
  ssize_t expix = 0;
  while (true) {
    common::Status status;
    ssize_t size;
    std::tie(status, size) = it->read(destts.data(), destxs.data(), chunk_size);
    if (status == common::Status::NoData() && size == 0) {
      EXPECT_EQ(expix, expected.size());
      break;
    }
    if (status == common::Status::Ok() || (status == common::Status::NoData() && size != 0)) {
      for (auto i = 0; i < size; ++i) {
        EXPECT_EQ(expected[expix + i], destxs[i]);
      }
      expix += size;
      continue;
    }
    LOG(FATAL) << status.ToString();
  }
}

TEST(TestNBTree, Test_nbtree_superblock_iteration) {
  std::vector<std::pair<Timestamp, Timestamp>> tss = {
    {      0, 1000000 },
    {   2000, 1000000 },
    {      0,  600000 },
    {   2000,  600000 },
    { 400000,  500000 },
  };
  for (auto be: tss) {
    test_nbtree_superblock_iter(be.first, be.second);
    test_nbtree_superblock_iter(be.second, be.first);
  }
}

void test_nbtree_superblock_aggregation(Timestamp begin, Timestamp end) {
  // Build this tree structure.
  Timestamp gen = 1000;
  size_t ncommits = 0;
  auto commit_counter = [&ncommits](LogicAddr) {
    ncommits++;
  };
  std::vector<double> xss;
  auto bstore = BlockStoreBuilder::create_memstore(commit_counter);
  std::vector<LogicAddr> empty;
  std::shared_ptr<NBTreeExtentsList> extents(new NBTreeExtentsList(42, empty, bstore));
  extents->force_init();
  RandomWalk rwalk(1.0, 0.1, 0.1);
  while (ncommits < NBTREE_FANOUT * NBTREE_FANOUT || gen <= 1000000ul) {  // we should build three levels
    double value = rwalk.next();
    Timestamp ts = gen++;
    extents->append(ts, value);
    if (ts >= std::min(begin, end) && ts < std::max(begin, end)) {
      xss.push_back(value);
    }
  }
  double first = xss.empty() ? 0 : xss.front();
  double last  = xss.empty() ? 0 : xss.back();
  auto expected = calculate_expected_value(xss);

  // Check actual output
  auto it = extents->aggregate(begin, end);
  common::Status status;
  size_t size = 100;
  std::vector<Timestamp> destts(size, 0);
  std::vector<AggregationResult> destxs(size, INIT_AGGRES);
  std::tie(status, size) = it->read(destts.data(), destxs.data(), size);

  EXPECT_EQ(status, common::Status::Ok());
  EXPECT_EQ(size, 1);

  auto actual = destxs.at(0);
  EXPECT_TRUE(fabs(actual.cnt - expected.cnt) <= 10e-5);
  EXPECT_TRUE(fabs(actual.sum - expected.sum) <= 1.0);  // why.
  EXPECT_TRUE(fabs(actual.min - expected.min) <= 10e-5);
  EXPECT_TRUE(fabs(actual.max - expected.max) <= 10e-5);
  EXPECT_TRUE(fabs(actual.first - first) <= 10e-5);
  EXPECT_TRUE(fabs(actual.last - last) <= 10e-5);

  // Subsequent call to `it->read` should fail
  std::tie(status, size) = it->read(destts.data(), destxs.data(), size);
  EXPECT_EQ(status, common::Status::NoData());
  EXPECT_EQ(size, 0);
}

TEST(TestNBtree, Test_nbtree_superblock_aggregation) {
  std::vector<std::pair<Timestamp, Timestamp>> tss = {
    {      0, 1000000 },
    {   2000, 1000000 },
    {      0,  600000 },
    {   2000,  600000 },
    { 400000,  500000 },
  };
  for (auto be: tss) {
    test_nbtree_superblock_aggregation(be.first, be.second);
    // TODO: fixme
    //test_nbtree_superblock_aggregation(be.second, be.first);
  }
}

void test_nbtree_recovery_with_retention(LogicAddr nblocks, LogicAddr nremoved) {
  // Build this tree structure.
  assert(nremoved <= nblocks);  // both numbers are actually a numbers
  Timestamp gen = 1000;
  Timestamp begin = gen, end = gen, last_ts = gen;
  size_t buffer_cnt = 0;
  std::shared_ptr<NBTreeExtentsList> extents;
  auto commit_counter = [&](LogicAddr) {
    buffer_cnt++;
    if (buffer_cnt == nremoved) {
      // one time event
      begin = gen;
    }
    end = last_ts;
  };
  auto bstore = BlockStoreBuilder::create_memstore(commit_counter);
  std::vector<LogicAddr> empty;
  extents.reset(new NBTreeExtentsList(42, empty, bstore));
  extents->force_init();
  RandomWalk rwalk(1.0, 0.1, 0.1);
  while (buffer_cnt < nblocks) {
    double value = rwalk.next();
    Timestamp ts = gen++;
    extents->append(ts, value);
    last_ts = ts;
  }
  // Remove old values
  std::dynamic_pointer_cast<MemStore, BlockStore>(bstore)->remove(nremoved);

  // Recovery
  auto rescue_points = extents->get_roots();
  // We shouldn't close `extents` to emulate program state after crush.
  std::shared_ptr<NBTreeExtentsList> recovered(new NBTreeExtentsList(42, rescue_points, bstore));
  recovered->force_init();

  auto it = recovered->search(begin, end);
  if (end > begin) {
    size_t sz = end - begin;
    std::vector<Timestamp> tss(sz, 0);
    std::vector<double> xss(sz, .0);
    common::Status stat;
    size_t outsz;
    std::tie(stat, outsz) = it->read(tss.data(), xss.data(), sz);
    EXPECT_EQ(outsz, sz);
    EXPECT_TRUE(stat == common::Status::Ok() || stat == common::Status::NoData());

    for(Timestamp ts: tss) {
      EXPECT_EQ(ts, begin);
      begin++;
    }
  } else {
    // No output expected
    size_t sz = 10;
    std::vector<Timestamp> tss(sz, 0);
    std::vector<double> xss(sz, .0);
    common::Status stat;
    size_t outsz;
    std::tie(stat, outsz) = it->read(tss.data(), xss.data(), sz);
    EXPECT_EQ(outsz, 0);
    EXPECT_EQ(stat, common::Status::NoData());
  }
}

TEST(TestNBTree, Test_nbtree_recovery_with_retention_1) {
  std::vector<std::pair<LogicAddr, LogicAddr>> addrlist = {
    { 1, 0 },
    { 1, 1 },
    { 2, 0 },
    { 2, 1 },
    { 33, 1 },
    { 33, 10 },
    { 33, 33 },
    { 33*33, 33 },
    { 33*33, 33 * 33 },
  };
  for(auto pair: addrlist) {
    test_nbtree_recovery_with_retention(pair.first, pair.second);
  }
}

void test_nbtree_superblock_candlesticks(size_t commit_limit, Timestamp delta) {
  // Build this tree structure.
  Timestamp begin = 1000;
  Timestamp end = begin;
  size_t ncommits = 0;
  auto commit_counter = [&ncommits](LogicAddr) {
    ncommits++;
  };
  auto bstore = BlockStoreBuilder::create_memstore(commit_counter);
  std::vector<LogicAddr> empty;
  std::shared_ptr<NBTreeExtentsList> extents(new NBTreeExtentsList(42, empty, bstore));
  extents->force_init();
  RandomWalk rwalk(1.0, 0.1, 0.1);
  while (ncommits < NBTREE_FANOUT * NBTREE_FANOUT) {  // we should build three levels
    double value = rwalk.next();
    Timestamp ts = end++;
    extents->append(ts, value);
  }

  // Check actual output
  NBTreeCandlestickHint hint;
  hint.min_delta = delta;
  auto it = extents->candlesticks(begin, end, hint);
  common::Status status;
  size_t size = 1000;
  std::vector<Timestamp> destts(size, 0);
  std::vector<AggregationResult> destxs(size, INIT_AGGRES);
  std::tie(status, size) = it->read(destts.data(), destxs.data(), size);
  EXPECT_EQ(status, common::Status::Ok());

  for(size_t i = 1; i < size; i++) {
    AggregationResult prev, curr;
    prev = destxs[i - 1];
    curr = destxs[i];
    EXPECT_TRUE(fabs(prev.last - curr.first) <= 10e-5);
  }
}

TEST(TestNBTree, Test_nbtree_candlesticks) {
  std::vector<std::pair<size_t, Timestamp>> cases = {
    { 1, 10 },
    { 10, 10 },
    { 10, 10000 },
    { 33, 10 },
    { 33, 100 },
    { 33, 1000 },
    { 33, 100000 },
    { 33 * 33, 10000 },
  };
  for (auto it: cases) {
    test_nbtree_superblock_candlesticks(it.first, it.second);
  }
}

// Check that subsequent reopen procedures doesn't increases file size
TEST(TestNBTree, Test_reopen_storage_twice) {
  std::vector<LogicAddr> addrlist;
  std::shared_ptr<BlockStore> bstore =
      BlockStoreBuilder::create_memstore();

  auto collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
  collection->force_init();

  std::vector<Timestamp> tss = {
    1000ul, 1001ul, 1002ul, 1003ul, 1004ul,
    1005ul, 1006ul, 1007ul, 1008ul, 1009ul,
  };
  std::vector<double> xss = {
    1, 2, 3, 4, 5, 6, 7, 8, 9, 0
  };

  for (u32 i = 0; i < xss.size(); i++) {
    collection->append(tss[i], xss[i]);
  }

  // Close first time
  addrlist = collection->close();

  EXPECT_EQ(addrlist.size(), 1);

  // Reopen first time (this will change tree configuration from single leaf node to superblock+leaf)
  collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
  collection->force_init();

  auto extents = collection->get_extents();
  for (size_t i = 0; i < extents.size(); i++) {
    auto extent = extents[i];
    check_tree_consistency(bstore, i, extent);
  }

  // Close second time
  auto addrlist2 = collection->close();

  EXPECT_EQ(addrlist2.size(), 1);
  EXPECT_EQ(addrlist.size(), addrlist2.size());
  for (auto i = 0; i < addrlist.size(); ++i) {
    EXPECT_EQ(addrlist[i], addrlist2[i]);
  }

  // Reopen second time (this should preserve 'superblock+leaf' tree configuration)
  collection = std::make_shared<NBTreeExtentsList>(42, addrlist2, bstore);
  collection->force_init();

  extents = collection->get_extents();
  for (size_t i = 0; i < extents.size(); i++) {
    auto extent = extents[i];
    check_tree_consistency(bstore, i, extent);
  }
}

// Check that late write is not possible after reopen
TEST(TestNBTree, Test_reopen_late_write) {
  std::vector<LogicAddr> addrlist;
  std::shared_ptr<BlockStore> bstore =
      BlockStoreBuilder::create_memstore();

  auto collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
  collection->force_init();

  std::vector<Timestamp> tss = {
    1000ul, 1001ul, 1002ul, 1003ul, 1004ul,
    1005ul, 1006ul, 1007ul, 1008ul, 1009ul,
  };
  std::vector<double> xss = {
    1, 2, 3, 4, 5, 6, 7, 8, 9, 0
  };

  for (u32 i = 0; i < xss.size(); i++) {
    collection->append(tss[i], xss[i]);
  }

  // Close first time
  addrlist = collection->close();
  EXPECT_EQ(addrlist.size(), 1);

  // Reopen first time
  collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
  collection->force_init();

  // Late write
  auto status = collection->append(tss.front(), xss.front());

  EXPECT_EQ(status, NBTreeAppendResult::FAIL_LATE_WRITE);
}

TEST(TestNBTree, Test_reopen_write_reopen) {
  std::vector<LogicAddr> addrlist;
  std::shared_ptr<BlockStore> bstore =
      BlockStoreBuilder::create_memstore();

  auto collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
  collection->force_init();

  std::vector<Timestamp> tss = {
    1000ul, 1001ul, 1002ul, 1003ul, 1004ul,
    1005ul, 1006ul, 1007ul, 1008ul, 1009ul,
  };
  std::vector<double> xss = {
    1, 2, 3, 4, 5, 6, 7, 8, 9, 0
  };

  for (u32 i = 0; i < xss.size(); i++) {
    collection->append(tss[i], xss[i]);
  }

  // Close first time
  addrlist = collection->close();

  EXPECT_EQ(addrlist.size(), 1);

  // Reopen first time (this will change tree configuration from single leaf node to superblock+leaf)
  collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
  collection->force_init();

  std::vector<Timestamp> tss2 = {
    1010ul, 1011ul, 1012ul, 1013ul, 1014ul
  };
  std::vector<double> xss2 = {
    1, 2, 3, 4, 5
  };
  for (u32 i = 0; i < xss2.size(); i++) {
    collection->append(tss2[i], xss2[i]);
  }

  // Close second time
  auto addrlist2 = collection->close();

  EXPECT_EQ(addrlist2.size(), 2);

  // Reopen second time
  collection = std::make_shared<NBTreeExtentsList>(42, addrlist2, bstore);
  collection->force_init();

  auto extents = collection->get_extents();
  for (size_t i = 0; i < extents.size(); i++) {
    auto extent = extents[i];
    check_tree_consistency(bstore, i, extent);
  }
}

void test_nbtree_group_aggregate_forward(size_t commit_limit, u64 step, int start_offset, const int ts_increment=1) {
  // Build this tree structure.
  Timestamp begin = 1000;
  Timestamp end = begin;
  size_t ncommits = 0;
  auto commit_counter = [&ncommits](LogicAddr) {
    ncommits++;
  };
  auto bstore = BlockStoreBuilder::create_memstore(commit_counter);
  std::vector<LogicAddr> empty;
  std::shared_ptr<NBTreeExtentsList> extents(new NBTreeExtentsList(42, empty, bstore));
  extents->force_init();
  RandomWalk rwalk(1.0, 0.1, 0.1);
  AggregationResult acc = INIT_AGGRES;
  std::vector<AggregationResult> buckets;
  auto query_begin = static_cast<u64>(static_cast<i64>(begin) + start_offset);
  auto bucket_ix = 0ull;
  while (ncommits < commit_limit) {
    auto current_bucket = (end - query_begin) / step;
    if (end >= query_begin && current_bucket > bucket_ix && acc.cnt) {
      bucket_ix = current_bucket;
      buckets.push_back(acc);
      acc = INIT_AGGRES;
    }
    double value = rwalk.next();
    Timestamp ts = end;
    end += ts_increment;
    extents->append(ts, value);
    if (ts >= query_begin) {
      acc.add(ts, value, true);
    }
  }
  if (acc.cnt > 0) {
    buckets.push_back(acc);
  }

  // Check actual output
  auto it = extents->group_aggregate(query_begin, end, step);
  common::Status status;
  size_t size = buckets.size();
  std::vector<Timestamp> destts(size, 0);
  std::vector<AggregationResult> destxs(size, INIT_AGGRES);
  size_t out_size;
  std::tie(status, out_size) = it->read(destts.data(), destxs.data(), size);
  EXPECT_EQ(out_size, buckets.size());
  EXPECT_EQ(status, common::Status::Ok());

  for(size_t i = 1; i < size; i++) {
    auto timestamp = destts.at(i);
    if ((timestamp - query_begin) % step != 0) {
      LOG(FATAL) << "Invalid timestamp";
    }
    EXPECT_LE(fabs(buckets.at(i).sum - destxs.at(i).sum), 0.0001);
    EXPECT_LE(fabs(buckets.at(i).sum - destxs.at(i).sum), 0.0001);
    EXPECT_LE(fabs(buckets.at(i).cnt - destxs.at(i).cnt), 0.0001);
    EXPECT_LE(fabs(buckets.at(i).min - destxs.at(i).min), 0.0001);
    EXPECT_LE(fabs(buckets.at(i).max - destxs.at(i).max), 0.0001);
    EXPECT_EQ(buckets.at(i)._begin, destxs.at(i)._begin);
    EXPECT_EQ(buckets.at(i)._end, destxs.at(i)._end);
    EXPECT_EQ(buckets.at(i).mints, destxs.at(i).mints);
    EXPECT_EQ(buckets.at(i).maxts, destxs.at(i).maxts);
  }
}

TEST(TestNBtree, Test_group_aggregate_forward) {
  const int N_runs = 1;
  std::vector<std::tuple<u32, u32, int, int>> cases = {
    std::make_tuple( 1,     100, 0, 1),
    std::make_tuple( 2,     100, 0, 1),
    std::make_tuple(10,     100, 0, 1),
    std::make_tuple(32,     100, 0, 1),
    std::make_tuple(32*32,  100, 0, 1),
    std::make_tuple(32*32,  100, 1, 1),
    std::make_tuple(32*32,  100,-1, 1),

    std::make_tuple( 1,    1000, 0, 1),
    std::make_tuple( 2,    1000, 0, 1),
    std::make_tuple(10,    1000, 0, 1),
    std::make_tuple(32,    1000, 0, 1),
    std::make_tuple(32*32, 1000, 0, 1),
    std::make_tuple(32*32, 1000, 1, 1),
    std::make_tuple(32*32, 1000,-1, 1),

    std::make_tuple( 1,   10000, 0, 1),
    std::make_tuple( 2,   10000, 0, 1),
    std::make_tuple(10,   10000, 0, 1),
    std::make_tuple(32,   10000, 0, 1),
    std::make_tuple(32*32,10000, 0, 1),
    std::make_tuple(32*32,10000, 1, 1),
    std::make_tuple(32*32,10000,-1, 1),

    std::make_tuple(    1, 100, 0, 100),
    std::make_tuple(   10, 100, 0, 100),
    std::make_tuple(   32, 100, 0, 100),
    std::make_tuple(32*32, 100, 0, 100),
    std::make_tuple(32*32, 100, 1, 100),
    std::make_tuple(32*32, 100,-1, 100),

    std::make_tuple(    1, 100, 0, 55),
    std::make_tuple(   10, 100, 0, 55),
    std::make_tuple(   32, 100, 0, 55),
    std::make_tuple(32*32, 100, 0, 55),
    std::make_tuple(32*32, 100, 1, 55),
    std::make_tuple(32*32, 100,-1, 55),

    std::make_tuple(    1, 100, 0, 1000),
    std::make_tuple(   10, 100, 0, 1000),
    std::make_tuple(   32, 100, 0, 1000),
    std::make_tuple(32*32, 100, 0, 1000),
    std::make_tuple(32*32, 100, 1, 1000),
    std::make_tuple(32*32, 100,-1, 1000),
  };
  for (int i = 0; i < N_runs; i++) {
    for (auto t: cases) {
      test_nbtree_group_aggregate_forward(std::get<0>(t), std::get<1>(t), std::get<2>(t), std::get<3>(t));
    }
  }
}

void test_nbtree_group_aggregate_backward(size_t commit_limit, u64 step, int start_offset, const int ts_inc = 1) {
  // Build this tree structure.
  Timestamp begin = 1000;
  Timestamp end = begin;
  size_t ncommits = 0;
  auto commit_counter = [&ncommits](LogicAddr) {
    ncommits++;
  };
  auto bstore = BlockStoreBuilder::create_memstore(commit_counter);
  std::vector<LogicAddr> empty;
  std::shared_ptr<NBTreeExtentsList> extents(new NBTreeExtentsList(42, empty, bstore));
  extents->force_init();

  // Original values
  RandomWalk rwalk(1.0, 0.1, 0.1);
  std::vector<Timestamp> tss;
  std::vector<double> xss;
  while (ncommits < commit_limit) {
    double value = rwalk.next();
    Timestamp ts = end;
    end += ts_inc;
    extents->append(ts, value);
    tss.push_back(ts);
    xss.push_back(value);
  }

  // Calculate aggregates in backward direction
  std::reverse(xss.begin(), xss.end());
  std::reverse(tss.begin(), tss.end());
  AggregationResult acc = INIT_AGGRES;
  std::vector<AggregationResult> buckets;
  auto bucket_ix = 0ull;
  auto query_begin = static_cast<u64>(static_cast<i64>(end) - start_offset);
  auto query_end = static_cast<u64>(static_cast<i64>(begin) + start_offset);
  for (auto ix = 0ul; ix < xss.size(); ix++) {
    auto current_bucket = (query_begin - tss[ix]) / step;
    if (tss[ix] <= query_begin && tss[ix] > query_end && current_bucket != bucket_ix && acc.cnt) {
      bucket_ix = current_bucket;
      buckets.push_back(acc);
      acc = INIT_AGGRES;
    }
    if (tss[ix] <= query_begin && tss[ix] > query_end) {
      acc.add(tss[ix], xss[ix], false);
    }
  }
  if (acc.cnt > 0) {
    buckets.push_back(acc);
  }

  // Check actual output
  auto it = extents->group_aggregate(query_begin, query_end, step);
  common::Status status;
  size_t size = buckets.size();
  std::vector<Timestamp> destts(size, 0);
  std::vector<AggregationResult> destxs(size, INIT_AGGRES);
  size_t out_size;
  std::tie(status, out_size) = it->read(destts.data(), destxs.data(), size);
  EXPECT_EQ(out_size, buckets.size());
  EXPECT_EQ(status, common::Status::Ok());

  for(size_t i = 0; i < size; i++) {
    auto timestamp = destts.at(i);
    if ((query_begin - timestamp) % step != 0) {
      LOG(FATAL) << "Invalid timestamp";
    }
    EXPECT_LE(fabs(buckets.at(i).sum - destxs.at(i).sum), 0.0001);
    EXPECT_LE(fabs(buckets.at(i).cnt - destxs.at(i).cnt), 0.0001);
    EXPECT_LE(fabs(buckets.at(i).min - destxs.at(i).min), 0.0001);
    EXPECT_LE(fabs(buckets.at(i).max - destxs.at(i).max), 0.0001);
    EXPECT_EQ(buckets.at(i)._begin, destxs.at(i)._begin);
    EXPECT_EQ(buckets.at(i)._end, destxs.at(i)._end);
    EXPECT_EQ(buckets.at(i).mints, destxs.at(i).mints);
    EXPECT_EQ(buckets.at(i).maxts, destxs.at(i).maxts);
  }
}

TEST(TestNBTree, Test_group_aggregate_backward) {
  const int N_runs = 1;
  std::vector<std::tuple<u32, u32, int, int>> cases = {
    std::make_tuple( 1,     100, 0, 1),
    std::make_tuple( 2,     100, 0, 1),
    std::make_tuple(10,     100, 0, 1),
    std::make_tuple(32,     100, 0, 1),
    std::make_tuple(32*32,  100, 0, 1),
    std::make_tuple(32*32,  100, 1, 1),
    std::make_tuple(32*32,  100,-1, 1),

    std::make_tuple( 1,    1000, 0, 1),
    std::make_tuple( 2,    1000, 0, 1),
    std::make_tuple(10,    1000, 0, 1),
    std::make_tuple(32,    1000, 0, 1),
    std::make_tuple(32*32, 1000, 0, 1),
    std::make_tuple(32*32, 1000, 1, 1),
    std::make_tuple(32*32, 1000,-1, 1),

    std::make_tuple( 1,   10000, 0, 1),
    std::make_tuple( 2,   10000, 0, 1),
    std::make_tuple(10,   10000, 0, 1),
    std::make_tuple(32,   10000, 0, 1),
    std::make_tuple(32*32,10000, 0, 1),
    std::make_tuple(32*32,10000, 1, 1),
    std::make_tuple(32*32,10000,-1, 1),

    std::make_tuple(    1, 100, 0, 100),
    std::make_tuple(   10, 100, 0, 100),
    std::make_tuple(   32, 100, 0, 100),
    std::make_tuple(32*32, 100, 0, 100),
    std::make_tuple(32*32, 100, 1, 100),
    std::make_tuple(32*32, 100,-1, 100),

    std::make_tuple(    1, 100, 0, 55),
    std::make_tuple(   10, 100, 0, 55),
    std::make_tuple(   32, 100, 0, 55),
    std::make_tuple(32*32, 100, 0, 55),
    std::make_tuple(32*32, 100, 1, 55),
    std::make_tuple(32*32, 100,-1, 55),

    std::make_tuple(    1, 100, 0, 1000),
    std::make_tuple(   10, 100, 0, 1000),
    std::make_tuple(   32, 100, 0, 1000),
    std::make_tuple(32*32, 100, 0, 1000),
    std::make_tuple(32*32, 100, 1, 1000),
    std::make_tuple(32*32, 100,-1, 1000),
  };
  for (int i = 0; i < N_runs; i++) {
    for (auto t: cases) {
      test_nbtree_group_aggregate_backward(std::get<0>(t), std::get<1>(t), std::get<2>(t), std::get<3>(t));
    }
  }
}


template<class Cont>
static void fill_leaf(IOVecLeaf* leaf, Cont tss) {
  for (auto ts: tss) {
    auto status = leaf->append(ts, ts*0.1);
    if (status != common::Status::Ok()) {
      throw std::runtime_error("unexpected error");
    }
  }
}

static LogicAddr save_leaf(IOVecLeaf* leaf, IOVecSuperblock* parent, std::shared_ptr<BlockStore> bstore) {
  common::Status status;
  LogicAddr  result;
  std::tie(status, result) = leaf->commit(bstore);
  if (status != common::Status::Ok()) {
    throw std::runtime_error("commit failed");
  }
  SubtreeRef ref = {};
  status = init_subtree_from_leaf(*leaf, ref);
  if (status != common::Status::Ok()) {
    throw std::runtime_error("can't init SubtreeRef");
  }
  ref.addr = result;
  status = parent->append(ref);
  if (status != common::Status::Ok()) {
    throw std::runtime_error("can't append");
  }
  return result;
}

/**
 * @brief Read block from block store, expect success
 */
static std::unique_ptr<IOVecBlock> read_block(std::shared_ptr<BlockStore> bstore, LogicAddr addr) {
  common::Status status;
  std::unique_ptr<IOVecBlock> block;
  std::tie(status, block) = bstore->read_iovec_block(addr);
  if (status != common::Status::Ok()) {
    throw std::runtime_error("can't read block");
  }
  return block;
}

static std::vector<Timestamp> extract_timestamps(RealValuedOperator& it) {
  std::vector<Timestamp> tss;
  std::vector<double> xss;
  const size_t size = 10000;
  tss.resize(size);
  xss.resize(size);
  size_t sz;
  common::Status status;
  std::tie(status, sz) = it.read(tss.data(), xss.data(), size);
  if (status != common::Status::Ok() && status != common::Status::NoData()) {
    throw std::runtime_error("can't read data");
  }
  tss.resize(sz);
  return tss;
}

/* Create the following structure:
 *          [inner]
 *         /   |   \
 *  [leaf0] [leaf1] [leaf2]
 */
void test_node_split_algorithm_lvl2(Timestamp pivot, std::map<int, std::vector<Timestamp>>& tss, int num_new_nodes) {
  std::shared_ptr<BlockStore> bstore = BlockStoreBuilder::create_memstore();
  const ParamId id = 42;
  LogicAddr prev = EMPTY_ADDR;
  IOVecSuperblock sblock(id, EMPTY_ADDR, 0, 1);
  // 0
  IOVecLeaf l0(id, prev, 0);
  fill_leaf(&l0, tss[0]);
  prev = save_leaf(&l0, &sblock, bstore);
  // 1
  IOVecLeaf l1(id, prev, 1);
  fill_leaf(&l1, tss[1]);
  prev = save_leaf(&l1, &sblock, bstore);
  // 2
  IOVecLeaf l2(id, prev, 2);
  fill_leaf(&l2, tss[2]);
  prev = save_leaf(&l2, &sblock, bstore);

  LogicAddr root;
  common::Status status;
  std::tie(status, root) = sblock.commit(bstore);
  EXPECT_EQ(status, common::Status::Ok());
  EXPECT_EQ(root - prev, 1);

  LogicAddr new_root;
  LogicAddr last_child;
  std::tie(status, new_root, last_child) = sblock.split(bstore, pivot, false);
  EXPECT_EQ(status, common::Status::Ok());
  EXPECT_EQ(new_root - root, num_new_nodes);

  IOVecSuperblock new_sblock(read_block(bstore, new_root));
  std::unique_ptr<RealValuedOperator> it = new_sblock.search(0, 100, bstore);
  auto actual = extract_timestamps(*it);

  std::unique_ptr<RealValuedOperator> orig_it = sblock.search(0, 100, bstore);
  auto expected = extract_timestamps(*orig_it);

  EXPECT_NE(actual.size(), 0);
  EXPECT_EQ(actual.size(), expected.size());
  for (size_t i = 0; i < expected.size(); ++i) {
    EXPECT_EQ(actual[i], expected[i]);
  }
}

TEST(TestNBtree, Test_node_split_algorithm_1) {
  /* Split middle node in:
   *          [inner]
   *         /   |   \
   *  [leaf0] [leaf1] [leaf2]
   *
   * The result should look like this:
   *
   *          ____[inner]____
   *         /    |     |    \
   *  [leaf0] [leaf1] [leaf2] [leaf3]
   *
   * 3 new nodes should be created
   */
  std::map<int, std::vector<Timestamp>> tss = {
    { 0, {  1,  2,  3,  4,  5,  6,  7,  8,  9, 10 }},
    { 1, { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 }},
    { 2, { 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 }},
  };
  test_node_split_algorithm_lvl2(15, tss, 3);
}

TEST(TestNBtree, Test_node_split_algorithm_2) {
  /* Split first leaf node on first element in:
   *          [inner]
   *         /   |   \
   *  [leaf0] [leaf1] [leaf2]
   *
   * The result should look like this:
   *          [inner]
   *         /   |   \
   *  [leaf0] [leaf1] [leaf2]
   *
   * 2 new nodes should be created
   */
  std::map<int, std::vector<Timestamp>> tss = {
    { 0, {  1,  2,  3,  4,  5,  6,  7,  8,  9, 10 }},
    { 1, { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 }},
    { 2, { 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 }},
  };
  test_node_split_algorithm_lvl2(1, tss, 2);
}

TEST(TestNBTree, Test_node_split_algorithm_3) {
  /* Split middle node in:
   *          [inner]
   *         /   |   \
   *  [leaf0] [leaf1] [leaf2]
   *
   * The result should look like this:
   *          [inner]
   *         /   |   \ \
   *  [leaf0] [leaf1] [leaf2] [leaf3]
   *
   * 3 new nodes should be created
   */
  std::map<int, std::vector<Timestamp>> tss = {
    { 0, {  1,  2,  3,  4,  5,  6,  7,  8,  9, 10 }},
    { 1, { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 }},
    { 2, { 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 }},
  };
  test_node_split_algorithm_lvl2(30, tss, 3);
}

static LogicAddr append_inner_node(IOVecSuperblock& root, IOVecSuperblock& child, std::shared_ptr<BlockStore> bstore) {
  common::Status status;
  LogicAddr child_addr;
  std::tie(status, child_addr) = child.commit(bstore);
  EXPECT_EQ(status, common::Status::Ok());
  SubtreeRef ref;
  status = init_subtree_from_subtree(child, ref);
  ref.addr = child_addr;
  EXPECT_EQ(status, common::Status::Ok());
  status = root.append(ref);
  EXPECT_EQ(status, common::Status::Ok());
  return child_addr;
}

static void check_backrefs(IOVecSuperblock& root, std::shared_ptr<BlockStore> bstore) {
  // Get the children and check connections
  std::vector<SubtreeRef> refs;
  auto status = root.read_all(&refs);
  EXPECT_TRUE(status == common::Status::Ok());
  LogicAddr prev_node_addr = EMPTY_ADDR;
  for (auto ref: refs) {
    IOVecSuperblock curr(read_block(bstore, ref.addr));
    EXPECT_EQ(curr.get_prev_addr(), prev_node_addr);
    prev_node_addr = ref.addr;
  }
}

/* Create the following structure:
 *
 *                   [inner0]
 *                  /        \
 *          [inner1]<---------[inner2]
 *         /   |    \            |
 *  [leaf0]<-[leaf1]<-[leaf2]  [leaf3]
 *
 * After the split the links between the inner nodes should be preserved, e.g.:
 *
 *                   [inner0]
 *                  /   |    \
 *          [inner1]<---------[inner3]
 *         /    |   \            |
 *  [leaf0]  [inn4]  [leaf3]  [leaf4]
 *          /     \
 *      [leaf1]  [leaf2]
 *
 * The node `inner1` is new so `inner2` should be cloned to update the link to `inner1`.
 */
void test_node_split_algorithm_lvl3(Timestamp pivot, std::map<int, std::vector<Timestamp>>& tss) {
  std::shared_ptr<BlockStore> bstore = BlockStoreBuilder::create_memstore();
  common::Status status;
  const ParamId id = 42;
  LogicAddr prev = EMPTY_ADDR;
  IOVecSuperblock inner0(id, EMPTY_ADDR, 0, 2);
  IOVecSuperblock inner1(id, EMPTY_ADDR, 0, 1);
  // 0
  IOVecLeaf l0(id, prev, 0);
  fill_leaf(&l0, tss[0]);
  prev = save_leaf(&l0, &inner1, bstore);
  // 1
  IOVecLeaf l1(id, prev, 1);
  fill_leaf(&l1, tss[1]);
  prev = save_leaf(&l1, &inner1, bstore);
  // 2
  IOVecLeaf l2(id, prev, 2);
  fill_leaf(&l2, tss[2]);
  prev = save_leaf(&l2, &inner1, bstore);
  auto inner1_addr = append_inner_node(inner0, inner1, bstore);

  IOVecSuperblock inner2(id, inner1_addr, 1, 1);
  // 3
  IOVecLeaf l3(id, EMPTY_ADDR, 0);
  fill_leaf(&l3, tss[3]);
  prev = save_leaf(&l3, &inner2, bstore);

  append_inner_node(inner0, inner2, bstore);

  LogicAddr inner0_addr;
  std::tie(status, inner0_addr) = inner0.commit(bstore);
  EXPECT_EQ(status, common::Status::Ok());

  LogicAddr new_inner0_addr;
  LogicAddr last_child;
  std::tie(status, new_inner0_addr, last_child) = inner0.split(bstore, pivot, true);
  EXPECT_EQ(status, common::Status::Ok());

  IOVecSuperblock new_sblock(read_block(bstore, new_inner0_addr));

  // Compare scan results
  std::unique_ptr<RealValuedOperator> it = new_sblock.search(0, 100, bstore);
  auto actual = extract_timestamps(*it);
  std::unique_ptr<RealValuedOperator> orig_it = inner0.search(0, 100, bstore);
  auto expected = extract_timestamps(*orig_it);
  EXPECT_NE(actual.size(), 0);
  EXPECT_EQ(actual.size(), expected.size());
  for (size_t i = 0; i < expected.size(); ++i) {
    EXPECT_EQ(actual[i], expected[i]);
  }

  // Compare aggregation results
  std::unique_ptr<AggregateOperator> new_agg = new_sblock.aggregate(0, 100, bstore);
  std::unique_ptr<AggregateOperator> old_agg = inner0.aggregate(0, 100, bstore);
  AggregationResult new_agg_res;
  Timestamp new_agg_ts;
  size_t size;
  std::tie(status, size) = new_agg->read(&new_agg_ts, &new_agg_res, 1);
  EXPECT_EQ(size, 1);
  EXPECT_TRUE(status == common::Status::Ok() || status == common::Status::NoData());
  AggregationResult old_agg_res;
  Timestamp old_agg_ts;
  std::tie(status, size) = old_agg->read(&old_agg_ts, &old_agg_res, 1);
  EXPECT_EQ(size, 1);
  EXPECT_TRUE(status == common::Status::Ok() || status == common::Status::NoData());

  EXPECT_EQ(old_agg_res.cnt,   new_agg_res.cnt);
  EXPECT_EQ(old_agg_res.first, new_agg_res.first);
  EXPECT_EQ(old_agg_res.last,  new_agg_res.last);
  EXPECT_EQ(old_agg_res.max,   new_agg_res.max);
  EXPECT_EQ(old_agg_res.min,   new_agg_res.min);
  EXPECT_EQ(old_agg_res.maxts, new_agg_res.maxts);
  EXPECT_EQ(old_agg_res.mints, new_agg_res.mints);
  EXPECT_EQ(old_agg_res.sum,   new_agg_res.sum);

  // Check the old root first
  check_backrefs(inner0, bstore);
  check_backrefs(new_sblock, bstore);
}

TEST(TestNBtree, Test_node_split_algorithm_10) {
  /* Split middle node in:
   *
   *                   [inner0]
   *                  /        \
   *         [inner1]<----------[inner2]
   *         /   |   \              |
   *  [leaf0] [leaf1] [leaf2]    [leaf3]
   *
   * The result should look like this:
   *
   *                   [inner0]
   *                  /        \
   *         [inner1]<----------[inner2]
   *         /   |   \              |
   *  [leaf0] [inner3] [leaf3]    [leaf4]
   *           /   \
   *       [leaf1] [leaf2]
   *
   */
  std::map<int, std::vector<Timestamp>> tss = {
    { 0, {  1,  2,  3,  4,  5,  6,  7,  8,  9, 10 }},
    { 1, { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 }},
    { 2, { 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 }},
    { 3, { 31, 32, 33, 34, 35, 36, 37, 38, 39, 40 }},
  };
  test_node_split_algorithm_lvl3(15, tss);
}

TEST(TestNBTree, Test_node_split_algorithm_11) {
  /* Split middle node in:
   *
   *                   [inner0]
   *                  /        \
   *         [inner1]<----------[inner2]
   *         /   |   \              |
   *  [leaf0] [leaf1] [leaf2]    [leaf3]
   *
   * The result should look like this:
   *
   *                   [inner0]
   *                  /        \
   *         [inner1]<----------[inner2]
   *         /   |   \              |
   *  [inner3] [leaf1] [leaf2]    [leaf3]
   *     |
   *  [leaf0]
   *
   */
  std::map<int, std::vector<Timestamp>> tss = {
    { 0, {  1,  2,  3,  4,  5,  6,  7,  8,  9, 10 }},
    { 1, { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 }},
    { 2, { 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 }},
    { 3, { 31, 32, 33, 34, 35, 36, 37, 38, 39, 40 }},
  };
  test_node_split_algorithm_lvl3(1, tss);
}

TEST(TestNBTree, Test_node_split_algorithm_12) {
  /* Split middle node in:
   *
   *                   [inner0]
   *                  /        \
   *         [inner1]<----------[inner2]
   *         /   |   \              |
   *  [leaf0] [leaf1] [leaf2]    [leaf3]
   *
   * The result should look like this:
   *
   *                   [inner0]
   *                  /        \
   *         [inner1]<----------[inner2]
   *         /   |   \              |
   *  [leaf0] [leaf1] [inner3]    [leaf4]
   *                   /   \
   *              [leaf2] [leaf3]
   *
   */
  std::map<int, std::vector<Timestamp>> tss = {
    { 0, {  1,  2,  3,  4,  5,  6,  7,  8,  9, 10 }},
    { 1, { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 }},
    { 2, { 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 }},
    { 3, { 31, 32, 33, 34, 35, 36, 37, 38, 39, 40 }},
  };
  test_node_split_algorithm_lvl3(25, tss);
}

TEST(TestNBtree, Test_node_split_algorithm_13) {
  /* Split middle node in:
   *
   *                   [inner0]
   *                  /        \
   *         [inner1]<----------[inner2]
   *         /   |   \              |
   *  [leaf0] [leaf1] [leaf2]    [leaf3]
   *
   * The result should look like this:
   *
   *                   [inner0]
   *                  /        \
   *         [inner1]<----------[inner2]
   *         /   |   \              |
   *  [leaf0] [leaf1] [inner3]    [leaf4]
   *                   /   \
   *              [leaf2] [leaf3]
   *
   */

  std::map<int, std::vector<Timestamp>> tss = {
    { 0, {  1,  2,  3,  4,  5,  6,  7,  8,  9, 10 }},
    { 1, { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 }},
    { 2, { 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 }},
    { 3, { 31, 32, 33, 34, 35, 36, 37, 38, 39, 40 }},
  };
  test_node_split_algorithm_lvl3(30, tss);
}

TEST(TestNBTree, Test_node_split_algorithm_14) {
  /* Split middle node in:
   *
   *                   [inner0]
   *                  /        \
   *         [inner1]<----------[inner2]
   *         /   |   \              |
   *  [leaf0] [leaf1] [leaf2]    [leaf3]
   *
   * The result should look like this:
   *
   *                   [inner0]
   *                  /        \
   *         [inner1]<----------[inner2]
   *         /   |   \              |
   *  [leaf0] [leaf1] [leaf2]    [inner3]
   *                              /   \
   *                         [leaf3] [leaf4]
   *
   */
  std::map<int, std::vector<Timestamp>> tss = {
    { 0, {  1,  2,  3,  4,  5,  6,  7,  8,  9, 10 }},
    { 1, { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 }},
    { 2, { 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 }},
    { 3, { 31, 32, 33, 34, 35, 36, 37, 38, 39, 40 }},
  };
  test_node_split_algorithm_lvl3(36, tss);
}

TEST(TestNBtree, Test_node_split_algorithm_15) {
  /* Split middle node in:
   *
   *                   [inner0]
   *                  /        \
   *         [inner1]<----------[inner2]
   *         /   |   \              |
   *  [leaf0] [leaf1] [leaf2]    [leaf3]
   *
   * The result should look like this:
   *
   *                   [inner0]
   *                  /        \
   *         [inner1]<----------[inner2]
   *         /   |   \              |
   *  [leaf0] [leaf1] [leaf2]    [inner3]
   *                                |
   *                             [leaf3]
   *
   */
  std::map<int, std::vector<Timestamp>> tss = {
    { 0, {  1,  2,  3,  4,  5,  6,  7,  8,  9, 10 }},
    { 1, { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 }},
    { 2, { 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 }},
    { 3, { 31, 32, 33, 34, 35, 36, 37, 38, 39, 40 }},
  };
  test_node_split_algorithm_lvl3(31, tss);
}

static std::tuple<int, int> count_nbtree_nodes(std::shared_ptr<BlockStore> bstore, LogicAddr root_addr) {
  std::queue<std::pair<LogicAddr, bool>> addrlist;
  addrlist.push(std::make_pair(root_addr, false));
  int number_of_inner_nodes = 0;
  int number_of_leaf_nodes = 0;
  while (!addrlist.empty()) {
    bool is_leaf;
    LogicAddr addr;
    std::tie(addr, is_leaf) = addrlist.front();
    addrlist.pop();
    if (is_leaf) {
      number_of_leaf_nodes++;
    } else {
      number_of_inner_nodes++;
      IOVecSuperblock sblock(read_block(bstore, addr));
      std::vector<SubtreeRef> refs;
      auto status = sblock.read_all(&refs);
      EXPECT_EQ(status, common::Status::Ok());
      for (auto r: refs) {
        LogicAddr a = r.addr;
        bool isl = r.type == NBTreeBlockType::LEAF;
        addrlist.push(std::make_pair(a, isl));
      }
    }
  }
  return std::tie(number_of_inner_nodes, number_of_leaf_nodes);
}

/**
 * @brief Test node split in the case when the node is being split twice
 * @param pivot1 is the first pivot point (when the first split occurs)
 * @param pivot2 is the second pivot point
 * @param tss
 */
void test_node_split_algorithm_lvl2_split_twice(Timestamp pivot1,
                                                Timestamp pivot2,
                                                std::map<int, std::vector<Timestamp>>& tss,
                                                int expected_inner_nodes,
                                                int expected_leaf_nodes
                                               ) {
  std::shared_ptr<BlockStore> bstore = BlockStoreBuilder::create_memstore();
  const ParamId id = 42;
  LogicAddr prev = EMPTY_ADDR;
  IOVecSuperblock sblock(id, EMPTY_ADDR, 0, 1);

  for(auto kv: tss) {
    IOVecLeaf leaf(id, prev, static_cast<u16>(kv.first));
    fill_leaf(&leaf, kv.second);
    prev = save_leaf(&leaf, &sblock, bstore);
  }

  LogicAddr root;
  common::Status status;
  std::tie(status, root) = sblock.commit(bstore);
  EXPECT_EQ(status, common::Status::Ok());
  EXPECT_EQ(root - prev, 1);

  // First split

  LogicAddr new_root1;
  LogicAddr last_child1;
  std::tie(status, new_root1, last_child1) = sblock.split(bstore, pivot1, false);
  EXPECT_EQ(status, common::Status::Ok());
  EXPECT_NE(new_root1 - root, 0);

  IOVecSuperblock new_sblock1(read_block(bstore, new_root1));
  std::unique_ptr<RealValuedOperator> it = new_sblock1.search(0, 100, bstore);
  auto actual = extract_timestamps(*it);

  std::unique_ptr<RealValuedOperator> orig_it = sblock.search(0, 100, bstore);
  auto expected = extract_timestamps(*orig_it);

  EXPECT_NE(actual.size(), 0);
  EXPECT_EQ(actual.size(), expected.size());
  for (size_t i = 0; i < actual.size(); ++i) {
    EXPECT_EQ(actual[i], expected[i]);
  }

  // Second split
  LogicAddr new_root2;
  LogicAddr last_child2;
  std::tie(status, new_root2, last_child2) = new_sblock1.split(bstore, pivot2, false);
  EXPECT_EQ(status, common::Status::Ok());
  EXPECT_NE(new_root2 - new_root1, 0);

  IOVecSuperblock new_sblock2(read_block(bstore, new_root2));
  it = new_sblock2.search(0, 100, bstore);
  actual = extract_timestamps(*it);

  EXPECT_EQ(actual.size(), expected.size());
  for (size_t i = 0; i < actual.size(); ++i) {
    EXPECT_EQ(actual[i], expected[i]);
  }

  // Check the structure
  int num_inner_nodes, num_leaf_nodes;
  std::tie(num_inner_nodes, num_leaf_nodes) = count_nbtree_nodes(bstore, new_root2);
  EXPECT_EQ(num_inner_nodes, expected_inner_nodes);
  EXPECT_EQ(num_leaf_nodes, expected_leaf_nodes);
}

TEST(TestNBtree, Test_node_split_algorithm_21) {
  /* Split middle node in:
   *          [inner]
   *         /   |   \
   *  [leaf0] [leaf1] [leaf2]
   *
   * The result of the first split should look like this:
   *          [ inner ]
   *         /   |   \ \
   *  [leaf0] [leaf1] [leaf2] [leaf3]
   *
   * The result of the first split should look like this:
   *          [  inner  ]
   *         /   |   \ \ \
   *  [leaf0] [leaf1] [leaf2] [leaf3] [leaf4]
   */
  std::map<int, std::vector<Timestamp>> tss = {
    { 0, {  1,  2,  3,  4,  5,  6,  7,  8,  9, 10 }},
    { 1, { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 }},
    { 2, { 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 }},
  };
  test_node_split_algorithm_lvl2_split_twice(15, 17, tss, 1, 5);
}

TEST(TestNBTree, Test_node_split_algorithm_22) {
  /* Split middle node in:
   *          [inner]
   *         /   |   \
   *  [leaf0] [leaf1] [leaf2] ... [leaf31]
   *
   * The result of the first split should look like this:
   *          [inner]
   *         /   |   \
   *  [leaf0] [inner] [leaf3] ... [leaf31]
   *           /   \
   *       [leaf1] [leaf2]
   *
   * The result of the first split should look like this:
   *          [inner]
   *         /   |   \
   *  [leaf0] [inner] [leaf4] ... [leaf34]
   *         /   |   \
   *  [leaf1] [leaf2] [leaf2]
   */
  std::map<int, std::vector<Timestamp>> tss = {
    { 0, {  1,  2,  3,  4,  5,  6,  7,  8,  9, 10 }},
    { 1, { 11, 12, 13, 14, 15, 16, 17, 18, 19, 20 }},
    { 2, { 21, 22, 23, 24, 25, 26, 27, 28, 29, 30 }},
  };
  for (int i = 3; i < 32; i++) {
    tss[i] = { static_cast<Timestamp>(i*10 + 1) };
  }
  test_node_split_algorithm_lvl2_split_twice(15, 17, tss, 2, 34);
}

TEST(TestNBTree, Test_value_filter_1) {
  ValueFilter filter;
  filter.less_than(10)
      .less_or_equal(10);

  EXPECT_TRUE(filter.validate() == false);
}

TEST(TestNBTree, Test_value_filter_2) {
  ValueFilter filter;
  filter.greater_than(10)
      .greater_or_equal(10);

  EXPECT_TRUE(filter.validate() == false);
}

TEST(TestNBTree, Test_value_filter_3) {
  ValueFilter filter;
  filter.less_than(100)
      .greater_than(10);

  EXPECT_TRUE(filter.validate());
  EXPECT_TRUE(filter.match(50));
  EXPECT_TRUE(!filter.match(10));
  EXPECT_TRUE(!filter.match(100));
}

TEST(TestNBtree, Test_value_filter_4) {
  ValueFilter filter;
  filter.less_or_equal(100)
      .greater_or_equal(10);

  EXPECT_TRUE(filter.validate());
  EXPECT_TRUE(filter.match(50));
  EXPECT_TRUE(filter.match(10));
  EXPECT_TRUE(filter.match(100));
  EXPECT_TRUE(!filter.match(101));
  EXPECT_TRUE(!filter.match(9));
}

TEST(TestNBtree, Test_value_filter_5) {
  ValueFilter filter;
  filter.less_than(100)
      .greater_than(10);

  EXPECT_TRUE(filter.validate());

  SubtreeRef ref{};
  ref.max = 1;
  ref.min = 0;

  EXPECT_TRUE(filter.get_overlap(ref) == RangeOverlap::NO_OVERLAP);

  ref.max = 10;
  ref.min = 0;

  EXPECT_TRUE(filter.get_overlap(ref) == RangeOverlap::NO_OVERLAP);

  ref.max = 20;
  ref.min = 10;

  EXPECT_TRUE(filter.get_overlap(ref) == RangeOverlap::PARTIAL_OVERLAP);

  ref.max = 50;
  ref.min = 20;

  EXPECT_TRUE(filter.get_overlap(ref) == RangeOverlap::FULL_OVERLAP);

  ref.max = 120;
  ref.min = 100;

  EXPECT_TRUE(filter.get_overlap(ref) == RangeOverlap::NO_OVERLAP);

  ref.max = 120;
  ref.min = 110;

  EXPECT_TRUE(filter.get_overlap(ref) == RangeOverlap::NO_OVERLAP);
}

TEST(TestNBtree, Test_value_filter_6) {
  ValueFilter filter;
  filter.less_or_equal(100)
      .greater_or_equal(10);

  EXPECT_TRUE(filter.validate());

  SubtreeRef ref{};
  ref.max = 1;
  ref.min = 0;

  EXPECT_TRUE(filter.get_overlap(ref) == RangeOverlap::NO_OVERLAP);

  ref.max = 10;
  ref.min = 0;

  EXPECT_TRUE(filter.get_overlap(ref) == RangeOverlap::PARTIAL_OVERLAP);

  ref.max = 20;
  ref.min = 10;

  EXPECT_TRUE(filter.get_overlap(ref) == RangeOverlap::FULL_OVERLAP);

  ref.max = 50;
  ref.min = 20;

  EXPECT_TRUE(filter.get_overlap(ref) == RangeOverlap::FULL_OVERLAP);

  ref.max = 120;
  ref.min = 100;

  EXPECT_TRUE(filter.get_overlap(ref) == RangeOverlap::PARTIAL_OVERLAP);

  ref.max = 120;
  ref.min = 110;

  EXPECT_TRUE(filter.get_overlap(ref) == RangeOverlap::NO_OVERLAP);
}

TEST(TestNBtree, Test_value_filter_7) {
  ValueFilter filter;
  filter.less_or_equal(20)
      .greater_or_equal(-20);

  EXPECT_TRUE(filter.validate());

  SubtreeRef ref{};
  ref.max = 100;
  ref.min = -100;

  EXPECT_TRUE(filter.get_overlap(ref) == RangeOverlap::PARTIAL_OVERLAP);
}

void test_nbtreeleaf_filter_operator(Timestamp begin, Timestamp end) {
  IOVecLeaf leaf(42, EMPTY_ADDR, 0);
  Timestamp first_timestamp = 100;
  std::vector<double> xss;
  std::vector<double> tss;
  RandomWalk rwalk1(100000.0, 2.0, 2.0);
  RandomWalk rwalk2(-100000.0, 2.0, 2.0);
  for (size_t ix = first_timestamp; true; ix++) {
    double val = ix % 2 == 0 ? rwalk1.next()
        : rwalk2.next();
    common::Status status = leaf.append(ix, val);
    if (status == common::Status::Overflow()) {
      break;
    }
    if (status == common::Status::Ok()) {
      if (begin < end) {
        if(val < 0 && ix >= begin && ix < end) {
          xss.push_back(val);
          tss.push_back(ix);
        }
      }
      else {
        if(val < 0 && ix <= begin && ix > end) {
          xss.push_back(val);
          tss.push_back(ix);
        }
      }
      continue;
    }
    LOG(FATAL) << status.ToString();
  }
  if (begin > end) {
    std::reverse(xss.begin(), xss.end());
    std::reverse(tss.begin(), tss.end());
  }

  ValueFilter filter;
  filter.less_than(0.0);
  auto op = leaf.filter(begin, end, filter);

  size_t sz = xss.size();
  std::vector<double> actxss;
  actxss.resize(sz);
  std::vector<Timestamp> acttss;
  acttss.resize(sz);
  common::Status status;
  size_t outsz;
  std::tie(status, outsz) = op->read(acttss.data(), actxss.data(), sz);
  EXPECT_TRUE(status == common::Status::Ok());

  EXPECT_TRUE(xss.size() != 0);
  EXPECT_TRUE(xss.size() == actxss.size());

  EXPECT_EQ(tss.size(), acttss.size());
  for (size_t i = 0; i < tss.size(); ++i) {
    EXPECT_EQ(tss[i], acttss[i]);
  }

  EXPECT_EQ(xss.size(), actxss.size());
  for (size_t i = 0; i < actxss.size(); ++i) {
    EXPECT_EQ(xss[i], actxss[i]);
  }
}

TEST(TestNBtree, Test_nbtreeleaf_filter_operator_fwd_0) {
  test_nbtreeleaf_filter_operator(10, 150);
}

TEST(TestNBtree, Test_nbtreeleaf_filter_operator_fwd_1) {
  test_nbtreeleaf_filter_operator(100, 200);
}

TEST(TestNBTree, Test_nbtreeleaf_filter_operator_fwd_2) {
  test_nbtreeleaf_filter_operator(200, 300);
}

TEST(TestNBTree, Test_nbtreeleaf_filter_operator_fwd_3) {
  test_nbtreeleaf_filter_operator(300, 1000);
}

TEST(TestNBtree, Test_nbtreeleaf_filter_operator_bwd_0) {
  test_nbtreeleaf_filter_operator(150, 10);
}

TEST(TestNBtree, Test_nbtreeleaf_filter_operator_bwd_1) {
  test_nbtreeleaf_filter_operator(200, 100);
}

TEST(TestNBtree, Test_nbtreeleaf_filter_operator_bwd_2) {
  test_nbtreeleaf_filter_operator(300, 200);
}

TEST(TestNBtree, Test_nbtreeleaf_filter_operator_bwd_3) {
  test_nbtreeleaf_filter_operator(1000, 300);
}

void test_nbtree_superblock_filter(size_t commit_limit, bool inv_direction) {
  // Build this tree structure.
  Timestamp begin = 1000;
  Timestamp end = begin;
  size_t ncommits = 0;
  auto commit_counter = [&ncommits](LogicAddr) {
    ncommits++;
  };
  auto bstore = BlockStoreBuilder::create_memstore(commit_counter);
  std::vector<LogicAddr> empty;
  std::shared_ptr<NBTreeExtentsList> extents(new NBTreeExtentsList(42, empty, bstore));
  std::vector<double> xss;
  std::vector<Timestamp> tss;
  extents->force_init();
  RandomWalk rwalk1(+1E6, 0.1, 0.1);
  RandomWalk rwalk2(-1E6, 0.1, 0.1);
  while (ncommits < commit_limit) {
    double value = end % 2 == 0
        ? rwalk1.next()
        : rwalk2.next();
    Timestamp ts = end++;
    extents->append(ts, value);
    if (value < 0) {
      if (!inv_direction || end != begin) {
        // Query in rev. direction won't return the
        // first value since the end of the range is
        // exclusive.
        tss.push_back(ts);
        xss.push_back(value);
      }
    }
  }
  if (inv_direction) {
    std::reverse(tss.begin(), tss.end());
    std::reverse(xss.begin(), xss.end());
  }

  // Check actual output
  ValueFilter flt;
  flt.less_than(0);
  std::unique_ptr<RealValuedOperator> it;

  if (inv_direction) {
    it = extents->filter(end, begin, flt);
  } else {
    it = extents->filter(begin, end, flt);
  }

  std::vector<Timestamp> actts;
  std::vector<double> actxs;

  while (true) {
    common::Status status;
    size_t size = 1000;
    std::vector<Timestamp> destts(size, 0);
    std::vector<double> destxs(size, 0);
    std::tie(status, size) = it->read(destts.data(), destxs.data(), size);

    if (status != common::Status::Ok() && status != common::Status::NoData()) {
      EXPECT_EQ(status, common::Status::Ok());
    }

    for(size_t i = 0; i < size; i++) {
      actts.push_back(destts[i]);
      actxs.push_back(destxs[i]);
    }

    if (status == common::Status::NoData()) {
      break;
    }
  }

  EXPECT_EQ(tss.size(), actts.size());
  EXPECT_EQ(xss.size(), actxs.size());

  for (size_t i = 0; i < tss.size(); i++) {
    EXPECT_EQ(tss.at(i), actts.at(i));
    EXPECT_EQ(xss.at(i), actxs.at(i));
  }
}

TEST(TestNBtree, Test_nbtree_superblock_filter_fwd_0) {
  test_nbtree_superblock_filter(10, false);
}

TEST(TestNBtree, Test_nbtree_superblock_filter_fwd_1) {
  test_nbtree_superblock_filter(100, false);
}

TEST(TestNBtree, Test_nbtree_superblock_filter_fwd_2) {
  test_nbtree_superblock_filter(1000, false);
}

TEST(TestNBtree, Test_nbtree_superblock_filter_bwd_0) {
  test_nbtree_superblock_filter(10, true);
}

TEST(TestNBtree, Test_nbtree_superblock_filter_bwd_1) {
  test_nbtree_superblock_filter(100, true);
}

TEST(TestNBtree, Test_nbtree_superblock_filter_bwd_2) {
  test_nbtree_superblock_filter(1000, true);
}

void test_nbtree_retention_consistency() {
  LogicAddr nstarting = 10;
  LogicAddr nremoved = 10;
  LogicAddr nblocks = 20;

  // Build this tree structure.
  Timestamp gen = 1000;
  Timestamp begin = gen, end = gen, last_ts = gen;
  size_t buffer_cnt = 0;
  std::shared_ptr<NBTreeExtentsList> extents;
  auto commit_counter = [&](LogicAddr) {
    buffer_cnt++;
    if (buffer_cnt == nremoved) {
      // one time event
      begin = gen;
    }
    end = last_ts;
  };

  auto bstore = BlockStoreBuilder::create_memstore(commit_counter);
  auto truncate = [&](LogicAddr n) {
    return std::dynamic_pointer_cast<MemStore, BlockStore>(bstore)->remove(n);
  };

  auto bottom = truncate(nstarting);

  std::vector<LogicAddr> initial_rlist = { EMPTY_ADDR, bottom - 1 };
  extents.reset(new NBTreeExtentsList(42, initial_rlist, bstore));
  extents->force_init();
  RandomWalk rwalk(1.0, 0.1, 0.1);
  while(buffer_cnt < nblocks) {
    double value = rwalk.next();
    Timestamp ts = gen++;
    extents->append(ts, value);
    last_ts = ts;
  }

  // Remove old values
  truncate(nremoved);

  // Recovery
  auto rescue_points = extents->close();

  // We shouldn't close `extents` to emulate program state after crush.
  std::shared_ptr<NBTreeExtentsList> recovered(new NBTreeExtentsList(42, rescue_points, bstore));
  recovered->force_init();

  auto it = recovered->search(begin, end);
  if (end > begin) {
    size_t sz = end - begin;
    std::vector<Timestamp> tss(sz, 0);
    std::vector<double> xss(sz, .0);
    common::Status stat;
    size_t outsz;
    std::tie(stat, outsz) = it->read(tss.data(), xss.data(), sz);
    if (outsz != sz) {
      EXPECT_EQ(outsz, sz);
    }
    if (stat != common::Status::Ok() && stat != common::Status::NoData()) {
      EXPECT_TRUE(stat == common::Status::Ok() || stat == common::Status::NoData());
    }
    for(Timestamp ts: tss) {
      EXPECT_EQ(ts, begin);
      begin++;
    }
  } else {
    // No output expected
    size_t sz = 10;
    std::vector<Timestamp> tss(sz, 0);
    std::vector<double> xss(sz, .0);
    common::Status stat;
    size_t outsz;
    std::tie(stat, outsz) = it->read(tss.data(), xss.data(), sz);
    if (outsz != 0) {
      EXPECT_EQ(outsz, 0);
    }
    if (stat != common::Status::NoData()) {
      EXPECT_TRUE(stat == common::Status::NoData());
    }
  }
}

TEST(TestNBtree, Test_nbtree_retention_consistency_1) {
  test_nbtree_retention_consistency();
}

void test_nbtree_scan_order_idempotence(size_t nremoved, size_t nblocks) {
  // Build this tree structure.
  Timestamp gen = 1000;
  Timestamp first_ts = gen, begin = gen, end = gen, last_ts = gen;
  size_t buffer_cnt = 0;
  std::shared_ptr<NBTreeExtentsList> extents;
  auto commit_counter = [&](LogicAddr) {
    buffer_cnt++;
    if (buffer_cnt == nremoved) {
      // one time event
      begin = gen;
    }
    end = last_ts;
  };

  auto bstore = BlockStoreBuilder::create_memstore(commit_counter);
  auto truncate = [&](LogicAddr n) {
    return std::dynamic_pointer_cast<MemStore, BlockStore>(bstore)->remove(n);
  };

  std::vector<LogicAddr> initial_rlist;
  extents.reset(new NBTreeExtentsList(42, initial_rlist, bstore));

  extents->force_init();
  RandomWalk rwalk(1.0, 0.1, 0.1);
  while (buffer_cnt < nblocks) {
    double value = rwalk.next();
    Timestamp ts = gen++;
    extents->append(ts, value);
    last_ts = ts;
  }

  // Remove old values
  truncate(nremoved);

  {
    auto it = extents->search(first_ts, end);
    size_t sz = begin < end ? end - begin : 0;
    size_t bufsz = sz == 0 ? 1 : sz;
    std::vector<Timestamp> tss(bufsz, 0);
    std::vector<double> xss(bufsz, .0);
    common::Status stat;
    size_t outsz;
    std::tie(stat, outsz) = it->read(tss.data(), xss.data(), bufsz);
    EXPECT_EQ(outsz, sz);
    EXPECT_TRUE(stat == common::Status::Ok() || stat == common::Status::NoData());
    auto itts = begin - 1;
    for (u32 ixts = 0; ixts < outsz; ixts++) {
      auto ts = tss.at(ixts);
      EXPECT_EQ(ts, itts);
      itts++;
    }
  }
  {
    auto it = extents->search(end, first_ts);
    // No output expected
    size_t sz = begin < end ? end - begin : 0;
    size_t bufsz = sz == 0 ? 1 : sz;
    std::vector<Timestamp> tss(bufsz, 0);
    std::vector<double> xss(bufsz, .0);
    common::Status stat;
    size_t outsz;
    std::tie(stat, outsz) = it->read(tss.data(), xss.data(), bufsz);
    EXPECT_EQ(outsz, sz);
    EXPECT_TRUE(stat == common::Status::Ok() || stat == common::Status::NoData());
    std::reverse(tss.begin(), tss.end());
    auto itts = begin + 1;
    for (u32 ixts = 0; ixts < outsz; ixts++) {
      auto ts = tss.at(ixts);
      EXPECT_EQ(ts, itts);
      itts++;
    }
  }
}

TEST(TestNBtree, Test_nbtree_scan_order_idempotence_0) {
  test_nbtree_scan_order_idempotence(10, 20);
}

TEST(TestNBtree, Test_nbtree_scan_order_idempotence_1) {
  test_nbtree_scan_order_idempotence(20, 20);
}

TEST(TestNBtree, Test_nbtree_scan_order_idempotence_2) {
  test_nbtree_scan_order_idempotence(34, 100);
}

void test_nbtree_aggregate_direction_independence(size_t nremoved, size_t nblocks) {
  // Build this tree structure.
  Timestamp gen = 1000;
  Timestamp first_ts = gen, begin = gen, end = gen;
  size_t buffer_cnt = 0;
  size_t sample_cnt = 0;

  // For each buffer writtent to the storage this queue will have a
  // sample-count value.
  std::queue<size_t> sample_counts;
  std::shared_ptr<NBTreeExtentsList> extents;
  auto commit_counter = [&](LogicAddr) {
    buffer_cnt++;
    sample_counts.push(sample_cnt);
    sample_cnt = 0;
    if (buffer_cnt == nremoved) {
      // one time event
      begin = gen;
    }
    end = gen;
  };

  auto bstore = BlockStoreBuilder::create_memstore(commit_counter);
  auto truncate = [&](LogicAddr n) {
    return std::dynamic_pointer_cast<MemStore, BlockStore>(bstore)->remove(n);
  };

  std::vector<LogicAddr> initial_rlist;
  extents.reset(new NBTreeExtentsList(42, initial_rlist, bstore));

  extents->force_init();
  RandomWalk rwalk(1.0, 0.1, 0.1);
  while (buffer_cnt < nblocks) {
    double value = rwalk.next();
    Timestamp ts = gen++;
    extents->append(ts, value);
    sample_cnt++;
  }

  // Remove old values
  truncate(nremoved);
  size_t evicted_cnt = 0;
  for (size_t i = 0; i < nremoved; i++) {
    evicted_cnt += sample_counts.front();
    sample_counts.pop();
  }

  size_t remained_cnt = 0;
  while (!sample_counts.empty()) {
    remained_cnt += sample_counts.front();
    sample_counts.pop();
  }
  remained_cnt += sample_cnt;

  // Test aggregate
  {
    size_t expected_cnt = remained_cnt;
    auto it = extents->aggregate(first_ts, end + 1);
    Timestamp ts = 0;
    AggregationResult xs = INIT_AGGRES;
    common::Status stat;
    size_t outsz;
    std::tie(stat, outsz) = it->read(&ts, &xs, 1);
    EXPECT_EQ(outsz, 1);
    EXPECT_TRUE(stat == common::Status::Ok() || stat == common::Status::NoData());
    EXPECT_EQ(xs.cnt, expected_cnt);
  }
  {
    size_t expected_cnt = remained_cnt;
    auto it = extents->aggregate(end, first_ts);
    Timestamp ts = 0;
    AggregationResult xs = INIT_AGGRES;
    common::Status stat;
    size_t outsz;
    std::tie(stat, outsz) = it->read(&ts, &xs, 1);
    EXPECT_EQ(outsz, 1);
    EXPECT_TRUE(stat == common::Status::Ok() || stat == common::Status::NoData());
    EXPECT_EQ(xs.cnt, expected_cnt);
  }
  // Test group-aggregate
  auto test_group_aggregate = [&](Timestamp from, Timestamp to, Timestamp step) {
    auto it = extents->group_aggregate(from, to, step);
    size_t sz = 0;
    auto total = (end - from + (step - 1)) / step;
    auto excluded = (begin - from - 1) / step;
    sz = total - excluded;
    size_t bufsz = sz + 1;
    std::vector<Timestamp> tss(bufsz, 0);
    std::vector<AggregationResult> xss(bufsz, INIT_AGGRES);
    common::Status stat;
    size_t outsz;
    std::tie(stat, outsz) = it->read(tss.data(), xss.data(), bufsz);
    EXPECT_EQ(outsz, sz);
    EXPECT_TRUE(stat == common::Status::Ok() || stat == common::Status::NoData());
    u64 expected_cnt = remained_cnt;
    u64 actual_cnt = 0;
    for (u32 i = 0; i < outsz; i++) {
      actual_cnt += xss.at(i).cnt;
    }
    EXPECT_EQ(actual_cnt, expected_cnt);
  };
  test_group_aggregate(first_ts, end, 10);
  test_group_aggregate(first_ts, end, 100);
  test_group_aggregate(first_ts, end, 1000);
  test_group_aggregate(first_ts, end, 10000);
}

TEST(TestNBTree, Test_nbtree_aggregate_direction_independence_0) {
  test_nbtree_aggregate_direction_independence(10, 20);
}

TEST(TestNBtree, Test_nbtree_aggregate_direction_independence_1) {
  test_nbtree_aggregate_direction_independence(33, 100);
}

TEST(TestNBtree, Test_nbtree_aggregate_direction_independence_2) {
  test_nbtree_aggregate_direction_independence(34, 100);
}


void test_nbtree_summary(size_t nremoved, size_t nblocks) {
  // Build this tree structure.
  Timestamp gen = 1000;
  double rwalk_value = 0.0;
  Timestamp begin = gen, end = gen;
  double first_xs = 0.0, last_xs = 0.0;
  size_t buffer_cnt = 0;
  size_t sample_cnt = 0;

  // For each buffer writtent to the storage this queue will have a
  // sample-count value.
  std::queue<size_t> sample_counts;
  std::shared_ptr<NBTreeExtentsList> extents;
  auto commit_counter = [&](LogicAddr) {
    buffer_cnt++;
    sample_counts.push(sample_cnt);
    sample_cnt = 0;
    if (buffer_cnt == nremoved) {
      // one time event
      begin = gen;
      first_xs = rwalk_value;
    }
    end = gen;
    last_xs = rwalk_value;
  };

  auto bstore = BlockStoreBuilder::create_memstore(commit_counter);
  auto truncate = [&](LogicAddr n) {
    return std::dynamic_pointer_cast<MemStore, BlockStore>(bstore)->remove(n);
  };

  std::vector<LogicAddr> initial_rlist;
  extents.reset(new NBTreeExtentsList(42, initial_rlist, bstore));

  extents->force_init();
  RandomWalk rwalk(1.0, 0.1, 0.1);
  while(buffer_cnt < nblocks) {
    rwalk_value = rwalk.next();
    //Timestamp ts = gen++;
    extents->append(++gen, rwalk_value);
    sample_cnt++;
  }

  // Remove old values
  truncate(nremoved);
  size_t evicted_cnt = 0;
  for (size_t i = 0; i < nremoved; i++) {
    evicted_cnt += sample_counts.front();
    sample_counts.pop();
  }

  size_t remained_cnt = 0;
  while (!sample_counts.empty()) {
    remained_cnt += sample_counts.front();
    sample_counts.pop();
  }
  remained_cnt += sample_cnt;

  // Test aggregate
  {
    size_t expected_cnt = remained_cnt;
    auto it = extents->aggregate(0, std::numeric_limits<Timestamp>::max());
    Timestamp ts = 0;
    AggregationResult xs = INIT_AGGRES;
    common::Status stat;
    size_t outsz;
    std::tie(stat, outsz) = it->read(&ts, &xs, 1);
    EXPECT_EQ(outsz, 1);
    EXPECT_TRUE(stat == common::Status::Ok() || stat == common::Status::NoData());

    EXPECT_EQ(xs.cnt, expected_cnt);
    EXPECT_EQ(xs.first, first_xs);
    EXPECT_EQ(xs.last, last_xs);
    EXPECT_EQ(xs._begin, begin);
    EXPECT_EQ(xs._end, end);

  }
}

TEST(TestNBtree, Test_nbtree_summary_0) {
  test_nbtree_summary(10, 20);
}

void test_nbtree_append_event(Timestamp begin,
                              Timestamp end,
                              Timestamp step,
                              std::vector<Timestamp> query_from,
                              std::vector<Timestamp> query_to) {
  std::shared_ptr<BlockStore> bstore = BlockStoreBuilder::create_memstore();
  std::vector<LogicAddr> addrlist;  // should be empty at first
  auto collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
  collection->force_init();

  std::map<Timestamp, std::string> allevents;

  for (auto i = begin; i < end; i+=step) {
    std::string event = "event-" + std::to_string(i);
    auto outres = collection->append(i, reinterpret_cast<const u8*>(event.data()), static_cast<u32>(event.size()));
    if (outres == NBTreeAppendResult::FAIL_BAD_ID ||
        outres == NBTreeAppendResult::FAIL_BAD_VALUE ||
        outres == NBTreeAppendResult::FAIL_LATE_WRITE) {
      LOG(FATAL) << "Append operation failed";
    }
    allevents[i] = event;
  }

  EXPECT_EQ(query_from.size(), query_to.size());

  for (u32 i = 0; i < query_from.size(); i++) {
    auto from = query_from.at(i);
    auto to = query_to.at(i);
    auto it = collection->search_binary(from, to);

    // Verify
    common::Status itstatus = common::Status::Ok();
    size_t itsize;
    Timestamp ts;
    std::string line;
    size_t elem_num = 0;
    while (itstatus == common::Status::Ok()) {
      std::tie(itstatus, itsize) = it->read(&ts, &line, 1);
      EXPECT_TRUE(itsize < 2);
      if (itsize) {
        elem_num++;
        EXPECT_TRUE(allevents.count(ts));
        auto expected = allevents[ts];
        EXPECT_EQ(expected, line);
        if (from < to) {
          EXPECT_GE(ts, from);
          EXPECT_LE(ts, to);
        }
        else {
          EXPECT_LE(ts, from);
          EXPECT_GE(ts, to);
        }
      }
    }
    EXPECT_NE(elem_num, 0);
  }
}

TEST(TestNBtree, Test_nbtree_append_event_0) {
  std::vector<Timestamp> from = {
    1100000, 1300000, 1300000, 100,
  };
  std::vector<Timestamp> to = {
    2100000, 2300000, 400000 , 2100000,
  };
  test_nbtree_append_event(1000000, 3000000, 20000, from, to);
}

TEST(TestNBtree, Test_nbtree_append_event_1) {
  std::vector<Timestamp> from = {
    2100000, 2300000, 4000000, 2100000,
  };
  std::vector<Timestamp> to = {
    1100000, 1300000, 1300000, 100,
  };
  test_nbtree_append_event(1000000, 3000000, 20000, from, to);
}

TEST(TestNBtree, Test_nbtree_append_event_2) {
  std::vector<Timestamp> from = {
    1100000, 1300000, 1300000, 100,
  };
  std::vector<Timestamp> to = {
    2100000, 2300000, 4000001, 2100000,
  };
  test_nbtree_append_event(1000001, 3000001, 20020, from, to);
}

TEST(TestNBtree, Test_nbtree_append_event_3) {
  std::vector<Timestamp> from = {
    2100000, 2300000, 4000001, 2100000,
  };
  std::vector<Timestamp> to = {
    1100000, 1300000, 1300000, 100,
  };
  test_nbtree_append_event(1000001, 3000001, 20020, from, to);
}

TEST(TestNBtree, Test_nbtree_append_event_4) {
  std::shared_ptr<BlockStore> bstore = BlockStoreBuilder::create_memstore();
  std::vector<LogicAddr> addrlist;  // should be empty at first
  auto collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
  collection->force_init();

  u8 data;
  auto outres = collection->append(1000000, &data, 0);
  EXPECT_TRUE(outres == NBTreeAppendResult::FAIL_BAD_VALUE);
}

TEST(TestNBtree, Test_nbtree_append_event_5) {
  std::shared_ptr<BlockStore> bstore = BlockStoreBuilder::create_memstore();
  std::vector<LogicAddr> addrlist;  // should be empty at first
  auto collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
  collection->force_init();

  u8 data = 1;
  auto outres = collection->append(1000000, &data, 1);
  EXPECT_TRUE(outres == NBTreeAppendResult::OK);
  outres = collection->append(1000010, &data, 1);
  EXPECT_TRUE(outres == NBTreeAppendResult::FAIL_LATE_WRITE);
}

void two_event_append_test(std::string first, std::string second) {
  std::shared_ptr<BlockStore> bstore = BlockStoreBuilder::create_memstore();
  std::vector<LogicAddr> addrlist;
  auto collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
  collection->force_init();

  Timestamp ts1 = 1000000;
  Timestamp ts2 = 2000000;
  auto outres = collection->append(ts1, reinterpret_cast<const u8*>(first.data()), first.size());
  EXPECT_TRUE(outres == NBTreeAppendResult::OK);
  outres = collection->append(ts2, reinterpret_cast<const u8*>(second.data()), second.size());  // This message spans block boundary
  EXPECT_TRUE(outres == NBTreeAppendResult::OK);

  // Read back
  auto it = collection->search_binary(ts1, ts2 + 1000);

  // Verify
  common::Status    itstatus = common::Status::Ok();
  size_t        itsize;
  Timestamp ts;
  std::string   line;

  // Read first
  std::tie(itstatus, itsize) = it->read(&ts, &line, 1);
  EXPECT_EQ(itsize, 1);
  EXPECT_EQ(line, first);
  EXPECT_EQ(ts, ts1);
  // Read 2nd
  std::tie(itstatus, itsize) = it->read(&ts, &line, 1);
  EXPECT_EQ(itsize, 1);
  EXPECT_EQ(line, second);
  EXPECT_EQ(ts, ts2);
}

TEST(TestNBtree, Test_nbtree_append_event_6) {
  std::string   shortevt(512, 's');
  std::string   longevt(STDB_LIMITS_MAX_EVENT_LEN, 'l');
  two_event_append_test(shortevt, longevt);
}

TEST(TestNBtree, Test_nbtree_append_event_7) {
  std::string   first(STDB_LIMITS_MAX_EVENT_LEN, '1');
  std::string   second(STDB_LIMITS_MAX_EVENT_LEN, '2');
  two_event_append_test(first, second);
}

TEST(TestNBtree, Test_nbtree_append_event_8) {
  std::shared_ptr<BlockStore> bstore = BlockStoreBuilder::create_memstore();
  std::vector<LogicAddr> addrlist;
  auto collection = std::make_shared<NBTreeExtentsList>(42, addrlist, bstore);
  collection->force_init();

  Timestamp ts1 = 1000000;
  std::string   event(4097, 'l'); // event too large
  auto outres = collection->append(ts1, reinterpret_cast<const u8*>(event.data()), event.size());
  EXPECT_TRUE(outres == NBTreeAppendResult::FAIL_BAD_VALUE);
}

}  // namespace storage
}  // namespace stdb

/*!
 * \file column_store_test.cc
 */
#include <iostream>

#include <apr.h>
#include <sqlite3.h>

#include "stdb/common/logging.h"
#include "stdb/core/metadatastorage.h"
#include "stdb/storage/column_store.h"
#include "stdb/query/queryplan.h"

#include "gtest/gtest.h"

namespace stdb {
namespace storage {

struct Initializer {
  Initializer() {
    sqlite3_initialize();
    apr_initialize();

    apr_pool_t *pool = nullptr;
    auto status = apr_pool_create(&pool, NULL);
    if (status != APR_SUCCESS) {
      LOG(FATAL) << "Can't create memory pool";
    }
    apr_dbd_init(pool);
  }
};

static Initializer initializer;
using namespace stdb::qp;

std::unique_ptr<MetadataStorage> create_metadatastorage() {
  // Create in-memory sqlite database.
  std::unique_ptr<MetadataStorage> meta;
  meta.reset(new MetadataStorage(":memory:"));
  return meta;
}

std::shared_ptr<ColumnStore> create_cstore() {
  std::shared_ptr<BlockStore> bstore = BlockStoreBuilder::create_memstore();
  std::shared_ptr<ColumnStore> cstore;
  cstore.reset(new ColumnStore(bstore));
  return cstore;
}

std::unique_ptr<CStoreSession> create_session(std::shared_ptr<ColumnStore> cstore) {
  std::unique_ptr<CStoreSession> session;
  session.reset(new CStoreSession(cstore));
  return session;
}

TEST(TestColumnStore, Test_columns_store_create_1) {
  std::shared_ptr<ColumnStore> cstore = create_cstore();
  std::unique_ptr<CStoreSession> session = create_session(cstore);
}

TEST(TestColumnStore, Test_column_store_add_values_3) {
  auto meta = create_metadatastorage();
  auto bstore = BlockStoreBuilder::create_memstore();
  auto cstore = create_cstore();
  auto sessiona = create_session(cstore);
  Sample sample;
  sample.payload.type = PAYLOAD_FLOAT;
  sample.paramid = 111;
  sample.timestamp = 111;
  sample.payload.float64 = 111;
  std::vector<u64> rpoints;
  auto status = sessiona->write(sample, &rpoints);  // series with id 111 doesn't exists
  EXPECT_TRUE(status == NBTreeAppendResult::FAIL_BAD_ID);
}

struct QueryProcessorMock : qp::IStreamProcessor {
  bool started = false;
  bool stopped = false;
  std::vector<Sample> samples;
  common::Status error = common::Status::Ok();

  virtual bool start() override {
    started = true;
    return true;
  }
  virtual void stop() override {
    stopped = true;
  }
  virtual bool put(const Sample &sample) override {
    samples.push_back(sample);
    return true;
  }
  virtual void set_error(common::Status err) override {
    error = err;
  }
};


void execute(std::shared_ptr<ColumnStore> cstore, IStreamProcessor* proc, ReshapeRequest const& req) {
  common::Status status;
  std::unique_ptr<qp::IQueryPlan> query_plan;
  std::tie(status, query_plan) = qp::QueryPlanBuilder::create(req);
  if (status != common::Status::Ok()) {
    throw std::runtime_error("Can't create query plan");
  }
  if (proc->start()) {
    QueryPlanExecutor executor;
    executor.execute(*cstore, std::move(query_plan), *proc);
    proc->stop();
  }
}

TEST(TestColumnStore, Test_column_store_query_1) {
  auto cstore = create_cstore();
  auto session = create_session(cstore);
  QueryProcessorMock qproc;
  Sample sample;
  sample.timestamp = 42;
  sample.payload.type = PAYLOAD_FLOAT;
  sample.paramid = 42;
  cstore->create_new_column(42);
  std::vector<u64> rpoints;
  session->write(sample, &rpoints);
  ReshapeRequest req = {};
  req.group_by.enabled = false;
  req.select.begin = 0;
  req.select.end = 100;
  req.select.columns.emplace_back();
  req.select.columns[0].ids.push_back(sample.paramid);
  req.order_by = OrderBy::SERIES;
  execute(cstore, &qproc, req);
  EXPECT_TRUE(qproc.error == common::Status::Ok());
  EXPECT_TRUE(qproc.samples.size() == 1);
  EXPECT_TRUE(qproc.samples.at(0).paramid == sample.paramid);
  EXPECT_TRUE(qproc.samples.at(0).timestamp == sample.timestamp);
}

static double fill_data_in(std::shared_ptr<ColumnStore> cstore, std::unique_ptr<CStoreSession>& session, ParamId id, Timestamp begin, Timestamp end) {
  assert(begin < end);
  cstore->create_new_column(id);
  Sample sample;
  sample.paramid = id;
  sample.payload.type = PAYLOAD_FLOAT;
  std::vector<u64> rpoints;
  double sum = 0;
  for (Timestamp ix = begin; ix < end; ix++) {
    sample.payload.float64 = ix*0.1;
    sample.timestamp = ix;
    session->write(sample, &rpoints);  // rescue points are ignored now
    sum += sample.payload.float64;
  }
  return sum;
}

static void test_column_store_query(Timestamp begin, Timestamp end) {
  auto cstore = create_cstore();
  auto session = create_session(cstore);
  std::vector<Timestamp> timestamps, invtimestamps;
  for (Timestamp ix = begin; ix < end; ix++) {
    timestamps.push_back(ix);
  }
  std::copy(timestamps.rbegin(), timestamps.rend(), std::back_inserter(invtimestamps));
  std::vector<ParamId> ids = {
    10,11,12,13,14,15,16,17,18,19
  };
  std::vector<ParamId> invids;
  std::copy(ids.rbegin(), ids.rend(), std::back_inserter(invids));
  for (auto id: ids) {
    fill_data_in(cstore, session, id, begin, end);
  }

  // Read in series order in forward direction
  auto read_ordered_by_series = [&](size_t base_ix, size_t inc) {
    QueryProcessorMock qproc;
    ReshapeRequest req = {};
    req.group_by.enabled = false;
    req.select.begin = begin;
    req.select.end = end;
    req.select.columns.emplace_back();
    for(size_t i = base_ix; i < ids.size(); i += inc) {
      req.select.columns[0].ids.push_back(ids[i]);
    }
    req.order_by = OrderBy::SERIES;
    execute(cstore, &qproc, req);
    EXPECT_TRUE(qproc.error == common::Status::Ok());
    EXPECT_TRUE(qproc.samples.size() == ids.size()/inc*timestamps.size());
    size_t niter = 0;
    for(size_t i = base_ix; i < ids.size(); i += inc) {
      for (auto tx: timestamps) {
        EXPECT_TRUE(qproc.samples.at(niter).paramid == ids[i]);
        EXPECT_TRUE(qproc.samples.at(niter).timestamp == tx);
        niter++;
      }
    }
  };

  // Read in series order in backward direction
  auto inv_read_ordered_by_series = [&](size_t base_ix, size_t inc) {
    QueryProcessorMock qproc;
    ReshapeRequest req = {};
    req.group_by.enabled = false;
    req.select.begin = end;
    req.select.end = begin-1; // we need to read data in range (begin-1, end] to hit value with `begin` timestamp
    req.select.columns.emplace_back();
    for(size_t i = base_ix; i < invids.size(); i += inc) {
      req.select.columns[0].ids.push_back(invids[i]);
    }
    req.order_by = OrderBy::SERIES;
    execute(cstore, &qproc, req);
    EXPECT_TRUE(qproc.error == common::Status::Ok());
    EXPECT_TRUE(qproc.samples.size() == invids.size()/inc*invtimestamps.size());
    size_t niter = 0;
    for(size_t i = base_ix; i < invids.size(); i += inc) {
      for (auto ts: invtimestamps) {
        EXPECT_TRUE(qproc.samples.at(niter).paramid == invids[i]);
        EXPECT_TRUE(qproc.samples.at(niter).timestamp == ts);
        niter++;
      }
    }
  };

  // Read in time order in forward direction
  auto read_ordered_by_time = [&](size_t base_ix, size_t inc) {
    QueryProcessorMock qproc;
    ReshapeRequest req = {};
    req.group_by.enabled = false;
    req.select.begin = begin;
    req.select.end = end;
    req.select.columns.emplace_back();
    for(size_t i = base_ix; i < ids.size(); i += inc) {
      req.select.columns[0].ids.push_back(ids[i]);
    }
    req.order_by = OrderBy::TIME;
    execute(cstore, &qproc, req);
    EXPECT_EQ(qproc.error, common::Status::Ok());
    EXPECT_EQ(qproc.samples.size(), ids.size()/inc*timestamps.size());
    size_t niter = 0;
    for (size_t ts = begin; ts < end; ts++) {
      for (size_t i = base_ix; i < ids.size(); i += inc) {
        if (qproc.samples.at(niter).paramid != ids[i]) {
          EXPECT_EQ(qproc.samples.at(niter).paramid, ids[i]);
        }
        if (qproc.samples.at(niter).timestamp != ts) {
          EXPECT_EQ(qproc.samples.at(niter).timestamp, ts);
        }
        niter++;
      }
    }
  };

  // Read in time order in backward direction
  auto inv_read_ordered_by_time = [&](size_t base_ix, size_t inc) {
    QueryProcessorMock qproc;
    ReshapeRequest req = {};
    req.group_by.enabled = false;
    req.select.begin = end;
    req.select.end = begin - 1;
    req.select.columns.emplace_back();
    for(size_t i = base_ix; i < invids.size(); i += inc) {
      req.select.columns[0].ids.push_back(invids[i]);
    }
    req.order_by = OrderBy::TIME;
    execute(cstore, &qproc, req);
    EXPECT_EQ(qproc.error, common::Status::Ok());
    EXPECT_EQ(qproc.samples.size(), invids.size()/inc*invtimestamps.size());
    size_t niter = 0;
    for (auto ts: invtimestamps) {
      for (size_t i = base_ix; i < invids.size(); i += inc) {
        EXPECT_EQ(qproc.samples.at(niter).paramid, invids[i]);
        EXPECT_EQ(qproc.samples.at(niter).timestamp, ts);
        niter++;
      }
    }
  };

  read_ordered_by_series(0, ids.size());  // read one series
  read_ordered_by_series(0, 2);  // read even
  read_ordered_by_series(1, 2);  // read odd
  read_ordered_by_series(0, 1);  // read all

  read_ordered_by_time(0, ids.size());  // read one series
  read_ordered_by_time(0, 2);  // read even
  read_ordered_by_time(1, 2);  // read odd
  read_ordered_by_time(0, 1);  // read all

  inv_read_ordered_by_series(0, ids.size());  // read one series
  inv_read_ordered_by_series(0, 2);  // read even
  inv_read_ordered_by_series(1, 2);  // read odd
  inv_read_ordered_by_series(0, 1);  // read all

  inv_read_ordered_by_time(0, ids.size());  // read one series
  inv_read_ordered_by_time(0, 2);  // read even
  inv_read_ordered_by_time(1, 2);  // read odd
  inv_read_ordered_by_time(0, 1);  // read all
}

TEST(TestColumnStore, Test_column_store_query_2) {
  test_column_store_query(10, 100);
  test_column_store_query(100, 1000);
  test_column_store_query(1000, 100000);
}

void test_groupby_query() {
  const Timestamp begin = 100;
  const Timestamp end  = 1100;
  auto cstore = create_cstore();
  auto session = create_session(cstore);
  std::vector<Timestamp> timestamps, invtimestamps;
  for (Timestamp ix = begin; ix < end; ix++) {
    timestamps.push_back(ix);
  }
  std::copy(timestamps.rbegin(), timestamps.rend(), std::back_inserter(invtimestamps));
  std::vector<ParamId> ids = {
    10,11,12,13,14,15,16,17,18,19,
    20,21,22,23,24,25,26,27,28,29,
  };
  std::unordered_map<ParamId, ParamId> translation_table;
  for (auto id: ids) {
    if (id < 20) {
      translation_table[id] = 1;
    } else {
      translation_table[id] = 2;
    }
  }
  std::shared_ptr<PlainSeriesMatcher> matcher = std::make_shared<PlainSeriesMatcher>();
  matcher->_add("_ten_", 1);
  matcher->_add("_twenty_", 2);
  std::vector<ParamId> invids;
  std::copy(ids.rbegin(), ids.rend(), std::back_inserter(invids));
  for (auto id: ids) {
    fill_data_in(cstore, session, id, begin, end);
  }

  // Read in series order in forward direction
  auto read_ordered_by_series = [&]() {
    QueryProcessorMock qproc;
    ReshapeRequest req = {};
    req.group_by.enabled = true;
    req.group_by.transient_map = translation_table;
    req.select.matcher = matcher;
    req.select.begin = begin;
    req.select.end = 1 + end;
    req.select.columns.emplace_back();
    req.select.columns.at(0).ids = ids;
    req.order_by = OrderBy::SERIES;
    execute(cstore, &qproc, req);
    EXPECT_TRUE(qproc.error == common::Status::Ok());
    EXPECT_TRUE(qproc.samples.size() == timestamps.size()*ids.size());
    size_t niter = 0;
    for(size_t id = 1; id < 3; id++) {
      for (auto tx: timestamps) {
        for (size_t k = 0; k < 10; k++) {
          EXPECT_TRUE(qproc.samples.at(niter).paramid == id);
          EXPECT_TRUE(qproc.samples.at(niter).timestamp == tx);
          niter++;
        }
      }
    }
  };

  // Read in series order in forward direction
  auto read_ordered_by_time = [&]() {
    QueryProcessorMock qproc;
    ReshapeRequest req = {};
    req.group_by.enabled = true;
    req.group_by.transient_map = translation_table;
    req.select.matcher = matcher;
    req.select.begin = begin;
    req.select.end = 1 + end;
    req.select.columns.emplace_back();
    req.select.columns.at(0).ids = ids;
    req.order_by = OrderBy::TIME;
    execute(cstore, &qproc, req);
    EXPECT_TRUE(qproc.error == common::Status::Ok());
    EXPECT_TRUE(qproc.samples.size() == timestamps.size()*ids.size());
    size_t niter = 0;
    for (auto tx: timestamps) {
      for(size_t id = 1; id < 3; id++) {
        for (size_t k = 0; k < 10; k++) {
          EXPECT_TRUE(qproc.samples.at(niter).paramid == id);
          EXPECT_TRUE(qproc.samples.at(niter).timestamp == tx);
          niter++;
        }
      }
    }
  };

  read_ordered_by_series();
  read_ordered_by_time();
}

TEST(TestNBtree, Test_column_store_group_by_1) {
  test_groupby_query();
}

void test_reopen(Timestamp begin, Timestamp end) {
  std::shared_ptr<BlockStore> bstore = BlockStoreBuilder::create_memstore();
  std::shared_ptr<ColumnStore> cstore;
  cstore.reset(new ColumnStore(bstore));
  auto session = create_session(cstore);
  std::vector<Timestamp> timestamps;
  for (Timestamp ix = begin; ix < end; ix++) {
    timestamps.push_back(ix);
  }
  std::vector<ParamId> ids = {
    10,11,12,13,14,15,16,17,18,19
  };
  std::vector<ParamId> invids;
  std::copy(ids.rbegin(), ids.rend(), std::back_inserter(invids));

  for (auto id: ids) {
    fill_data_in(cstore, session, id, begin, end);
  }

  session.reset();
  auto mapping = cstore->close();

  // Reopen
  cstore.reset(new ColumnStore(bstore));
  cstore->open_or_restore(mapping);
  session = create_session(cstore);

  QueryProcessorMock qproc;
  ReshapeRequest req = {};
  req.group_by.enabled = false;
  req.select.begin = begin;
  req.select.end = end;
  req.select.columns.emplace_back();
  for(size_t i = 0; i < ids.size(); i++) {
    req.select.columns[0].ids.push_back(ids[i]);
  }
  req.order_by = OrderBy::SERIES;
  execute(cstore, &qproc, req);

  // Check everything
  EXPECT_TRUE(qproc.error == common::Status::Ok());
  EXPECT_TRUE(qproc.samples.size() == ids.size()*timestamps.size());
  size_t niter = 0;
  for(size_t i = 0; i < ids.size(); i++) {
    for (auto tx: timestamps) {
      EXPECT_TRUE(qproc.samples.at(niter).paramid == ids[i]);
      EXPECT_TRUE(qproc.samples.at(niter).timestamp == tx);
      niter++;
    }
  }
}

TEST(TestNBtree, Test_column_store_reopen_1) {
  test_reopen(100, 200);     // 100 el.
}

TEST(TestNBtree, Test_column_store_reopen_2) {
  test_reopen(1000, 2000);   // 1000 el.
}

TEST(TestNBtree, Test_column_store_reopen_3) {
  test_reopen(1000, 11000);  // 10000 el.
}

void test_aggregation(Timestamp begin, Timestamp end) {
  auto cstore = create_cstore();
  auto session = create_session(cstore);
  std::vector<ParamId> ids = {
    10,11,12,13,14,15,16,17,18,19
  };
  std::vector<double> sums;
  for (auto id: ids) {
    double sum = fill_data_in(cstore, session, id, begin, end);
    sums.push_back(sum);
  }
  QueryProcessorMock mock;
  ReshapeRequest req = {};
  req.agg.enabled = true;
  std::vector<AggregationFunction> func(ids.size(), AggregationFunction::SUM);
  std::swap(req.agg.func, func);
  req.group_by.enabled = false;
  req.order_by = OrderBy::SERIES;
  req.select.begin = begin;
  req.select.end = end;
  req.select.columns.push_back({ids});

  execute(cstore, &mock, req);

  EXPECT_EQ(mock.samples.size(), ids.size());
  for (auto i = 0u; i < mock.samples.size(); i++) {
    EXPECT_EQ(mock.samples.at(i).paramid, ids.at(i));
    EXPECT_LT(fabs(mock.samples.at(i).payload.float64 - sums.at(i)), 10E-5);
  }
}

TEST(TestNBtree, Test_column_store_aggregation_1) {
  test_aggregation(100, 1100);
}

TEST(TestNBtree, Test_column_store_aggregation_2) {
  test_aggregation(1000, 11000);
}

TEST(TestNBtree, Test_column_store_aggregation_3) {
  test_aggregation(10000, 110000);
}

void test_aggregation_group_by(Timestamp begin, Timestamp end) {
  auto cstore = create_cstore();
  auto session = create_session(cstore);
  std::vector<ParamId> ids = {
    10,11,12,13,14,15,16,17,18,19,
    20,21,22,23,24,25,26,27,28,29
  };
  std::unordered_map<ParamId, ParamId> translation_table;
  for (auto id: ids) {
    if (id < 20) {
      translation_table[id] = 1;
    } else {
      translation_table[id] = 2;
    }
  }
  std::shared_ptr<PlainSeriesMatcher> matcher = std::make_shared<PlainSeriesMatcher>();
  matcher->_add("_ten_", 1);
  matcher->_add("_twenty_", 2);
  double sum1 = 0, sum2 = 0;
  for (auto id: ids) {
    double sum = fill_data_in(cstore, session, id, begin, end);
    if (id < 20) {
      sum1 += sum;
    } else {
      sum2 += sum;
    }
  }
  QueryProcessorMock mock;
  ReshapeRequest req = {};
  req.agg.enabled = true;
  std::vector<AggregationFunction> func(ids.size(), AggregationFunction::SUM);
  std::swap(req.agg.func, func);
  req.group_by.enabled = true;
  req.select.matcher = matcher;
  req.group_by.transient_map = translation_table;
  req.order_by = OrderBy::SERIES;
  req.select.begin = begin;
  req.select.end = end;
  req.select.columns.push_back({ids});

  execute(cstore, &mock, req);

  std::vector<double> sums = {sum1, sum2};
  ids = {1, 2};

  EXPECT_EQ(mock.samples.size(), ids.size());
  for (auto i = 0u; i < mock.samples.size(); i++) {
    EXPECT_EQ(mock.samples.at(i).paramid, ids.at(i));
    EXPECT_LE(fabs(mock.samples.at(i).payload.float64 - sums.at(i)), 10E-5);
  }
}

TEST(TestNBtree, Test_column_store_aggregation_group_by_1) {
  test_aggregation_group_by(100, 1100);
}

TEST(TestNBtree, Test_column_store_aggregation_group_by_2) {
  test_aggregation_group_by(1000, 11000);
}

struct TupleQueryProcessorMock : qp::IStreamProcessor {
  bool started = false;
  bool stopped = false;
  std::vector<u64> bitmaps;
  std::vector<u64> paramids;
  std::vector<u64> timestamps;
  std::vector<std::vector<double>> columns;
  common::Status error = common::Status::Ok();

  TupleQueryProcessorMock(u32 ncol) {
    columns.resize(ncol);
  }

  virtual bool start() override {
    started = true;
    return true;
  }
  virtual void stop() override {
    stopped = true;
  }
  virtual bool put(const Sample &sample) override {
    if ((sample.payload.type & PAYLOAD_TUPLE) != PAYLOAD_TUPLE) {
      LOG(FATAL) << "Tuple expected";
    }
    union {
      double d;
      u64    u;
    } bitmap;
    bitmap.d = sample.payload.float64;
    bitmaps.push_back(bitmap.u);
    paramids.push_back(sample.paramid);
    timestamps.push_back(sample.timestamp);
    double const* tup = reinterpret_cast<double const*>(sample.payload.data);
    for (auto i = 0u; i < columns.size(); i++) {
      EXPECT_TRUE((bitmap.u & (1 << i)) != 0);
      columns.at(i).push_back(tup[i]);
    }
    return true;
  }
  virtual void set_error(common::Status err) override {
    error = err;
  }
};

void test_join(Timestamp begin, Timestamp end) {
  auto cstore = create_cstore();
  auto session = create_session(cstore);
  std::vector<ParamId> col1 = {
    10,11,12,13,14,15,16,17,18,19
  };
  std::vector<ParamId> col2 = {
    20,21,22,23,24,25,26,27,28,29
  };
  std::vector<Timestamp> timestamps;
  for (Timestamp ix = begin; ix < end; ix++) {
    timestamps.push_back(ix);
  }
  for (auto id: col1) {
    fill_data_in(cstore, session, id, begin, end);
  }
  for (auto id: col2) {
    fill_data_in(cstore, session, id, begin, end);
  }

  {
    TupleQueryProcessorMock mock(2);
    ReshapeRequest req = {};
    req.agg.enabled = false;
    req.group_by.enabled = false;
    req.order_by = OrderBy::SERIES;
    req.select.begin = begin;
    req.select.end = end;
    req.select.columns.push_back({col1});
    req.select.columns.push_back({col2});

    execute(cstore, &mock, req);

    EXPECT_TRUE(mock.error == common::Status::Ok());
    u32 ix = 0;
    for (auto id: col1) {
      for (auto ts: timestamps) {
        EXPECT_TRUE(mock.paramids.at(ix) == id);
        EXPECT_TRUE(mock.timestamps.at(ix) == ts);
        double expected = ts*0.1;
        double col0 = mock.columns[0][ix];
        double col1 = mock.columns[1][ix];
        EXPECT_LE(fabs(expected - col0), 10E-10);
        EXPECT_LE(fabs(col0 - col1), 10E-10);
        ix++;
      }
    }
  }

  {
    TupleQueryProcessorMock mock(2);
    ReshapeRequest req = {};
    req.agg.enabled = false;
    req.group_by.enabled = false;
    req.order_by = OrderBy::TIME;
    req.select.begin = begin;
    req.select.end = end;
    req.select.columns.push_back({col1});
    req.select.columns.push_back({col2});

    execute(cstore, &mock, req);

    EXPECT_TRUE(mock.error == common::Status::Ok());
    u32 ix = 0;
    for (auto ts: timestamps) {
      for (auto id: col1) {
        EXPECT_TRUE(mock.paramids.at(ix) == id);
        EXPECT_TRUE(mock.timestamps.at(ix) == ts);
        double expected = ts*0.1;
        double col0 = mock.columns[0][ix];
        double col1 = mock.columns[1][ix];
        EXPECT_LE(fabs(expected - col0), 10E-10);
        EXPECT_LE(fabs(col0 - col1), 10E-10);
        ix++;
      }
    }
  }
}

TEST(TestNBtree, Test_column_store_join_1) {
  test_join(100, 1100);
}

void test_group_aggregate(Timestamp begin, Timestamp end) {
  auto cstore = create_cstore();
  auto session = create_session(cstore);
  std::vector<ParamId> col = {
    10,11,12,13,14,15,16,17,18,19
  };
  std::vector<Timestamp> timestamps;
  for (Timestamp ix = begin; ix < end; ix++) {
    timestamps.push_back(ix);
  }
  for (auto id: col) {
    fill_data_in(cstore, session, id, begin, end);
  }

  auto test_series_order = [&](size_t step)
  {
    std::vector<Timestamp> model_timestamps;
    for (size_t i = 0u; i < timestamps.size(); i += step) {
      model_timestamps.push_back(timestamps.at(i));
    }
    TupleQueryProcessorMock mock(1);
    ReshapeRequest req = {};
    req.agg.enabled = true;
    req.agg.step = step;
    std::vector<AggregationFunction> func(col.size(), AggregationFunction::MIN);
    std::swap(req.agg.func, func);
    req.group_by.enabled = false;
    req.order_by = OrderBy::SERIES;
    req.select.begin = begin;
    req.select.end = end;
    req.select.columns.push_back({col});

    execute(cstore, &mock, req);

    EXPECT_TRUE(mock.error == common::Status::Ok());
    u32 ix = 0;
    for (auto id: col) {
      for (auto ts: model_timestamps) {
        EXPECT_TRUE(mock.paramids.at(ix) == id);
        EXPECT_TRUE(mock.timestamps.at(ix) == ts);
        double expected = ts*0.1;
        double xs = mock.columns[0][ix];
        EXPECT_LE(fabs(expected - xs), 10E-10);
        ix++;
      }
    }
    EXPECT_TRUE(ix != 0);
  };
  auto test_time_order = [&](size_t step)
  {
    std::vector<Timestamp> model_timestamps;
    for (size_t i = 0u; i < timestamps.size(); i += step) {
      model_timestamps.push_back(timestamps.at(i));
    }
    TupleQueryProcessorMock mock(1);
    ReshapeRequest req = {};
    req.agg.enabled = true;
    req.agg.step = step;
    std::vector<AggregationFunction> func(col.size(), AggregationFunction::MIN);
    std::swap(req.agg.func, func);
    req.group_by.enabled = false;
    req.order_by = OrderBy::TIME;
    req.select.begin = begin;
    req.select.end = end;
    req.select.columns.push_back({col});

    execute(cstore, &mock, req);

    EXPECT_TRUE(mock.error == common::Status::Ok());
    u32 ix = 0;
    for (auto ts: model_timestamps) {
      for (auto id: col) {
        EXPECT_TRUE(mock.paramids.at(ix) == id);
        EXPECT_TRUE(mock.timestamps.at(ix) == ts);
        double expected = ts*0.1;
        double xs = mock.columns[0][ix];
        EXPECT_LE(fabs(expected - xs), 10E-10);
        ix++;
      }
    }
    EXPECT_TRUE(ix != 0);
  };
  test_series_order(10);
  test_series_order(100);
  test_time_order(10);
  test_time_order(100);
}

TEST(TestNBtree, Test_column_store_group_aggregate_1) {
  test_group_aggregate(100, 1100);
}

TEST(TestNBtree, Test_column_store_group_aggregate_2) {
  test_group_aggregate(1000, 11000);
}

//! Tests aggregate query in conjunction with group-by clause
void test_aggregate_and_group_by(Timestamp begin, Timestamp end) {
  auto cstore = create_cstore();
  auto session = create_session(cstore);
  std::vector<ParamId> ids = {
    10,11,12,13,14,15,16,17,18,19
  };
  std::vector<double> sums = { 0.0, 0.0 };
  for (auto id: ids) {
    double sum = fill_data_in(cstore, session, id, begin, end);
    if (id % 2 == 0) {
      sums[1] += sum;
    } else {
      sums[0] += sum;
    }
  }
  QueryProcessorMock mock;
  ReshapeRequest req = {};
  req.agg.enabled = true;
  std::vector<AggregationFunction> func(ids.size(), AggregationFunction::SUM);
  std::swap(req.agg.func, func);
  req.group_by.enabled = false;
  req.order_by = OrderBy::SERIES;
  req.select.begin = begin;
  req.select.end = end;
  req.select.columns.push_back({ids});
  req.group_by.enabled = true;
  req.select.matcher = std::make_shared<PlainSeriesMatcher>(1);
  req.select.matcher->_add("odd", 100);
  req.select.matcher->_add("even", 200);
  req.group_by.transient_map = {
    { 11, 100 },
    { 13, 100 },
    { 15, 100 },
    { 17, 100 },
    { 19, 100 },
    { 10, 200 },
    { 12, 200 },
    { 14, 200 },
    { 16, 200 },
    { 18, 200 },
  };

  execute(cstore, &mock, req);

  std::vector<ParamId> gids = { 100, 200 };

  EXPECT_EQ(mock.samples.size(), gids.size());
  for (auto i = 0u; i < mock.samples.size(); i++) {
    EXPECT_EQ(mock.samples.at(i).paramid, gids.at(i));
    EXPECT_LE(fabs(mock.samples.at(i).payload.float64 - sums.at(i)), 10E-5);
  }
}

TEST(TestNBtree, Test_column_store_aggregate_group_by_1) {
  test_aggregate_and_group_by(10, 110);
}

TEST(TestNBtree, Test_column_store_aggregate_group_by_2) {
  test_aggregate_and_group_by(100, 1100);
}

TEST(TestNBtree, Test_column_store_aggregate_group_by_3) {
  test_aggregate_and_group_by(1000, 11000);
}

static double fill_data2(std::shared_ptr<ColumnStore> cstore,
                         std::unique_ptr<CStoreSession>& session,
                         ParamId id,
                         Timestamp begin,
                         Timestamp end) {
  assert(begin < end);
  cstore->create_new_column(id);
  Sample sample;
  sample.paramid = id;
  sample.payload.type = PAYLOAD_FLOAT;
  std::vector<u64> rpoints;
  double sum = 0;
  for (Timestamp ix = begin; ix < end; ix++) {
    sample.payload.float64 = ix % 2 == 0 ? -0.1*ix : 0.1*ix;
    sample.timestamp = ix;
    session->write(sample, &rpoints);  // rescue points are ignored now
    sum += sample.payload.float64;
  }
  return sum;
}

static void test_column_store_filter_query(Timestamp begin, Timestamp end) {
  auto cstore = create_cstore();
  auto session = create_session(cstore);
  std::vector<Timestamp> timestamps, invtimestamps;
  for (Timestamp ix = begin; ix < end; ix++) {
    timestamps.push_back(ix);
  }
  std::copy(timestamps.rbegin(),
            timestamps.rend(),
            std::back_inserter(invtimestamps));
  std::vector<ParamId> ids = {
    10,11,12,13,14,15,16,17,18,19
  };
  std::vector<ParamId> invids;
  std::copy(ids.rbegin(), ids.rend(), std::back_inserter(invids));
  for (auto id: ids) {
    fill_data2(cstore, session, id, begin, end);
  }

  // Read in series order in forward direction
  auto read_ordered_by_series = [&]() {
    QueryProcessorMock qproc;
    ReshapeRequest req = {};
    req.group_by.enabled = false;
    req.select.begin = begin;
    req.select.end = end;
    req.select.columns.emplace_back();
    for(size_t i = 0; i < ids.size(); i++) {
      req.select.columns[0].ids.push_back(ids[i]);
    }
    req.order_by = OrderBy::SERIES;

    Filter filter;
    filter.enabled = true;
    filter.ge = 0.0;
    filter.flags = Filter::GE;
    req.select.filters.push_back(filter);

    execute(cstore, &qproc, req);
    EXPECT_TRUE(qproc.error == common::Status::Ok());
    EXPECT_TRUE(qproc.samples.size() == ids.size()*timestamps.size()/2);
    size_t niter = 0;
    for(size_t i = 0; i < ids.size(); i++) {
      for (size_t j = 1; j < timestamps.size(); j += 2) {
        auto tx = timestamps[j];
        EXPECT_TRUE(qproc.samples.at(niter).paramid == ids[i]);
        EXPECT_TRUE(qproc.samples.at(niter).timestamp == tx);
        niter++;
      }
    }
  };

  // Read in time order in forward direction
  auto read_ordered_by_time = [&]() {
    QueryProcessorMock qproc;
    ReshapeRequest req = {};
    req.group_by.enabled = false;
    req.select.begin = begin;
    req.select.end = end;
    req.select.columns.emplace_back();
    for(size_t i = 0; i < ids.size(); i++) {
      req.select.columns[0].ids.push_back(ids[i]);
    }
    req.order_by = OrderBy::TIME;

    Filter filter;
    filter.enabled = true;
    filter.ge = 0.0;
    filter.flags = Filter::GE;
    req.select.filters.push_back(filter);

    execute(cstore, &qproc, req);
    EXPECT_TRUE(qproc.error == common::Status::Ok());
    EXPECT_TRUE(qproc.samples.size() == ids.size()*timestamps.size() / 2);
    size_t niter = 0;
    for (size_t j = 1; j < timestamps.size(); j += 2) {
      for(size_t i = 0; i < ids.size(); i++) {
        auto tx = timestamps[j];
        EXPECT_TRUE(qproc.samples.at(niter).paramid == ids[i]);
        EXPECT_TRUE(qproc.samples.at(niter).timestamp == tx);
        niter++;
      }
    }
  };

  auto read_backward_ordered_by_series = [&]() {
    QueryProcessorMock qproc;
    ReshapeRequest req = {};
    req.group_by.enabled = false;
    req.select.begin = end;
    req.select.end = begin;
    req.select.columns.emplace_back();
    for(size_t i = 0; i < ids.size(); i++) {
      req.select.columns[0].ids.push_back(ids[i]);
    }
    req.order_by = OrderBy::SERIES;

    Filter filter;
    filter.enabled = true;
    filter.ge = 0.0;
    filter.flags = Filter::GE;
    req.select.filters.push_back(filter);

    execute(cstore, &qproc, req);
    EXPECT_TRUE(qproc.error == common::Status::Ok());
    EXPECT_TRUE(qproc.samples.size() == ids.size()*invtimestamps.size()/2);
    size_t niter = 0;
    for(size_t i = 0; i < ids.size(); i++) {
      for (size_t j = 0; j < invtimestamps.size(); j += 2) {
        auto tx = invtimestamps[j];
        EXPECT_TRUE(qproc.samples.at(niter).paramid == ids[i]);
        EXPECT_TRUE(qproc.samples.at(niter).timestamp == tx);
        niter++;
      }
    }
  };

  // Read in time order in forward direction
  auto read_backward_ordered_by_time = [&]() {
    QueryProcessorMock qproc;
    ReshapeRequest req = {};
    req.group_by.enabled = false;
    req.select.begin = end;
    req.select.end = begin;
    req.select.columns.emplace_back();
    for(size_t i = 0; i < ids.size(); i++) {
      req.select.columns[0].ids.push_back(ids[i]);
    }
    req.order_by = OrderBy::TIME;

    Filter filter;
    filter.enabled = true;
    filter.ge = 0.0;
    filter.flags = Filter::GE;
    req.select.filters.push_back(filter);

    execute(cstore, &qproc, req);
    EXPECT_TRUE(qproc.error == common::Status::Ok());
    EXPECT_TRUE(qproc.samples.size() == invids.size()*invtimestamps.size()/2);
    size_t niter = 0;
    for (size_t j = 0; j < invtimestamps.size(); j += 2) {
      for(size_t i = 0; i < invids.size(); i++) {
        auto tx = invtimestamps[j];
        EXPECT_TRUE(qproc.samples.at(niter).paramid == invids[i]);
        EXPECT_TRUE(qproc.samples.at(niter).timestamp == tx);
        niter++;
      }
    }
  };

  read_ordered_by_series();
  read_ordered_by_time();
  read_backward_ordered_by_series();
  read_backward_ordered_by_time();
}

TEST(TestNBtree, Test_column_store_filter_query_0) {
  test_column_store_filter_query(10, 100);
}

TEST(TestNBtree, Test_column_store_filter_query_1) {
  test_column_store_filter_query(100, 1000);
}

TEST(TestNBtree, Test_column_store_filter_query_2) {
  test_column_store_filter_query(1000, 100000);
}

void test_group_aggregate_filter(Timestamp begin, Timestamp end) {
  auto cstore = create_cstore();
  auto session = create_session(cstore);
  std::vector<ParamId> col = {
    10,11,12,13,14,15,16,17,18,19
  };
  std::vector<Timestamp> timestamps;
  for (Timestamp ix = begin; ix < end; ix++) {
    timestamps.push_back(ix);
  }
  for (auto id: col) {
    fill_data2(cstore, session, id, begin, end);
  }

  auto test_series_order = [&](size_t step)
  {
    std::vector<Timestamp> model_timestamps;
    for (size_t i = 0u; i < timestamps.size(); i += step) {
      model_timestamps.push_back(timestamps.at(i));
    }
    TupleQueryProcessorMock mock(1);
    ReshapeRequest req = {};
    req.agg.enabled = true;
    req.agg.step = step;
    req.agg.func = {AggregationFunction::MEAN};
    req.group_by.enabled = false;
    req.order_by = OrderBy::SERIES;
    req.select.begin = begin;
    req.select.end = end;
    req.select.columns.push_back({col});

    Filter filter;
    filter.enabled = true;
    filter.ge = 0.0;
    filter.flags = Filter::GE;
    req.select.filters.push_back(filter);

    execute(cstore, &mock, req);

    EXPECT_TRUE(mock.error == common::Status::Ok());
    EXPECT_TRUE(mock.paramids.size() != 0);

    size_t idix = 0;
    Timestamp prevts = begin;
    for (size_t ix = 0; ix < mock.paramids.size(); ix++) {
      auto id = mock.paramids.at(ix);
      auto ts = mock.timestamps.at(ix);
      auto xs = mock.columns[0].at(ix);
      if (id != col.at(idix)) {
        idix++;
        prevts = begin;
      }
      EXPECT_EQ(id, col.at(idix));
      EXPECT_TRUE(ts >= prevts);
      EXPECT_TRUE(xs >= 0.0);
    }
  };

  auto test_time_order = [&](size_t step)
  {
    std::vector<Timestamp> model_timestamps;
    for (size_t i = 0u; i < timestamps.size(); i += step) {
      model_timestamps.push_back(timestamps.at(i));
    }
    TupleQueryProcessorMock mock(1);
    ReshapeRequest req = {};
    req.agg.enabled = true;
    req.agg.step = step;
    req.agg.func = {AggregationFunction::MEAN};
    req.group_by.enabled = false;
    req.order_by = OrderBy::TIME;
    req.select.begin = begin;
    req.select.end = end;
    req.select.columns.push_back({col});

    Filter filter;
    filter.enabled = true;
    filter.ge = 0.0;
    filter.flags = Filter::GE;
    req.select.filters.push_back(filter);

    execute(cstore, &mock, req);

    EXPECT_TRUE(mock.error == common::Status::Ok());
    EXPECT_TRUE(mock.paramids.size() != 0);

    Timestamp prevts = begin;
    for (size_t ix = 0; ix < mock.paramids.size(); ix++) {
      auto ts = mock.timestamps.at(ix);
      auto xs = mock.columns[0].at(ix);
      EXPECT_TRUE(ts >= prevts);
      EXPECT_TRUE(xs >= 0.0);
    }
  };

  auto test_series_order2 = [&](size_t step)
  {
    std::vector<Timestamp> model_timestamps;
    for (size_t i = 0u; i < timestamps.size(); i += step) {
      model_timestamps.push_back(timestamps.at(i));
    }
    TupleQueryProcessorMock mock(2);
    ReshapeRequest req = {};
    req.agg.enabled = true;
    req.agg.step = step;
    req.agg.func = { AggregationFunction::MIN, AggregationFunction::MAX };
    req.group_by.enabled = false;
    req.order_by = OrderBy::SERIES;
    req.select.begin = begin;
    req.select.end = end;
    req.select.columns.push_back({col});

    Filter f1{};
    f1.enabled = true;
    f1.lt = 0.0;
    f1.flags = Filter::LT;
    req.select.filters.push_back(f1);

    Filter f2{};
    f2.enabled = true;
    f2.ge = 0.0;
    f2.flags = Filter::GE;
    req.select.filters.push_back(f2);

    execute(cstore, &mock, req);

    EXPECT_TRUE(mock.error == common::Status::Ok());
    EXPECT_TRUE(mock.paramids.size() != 0);

    size_t idix = 0;
    Timestamp prevts = begin;
    for (size_t ix = 0; ix < mock.paramids.size(); ix++) {
      auto id = mock.paramids.at(ix);
      auto ts = mock.timestamps.at(ix);
      auto x1 = mock.columns[0].at(ix);  // min
      auto x2 = mock.columns[1].at(ix);  // max
      if (id != col.at(idix)) {
        idix++;
        prevts = begin;
      }
      EXPECT_EQ(id, col.at(idix));
      EXPECT_TRUE(ts >= prevts);
      EXPECT_TRUE(x1 <  0.0);
      EXPECT_TRUE(x2 >= 0.0);
    }
  };

  test_series_order(10);
  test_series_order(100);
  test_time_order(10);
  test_time_order(100);
  test_series_order2(10);
  test_series_order2(100);
}

TEST(TestNBtree, Test_group_aggregate_filter_query_1) {
  test_group_aggregate_filter(100, 1100);
}

TEST(TestNBtree, Test_group_aggregate_filter_query_2) {
  test_group_aggregate_filter(1000, 11000);
}

void test_open_or_restore(Timestamp begin, Timestamp end, bool graceful_shutdown, bool force_init) {
  u32 append_count = 0;
  u32 read_count = 0;
  auto bstore = BlockStoreBuilder::create_memstore([&append_count](LogicAddr) { append_count++; },
                                                   [&read_count](LogicAddr) { read_count++; });
  std::shared_ptr<ColumnStore> cstore;
  cstore.reset(new ColumnStore(bstore));
  auto session = create_session(cstore);
  std::vector<Timestamp> timestamps;
  for (Timestamp ix = begin; ix < end; ix++) {
    timestamps.push_back(ix);
  }
  std::vector<ParamId> ids = {
    10,11,12,13,14,15,16,17,18,19
  };
  std::vector<ParamId> invids;
  std::copy(ids.rbegin(), ids.rend(), std::back_inserter(invids));

  for (auto id: ids) {
    fill_data_in(cstore, session, id, begin, end);
  }

  auto checkpoint = bstore->get_write_pos();
  session.reset();
  std::unordered_map<ParamId, std::vector<LogicAddr>> mapping;
  if (graceful_shutdown) {
    mapping = cstore->close();
  } else {
    auto nbtree_list = cstore->_get_columns();
    for (auto& kv: nbtree_list) {
      std::shared_ptr<NBTreeExtentsList> nbtree = kv.second;
      auto rpoints = nbtree->get_roots();
      mapping[kv.first] = std::move(rpoints);
    }
  }

  cstore.reset();

  if (!graceful_shutdown) {
    bstore->reset_write_pos(checkpoint);
  }
  u32 nwrites = append_count;
  u32 nreads = read_count;
  cstore.reset(new ColumnStore(bstore));
  cstore->open_or_restore(mapping, force_init);

  if (force_init || graceful_shutdown == false) {
    if (append_count != 0) {
      EXPECT_TRUE(nreads < read_count);
    }
  }
  if (force_init == false && graceful_shutdown == false) {
    if (append_count != 0) {
      EXPECT_TRUE(nwrites < append_count);
    }
  }
  if (force_init == false && graceful_shutdown == true) {
    EXPECT_TRUE(nreads == read_count);
    EXPECT_TRUE(nwrites == append_count);
  }

  session = create_session(cstore);
  nwrites = append_count;
  nreads = read_count;
  for (auto id: ids) {
    fill_data_in(cstore, session, id, end, end + 1);
  }
  if (force_init) {
    EXPECT_TRUE(nreads == read_count);
    EXPECT_TRUE(nwrites == append_count);
  } else {
    if (nwrites != 0) {
      EXPECT_TRUE(nreads != read_count);
      EXPECT_TRUE(nwrites == append_count);
    }
  }
}

TEST(TestNBtree, Test_column_store_open_1) {
  test_open_or_restore(100, 200, true, false);
}

TEST(TestNBtree, Test_column_store_open_2) {
  test_open_or_restore(1000, 2000, true, false);
}

TEST(TestNBtree, Test_column_store_open_3) {
  test_open_or_restore(1000, 11000, true, false);
}

TEST(TestNBtree, Test_column_store_open_4) {
  test_open_or_restore(100, 200, true, true);
}

TEST(TestNBtree, Test_column_store_open_5) {
  test_open_or_restore(1000, 2000, true, true);
}

TEST(TestNBtree, Test_column_store_open_6) {
  test_open_or_restore(1000, 11000, true, true);
}

TEST(TestNBtree, Test_column_store_repair_0) {
  test_open_or_restore(100, 200, false, false);
}

TEST(TestNBtree, Test_column_store_repair_1) {
  test_open_or_restore(1000, 2000, false, false);
}

TEST(TestNBtree, Test_column_store_repair_2) {
  test_open_or_restore(1000, 11000, false, false);
}

TEST(TestNBtree, Test_column_store_repair_3) {
  test_open_or_restore(100, 200, false, true);
}

TEST(TestNBtree, Test_column_store_repair_4) {
  test_open_or_restore(1000, 2000, false, true);
}

TEST(TestNBtree, Test_column_store_repair_5) {
  test_open_or_restore(1000, 11000, false, true);
}

/**
 * @brief Veryfy that the column is safe to read and write after recovery in wal mode
 * @param begin
 * @param end
 */
void test_restored_column_safety(Timestamp begin, Timestamp end) {
  auto bstore = BlockStoreBuilder::create_memstore();
  std::shared_ptr<ColumnStore> cstore;
  cstore.reset(new ColumnStore(bstore));
  auto session = create_session(cstore);
  std::vector<Timestamp> timestamps;
  for (Timestamp ix = begin; ix < end; ix++) {
    timestamps.push_back(ix);
  }
  std::vector<ParamId> ids = {
    10,11,12,13,14,15,16,17,18,19
  };
  std::vector<ParamId> invids;
  std::copy(ids.rbegin(), ids.rend(), std::back_inserter(invids));

  for (auto id: ids) {
    fill_data_in(cstore, session, id, begin, end);
  }

  session.reset();
  std::unordered_map<ParamId, std::vector<LogicAddr>> mapping;
  // Emulate failure
  auto nbtree_list = cstore->_get_columns();
  for (auto& kv: nbtree_list) {
    std::shared_ptr<NBTreeExtentsList> nbtree = kv.second;
    auto rpoints = nbtree->get_roots();
    mapping[kv.first] = std::move(rpoints);
  }
  cstore.reset(new ColumnStore(bstore));

  // Restore database
  cstore->open_or_restore(mapping, false);

  auto usize = cstore->_get_uncommitted_memory();
  EXPECT_EQ(usize, 0);

  // Read half of series
  QueryProcessorMock qproc;
  ReshapeRequest req = {};
  req.group_by.enabled = false;
  req.select.begin = begin;
  req.select.end = end;
  req.select.columns.emplace_back();
  for(size_t i = 0; i < ids.size()/2; i++) {
    req.select.columns[0].ids.push_back(ids[i]);
  }
  req.order_by = OrderBy::SERIES;
  execute(cstore, &qproc, req);

  EXPECT_TRUE(qproc.error == common::Status::Ok());

  // Try to update the rest series
  session = create_session(cstore);
  for(size_t i = ids.size()/2; i < ids.size(); i++) {
    auto id = ids.at(i);
    fill_data_in(cstore, session, id, end, end + 1);
  }
}

TEST(TestNBtree, Test_restored_column_safety_0) {
  test_restored_column_safety(100, 200);
}

TEST(TestNBtree, Test_restored_column_safety_1) {
  test_restored_column_safety(1000, 2000);
}

TEST(TestNBtree, Test_restored_column_safety_2) {
  test_restored_column_safety(1000, 11000);
}

}  // namespace storage
}  // namespace stdb

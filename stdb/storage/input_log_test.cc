/*!
 * \file input_log_test.cc
 */
#include <iostream>

#include <apr.h>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/regex.hpp>
#include <map>

#include "stdb/storage/input_log.h"

#include "gtest/gtest.h"

namespace stdb {
namespace storage {

bool volume_filename_is_ok(std::string name) {
  static const char* exp = "inputlog\\d+_\\d+\\.ils";
  static const boost::regex regex(exp);
  return boost::regex_match(name, regex);
}

struct AprInitializer {
  AprInitializer() {
    apr_initialize();
  }
};

static AprInitializer initializer;
static LogSequencer sequencer;

TEST(TestInputLog, Test_input_roundtrip) {
  std::vector<u64> stale_ids;
  std::vector<std::tuple<u64, u64, double>> exp, act;
  {
    InputLog ilog(&sequencer, "./", 100, 4096, 0);
    for (int i = 0; i < 10000; i++) {
      double val = static_cast<double>(rand()) / RAND_MAX;
      common::Status status = ilog.append(42, i, val, &stale_ids);
      exp.push_back(std::make_tuple(42, i, val));
      if (status == common::Status::Overflow()) {
        ilog.rotate();
      }
    }
  }
  EXPECT_TRUE(stale_ids.empty());
  {
    InputLog ilog("./", 0);
    while (true) {
      InputLogRow buffer[1024];
      common::Status status;
      u32 outsz;
      std::tie(status, outsz) = ilog.read_next(1024, buffer);
      EXPECT_EQ(status, common::Status::Ok());
      for(u32 i = 0; i < outsz; i++) {
        auto id = buffer[i].id;
        auto payload = boost::get<InputLogDataPoint>(buffer[i].payload);
        act.push_back(std::make_tuple(id, payload.timestamp, payload.value));
      }
      if (outsz == 0) {
        break;
      }
    }
    ilog.reopen();
    ilog.delete_files();
  }
  EXPECT_EQ(exp.size(), act.size());
  for (u32 i = 0; i < exp.size(); i++) {
    EXPECT_EQ(std::get<0>(exp.at(i)), std::get<0>(act.at(i)));
    EXPECT_EQ(std::get<1>(exp.at(i)), std::get<1>(act.at(i)));
    EXPECT_EQ(std::get<2>(exp.at(i)), std::get<2>(act.at(i)));
  }
}

TEST(TestInputLog, Test_input_rotation) {
  u32 N = 10;
  InputLog ilog(&sequencer, "./", N, 4096, 0);

  // This amount of data should saturate the log (random data is not
  // very compressable).
  std::vector<u64> stale_ids;
  for (int i = 0; i < 10000; i++) {
    double val = static_cast<double>(rand()) / RAND_MAX;
    common::Status status = ilog.append(42, i, val, &stale_ids);
    if (status == common::Status::Overflow()) {
      ilog.rotate();
    }
  }

  // Check number of files (should be N)
  std::vector<std::string> names;
  for (auto it = boost::filesystem::directory_iterator("./");
       it != boost::filesystem::directory_iterator(); it++) {
    boost::filesystem::path path = *it;
    if (!boost::starts_with(path.filename().string(), "inputlog")) {
      continue;
    }
    if (path.extension().string() != ".ils") {
      continue;
    }
    names.push_back(path.filename().string());
  }

  EXPECT_EQ(names.size(), N);

  for (auto name: names) {
    EXPECT_TRUE(volume_filename_is_ok(name));
  }
}

TEST(TestInputLog, Test_input_volume_read_next_frame) {
  std::vector<std::tuple<u64, u64, double>> exp, act;
  const char* filename = "./tmp_test_vol.ilog";
  {
    LZ4Volume volume(&sequencer, filename, 0x10000);
    for (int i = 0; i < 10000; i++) {
      double val = static_cast<double>(rand()) / RAND_MAX;
      common::Status status = volume.append(42, i, val);
      exp.push_back(std::make_tuple(42, i, val));
      if (status == common::Status::Overflow()) {
        break;
      }
    }
  }
  {
    LZ4Volume volume(filename);
    volume.open_ro();
    while (true) {
      common::Status status;
      const LZ4Volume::Frame* frame;
      std::tie(status, frame) = volume.read_next_frame();
      EXPECT_EQ(status, common::Status::Ok());
      if (frame == nullptr) {
        // Done iterating
        break;
      }
      for(u32 i = 0; i < frame->data_points.size; i++) {
        act.push_back(std::make_tuple(frame->data_points.ids[i],
                                      frame->data_points.tss[i],
                                      frame->data_points.xss[i]));
      }
    }
    volume.delete_file();
  }
  EXPECT_EQ(exp.size(), act.size());
  for (u32 i = 0; i < exp.size(); i++) {
    EXPECT_EQ(std::get<0>(exp.at(i)), std::get<0>(act.at(i)));
    EXPECT_EQ(std::get<1>(exp.at(i)), std::get<1>(act.at(i)));
    EXPECT_EQ(std::get<2>(exp.at(i)), std::get<2>(act.at(i)));
  }
}

TEST(TestInputLog, Test_input_roundtrip_with_frames) {
  std::vector<std::tuple<u64, u64, double>> exp, act;
  std::vector<u64> stale_ids;
  {
    InputLog ilog(&sequencer, "./", 100, 4096, 0);
    for (int i = 0; i < 10000; i++) {
      double val = static_cast<double>(rand()) / RAND_MAX;
      common::Status status = ilog.append(42, i, val, &stale_ids);
      exp.push_back(std::make_tuple(42, i, val));
      if (status == common::Status::Overflow()) {
        ilog.rotate();
      }
    }
  }
  EXPECT_TRUE(stale_ids.empty());
  {
    InputLog ilog("./", 0);
    while (true) {
      common::Status status;
      const LZ4Volume::Frame* frame;
      std::tie(status, frame) = ilog.read_next_frame();
      if (frame == nullptr) {
        EXPECT_EQ(status, common::Status::NoData());
        break;
      }
      EXPECT_EQ(status, common::Status::Ok());
      for(u32 i = 0; i < frame->data_points.size; i++) {
        act.push_back(std::make_tuple(frame->data_points.ids[i],
                                      frame->data_points.tss[i],
                                      frame->data_points.xss[i]));
      }
    }
    ilog.reopen();
    ilog.delete_files();
  }
  EXPECT_EQ(exp.size(), act.size());
  for (u32 i = 0; i < exp.size(); i++) {
    EXPECT_EQ(std::get<0>(exp.at(i)), std::get<0>(act.at(i)));
    EXPECT_EQ(std::get<1>(exp.at(i)), std::get<1>(act.at(i)));
    EXPECT_EQ(std::get<2>(exp.at(i)), std::get<2>(act.at(i)));
  }
}

void test_input_roundtrip_no_conflicts(int ccr) {
  std::map<u64, std::vector<std::tuple<u64, double>>> exp, act;
  std::vector<u64> stale_ids;
  std::vector<ParamId> ids;
  {
    ShardedInputLog slog(ccr, "./", 100, 4096);
    auto fill_data_in = [&](InputLog& ilog, ParamId series) {
      for (int i = 0; i < 10000; i++) {
        double val = static_cast<double>(rand()) / RAND_MAX;
        common::Status status = ilog.append(series, i, val, &stale_ids);
        exp[series].push_back(std::make_tuple(i, val));
        if (status == common::Status::Overflow()) {
          ilog.rotate();
        }
      }
    };
    for (int i = 0; i < ccr; i++) {
      ParamId id = (i + 1) * 111;
      fill_data_in(slog.get_shard(i), id);
      ids.push_back(id);
    }
  }
  {
    ShardedInputLog slog(0, "./");
    // Read by one
    while (true) {
      ParamId id;
      Timestamp ts;
      double xs;
      common::Status status;
      u32 outsize;
      std::tie(status, outsize) = slog.read_next(1, &id, &ts, &xs);
      if (outsize == 1) {
        act[id].push_back(std::make_tuple(ts, xs));
      }
      if (status == common::Status::NoData()) {
        // EOF
        break;
      } else if (status != common::Status::Ok()) {
        LOG(ERROR) << "Read failed " << status.ToString();
      }
    }
    slog.reopen();
    slog.delete_files();
  }
  for (auto id: ids) {
    const std::vector<std::tuple<u64, double>>& expected = exp[id];
    const std::vector<std::tuple<u64, double>>& actual = act[id];
    EXPECT_EQ(expected.size(), actual.size());
    for (u32 i = 0; i < exp.size(); i++) {
      if (std::get<0>(expected.at(i)) != std::get<0>(actual.at(i))) {
        EXPECT_EQ(std::get<0>(expected.at(i)), std::get<0>(actual.at(i)));
      }
      if (std::get<1>(expected.at(i)) != std::get<1>(actual.at(i))) {
        EXPECT_EQ(std::get<1>(expected.at(i)), std::get<1>(actual.at(i)));
      }
    }
  }
}

TEST(TestInputLog, Test_input_roundtrip_with_shardedlog_no_conflicts_1) {
  test_input_roundtrip_no_conflicts(1);
}

TEST(TestInputLog, Test_input_roundtrip_with_shardedlog_no_conflicts_2) {
  test_input_roundtrip_no_conflicts(2);
}

TEST(TestInputLog, Test_input_roundtrip_with_shardedlog_no_conflicts_3) {
  test_input_roundtrip_no_conflicts(4);
}

TEST(TestInputLog, Test_input_roundtrip_with_shardedlog_no_conflicts_4) {
  test_input_roundtrip_no_conflicts(8);
}

void test_input_roundtrip_with_conflicts(int ccr, int rowsize) {
  // This test simulates simultaneous concurrent write. Each "thread"
  // writes it's own series. Periodically the threads are switched
  // and as result, every log should have all series.
  std::map<u64, std::vector<std::tuple<u64, double>>> exp, act;
  std::vector<u64> stale_ids;
  std::vector<ParamId> ids;
  {
    ShardedInputLog slog(ccr, "./", 100, 4096);
    std::vector<InputLog*> ilogs;
    for (int i = 0; i < ccr; i++) {
      ilogs.push_back(&slog.get_shard(i));
      ids.push_back((i + 1)*1111);
    }
    int oldshift = 0;
    for (int i = 0; i < 10000*ccr; i++) {
      int shift = i / rowsize;
      if (shift != oldshift) {
        // Simulate disconnection
        for (auto it: ilogs) {
          it->flush(&stale_ids);
        }
      }
      oldshift = shift;
      int logix = (i + shift) % ilogs.size();
      double val = static_cast<double>(rand()) / RAND_MAX;
      ParamId id = ids.at(i % ids.size());
      common::Status status = ilogs.at(logix)->append(id, i, val, &stale_ids);
      exp[id].push_back(std::make_tuple(i, val));
      if (status == common::Status::Overflow()) {
        ilogs.at(logix)->rotate();
      }
    }
  }
  {
    ShardedInputLog slog(0, "./");
    // Read by one
    while (true) {
      ParamId id;
      Timestamp ts;
      double xs;
      common::Status status;
      u32 outsize;
      std::tie(status, outsize) = slog.read_next(1, &id, &ts, &xs);
      if (outsize == 1) {
        act[id].push_back(std::make_tuple(ts, xs));
      }
      if (status == common::Status::NoData()) {
        // EOF
        break;
      } else if (status != common::Status::Ok()) {
        LOG(ERROR) << "Read failed " << status.ToString();
      }
    }
    slog.reopen();
    slog.delete_files();
  }
  for (auto id: ids) {
    const std::vector<std::tuple<u64, double>>& expected = exp[id];
    const std::vector<std::tuple<u64, double>>& actual = act[id];
    EXPECT_EQ(expected.size(), actual.size());
    for (u32 i = 0; i < exp.size(); i++) {
      if (std::get<0>(expected.at(i)) != std::get<0>(actual.at(i))) {
        EXPECT_EQ(std::get<0>(expected.at(i)), std::get<0>(actual.at(i)));
      }
      if (std::get<1>(expected.at(i)) != std::get<1>(actual.at(i))) {
        EXPECT_EQ(std::get<1>(expected.at(i)), std::get<1>(actual.at(i)));
      }
    }
  }
}

TEST(TestInputLog, Test_input_roundtrip_with_shardedlog_with_conflicts_1) {
  test_input_roundtrip_with_conflicts(2, 1000);
}

TEST(TestInputLog, Test_input_roundtrip_with_shardedlog_with_conflicts_2) {
  test_input_roundtrip_with_conflicts(2, 100);
}

TEST(TestInputLog, Test_input_roundtrip_with_shardedlog_with_conflicts_3) {
  test_input_roundtrip_with_conflicts(4, 1000);
}

TEST(TestInputLog, Test_input_roundtrip_with_shardedlog_with_conflicts_4) {
  test_input_roundtrip_with_conflicts(4, 100);
}

void test_input_roundtrip_vartype(int N, int sname_freq, int recovery_freq, int dpoint_freq) {
  assert(sname_freq <= dpoint_freq);
  assert(recovery_freq <= dpoint_freq);
  assert(sname_freq <= recovery_freq);
  std::vector<u64> stale_ids;
  typedef std::tuple<u64, u64, double> DataPoint;
  typedef std::tuple<u64, std::string> SeriesName;
  typedef std::tuple<u64, std::vector<u64>> RescuePoint;
  typedef boost::variant<DataPoint, SeriesName, RescuePoint> InputValue;
  std::vector<InputValue> exp, act;
  {
    InputLog ilog(&sequencer, "./", 200, 4096, 0);
    for (int i = 0; i < N; i++) {
      int variant = rand() % dpoint_freq;
      common::Status status = common::Status::Ok();
      if (variant >= std::max(sname_freq, recovery_freq)) {
        double val = static_cast<double>(rand()) / RAND_MAX;
        DataPoint point = std::make_tuple(42, i, val);
        status = ilog.append(std::get<0>(point),
                             std::get<1>(point),
                             std::get<2>(point),
                             &stale_ids);
        exp.push_back(point);
      } else if (variant < sname_freq) {
        std::string text = "foo bar=" + std::to_string(rand() % 1000);
        SeriesName sname = std::make_tuple(42, text);
        status = ilog.append(42, text.data(), text.length(), &stale_ids);
        exp.push_back(sname);
      } else {
        std::vector<u64> val = { static_cast<u64>(rand()) };
        RescuePoint point = std::make_tuple(42, val);
        status = ilog.append(42, val.data(), val.size(), &stale_ids);
        exp.push_back(point);
      }
      if (status == common::Status::Overflow()) {
        ilog.rotate();
      }
    }
  }
  struct Redirect : boost::static_visitor<> {
    std::vector<InputValue>* output;
    u64 id;
    void operator () (const InputLogDataPoint& val) {
      DataPoint tup = std::make_tuple(id, val.timestamp, val.value);
      output->push_back(tup);
    }
    void operator () (const InputLogSeriesName& val) {
      SeriesName tup = std::make_tuple(id, val.value);
      output->push_back(tup);
    }
    void operator () (const InputLogRecoveryInfo& val) {
      RescuePoint tup = std::make_tuple(id, val.data);
      output->push_back(tup);
    }
  };
  EXPECT_TRUE(stale_ids.empty());
  {
    InputLog ilog("./", 0);
    while (true) {
      InputLogRow buffer[1024];
      common::Status status;
      u32 outsz;
      std::tie(status, outsz) = ilog.read_next(1024, buffer);
      EXPECT_EQ(status, common::Status::Ok());
      for (u32 i = 0; i < outsz; i++) {
        auto id = buffer[i].id;
        Redirect redir;
        redir.output = &act;
        redir.id = id;
        buffer[i].payload.apply_visitor(redir);
      }
      if (outsz == 0) {
        break;
      }
    }
    ilog.reopen();
    ilog.delete_files();
  }
  struct Visitor : boost::static_visitor<> {
    InputValue expected;
    u32 ix;

    void operator () (const DataPoint& dp) {
      if (dp != boost::get<DataPoint>(expected)) {
        LOG(FATAL) << "Unexpected data point at " << ix;
      }
    }
    void operator () (const SeriesName& sn) {
      if (sn != boost::get<SeriesName>(expected)) {
        LOG(FATAL) << "Unexpected series name at " << ix;
      }
    }
    void operator () (const RescuePoint& re) {
      if (re != boost::get<RescuePoint>(expected)) {
        LOG(FATAL) << "Unexpected rescue point at " << ix;
      }
    }
  };
  EXPECT_EQ(exp.size(), act.size());
  for (u32 i = 0; i < exp.size(); i++) {
    Visitor visitor;
    visitor.expected = exp.at(i);
    visitor.ix = i;
    act.at(i).apply_visitor(visitor);
  }
}

TEST(TestInputLog, Test_input_roundtrip_vartype_0) {
  // Only sname values
  test_input_roundtrip_vartype(10000, 100, 100, 100);
}

TEST(TestInputLog, Test_input_roundtrip_vartype_1) {
  test_input_roundtrip_vartype(10000, 0, 0, 100);
}

TEST(TestInputLog, Test_input_roundtrip_vartype_2) {
  test_input_roundtrip_vartype(10000, 5, 5, 100);
}

TEST(TestInputLog, Test_input_roundtrip_vartype_3) {
  test_input_roundtrip_vartype(10000, 5, 10, 100);
}

TEST(TestInputLog, Test_input_roundtrip_vartype_4) {
  test_input_roundtrip_vartype(10000, 10, 30, 100);
}

TEST(TestInputLog, Test_input_roundtrip_vartype_5) {
  test_input_roundtrip_vartype(10000, 0, 100, 100);
}

void test_input_roundtrip_with_conflicts_and_vartype(int ccr, int rowsize, int sname_freq, int recovery_freq, int dpoint_freq) {
  // This test simulates simultaneous concurrent write. Each "thread"
  // writes it's own series and metadata. Periodically the threads are switched
  // and as result, every log should have all series.
  assert(sname_freq <= dpoint_freq);
  assert(recovery_freq <= dpoint_freq);
  assert(sname_freq <= recovery_freq);
  typedef std::tuple<u64, double> DataPoint;
  typedef std::string             SeriesName;
  typedef std::vector<u64>        RescuePoint;
  typedef boost::variant<DataPoint, SeriesName, RescuePoint> InputValue;
  std::map<u64, std::vector<InputValue>> exp, act;
  std::vector<u64> stale_ids;
  std::vector<ParamId> ids;
  {
    ShardedInputLog slog(ccr, "./", 200, 4096);
    std::vector<InputLog*> ilogs;
    for (int i = 0; i < ccr; i++) {
      ilogs.push_back(&slog.get_shard(i));
      ids.push_back((i + 1)*1111);
    }
    int oldshift = 0;
    for (int i = 0; i < 10000*ccr; i++) {
      int shift = i / rowsize;
      if (shift != oldshift) {
        // Simulate disconnection
        for (auto it: ilogs) {
          it->flush(&stale_ids);
        }
      }
      oldshift = shift;
      int logix = (i + shift) % ilogs.size();
      ParamId id = ids.at(i % ids.size());
      int variant = rand() % dpoint_freq;
      common::Status status = common::Status::Ok();
      if (variant >= std::max(sname_freq, recovery_freq)) {
        double val = static_cast<double>(rand()) / RAND_MAX;
        DataPoint point = std::make_tuple(i, val);
        status = ilogs.at(logix)->append(id, std::get<0>(point), std::get<1>(point), &stale_ids);
        exp[id].push_back(point);
      } else if (variant < sname_freq) {
        SeriesName sname = "foo bar=" + std::to_string(rand() % 1000);
        status = ilogs.at(logix)->append(id, sname.data(), sname.length(), &stale_ids);
        exp[id].push_back(sname);
      }  else {
        RescuePoint point = { static_cast<u64>(rand()) };
        status = ilogs.at(logix)->append(id, point.data(), point.size(), &stale_ids);
        exp[id].push_back(point);
      }
      if (status == common::Status::Overflow()) {
        ilogs.at(logix)->rotate();
      }
    }
  }
  {
    struct Redirect : boost::static_visitor<> {
      std::map<u64, std::vector<InputValue>>* output;
      u64 id;

      void operator () (const InputLogDataPoint& val) {
        DataPoint tup = std::make_tuple(val.timestamp, val.value);
        (*output)[id].push_back(tup);
      }
      void operator () (const InputLogSeriesName& val) {
        (*output)[id].push_back(val.value);
      }
      void operator () (const InputLogRecoveryInfo& val) {
        (*output)[id].push_back(val.data);
      }
    };
    ShardedInputLog slog(0, "./");
    // Read by one
    while (true) {
      InputLogRow row;
      common::Status status;
      u32 outsize;
      std::tie(status, outsize) = slog.read_next(1, &row);
      if (outsize == 1) {
        Redirect visitor;
        visitor.id = row.id;
        visitor.output = &act;
        row.payload.apply_visitor(visitor);
      }
      if (status == common::Status::NoData()) {
        // EOF
        break;
      } else if (status != common::Status::Ok()) {
        LOG(ERROR) << "Read failed " << status.ToString();
      }
    }
    slog.reopen();
    slog.delete_files();
  }
  struct Visitor : boost::static_visitor<> {
    InputValue expected;
    u32 ix;

    void operator () (const DataPoint& dp) {
      DataPoint exp = boost::get<DataPoint>(expected);
      if (dp != exp) {
        LOG(FATAL) << "Unexpected point at " << ix;
      }
    }
    void operator () (const SeriesName& sn) {
      SeriesName exp = boost::get<SeriesName>(expected);
      if (sn != exp) {
        LOG(FATAL) << "Unexpected series name at " << ix << ", expected: " << exp << ", actual: " << sn;
      }
    }
    void operator () (const RescuePoint& re) {
      if (re != boost::get<RescuePoint>(expected)) {
        LOG(FATAL) << "Unexpected rescue point at " << ix;
      }
    }
  };
  for (auto id: ids) {
    const std::vector<InputValue>& expected = exp[id];
    const std::vector<InputValue>& actual   = act[id];
    EXPECT_EQ(expected.size(), actual.size());
    for (u32 i = 0; i < expected.size(); i++) {
      Visitor visitor;
      visitor.expected = expected.at(i);
      visitor.ix = i;
      actual.at(i).apply_visitor(visitor);
    }
  }
}

TEST(TestInputLog, Test_input_roundtrip_with_conflicts_and_vartype_0) {
  test_input_roundtrip_with_conflicts_and_vartype(2, 100, 100, 100, 100);
}

TEST(TestInputLog, Test_input_roundtrip_with_conflicts_and_vartype_1) {
  test_input_roundtrip_with_conflicts_and_vartype(8, 1000, 100, 100, 100);
}

TEST(TestInputLog, Test_input_roundtrip_with_conflicts_and_vartype_2) {
  test_input_roundtrip_with_conflicts_and_vartype(2, 100, 0, 0, 100);
}

TEST(TestInputLog, Test_input_roundtrip_with_conflicts_and_vartype_3) {
  test_input_roundtrip_with_conflicts_and_vartype(8, 1000, 0, 0, 100);
}

TEST(TestInputLog, Test_input_roundtrip_with_conflicts_and_vartype_4) {
  test_input_roundtrip_with_conflicts_and_vartype(2, 100, 5, 5, 100);
}

TEST(TestInputLog, Test_input_roundtrip_with_conflicts_and_vartype_5) {
  test_input_roundtrip_with_conflicts_and_vartype(8, 1000, 5, 5, 100);
}

TEST(TestInputLog, Test_input_roundtrip_with_conflicts_and_vartype_6) {
  test_input_roundtrip_with_conflicts_and_vartype(2, 100, 5, 10, 100);
}

TEST(TestInputLog, est_input_roundtrip_with_conflicts_and_vartype_7) {
  test_input_roundtrip_with_conflicts_and_vartype(8, 1000, 5, 10, 100);
}

TEST(TestInputLog, Test_input_roundtrip_with_conflicts_and_vartype_8) {
  test_input_roundtrip_with_conflicts_and_vartype(2, 100, 10, 30, 100);
}

TEST(TestInputLog, Test_input_roundtrip_with_conflicts_and_vartype_9) {
  test_input_roundtrip_with_conflicts_and_vartype(8, 1000, 10, 30, 100);
}

TEST(TestInputLog, Test_input_roundtrip_with_conflicts_and_vartype_10) {
  test_input_roundtrip_with_conflicts_and_vartype(2, 100, 0, 100, 100);
}

TEST(TestInputLog, Test_input_roundtrip_with_conflicts_and_vartype_11) {
  test_input_roundtrip_with_conflicts_and_vartype(8, 1000, 0, 100, 100);
}

TEST(TestInputLog, Test_input_roundtrip_with_conflicts_and_vartype_12) {
  test_input_roundtrip_with_conflicts_and_vartype(80, 1000, 0, 100, 100);
}

}  // namespace storage
}  // namespace stdb

/*!
 * \file controller_test.cc
 */
#include "stdb/core/controller.h"

#include "gtest/gtest.h"

#include "stdb/core/cursor.h"
#include "stdb/query/queryparser.h"
#include "stdb/common/datetime.h"

namespace stdb {

static std::string make_select_meta_query() {
  std::stringstream ss;
  ss << "{ \"select\": \"meta:names:cpu\",";
  ss << "   \"where\":" << "[ { \"ip\" : \"127.0.0.1\" } ]";
  ss << "}";
  return ss.str();
}

static std::string make_scan_query(Timestamp begin, Timestamp end, qp::OrderBy order) {
  std::stringstream str;
  str << "{ \"select\": \"cpu\", \"range\": { \"from\": " << "\"" << DateTimeUtil::to_iso_string(begin) << "\"";
  str << ", \"to\": " << "\"" << DateTimeUtil::to_iso_string(end) << "\"" << "},";
  str << " \"group-by-tag\": " << "[ \"color\" ],"; 
  str << "  \"order-by\": " << (order == qp::OrderBy::SERIES ? "\"series\"," : "\"time\",");
  str << "  \"where\": " << "[ { \"loc\" : \"beijing\", \"color\" : \"red\" }, { \"loc\" : \"shanghai\", \"color\" : \"red\" } ]";
  str << "}";
  return str.str();
}

TEST(TestController, Test1) {
  initialize();
  auto controller = Controller::Get();
  controller->init(".stdbrc");
}

TEST(TestController, Test2) {
  auto controller = Controller::Get();
  controller->new_standalone_database(
      false,
      "test1",
      "/tmp/test_controller/metapath/",
      "/tmp/test_controller/volumes/",
      2,
      1024 * 1024,
      true);

  auto database = controller->open_standalone_database("test1");
  {
    auto session = database->create_session();
    {
      std::string series = "cpu loc=shanghai color=red";
      u64 id;
      auto status = session->init_series_id(series.c_str(), series.c_str() + series.size(), &id);
      LOG(INFO) << "sample.paramid=" << id;

      for (auto i = 0; i < 18; ++i) {
        Sample sample;
        sample.paramid = id;
        sample.payload.float64 = 120.0 + i * 0.001;
        sample.payload.type = PAYLOAD_FLOAT; 
        sample.timestamp = (20120010UL + i) * 1000;

        session->write(sample);
      }
    }
    {
      std::string series = "cpu loc=beijing color=red";
      u64 id;
      auto status = session->init_series_id(series.c_str(), series.c_str() + series.size(), &id);
      LOG(INFO) << "sample.paramid=" << id;

      for (auto i = 0; i < 18; ++i) {
        Sample sample;
        sample.paramid = id;
        sample.payload.float64 = 120.0 + i * 0.001;
        sample.payload.type = PAYLOAD_FLOAT; 
        sample.timestamp = (20120010UL + i) * 1000;

        session->write(sample);
      }
    }

    ConcurrentCursor concurrent_cursor;
    std::string query_json = make_scan_query(20120010000ul + 10 * 1000, 20120010000ul + 12 * 1000, qp::OrderBy::TIME);
    // std::string query_json = make_select_meta_query();
    session->query(&concurrent_cursor, query_json.c_str());

    char buf[1024];
    auto rdsize = concurrent_cursor.read(buf, 1024);
    LOG(INFO) << "rdsize=" << rdsize;
    for (auto i = 0; i < rdsize; i += sizeof(Sample)) {
      Sample *sample = (Sample*)(buf + i);
      LOG(INFO) << "paramid=" << sample->paramid
          << " timestamp=" << sample->timestamp
          << " payload=" << sample->payload.size
          << " payload.float64=" << sample->payload.float64
          << " sizeof(Sample)=" << sizeof(Sample);
    }
  }
  controller->close();
}

}  // namespace stdb

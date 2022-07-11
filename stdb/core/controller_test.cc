/*!
 * \file controller_test.cc
 */
#include "stdb/core/controller.h"

#include "gtest/gtest.h"

#include "stdb/core/cursor.h"
#include "stdb/query/queryparser.h"
#include "stdb/common/datetime.h"

namespace stdb {

void initialize_controller() {
  static bool initialized = false;
  if (!initialized) {
    initialize();
    auto controller = Controller::Get();
    controller->init(".stdbrc");

    initialized = true;
  }
}

void create_database(const char* name, bool is_moving) {
  initialize_controller();

  auto controller = Controller::Get();
  std::string metapath = std::string("/tmp/") + name + "/metapath/";
  std::string volumepath = std::string("/tmp/") + name + "/volumes/";
  controller->new_standalone_database(
      is_moving,
      name,
      metapath.c_str(),
      volumepath.c_str(),
      2,
      1024 * 1024,
      true);
}

std::shared_ptr<DatabaseSession> open_database_session(const char* name) {
  initialize_controller();

  auto controller = Controller::Get();
  auto database = controller->open_standalone_database(name);
  auto session = database->create_session();
  return session;
}

void close_database(const char* name) {
  auto controller = Controller::Get();
  controller->close_database(name);
}

void ingest_data(std::shared_ptr<DatabaseSession>& session, Timestamp start, u64 data_point_size) {
  const char* tag1[] = {
    "loc=shanghai",
    "loc=beijing",
    "loc=changsha",
  };
  const char* tag2[] = {
    "color=black",
    "color=white",
  };

  for (auto i = 0; i < sizeof(tag1) / sizeof(tag1[0]); ++i) {
    for (auto j = 0; j < sizeof(tag2) / sizeof(tag2[0]); ++j) {
      std::string series = std::string("weight ") + tag1[i] + " " + tag2[j];
      Location location = { 120.00, 30.00 };
      u64 id;

      auto status = session->init_series_id(series.c_str(), series.c_str() + series.size(), location, &id);
      EXPECT_TRUE(status.IsOk());

      for (auto z = 0; z < data_point_size; ++z) {
        Sample sample;
        sample.paramid = id;
        sample.payload.float64 = 120.0 + z * 0.001;
        sample.payload.type = PAYLOAD_FLOAT; 
        sample.timestamp = start + z * 1000;

        session->write(sample);
      }
    }
  }
}

static std::string make_scan_query(Timestamp begin, Timestamp end, qp::OrderBy order) {
  std::stringstream str;
  str << "{ \"select\": \"weight\", \"range\": { \"from\": " << "\"" << DateTimeUtil::to_iso_string(begin) << "\"";
  str << ", \"to\": " << "\"" << DateTimeUtil::to_iso_string(end) << "\"" << "},";
  // str << " \"group-by-tag\": " << "[ \"color\" ],"; 
  str << "  \"order-by\": " << (order == qp::OrderBy::SERIES ? "\"series\"," : "\"time\",");
  str << "  \"where\": " << "[ { \"loc\" : \"beijing\", \"color\" : \"black\" }, { \"loc\" : \"changsha\", \"color\" : \"black\" } ]";
  str << "}";
  return str.str();
}

TEST(TestController, Test1) {
  create_database("test1", false);
  
  {
    auto session = open_database_session("test1");
    ingest_data(session, 1000, 40);

    ConcurrentCursor concurrent_cursor;
    std::string query_json = make_scan_query(1000UL + 10 * 1000, 1000ul + 12 * 1000, qp::OrderBy::TIME);
    session->query(&concurrent_cursor, query_json.c_str());

    char buf[1024];
    char str_buffer[32];

    auto rdsize = concurrent_cursor.read(buf, 1024);
    EXPECT_EQ(4, rdsize / sizeof(Sample));
    LOG(INFO) << "rdsize=" << rdsize;
    for (auto i = 0; i < rdsize; i += sizeof(Sample)) {
      Sample *sample = (Sample*)(buf + i);
      auto str_size = session->get_series_name(sample->paramid, str_buffer, 32);
      if (i == 0) {
        EXPECT_STREQ(std::string(str_buffer, str_size).c_str(), "weight color=black loc=beijing");
      }

      LOG(INFO) << std::string(str_buffer, str_size);

      LOG(INFO) << "paramid=" << sample->paramid
          << " timestamp=" << sample->timestamp
          << " payload=" << sample->payload.size
          << " payload.float64=" << sample->payload.float64
          << " sizeof(Sample)=" << sizeof(Sample);
    }
  }
}

}  // namespace stdb

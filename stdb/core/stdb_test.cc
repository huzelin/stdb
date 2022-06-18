/*!
 * \file stdb.h
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
#include "stdb/core/stdb.h"

#include "gtest/gtest.h"

#include "stdb/common/logging.h"

namespace stdb {
namespace core {

TEST(TestSTDB, initialize) {
  stdb_initialize();  
}

TEST(TestSTDB, create_database) {
  stdb_create_database_ex("test", "./metas", "./volumes", 1, 4096 * 1024, true);
}

TEST(TestSTDB, open_database) {
  FineTuneParams fine_tune_params;
  auto database = stdb_open_database("./metas/test.stdb", fine_tune_params);
  stdb_close_database(database);
}

TEST(TestSTDB, session) {
  FineTuneParams fine_tune_params;
  auto database = stdb_open_database("./metas/test.stdb", fine_tune_params);

  auto session = stdb_create_session(database);
  stdb_destroy_session(session);
}

TEST(TestSTDB, parseTimestamp) {
  Sample sample;
  const char* timestamp_str = "20060102T150405.999999999";  // ISO timestamp
  stdb_parse_timestamp(timestamp_str, &sample);
  Timestamp expected = 1136214245999999999ul;
  EXPECT_EQ(expected, sample.timestamp);
}

void insert_series_data(const char* series) {
  FineTuneParams fine_tune_params;
  auto database = stdb_open_database("./metas/test.stdb", fine_tune_params);
  auto session = stdb_create_session(database);

  Sample sample;
  for (auto i = 0; i < 1000000; ++i) {
    stdb_series_to_param_id(session, series, series + strlen(series), &sample);
    sample.timestamp = 20000000 + i;
    sample.payload.float64 = i;
    sample.payload.size = 0;
    sample.payload.type = PAYLOAD_FLOAT;
    auto ret = stdb_write_sample(session, &sample);
    // EXPECT_EQ(0, ret);
    if (i % 10000 == 0) {
      LOG(INFO) << i;
    }
  }
  stdb_destroy_session(session);
  stdb_close_database(database);
}

void init_mock_data() {
  static bool inited = false;
  if (inited) return;

  {
    const char* series = "test1 name=a val=1";
    insert_series_data(series);
  }
  {
    const char* series = "test2 name=a val=1";
    insert_series_data(series);
  }
  {
    const char* series = "test3 name=b val=3";
    insert_series_data(series);
  }
  inited = true;
}

TEST(TestSTDB, name_to_param_id_list) {
  init_mock_data();

  FineTuneParams fine_tune_params;
  auto database = stdb_open_database("./metas/test.stdb", fine_tune_params);

  auto session = stdb_create_session(database);

  char buf[8192];
  stdb_json_stats(database, buf, 8192);
  LOG(INFO) << buf;

  {
    const char* series = "test1 name=a val=1";
    Sample sample;
    stdb_series_to_param_id(session, series, series + strlen(series), &sample);
    EXPECT_EQ(1024, sample.paramid);
  }
  {
    const char* series = "test2 name=a val=1";
    Sample sample;
    stdb_series_to_param_id(session, series, series + strlen(series), &sample);
    EXPECT_EQ(1025, sample.paramid);
  }
  {
    const char* series = "test3 name=b val=3";
    Sample sample;
    stdb_series_to_param_id(session, series, series + strlen(series), &sample);
    EXPECT_EQ(1026, sample.paramid);
  }

  stdb_destroy_session(session);
  stdb_close_database(database);
}

TEST(TestSTDB, query) {
  
}

TEST(TestSTDB, suggest) {

}

TEST(TestSTDB, search) {

}

}  // namespace core
}  // namespace stdb

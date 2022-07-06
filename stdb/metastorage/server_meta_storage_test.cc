/*!
 * \file server_meta_storage_test.cc
 */
#include "stdb/metastorage/server_meta_storage.h"

#include "stdb/common/apr_utils.h"

#include "gtest/gtest.h"

namespace stdb {

TEST(TestServerMetaStorage, Test1) {
  initialize();

  ServerMetaStorage storage("/tmp/test_server_meta_storage.sqlite");
  storage.init_config("db1", "2022.07.01", "FixedMemory");

  auto val = storage.get_database_name();
  EXPECT_STREQ("db1", val.c_str());

  val = storage.get_creation_datetime();
  EXPECT_STREQ("2022.07.01", val.c_str());

  val = storage.get_bstore_type();
  EXPECT_STREQ("FixedMemory", val.c_str());

  storage.set_config_param("my_key", "my_value");
  EXPECT_TRUE(storage.get_config_param("my_key", &val));
  EXPECT_STREQ("my_value", val.c_str());

  EXPECT_FALSE(storage.set_config_param("my_key", "my_value2"));
  EXPECT_TRUE(storage.get_config_param("my_key", &val));
  EXPECT_STREQ("my_value", val.c_str());
}

TEST(TestServerMetaStorage, Test2) {
  ServerMetaStorage storage("/tmp/test_server_meta_storage.sqlite");

  std::vector<ServerMetaStorage::SeriesT> items;
  std::vector<Location> locations;

  std::string series = "size city=beijing color=red";
  storage.sync_with_metadata_storage(
      [&](std::vector<ServerMetaStorage::SeriesT>* items, std::vector<Location>* locations) {
        for (auto i = 0; i < 1024; ++i) {
          items->push_back(std::make_tuple(series.c_str(), series.size(), i + 1));
          Location location = { 120.00 + i * 0.001, 30.00 + i * 0.001 };
          locations->push_back(location);
        }
      });

  auto prev_largest_id = storage.get_prev_largest_id();
  EXPECT_EQ(1024, prev_largest_id.value());
}

TEST(TestServerMetaStorage, Test3) {
  ServerMetaStorage storage("/tmp/test_server_meta_storage.sqlite");
  PlainSeriesMatcher plain_series_matcher;
  auto status = storage.load_matcher_data(plain_series_matcher);
  EXPECT_TRUE(status.IsOk());
}

}  // namespace stdb

/*!
 * \file worker_meta_storage_test.cc
 */
#include "stdb/core/worker_meta_storage.h"

#include "gtest/gtest.h"

#include "stdb/common/apr_utils.h"
#include "stdb/common/file_utils.h"

namespace stdb {

TEST(TestWorkerMetaStorage, Test1) {
  initialize();

  common::RemoveFile("/tmp/test_worker_meta_storage.sqlite");
  std::shared_ptr<Synchronization> synchronization(new Synchronization());
  WorkerMetaStorage storage("/tmp/test_worker_meta_storage.sqlite", "db1", synchronization);

  WorkerMetaStorage::VolumeDesc volume_desc;
  for (auto i = 0; i < 1024; ++i) {
    volume_desc.id = i;
    volume_desc.path = std::string("/tmp/") + std::to_string(i);
    volume_desc.version = STDB_VERSION;
    volume_desc.nblocks = i;
    volume_desc.capacity = 1024;
    volume_desc.generation = 1;
    storage.add_volume(volume_desc);
  }

  auto volumes = storage.get_volumes();
  EXPECT_EQ(1024, volumes.size());

  for (auto& volume : volumes) {
    EXPECT_EQ(volume.id, volume.nblocks);
  }

  EXPECT_STREQ("db1", storage.get_dbname().c_str());
}

TEST(TestWorkerMetaStorage, Test2) {
  std::shared_ptr<Synchronization> synchronization(new Synchronization());
  WorkerMetaStorage storage("/tmp/test_worker_meta_storage.sqlite", "db1", synchronization);

  WorkerMetaStorage::VolumeDesc volume_desc;

  volume_desc.id = 1;
  volume_desc.path = std::string("/tmp/") + std::to_string(1);
  volume_desc.version = STDB_VERSION;
  volume_desc.nblocks = 512;
  volume_desc.capacity = 1024;
  volume_desc.generation = 1;

  storage.update_volume(volume_desc);
  EXPECT_EQ(1, storage.pending_size());

  storage.sync_with_metadata_storage();
  auto volumes = storage.get_volumes();
  for (auto& volume : volumes) {
    if (volume.id == 1) {
      EXPECT_EQ(512, volume.nblocks);
    }
  }
}

TEST(TestWorkerMetaStorage, Test3) {
  std::shared_ptr<Synchronization> synchronization(new Synchronization());
  WorkerMetaStorage storage("/tmp/test_worker_meta_storage.sqlite", "db1", synchronization);

  std::vector<u64> recue_points;
  recue_points.emplace_back(23);
  storage.add_rescue_point(10086, recue_points);

  storage.sync_with_metadata_storage();

  std::unordered_map<u64, std::vector<u64>> mapping;
  storage.load_rescue_points(mapping);
  EXPECT_EQ(1, mapping.size());
  auto iter = mapping.begin();
  EXPECT_EQ(10086, iter->first);
  EXPECT_EQ(1, iter->second.size());
  EXPECT_EQ(23, iter->second[0]);
}

}  // namespace stdb

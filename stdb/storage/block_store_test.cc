/*!
 * \file block_store_test.cc
 */
#include <iostream>

#include <boost/filesystem.hpp>

#include <apr.h>

#include "stdb/storage/block_store.h"
#include "stdb/storage/volume.h"

#include "gtest/gtest.h"

namespace stdb {
namespace storage {

struct VolumeRegistryMock : VolumeRegistry {
  std::vector<VolumeDesc> volumes;
  std::string dbname;

  std::vector<VolumeDesc> get_volumes() {
    return volumes;
  }

  void add_volume(const VolumeDesc &vol) {
    volumes.push_back(vol);
  }

  void update_volume(const VolumeDesc &vol) {
    auto ix = vol.id;
    auto volume = volumes.at(ix);
    volume.capacity = vol.capacity;
    volume.nblocks = vol.nblocks;
    volume.generation = vol.generation;
  }

  std::string get_dbname() {
    return dbname;
  }
  
  size_t pending_size() {
    return 0;
  }
};

struct Initializer {
  Initializer() {
    apr_initialize();
  }
};

Initializer initializer;

static const std::vector<u32> CAPACITIES = { 8, 8 };  // two 64KB volumes
static const std::vector<std::string> VOLPATH = { "volume0", "volume1" };
static const std::vector<std::string> EXP_VOLPATH = { "test_0.vol" };

static void create_blockstore() {
  Volume::create_new(VOLPATH[0].c_str(), CAPACITIES[0]);
  Volume::create_new(VOLPATH[1].c_str(), CAPACITIES[1]);
}

static void create_expandable_storage() {
  Volume::create_new(EXP_VOLPATH[0].c_str(), CAPACITIES[0]);
}

static std::shared_ptr<FixedSizeFileStorage> open_blockstore() {
  std::shared_ptr<VolumeRegistryMock> vrmock(new VolumeRegistryMock());
  vrmock->volumes = {
    { 0, VOLPATH[0], 0, 0, CAPACITIES[0], 0 },
    { 1, VOLPATH[1], 0, 0, CAPACITIES[1], 0 },
  };
  vrmock->dbname = "test";
  auto bstore = FixedSizeFileStorage::open(vrmock);
  return bstore;
}

static std::shared_ptr<ExpandableFileStorage> open_expandable_storage(
    std::shared_ptr<VolumeRegistryMock> *mock = 0) {
  std::shared_ptr<VolumeRegistryMock> vrmock(new VolumeRegistryMock());
  vrmock->volumes = {
    { 0, EXP_VOLPATH[0], 0, 0, CAPACITIES[0], 0 },
  };
  vrmock->dbname = "test";
  auto bstore = ExpandableFileStorage::open(vrmock);
  if (mock) {
    *mock = vrmock;
  }
  return bstore;
}


static void delete_blockstore() {
  apr_pool_t* pool;
  apr_pool_create(&pool, nullptr);
  apr_file_remove(VOLPATH[0].c_str(), pool);
  apr_file_remove(VOLPATH[1].c_str(), pool);
  apr_pool_destroy(pool);
}

static void delete_expandable_storage() {
  apr_pool_t* pool;
  apr_pool_create(&pool, nullptr);
  apr_file_remove(EXP_VOLPATH[0].c_str(), pool);
  apr_pool_destroy(pool);
}

TEST(TestBlockStore, Test_blockstore_0) {
  delete_blockstore();
  create_blockstore();

  auto bstore = open_blockstore();
  std::shared_ptr<IOVecBlock> block;
  common::Status status;
  // Should be unreadable
  std::tie(status, block) = bstore->read_iovec_block(0);
  EXPECT_NE(status, common::Status::Ok());

  // Append first block
  auto buffer = std::make_shared<IOVecBlock>();
  buffer->add();
  buffer->get_data(0)[0] = 1;
  LogicAddr addr;
  std::tie(status, addr) = bstore->append_block(*buffer);

  EXPECT_EQ(common::Status::Ok(), status);
  EXPECT_EQ(0, addr);

  // Should be readable now
  std::tie(status, block) = bstore->read_iovec_block(0);
  EXPECT_EQ(common::Status::Ok(), status);

  const u8* block_data = block->get_cdata(0);
  size_t block_size = block->get_size(0);

  EXPECT_EQ(block_size, 4096);
  EXPECT_EQ(block_data[0], 1);

  delete_blockstore();
}

TEST(TestBlockStore, Test_blockstore_1) {
  delete_blockstore();
  create_blockstore();
  auto bstore = open_blockstore();


  // Fill data in
  LogicAddr addr;
  common::Status status;

  for (int i = 0; i < 17; i++) {
    auto buffer = std::make_shared<IOVecBlock>();
    buffer->add();
    buffer->get_data(0)[0] = static_cast<u8>(i);
    std::tie(status, addr) = bstore->append_block(*buffer);
    EXPECT_EQ(status, common::Status::Ok());
  }
  EXPECT_EQ(addr, (2ull << 32));

  std::shared_ptr<IOVecBlock> block;

  // Should be unreadable now
  std::tie(status, block) = bstore->read_iovec_block(0);
  EXPECT_EQ(common::Status::Unavailable(), status);

  // Reada this block
  std::tie(status, block) = bstore->read_iovec_block(2ull << 32);
  EXPECT_EQ(common::Status::Ok(), status);

  const u8* block_data = block->get_cdata(0);
  size_t block_size = block->get_size(0);

  EXPECT_EQ(block_size, 4096);
  EXPECT_EQ(block_data[0], 16);

  delete_blockstore();
}

TEST(TestBlockStore, Test_blockstore_3) {
  delete_expandable_storage();
  create_expandable_storage();
  auto bstore = open_expandable_storage();
  std::shared_ptr<IOVecBlock> block;
  common::Status status;

  // Should be clean
  std::tie(status, block) = bstore->read_iovec_block(0);
  EXPECT_NE(status, common::Status::Ok());

  // Append first block
  auto buffer = std::make_shared<IOVecBlock>();
  buffer->add();
  buffer->get_data(0)[0] = 1;
  LogicAddr addr;
  std::tie(status, addr) = bstore->append_block(*buffer);
  EXPECT_EQ(status, common::Status::Ok());
  EXPECT_EQ(addr, 0);

  // Should be readable now
  std::tie(status, block) = bstore->read_iovec_block(0);
  EXPECT_EQ(status, common::Status::Ok());

  const u8* block_data = block->get_cdata(0);
  size_t block_size = block->get_size(0);

  EXPECT_EQ(block_size, 4096);
  EXPECT_EQ(block_data[0], 1);

  delete_expandable_storage();
}

TEST(TestBlockStore, Test_blockstore_4) {
  delete_expandable_storage();
  const char* expected_path = "test_1.vol";
  boost::filesystem::remove(expected_path);
  create_expandable_storage();
  std::shared_ptr<VolumeRegistryMock> mock;
  auto bstore = open_expandable_storage(&mock);
  std::shared_ptr<IOVecBlock> block;
  common::Status status;
  bool exist = boost::filesystem::exists(expected_path);
  EXPECT_TRUE(!exist);

  for (u32 i = 0; i < CAPACITIES.at(0); i++) {
    auto buffer = std::make_shared<IOVecBlock>();
    buffer->add();
    buffer->get_data(0)[0] = 1;
    LogicAddr addr;
    std::tie(status, addr) = bstore->append_block(*buffer);
    EXPECT_EQ(status, common::Status::Ok());
    EXPECT_EQ(addr, i);
    exist = boost::filesystem::exists(expected_path);
    EXPECT_TRUE(!exist);
  }
  auto buffer = std::make_shared<IOVecBlock>();
  buffer->add();
  buffer->get_data(0)[0] = 1;
  LogicAddr addr;
  std::tie(status, addr) = bstore->append_block(*buffer);
  EXPECT_EQ(status, common::Status::Ok());

  std::string new_vol_path = mock->volumes.at(1).path;
  u32 new_vol_id = mock->volumes.at(1).id;
  exist = boost::filesystem::exists(expected_path);
  EXPECT_TRUE(exist);
  EXPECT_EQ(new_vol_id, 1);
  EXPECT_STREQ(new_vol_path.c_str(), std::string(expected_path).c_str());

  // Should be readable now
  std::tie(status, block) = bstore->read_iovec_block(addr);
  EXPECT_EQ(status, common::Status::Ok());

  const u8* block_data = block->get_cdata(0);
  size_t    block_size = block->get_size(0);

  EXPECT_EQ(block_size, 4096);
  EXPECT_EQ(block_data[0], 1);

  boost::filesystem::remove(expected_path);
  delete_expandable_storage();
}

}  // namespace storage
}  // namespace stdb

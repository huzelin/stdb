/**
 * \file worker_meta_storage.h
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
 */
#ifndef STDB_METASTORAGE_WORKER_META_STORAGE_H_
#define STDB_METASTORAGE_WORKER_META_STORAGE_H_

#include <cstddef>
#include <memory>
#include <vector>
#include <mutex>
#include <condition_variable>
#include <boost/optional.hpp>

#include "stdb/index/seriesparser.h"
#include "stdb/metastorage/meta_storage.h"
#include "stdb/metastorage/synchronization.h"
#include "stdb/storage/volume_registry.h"

namespace stdb {

/** Sqlite3 backed storage for worker-side metadata.
 * Metadata includes:
 * - Volumes list
 * - Configuration data
 */
struct WorkerMetaStorage : public storage::VolumeRegistry, public MetaStorage {
 public:
  /** Create new or open existing db.
   * @throw std::runtime_error in a case of error
   */
  WorkerMetaStorage(const char* db, std::shared_ptr<Synchronization>& synchronization);

  /** Initialize volumes table
   * @throw std::runtime_error in a case of error
   */
  void init_volumes(const std::vector<VolumeDesc>& volumes);

  /** Read list of volumes and their sequence numbers.
   * @throw std::runtime_error in a case of error
   */
  virtual std::vector<VolumeDesc> get_volumes() override;

  /**
   * @brief Add NEW volume synchroniously
   * @param vol is a volume description
   */
  virtual void add_volume(const VolumeDesc& vol) override;

  /**
   * @brief Add/update volume metadata asynchronously
   * @param vol is a volume description
   */
  virtual void update_volume(const VolumeDesc& vol) override;

  virtual size_t pending_size() override { return pending_volumes_.size(); }

  virtual std::string get_dbname() override;

  common::Status load_rescue_points(std::unordered_map<u64, std::vector<u64>>& mapping);

  // Synchronization
  void sync_with_metadata_storage();
  
  // add rescue point
  void add_rescue_point(ParamId id, const std::vector<u64>& val);

 private:
  /** Insert or update rescue provided points (generate sql query and execute it).
  */
  void upsert_rescue_points(std::unordered_map<ParamId, std::vector<u64> > &&input);

  /**
   * @brief Update volume descriptors
   * This function performs partial update (nblocks, capacity, generation) of the stdb_volumes
   * table.
   * New volume should be added using the `add_volume` function.
   */
  void upsert_volume_records(std::unordered_map<u32, VolumeDesc>&& input);

  /** Create tables if database is empty
   * @throw std::runtime_error in a case of error
   */
  void create_worker_tables();

  // Synchronization
  mutable std::mutex                                tran_lock_;

  std::unordered_map<ParamId, std::vector<u64>> pending_rescue_points_;
  std::unordered_map<u32, VolumeDesc>               pending_volumes_;
  std::shared_ptr<Synchronization>                 synchronization_;
};

}  // namespace stdb

#endif  // STDB_METASTORAGE_WORKER_META_STORAGE_H_

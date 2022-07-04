/*!
 * \file meta_database.cc
 */
#include "stdb/core/meta_database.h"

namespace stdb {

MetaDatabase::MetaDatabase() {
  metadata_.reset(new MetadataStorage(":memory"));

  start_sync_worker();
}

MetaDatabase::MetaDatabase(const char* path) {
  metadata_.reset(new MetadataStorage(path));


}

}  // namespace stdb

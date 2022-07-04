/*!
 * \file meta_database.h
 */
#include "stdb/core/metadatastorage.h"
#include "stdb/index/seriesparser.h"
#include "stdb/query/queryprocessor_framework.h"

namespace stdb {

class MetaDatabase;

class MetaDatabaseSession : public std::enable_shared_from_this<MetaDatabaseSession> {
 protected:
  std::shared_ptr<MetaDatabase> Metadatabase_;
  PlainSeriesMatcher local_matcher_;

 public:
  MetaDatabaseSession(std::shared_ptr<MetaDatabase> meta_database);
  virtual ~MetaDatabaseSession();

  common::Status init_series_id(const char* begin, const char* end, i64* id);
};

class MetaDatabase : public std::enable_shared_from_this<MetaDatabase> {
 protected:
  SeriesMatcher global_matcher_;
  std::shared_ptr<MetadataStorage> metadata_;

  void start_sync_worker();

 public:
  // Create empty in-memmory meta database.
  MetaDatabase();

  // Open meta database.
  MetaStorage(const char* path);
};

}  // namespace stdb

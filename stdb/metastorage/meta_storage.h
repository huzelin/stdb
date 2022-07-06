/*!
 * \file metastorage.h
 */
#ifndef STDB_METASTORAGE_META_STORAGE_H_
#define STDB_METASTORAGE_META_STORAGE_H_

#include "stdb/common/apr_utils.h"
#include "stdb/index/seriesparser.h"

namespace stdb {

struct MetaStorage {
 protected:
  // Typedefs
  typedef std::unique_ptr<apr_pool_t, decltype(&delete_apr_pool)> PoolT;
  typedef const apr_dbd_driver_t* DriverT;
  typedef std::unique_ptr<apr_dbd_t, AprHandleDeleter> HandleT;
  typedef apr_dbd_prepared_t* PreparedT;

  // Members
  PoolT           pool_;
  DriverT         driver_;
  HandleT         handle_;

  void create_tables();
  
  void begin_transaction();
  void end_transaction();

  /** Execute query that doesn't return anything.
   * @throw std::runtime_error in a case of error
   * @return number of rows changed
   */
  int execute_query(std::string query);

  typedef std::vector<std::string> UntypedTuple;

  /** Execute select query and return untyped results.
   * @throw std::runtime_error in a case of error
   * @return bunch of strings with results
   */
  std::vector<UntypedTuple> select_query(const char* query) const;

 public:
  typedef PlainSeriesMatcher::SeriesNameT SeriesT;
  
  MetaStorage(const char* db);
  virtual ~MetaStorage();

  /** Initialize config 
   * @throw std::runtime_error in a case of error
   */
  void init_config(const char* db_name,
                   const char* creation_datetime,
                   const char* bstore_type);

  /**
   * @brief Get value of the configuration parameter
   * @param param_name is a name of the configuration parameter
   * @param value is a pointer that should receive configuration value
   * @return true on succes, false otherwise
   */
  bool get_config_param(const std::string& param_name, std::string* value);
  bool set_config_param(const std::string& param_name, const std::string& value, const std::string& comment = "");

  // Return the database name
  std::string get_database_name();
  // Return the creation datetime
  std::string get_creation_datetime();
  // Return the bstore type
  std::string get_bstore_type();
};

}  // namespace stdb

#endif  // STDB_METASTORAGE_META_STORAGE_H_

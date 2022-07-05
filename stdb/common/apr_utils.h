/*!
 * \file apr_utils.h
 */
#ifndef STDB_COMMON_APR_UTILS_H_
#define STDB_COMMON_APR_UTILS_H_

#include <apr.h>
#include <apr_dbd.h>

namespace stdb {

//! apr initialize
void initialize();

//! Delete apr pool
void delete_apr_pool(apr_pool_t* p);

//! APR DBD handle deleter
struct AprHandleDeleter {
  const apr_dbd_driver_t* driver;
  AprHandleDeleter(const apr_dbd_driver_t* driver);
  void operator()(apr_dbd_t* handle);
};

}  // namespace stdb

#endif  // STDB_COMMON_APR_UTILS_H_

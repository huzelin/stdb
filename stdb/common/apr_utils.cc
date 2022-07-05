/*!
 * \file apr_utils.cc
 */
#include "stdb/common/apr_utils.h"

#include "stdb/common/logging.h"

namespace stdb {

//! Pool for `apr_dbd_init`
static apr_pool_t* g_dbd_pool = nullptr;

void initialize() {
  // initialize libapr
  apr_initialize();
  // initialize aprdbd
  auto status = apr_pool_create(&g_dbd_pool, nullptr);
  if (status != APR_SUCCESS) {
    LOG(FATAL) << "Initialization error";
  }
  status = apr_dbd_init(g_dbd_pool);
  if (status != APR_SUCCESS) {
    LOG(FATAL) << "DBD initialization error";
  }

  const apr_dbd_driver_t* driver = NULL;
  apr_dbd_t* handle = NULL;
  auto rv = apr_dbd_get_driver(g_dbd_pool, "sqlite3", &driver);
  if (rv != APR_SUCCESS) {
    LOG(FATAL) << "apr dbd has no sqlite3 driver";
  }
}

void delete_apr_pool(apr_pool_t *p) {
  if (p) {
    apr_pool_destroy(p);
  }
}

AprHandleDeleter::AprHandleDeleter(const apr_dbd_driver_t *driver)
  : driver(driver) { }

void AprHandleDeleter::operator()(apr_dbd_t* handle) {
  if (driver != nullptr && handle != nullptr) {
    apr_dbd_close(driver, handle);
  }
}

}  // namespace stdb

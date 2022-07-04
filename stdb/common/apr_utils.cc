/*!
 * \file apr_utils.cc
 */
#include "stdb/common/apr_utils.h"

namespace stdb {

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

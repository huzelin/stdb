/*!
 * \file role.h
 */
#ifndef STDB_CORE_ROLE_H_
#define STDB_CORE_ROLE_H_

#include "stdb/common/basic.h"

namespace stdb {

enum class Role {
  kServer,
  kWorker,
  kStandalone,
};

}  // namespace stdb

#endif  // STDB_CORE_ROLE_H_

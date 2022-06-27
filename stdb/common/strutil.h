/*!
 * \file strutil.h
 */
#ifndef STDB_COMMON_STRUTIL_H_
#define STDB_COMMON_STRUTIL_H_

#include <string>
#include <tuple>
#include <vector>

namespace stdb {
namespace common {

struct StrUtil {
  static bool split(const char* str, uint32_t len,
                    std::vector<std::tuple<const char*, uint32_t>>& rets);
};

}  // namespace common
}  // namespace stdb

#endif  // STDB_COMMON_STRUTIL_H_

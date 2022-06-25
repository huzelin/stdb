/*!
 * \file strutil.cc
 */
#include "stdb/common/strutil.h"

namespace stdb {
namespace common {

bool StrUtil::split(
    const char* str,
    uint32_t len,
    std::vector<std::tuple<const char*, uint32_t>>& rets) {
  uint32_t pos = 0;
  for (; pos < len && str[pos] == ' '; ++pos);

  enum State {
    kSeries,
    kValue,
    kNone,
  };

  State state = kNone;
  uint32_t start = pos;
  for (; pos < len; ++pos) {
    if (str[pos] == ' ') {
      if (state == kValue) {
        rets.push_back(std::make_tuple(str + start, pos - start));
        state = kNone;
      }
    } else if (str[pos] == '"') {
      if (state != kSeries) {
        if (state != kNone) {
          return false;
        }
        state = kSeries;
        start = pos + 1;
      } else {
        rets.push_back(std::make_tuple(str + start, pos - start));
        state = kNone;
      }
    } else if (state == kNone) {
      state = kValue;
      start = pos;
    }
  }

  if (state == kValue && pos > start) {
    rets.push_back(std::make_tuple(str + start, pos - start));
  }
  return true;
}

}  // namespace common
}  // namespace stdb

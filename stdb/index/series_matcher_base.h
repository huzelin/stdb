/**
 * \file series_matcher_base.h
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
#ifndef STDB_CORE_SERIES_MATCHER_BASE_H_
#define STDB_CORE_SERIES_MATCHER_BASE_H_

#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <stdint.h>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "stdb/common/basic.h"
#include "stdb/common/status.h"
#include "stdb/index/stringpool.h"

namespace stdb {

static const i64 STDB_STARTING_SERIES_ID = 1024;

struct SeriesMatcherBase {
  //! Series name descriptor - pointer to string, length, series id.
  typedef std::tuple<const char*, int, i64> SeriesNameT;

  virtual ~SeriesMatcherBase() = default;

  /** Add new string to matcher.
  */
  virtual i64 add(const char* begin, const char* end) = 0;
  virtual i64 add(const char* begin, const char* end, const Location& location) = 0;

  /** Add value to matcher. This function should be
   * used only to load data to matcher. Internal
   * `series_id` counter wouldn't be affected by this call, so
   * it should be set up propertly in constructor.
   */
  virtual void _add(const std::string& series, i64 id) = 0;
  virtual void _add(const std::string& series, i64 id, const Location& location) = 0;

  /** Add value to matcher. This function should be
   * used only to load data to matcher. Internal
   * `series_id` counter wouldn't be affected by this call, so
   * it should be set up propertly in constructor.
   */
  virtual void _add(const char* begin, const char* end, i64 id) = 0;
  virtual void _add(const char* begin, const char* end, i64 id, const Location& location) = 0;

  /**
   * Match string and return it's id. If string is new return 0.
   */
  virtual i64 match(const char* begin, const char* end) const = 0;

  /**
   * Convert id to string
   */
  virtual StringT id2str(i64 tokenid) const = 0;

  /**
   * pull new series
   */
  virtual void pull_new_series(std::vector<SeriesNameT>* buffer) = 0;

  /**
   * pull new series
   */
  virtual void pull_new_series(std::vector<SeriesNameT>* name_buffer, std::vector<Location>* location_buffer) = 0;

  /**
   * get all ids
   */
  virtual std::vector<i64> get_all_ids() const = 0;
};

}  // namespace stdb

#endif  // STDB_CORE_SERIES_MATCHER_BASE_H_

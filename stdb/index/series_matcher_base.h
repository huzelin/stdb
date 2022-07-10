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
#ifndef STDB_INDEX_SERIES_MATCHER_BASE_H_
#define STDB_INDEX_SERIES_MATCHER_BASE_H_

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

  /** Add new string to matcher for moving obj
   * @param begin The begin of series str
   * @param end The end of series str
   * @return the new id
   */
  virtual i64 add(const char* begin, const char* end) = 0;

  /** Add new string to matcher for static obj
   * @param begin The begin of series str
   * @param end The end of series str
   * @param location The static location of obj
   * @return the new id
   */
  virtual i64 add(const char* begin, const char* end, const Location& location) = 0;

  /** Add value to matcher. This function should be used only to load data to matcher.
   * @param series The moving obj's series name
   * @param id The moving obj's id
   */
  virtual void _add(const std::string& series, i64 id) = 0;
  
  /** Add value to matcher. This function should be used only to load data to
   * matcher.
   * @param series The static obj's series name
   * @param location The static obj's location
   * @param id The static obj's id
   */
  virtual void _add(const std::string& series, const Location& location, i64 id) = 0;

  /** Add value to matcher. This function should be used only to load data to matcher.
   * @param begin The begin of moving obj's series name
   * @param end The end of moving obj's series name
   * @param id id of moving obj
   */
  virtual void _add(const char* begin, const char* end, i64 id) = 0;

  /** Add value to matcher. This function should be used only to load data to matcher.
   * @param begin The begin of static obj's series name
   * @param end The end of static obj's series name
   * @param location The location of static obj
   * @param id id of static obj
   */
  virtual void _add(const char* begin, const char* end, const Location& location, i64 id) = 0;

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

#endif  // STDB_INDEX_SERIES_MATCHER_BASE_H_

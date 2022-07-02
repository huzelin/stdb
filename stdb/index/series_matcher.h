/**
 * \file series_matcher.h
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
#ifndef STDB_CORE_SERIES_MATCHER_H_
#define STDB_CORE_SERIES_MATCHER_H_

#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <stdint.h>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "stdb/index/series_matcher_base.h"
#include "stdb/index/invertedindex.h"

namespace stdb {

/** Series index. Can be used to retreive series names and ids by tags.
  * Implements inverted index with compression and other optimizations.
  * It's more efficient than PlainSeriesMatcher but it's costly to have
  * many instances in one application.
  */
struct SeriesMatcher : SeriesMatcherBase {
  //! Series name descriptor - pointer to string, length, series id.
  // typedef std::tuple<const char*, int, i64> SeriesNameT;

  typedef StringTools::TableT TableT;
  typedef StringTools::InvT   InvT;

  Index                    index;      //! Series name index and storage
  TableT                   table;      //! Series table (name to id mapping)
  InvT                     inv_table;  //! Ids table (id to name mapping)
  i64                      series_id;  //! Series ID counter, positive values
  //! are resurved for metrics, negative are for events
  std::vector<SeriesNameT> names;      //! List of recently added names
  std::vector<Location> locations;     //! List of recently added locations.
  mutable std::mutex       mutex;      //! Mutex for shared data

  SeriesMatcher(i64 starting_id = STDB_STARTING_SERIES_ID);

  /** Add new string to matcher.
  */
  i64 add(const char* begin, const char* end) override;
  i64 add(const char* begin, const char* end, const Location& location) override;

  /** Add value to matcher. This function should be
   * used only to load data to matcher. Internal
   * `series_id` counter wouldn't be affected by this call, so
   * it should be set up propertly in constructor.
   */
  void _add(std::string series, i64 id) override;
  void _add(std::string series, i64 id, const Location& location) override;

  /** Add value to matcher. This function should be
   * used only to load data to matcher. Internal
   * `series_id` counter wouldn't be affected by this call, so
   * it should be set up propertly in constructor.
   */
  void _add(const char* begin, const char* end, i64 id) override;
  void _add(const char* begin, const char* end, i64 id, const Location& location) override;

  /**
   * Match string and return it's id. If string is new return 0.
   */
  i64 match(const char* begin, const char* end) const override;

  /**
   * Convert id to string
   */
  StringT id2str(i64 tokenid) const override;

  /** Push all new elements to the buffer.
   * @param buffer is an output parameter that will receive new elements
   */
  void pull_new_series(std::vector<SeriesNameT>* buffer) override;

  /**
   * pull new series to the buffer
   */
  void pull_new_series(std::vector<SeriesNameT>* name_buffer, std::vector<Location>* location_buffer) override;
  
  /**
   * get all the ids
   */
  std::vector<i64> get_all_ids() const override;

  std::vector<SeriesNameT> search(IndexQueryNodeBase const& query) const;

  std::vector<StringT> suggest_metric(std::string prefix) const;

  std::vector<StringT> suggest_tags(std::string metric, std::string tag_prefix) const;

  std::vector<StringT> suggest_tag_values(std::string metric, std::string tag, std::string value_prefix) const;

  size_t memory_use() const;

  size_t index_memory_use() const;

  size_t pool_memory_use() const;

 protected:
  i64 add_impl(const char* begin, const char* end);
};

}  // namespace stdb

#endif  // STDB_CORE_SERIES_MATCHER_H_

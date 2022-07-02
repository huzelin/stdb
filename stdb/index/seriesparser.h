/**
 * \file seriesparser.h
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
#ifndef STDB_CORE_SERIES_PARSER_H_
#define STDB_CORE_SERIES_PARSER_H_

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
#include "stdb/index/series_matcher.h"
#include "stdb/index/plain_series_matcher.h"

namespace stdb {

/** Namespace class to store all parsing related things.
 */
struct SeriesParser {
  /** Convert input string to normal form.
   * In normal form metric name is followed by the list of key
   * value pairs in alphabetical order. All keys should be unique and
   * separated from metric name and from each other by exactly one space.
   * @param begin points to the begining of the input string
   * @param end points to the to the end of the string
   * @param out_begin points to the begining of the output buffer (should be not less then input buffer)
   * @param out_end points to the end of the output buffer
   * @param keystr_begin points to the begining of the key string (string with key-value pairs)
   * @return STDB_SUCCESS if everything is OK, error code otherwise
   */
  static common::Status to_canonical_form(const char* begin, const char* end, char* out_begin,
                                          char* out_end, const char** keystr_begin,
                                          const char** keystr_end);

  typedef StringTools::StringT StringT;

  /** Remove redundant tags from input string. Leave only metric and tags from the list.
   * If 'inv' is set leave tags that are not in the set.
   */
  static std::tuple<common::Status, StringT> filter_tags(StringT const& input,
                                                         StringTools::SetT const& tags,
                                                         char* out,
                                                         bool inv = false);
};

enum class GroupByOpType {
  PIVOT,
  GROUP,
};

/** Group-by processor. Maps set of global series names to
 * some other set of local series ids.
 */
struct GroupByTag {
  //! Mapping from global parameter ids to local parameter ids
  std::unordered_map<ParamId, ParamId> ids_;
  //! Shared series matcher
  SeriesMatcher const& matcher_;
  //! Previous string pool offset
  StringPoolOffset offset_;
  //! Previous string pool size
  size_t prev_size_;
  //! Metric names
  std::vector<std::string> metrics_;
  //! List of function names (for aggregate queries)
  std::vector<std::string> funcs_;
  //! List of tags of interest
  std::vector<std::string> tags_;
  //! Local string pool. All transient series names lives here.
  PlainSeriesMatcher local_matcher_;
  //! List of string already added string pool
  StringTools::SetT snames_;
  GroupByOpType type_;

  //! Main c-tor
  GroupByTag(const SeriesMatcher &matcher, std::string metric, std::vector<std::string> const& tags, GroupByOpType op);
  GroupByTag(const SeriesMatcher &matcher,
             const std::vector<std::string>& metrics,
             const std::vector<std::string>& func_names,
             std::vector<std::string> const& tags,
             GroupByOpType op);

  void refresh_();

  PlainSeriesMatcher& get_series_matcher();
  std::unordered_map<ParamId, ParamId> get_mapping() const;
};

}  // namespace stdb

#endif  // STDB_CORE_SERIES_PARSER_H_

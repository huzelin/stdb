/**
 * \file series_matcher.cc
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
#include "stdb/index/series_matcher.h"

#include <string>
#include <map>
#include <algorithm>
#include <regex>

#include "stdb/common/exception.h"
#include "stdb/common/logging.h"

namespace stdb {

static const StringT EMPTY = std::make_pair(nullptr, 0);

SeriesMatcher::SeriesMatcher(i64 starting_id)
    : table(StringTools::create_table(0x1000))
      , series_id(starting_id) {
  if (starting_id == 0u) {
    STDB_THROW("Bad series ID, starting_id=", starting_id);
  }
}

i64 SeriesMatcher::add(const char* begin, const char* end) {
  std::lock_guard<std::mutex> guard(mutex);
  return add_impl(begin, end);
}

i64 SeriesMatcher::add(const char* begin, const char* end, const Location& location) {
  std::lock_guard<std::mutex> guard(mutex);
  locations.push_back(location);
  return add_impl(begin, end);
}

i64 SeriesMatcher::add_impl(const char* begin, const char* end) {
  auto prev_id = series_id++;
  auto id = prev_id;
  if (*begin == '!') {
    // Series name starts with ! which mean that we're dealing with event
    id = -1 * id;
  }
  common::Status status;
  StringT sname;
  std::tie(status, sname) = index.append(begin, end);
  if (!status.IsOk()) {
    series_id = id;
    return 0;
  }
  auto tup = std::make_tuple(std::get<0>(sname), std::get<1>(sname), id);
  table[sname] = id;
  inv_table[id] = sname;
  names.push_back(tup);
  return id;
}

void SeriesMatcher::_add(const std::string& series, i64 id) {
  if (series.empty()) {
    return;
  }
  _add(series.data(), series.data() + series.size(), id);
}

void SeriesMatcher::_add(const std::string& series, i64 id, const Location& location) {
  if (series.empty()) {
    return;
  }
  _add(series.data(), series.data() + series.size(), id, location);
}

void SeriesMatcher::_add(const char*  begin, const char* end, i64 id) {
  std::lock_guard<std::mutex> guard(mutex);
  common::Status status;
  StringT sname;
  std::tie(status, sname) = index.append(begin, end);
  table[sname] = id;
  inv_table[id] = sname;
}

void SeriesMatcher::_add(const char* begin, const char* end, i64 id, const Location& location) {
  std::lock_guard<std::mutex> guard(mutex);
  common::Status status;
  StringT sname;
  std::tie(status, sname) = index.append(begin, end);
  table[sname] = id;
  inv_table[id] = sname;
  // TODO: add spatial index.
}

i64 SeriesMatcher::match(const char* begin, const char* end) const {
  int len = static_cast<int>(end - begin);
  StringT str = std::make_pair(begin, len);

  std::lock_guard<std::mutex> guard(mutex);
  auto it = table.find(str);
  if (it == table.end()) {
    return 0ul;
  }
  return it->second;
}

StringT SeriesMatcher::id2str(i64 tokenid) const {
  std::lock_guard<std::mutex> guard(mutex);
  auto it = inv_table.find(tokenid);
  if (it == inv_table.end()) {
    return EMPTY;
  }
  return it->second;
}

void SeriesMatcher::pull_new_series(std::vector<SeriesNameT> *buffer) {
  std::lock_guard<std::mutex> guard(mutex);
  std::swap(names, *buffer);
}

void SeriesMatcher::pull_new_series(std::vector<SeriesNameT>* name_buffer, std::vector<Location>* location_buffer) {
  std::lock_guard<std::mutex> guard(mutex);
  std::swap(names, *name_buffer);
  std::swap(locations, *location_buffer);
}

std::vector<i64> SeriesMatcher::get_all_ids() const {
  std::vector<i64> result;
  {
    std::lock_guard<std::mutex> guard(mutex);
    for (auto const &tup: inv_table) {
      result.push_back(tup.first);
    }
  }
  std::sort(result.begin(), result.end());
  return result;
}

std::vector<SeriesMatcher::SeriesNameT> SeriesMatcher::search(IndexQueryNodeBase const& query) const {
  std::vector<SeriesMatcher::SeriesNameT> result;
  std::lock_guard<std::mutex> guard(mutex);
  auto resultset = query.query(index);
  for (auto it = resultset.begin(); it != resultset.end(); ++it) {
    auto str = *it;
    auto fit = table.find(str);
    if (fit == table.end()) {
      STDB_THROW("Invalid index state");
    }
    result.push_back(std::make_tuple(str.first, str.second, fit->second));
  }
  return result;
}

std::vector<StringT> SeriesMatcher::suggest_metric(std::string prefix) const {
  std::vector<StringT> results;
  std::lock_guard<std::mutex> guard(mutex);
  results = index.get_topology().list_metric_names();
  auto resit = std::remove_if(results.begin(), results.end(), [prefix](StringT val) {
    if (val.second < prefix.size()) {
      return true;
    }
    auto eq = std::equal(prefix.begin(), prefix.end(), val.first);
    return !eq;
  });
  results.erase(resit, results.end());
  return results;
}

std::vector<StringT> SeriesMatcher::suggest_tags(std::string metric, std::string tag_prefix) const {
  std::vector<StringT> results;
  std::lock_guard<std::mutex> guard(mutex);
  results = index.get_topology().list_tags(tostrt(metric));
  auto resit = std::remove_if(results.begin(), results.end(), [tag_prefix](StringT val) {
    if (val.second < tag_prefix.size()) {
       return true;
    }
    auto eq = std::equal(tag_prefix.begin(), tag_prefix.end(), val.first);
    return !eq;
  });
  results.erase(resit, results.end());
  return results;
}

std::vector<StringT> SeriesMatcher::suggest_tag_values(std::string metric, std::string tag, std::string value_prefix) const {
  std::vector<StringT> results;
  std::lock_guard<std::mutex> guard(mutex);
  results = index.get_topology().list_tag_values(tostrt(metric), tostrt(tag));
  auto resit = std::remove_if(results.begin(), results.end(), [value_prefix](StringT val) {
    if (val.second < value_prefix.size()) {
      return true;
    }
    auto eq = std::equal(value_prefix.begin(), value_prefix.end(), val.first);
    return !eq;
  });
  results.erase(resit, results.end());
  return results;
}

size_t SeriesMatcher::memory_use() const {
  return index.memory_use();
}

size_t SeriesMatcher::index_memory_use() const {
  return index.index_memory_use();
}

size_t SeriesMatcher::pool_memory_use() const {
  return index.pool_memory_use();
}

}  // namespace stdb

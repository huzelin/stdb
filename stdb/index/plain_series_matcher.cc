/**
 * \file plain_series_matcher.cc
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
#include "stdb/index/plain_series_matcher.h"

#include <string>
#include <map>
#include <algorithm>
#include <regex>

#include "stdb/common/exception.h"
#include "stdb/common/logging.h"

namespace stdb {

static const StringT EMPTY = std::make_pair(nullptr, 0);

PlainSeriesMatcher::PlainSeriesMatcher(i64 starting_id)
    : table(StringTools::create_table(0x1000))
    , series_id(starting_id) {
  if (starting_id == 0u) {
    STDB_THROW("Bad series ID, starting_id=", starting_id);
  }
}

i64 PlainSeriesMatcher::add(const char* begin, const char* end) {
  std::lock_guard<std::mutex> guard(mutex);
  return add_impl(begin, end);
}

i64 PlainSeriesMatcher::add(const char* begin, const char* end, const Location& location) {
  std::lock_guard<std::mutex> guard(mutex);
  locations.push_back(location);
  return add_impl(begin, end);
}

i64 PlainSeriesMatcher::add_impl(const char* begin, const char* end) {
  auto id = series_id++;
  StringT pstr = pool.add(begin, end);
  auto tup = std::make_tuple(std::get<0>(pstr), std::get<1>(pstr), id);
  table[pstr] = id;
  inv_table[id] = pstr;
  names.push_back(tup);
  return id;
}

void PlainSeriesMatcher::_add(const std::string& series, i64 id) {
  if (series.empty()) {
    return;
  }
  _add(series.data(), series.data() + series.size(), id);
}

void PlainSeriesMatcher::_add(const std::string& series, i64 id, const Location& location) {
  if (series.empty()) {
    return;
  }
  _add(series.data(), series.data() + series.size(), id, location);
}

void PlainSeriesMatcher::_add(const char* begin, const char* end, i64 id) {
  StringT pstr = pool.add(begin, end);
  std::lock_guard<std::mutex> guard(mutex);
  table[pstr] = id;
  inv_table[id] = pstr;
}

void PlainSeriesMatcher::_add(const char* begin, const char* end, i64 id, const Location& location) {
  StringT pstr = pool.add(begin, end);
  std::lock_guard<std::mutex> guard(mutex);
  table[pstr] = id;
  inv_table[id] = pstr;
  // TODO: add spatial index.
}

i64 PlainSeriesMatcher::match(const char* begin, const char* end) const {
  int len = static_cast<int>(end - begin);
  StringT str = std::make_pair(begin, len);

  std::lock_guard<std::mutex> guard(mutex);
  auto it = table.find(str);
  if (it == table.end()) {
    return 0ul;
  }
  return it->second;
}

StringT PlainSeriesMatcher::id2str(i64 tokenid) const {
  std::lock_guard<std::mutex> guard(mutex);
  auto it = inv_table.find(tokenid);
  if (it == inv_table.end()) {
    return EMPTY;
  }
  return it->second;
}

void PlainSeriesMatcher::pull_new_series(std::vector<SeriesNameT> *buffer) {
  std::lock_guard<std::mutex> guard(mutex);
  std::swap(names, *buffer);
}

void PlainSeriesMatcher::pull_new_series(std::vector<SeriesNameT>* name_buffer, std::vector<Location>* location_buffer) {
  std::lock_guard<std::mutex> guard(mutex);
  std::swap(names, *name_buffer);
  std::swap(locations, *location_buffer);
}

std::vector<i64> PlainSeriesMatcher::get_all_ids() const {
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

std::vector<PlainSeriesMatcher::SeriesNameT> PlainSeriesMatcher::regex_match(const char* rexp) const {
  StringPoolOffset offset = {};
  size_t size = 0;
  return regex_match(rexp, &offset, &size);
}

std::vector<PlainSeriesMatcher::SeriesNameT> PlainSeriesMatcher::regex_match(const char* rexp, StringPoolOffset* offset, size_t *prevsize) const {
  std::vector<SeriesNameT> series;
  std::vector<LegacyStringPool::StringT> res = pool.regex_match(rexp, offset, prevsize);

  std::lock_guard<std::mutex> guard(mutex);
  std::transform(res.begin(), res.end(), std::back_inserter(series), [this](StringT s) {
    auto it = table.find(s);
    if (it == table.end()) {
      // We should always find id by string, otherwise - invariant is
      // broken (due to memory corruption most likely).
      STDB_THROW("Invalid string-pool");
    }
    return std::make_tuple(s.first, s.second, it->second);
  });
  return series;
}

}  // namespace stdb

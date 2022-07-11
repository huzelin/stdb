/**
 * \file seriesparser.cc
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
#include "stdb/index/seriesparser.h"

#include <string>
#include <map>
#include <algorithm>
#include <regex>

#include "stdb/common/exception.h"
#include "stdb/common/logging.h"

namespace stdb {

//! Move pointer to the of the whitespace, return this pointer or end on error
static const char* skip_space(const char* p, const char* end) {
  while (p < end && (*p == ' ' || *p == '\t')) {
    p++;
  }
  return p;
}

static StringTools::StringT get_tag_name(const char* p, const char* end) {
  StringTools::StringT EMPTY = { nullptr, 0 };
  auto begin = p;
  while (p < end && *p != '=' && *p != ' ' && *p != '\t') {
    p++;
  }
  if (p == end || *p != '=') {
    return EMPTY;
  }
  return { begin, p - begin };
}

static const char ESC_CHAR = '\\';

static const char* copy_until(const char* begin, const char* end, const char pattern, char** out) {
  if (begin == end) {
    return begin;
  }
  const char* escape_char = *begin == ESC_CHAR ? begin : nullptr;
  char* it_out = *out;
  while (true) {
    *it_out = *begin;
    it_out++;
    begin++;
    if (begin == end) {
      break;
    }
    if (*begin == pattern) {
      if (std::prev(begin) != escape_char) {
        break;
      }
    } else if (*begin == ESC_CHAR) {
      escape_char = begin;
    }
  }
  *out = it_out;
  return begin;
}

//! Move pointer to the beginning of the next tag, return this pointer or end on error
static const char* skip_tag(const char* begin, const char* end, bool *error) {
  const char* escape_char = nullptr;
  // skip until '='
  const char* p = begin;
  while (p < end) {
    if (*p == '=') {
      break;
    } else if (*p == ' ') {
      if (std::prev(p) != escape_char) {
        break;
      }
    } else if (*p == ESC_CHAR) {
      escape_char = p;
    }
    p++;
  }
  if (p == begin || p == end || *p != '=') {
    *error = true;
    return end;
  }
  // skip until ' '
  const char* c = p;
  while (c < end) {
    if (*c == ' ') {
      if (std::prev(c) != escape_char) {
        break;
      }
    } else if (*c == ESC_CHAR) {
      escape_char = c;
    }
    c++;
  }
  *error = c == p;
  return c;
}

common::Status SeriesParser::to_canonical_form(const char* begin, const char* end,
                                               char* out_begin, char* out_end,
                                               const char** keystr_begin,
                                               const char** keystr_end) {
  // Verify args
  if (end < begin) {
    return common::Status::BadArg();
  }
  if (out_end < out_begin) {
    return common::Status::BadArg();
  }
  int series_name_len = end - begin;
  if (series_name_len > LIMITS_MAX_SNAME) {
    return common::Status::BadData();
  }
  if (series_name_len > (out_end - out_begin)) {
    return common::Status::BadArg();
  }

  char* it_out = out_begin;
  const char* it = begin;
  // Get metric name
  it = skip_space(it, end);
  it = copy_until(it, end, ' ', &it_out);
  it = skip_space(it, end);

  if (it == end) {
    // At least one tag should be specified
    return common::Status::BadData();
  }

  *keystr_begin = it_out;

  // Get pointers to the keys
  const char* tags[LIMITS_MAX_TAGS];
  auto ix_tag = 0u;
  bool error = false;
  while (it < end && ix_tag < LIMITS_MAX_TAGS) {
    tags[ix_tag] = it;
    it = skip_tag(it, end, &error);
    it = skip_space(it, end);
    if (!error) {
      ix_tag++;
    } else {
      break;
    }
  }
  if (error) {
    // Bad string
    return common::Status::BadData();
  }
  if (ix_tag == 0) {
    // User should specify at least one tag
    return common::Status::BadData();
  }

  auto sort_pred = [end](const char* lhs, const char* rhs) {
    // lhs should be always less thenn rhs
    auto lenl = 0u;
    auto lenr = 0u;
    if (lhs < rhs) {
      lenl = rhs - lhs;
      lenr = end - rhs;
    } else {
      lenl = end - lhs;
      lenr = lhs - rhs;
    }
    auto it = 0u;
    while (true) {
      if (it >= lenl || it >= lenr) {
        return it < lenl;
      }
      if (lhs[it] == '=' || rhs[it] == '=') {
        return lhs[it] == '=' && rhs[it] != '=';
      }
      if (lhs[it] < rhs[it]) {
        return true;
      } else if (lhs[it] > rhs[it]) {
        return false;
      }
      it++;
    }
    return true;
  };
  std::sort(tags, tags + ix_tag, std::ref(sort_pred));  // std::sort can't move from predicate if predicate is a rvalue
  // nor it can pass predicate by reference
  // Copy tags to output string
  for (auto i = 0u; i < ix_tag; i++) {
    // insert space
    *it_out++ = ' ';
    // insert tag
    const char* tag = tags[i];
    copy_until(tag, end, ' ', &it_out);
  }
  *keystr_begin = skip_space(*keystr_begin, out_end);
  *keystr_end = it_out;
  return common::Status::Ok();
}

std::tuple<common::Status, SeriesParser::StringT> SeriesParser::filter_tags(
    SeriesParser::StringT const& input, const StringTools::SetT &tags, char* out, bool inv) {
  StringT NO_RESULT = {};
  char* out_begin = out;
  char* it_out = out;
  const char* it = input.first;
  const char* end = it + input.second;

  // Get metric name
  it = skip_space(it, end);
  it = copy_until(it, end, ' ', &it_out);
  it = skip_space(it, end);

  if (it == end) {
    // At least one tag should be specified
    return std::make_tuple(common::Status::BadData(), NO_RESULT);
  }

  // Get pointers to the keys
  const char* last_tag;
  auto ix_tag = 0u;
  bool error = false;
  while (it < end && ix_tag < LIMITS_MAX_TAGS) {
    last_tag = it;
    it = skip_tag(it, end, &error);
    if (!error) {
      // Check tag
      StringT tag = get_tag_name(last_tag, it);
      auto ntags = tags.count(tag);
      if ((!inv && ntags) || (inv && !ntags)) {
        *it_out = ' ';
        it_out++;
        auto sz = it - last_tag;
        memcpy((void*)it_out, (const void*)last_tag, sz);
        it_out += sz;
        ix_tag++;
      }
    } else {
      break;
    }
    it = skip_space(it, end);
  }

  if (error) {
    // Bad string
    return std::make_tuple(common::Status::BadData(), NO_RESULT);
  }

  if (ix_tag == 0) {
    // User should specify at least one tag
    return std::make_tuple(common::Status::BadData(), NO_RESULT);
  } 

  return std::make_tuple(common::Status::Ok(), std::make_pair(out_begin, it_out - out_begin));
}

GroupByTag::GroupByTag(
    const SeriesMatcher &matcher,
    std::string metric,
    std::vector<std::string> const& tags,
    GroupByOpType op) :
    matcher_(matcher),
    offset_{},
    prev_size_(0),
    metrics_({metric}),
    funcs_(),
    tags_(tags),
    local_matcher_(1ul),
    snames_(StringTools::create_set(64)),
    type_(op) {
  refresh_();
}

GroupByTag::GroupByTag(const SeriesMatcher &matcher,
                       const std::vector<std::string>& metrics,
                       const std::vector<std::string> &func_names,
                       std::vector<std::string> const& tags,
                       GroupByOpType op) :
    matcher_(matcher),
    offset_{},
    prev_size_(0),
    metrics_(metrics),
    funcs_(func_names),
    tags_(tags),
    local_matcher_(1ul),
    snames_(StringTools::create_set(64)),
    type_(op) {
  refresh_();
}

PlainSeriesMatcher &GroupByTag::get_series_matcher() {
  return local_matcher_;
}

std::unordered_map<ParamId, ParamId> GroupByTag::get_mapping() const {
  return ids_;
}

void GroupByTag::refresh_() {
  int mindex = 0;
  for (auto metric: metrics_) {
    IncludeIfHasTag tag_query(metric, tags_);
    auto results = matcher_.search(tag_query);
    auto filter = StringTools::create_set(tags_.size());
    for (const auto& tag: tags_) {
      filter.insert(std::make_pair(tag.data(), tag.size()));
    }
    char buffer[LIMITS_MAX_SNAME];
    for (auto item: results) {
      common::Status status;
      SeriesParser::StringT result, stritem;
      stritem = std::make_pair(std::get<0>(item), std::get<1>(item));
      std::tie(status, result) = SeriesParser::filter_tags(stritem, filter, buffer, type_ == GroupByOpType::INVERT);
      if (status.IsOk()) {
        if (funcs_.size() != 0) {
          // Update metric name using aggregate function, e.g. cpu key=val -> cpu:max key=val
          const auto& fname = funcs_.at(mindex);
          std::string name(result.first, result.first + result.second);
          auto pos = name.find_first_of(' ');
          if (pos != std::string::npos) {
            std::string str = name.substr(0, pos) + ":" + fname + name.substr(pos);
            std::copy(str.begin(), str.end(), buffer);
            result = std::make_pair(buffer, static_cast<u32>(str.size()));
          }
        }
        if (snames_.count(result) == 0) {
          // put result to local stringpool and ids list
          auto localid = local_matcher_.add(result.first, result.first + result.second);
          auto str = local_matcher_.id2str(localid);
          snames_.insert(str);
          ids_[static_cast<ParamId>(std::get<2>(item))] = static_cast<ParamId>(localid);
        } else {
          // local name already created
          auto localid = local_matcher_.match(result.first, result.first + result.second);
          if (localid == 0ul) {
            STDB_THROW("inconsistent matcher state");
          }
          ids_[static_cast<ParamId>(std::get<2>(item))] = static_cast<ParamId>(localid);
        }
      }
    }
    mindex++;
  }
}

}  // namespace stdb

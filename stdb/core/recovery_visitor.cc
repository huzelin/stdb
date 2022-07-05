/*!
 * \file recovery_visitor.cc
 */
#include "stdb/core/recovery_visitor.h"

#include "stdb/core/database.h"

namespace stdb {

bool RecoveryVisitor::operator()(const storage::InputLogDataPoint& point) {
  sample.timestamp        = point.timestamp;
  sample.payload.float64  = point.value;
  sample.payload.size     = sizeof(Sample);
  sample.payload.type     = PAYLOAD_FLOAT;
  auto result = worker_database_->recovery_write(sample, updated_ids.count(sample.paramid));

  switch (result) {
    case storage::NBTreeAppendResult::FAIL_BAD_VALUE: {
      LOG(INFO) << "WAL recovery failed";
      return false;
    }

    case storage::NBTreeAppendResult::FAIL_BAD_ID: {
      nlost++;
    } break;

    case storage::NBTreeAppendResult::OK_FLUSH_NEEDED:
    case storage::NBTreeAppendResult::OK: {
      updated_ids.insert(sample.paramid);
      nsamples++;
    } break;

    default:
      break;
  }
  return true;
}

bool RecoveryVistor::operator()(const storage::InputLogSeriesName& sname) {
  auto strref = database_->global_matcher().id2str(curr_id);
  if (strref.second) {
    // Fast path, name already added
    return true;
  }
  bool create_new = false;
  auto begin = sname.value.data();
  auto end = begin + sname.value.length();
  auto id = database_->global_matcher().match(begin, end);
  if (id == 0) {
    // create new series
    id = curr_id;
    StringT prev = database_->global_matcher().id2str(id);
    if (prev.second != 0) {
      // sample.paramid maps to some other series
      LOG(ERROR) << "Series id conflict. Id " << id
          << " is already taken by " << std::string(prev.first, prev.first + prev.second)
          << ". Series name " << sname.value << " is skipped.";
      return true;
    }
    database_->global_matcher()._add(sname.value, id);
    create_new = true;
  }
  if (create_new) {
    database_->recovery_create_name_column(id);
    restored_ids->push_back(id);
  }
  return true;
}

bool RecoveryVisitor::operator()(const storage::InputLogRecoveryInfo& rinfo) {
  std::vector<storage::LogicAddr> rpoints(rinfo.data);
  auto it = mapping->find(curr_id);
  if (it != mapping->end()) {
    // Check if rescue points are of newer version.
    // If *it is newer than rinfo.data then return. Otherwise,
    // update rescue points list using rinfo.data.
    const std::vector<storage::LogicAddr> &current = it->second;
    if (rinfo.data.empty()) {
      return true;
    }
    if (!current.empty()) {
      if (current.size() > rinfo.data.size()) {
        return true;
      }
      auto curr_max = std::max_element(current.begin(), current.end());
      auto data_max = std::max_element(rinfo.data.begin(), rinfo.data.end());
      if (*data_max >= top_addr || *curr_max > *data_max) {
        return true;
      }
    }
  }
  (*mapping)[curr_id] = rpoints;
  database_->recovery_update_rescue_points(curr_id, std::move(rpoints));
}

}  // namespace stdb

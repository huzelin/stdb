/*!
 * \file database.cc
 */
#include "stdb/core/database.h"

#include "stdb/common/logging.h"

namespace stdb {

void Database::initialize(const FineTuneParams& params) {
  if (params.input_log_path) {
    LOG(INFO) << "WAL enabled, path: " << params.input_log_path
        << ", nvolumes: " << params.input_log_volume_numb
        <<  ", volume-size: " << params.input_log_volume_size;

    if (!boost::filesystem::exists(params.input_log_path)) {
      boost::filesystem::create_directories(params.input_log_path);
    }
    inputlog_.reset(new storage::ShardedInputLog(
            static_cast<int>(params.input_log_concurrency),
            params.input_log_path,
            params.input_log_volume_numb,
            params.input_log_volume_size));
    input_log_path_ = params.input_log_path;
  }
}

void Database::set_input_log(std::shared_ptr<storage::ShardedInputLog> inputlog, const std::string& input_log_path) {
  inputlog_ = inputlog;
  input_log_path_ = input_log_path;
}

bool Database::wal_recovery_is_enabled(const FineTuneParams &params, int* ccr) {
  auto run_wal_recovery = false;
  *ccr = 0;
  if (params.input_log_path) {
    common::Status status;
    std::tie(status, *ccr) = storage::ShardedInputLog::find_logs(params.input_log_path);
    if (status.IsOk() && *ccr > 0) {
      run_wal_recovery = true;
    }
  }
  return run_wal_recovery;
}

storage::InputLog* Database::get_input_log() {
  static size_t s_known_hashes[MAX_THREADS];
  static std::atomic<int> s_hash_counter = { 0 };
  if (inputlog_) {
    std::hash<std::thread::id> thash;
    size_t hash = thash(std::this_thread::get_id());
    // Check if the hash was seen previously
    int nhashes = s_hash_counter.load();
    for (int i = 0; i < nhashes; i++) {
      if (s_known_hashes[i] == hash) {
        return &inputlog_->get_shard(i);
      }
    }
    // Insert new value
    int ixnew = s_hash_counter++;
    s_known_hashes[ixnew] = hash;
    return &inputlog_->get_shard(ixnew);
  }
  return nullptr;
}

std::tuple<common::Status, std::string> Database::parse_query(
    boost::property_tree::ptree const& ptree,
    qp::ReshapeRequest*                req) {
  using namespace qp;

  QueryKind kind;
  common::Status status;

  ErrorMsg error_msg;
  std::tie(status, kind, error_msg) = QueryParser::get_query_kind(ptree);
  if (status != common::Status::Ok()) {
    return std::make_tuple(status, error_msg.data());
  }
  auto global_matcher = this->global_matcher(); 
  switch (kind) {
    case QueryKind::SELECT_META:
      LOG(ERROR) << "Metadata query is not supported";
      return std::make_tuple(common::Status::BadArg(), "Metadata query is not supported");

    case QueryKind::SELECT_EVENTS:
      std::tie(status, *req, error_msg) = QueryParser::parse_select_events_query(ptree, *global_matcher);
      if (status != common::Status::Ok()) {
        return std::make_tuple(status, error_msg.data());
      }
      break;

    case QueryKind::AGGREGATE:
      std::tie(status, *req, error_msg) = QueryParser::parse_aggregate_query(ptree, *global_matcher);
      if (status != common::Status::Ok()) {
        return std::make_tuple(status, error_msg.data());
      }
      break;

    case QueryKind::GROUP_AGGREGATE:
      std::tie(status, *req, error_msg) = QueryParser::parse_group_aggregate_query(ptree, *global_matcher);
      if (status != common::Status::Ok()) {
        return std::make_tuple(status, error_msg.data());
      }
      break;

    case QueryKind::GROUP_AGGREGATE_JOIN:
      std::tie(status, *req, error_msg) = QueryParser::parse_group_aggregate_join_query(ptree, *global_matcher);
      if (status != common::Status::Ok()) {
        return std::make_tuple(status, error_msg.data());
      }
      break;

    case QueryKind::SELECT:
      std::tie(status, *req, error_msg) = QueryParser::parse_select_query(ptree, *global_matcher);
      if (status != common::Status::Ok()) {
        return std::make_tuple(status, error_msg.data());
      }
      break;

    case QueryKind::JOIN:
      std::tie(status, *req, error_msg) = QueryParser::parse_join_query(ptree, *global_matcher);
      if (status != common::Status::Ok()) {
        return std::make_tuple(status, error_msg.data());
      }
      break;
  };
  return std::make_tuple(common::Status::Ok(), ErrorMsg());
}

}  // namespace stdb

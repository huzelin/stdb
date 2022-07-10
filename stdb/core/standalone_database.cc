/*!
 * \file standalone_database.cc
 */
#include "stdb/core/standalone_database.h"

#include <memory>

#include "stdb/core/controller.h"
#include "stdb/core/standalone_database_session.h"
#include "stdb/query/queryparser.h"
#include "stdb/query/queryprocessor.h"

namespace stdb {

StandaloneDatabase::StandaloneDatabase(
    std::shared_ptr<Synchronization> synchronization,
    std::shared_ptr<SyncWaiter> sync_waiter,
    bool is_moving) : Database(is_moving), sync_waiter_(sync_waiter) {
  worker_database_.reset(new WorkerDatabase(synchronization, is_moving));
  server_database_.reset(new ServerDatabase(is_moving));
}

StandaloneDatabase::StandaloneDatabase(
    const char* server_path, const char* worker_path,
    const FineTuneParams& params,
    std::shared_ptr<Synchronization> synchronization,
    std::shared_ptr<SyncWaiter> sync_waiter,
    bool is_moving) : Database(is_moving), sync_waiter_(sync_waiter) {
  server_database_.reset(new ServerDatabase(server_path, params, is_moving));
  worker_database_.reset(new WorkerDatabase(worker_path, params, synchronization, is_moving));
}

void StandaloneDatabase::initialize(const FineTuneParams& params) {
  server_database_->run_recovery(params, this);
  worker_database_->run_recovery(params, this);

  Database::initialize(params);

  server_database_->set_input_log(inputlog(), input_log_path());
  worker_database_->set_input_log(inputlog(), input_log_path());
}

void StandaloneDatabase::close() {
  server_database_->close();
  worker_database_->close();
}

void StandaloneDatabase::sync() {
  server_database_->sync();
  worker_database_->sync();
}

std::shared_ptr<DatabaseSession> StandaloneDatabase::create_session() {
  std::shared_ptr<storage::CStoreSession> session =
      std::make_shared<storage::CStoreSession>(worker_database_->cstore());
  return std::make_shared<StandaloneDatabaseSession>(shared_from_this(),
                                                     session,
                                                     sync_waiter_);
}

void StandaloneDatabase::query(StandaloneDatabaseSession* session, InternalCursor* cur, const char* query) {
  using namespace qp;

  boost::property_tree::ptree ptree;
  common::Status status;
  ErrorMsg error_msg;
  // session->clear_series_matcher();
  std::tie(status, ptree, error_msg) = QueryParser::parse_json(query);
  if (status != common::Status::Ok()) {
    cur->set_error(status, error_msg.data());
    return;
  }
  QueryKind kind;
  std::tie(status, kind, error_msg) = QueryParser::get_query_kind(ptree);
  if (status != common::Status::Ok()) {
    cur->set_error(status, error_msg.data());
    return;
  }
  std::shared_ptr<IStreamProcessor> proc;
  ReshapeRequest req;

  if (kind == QueryKind::SELECT_META) {
    std::vector<ParamId> ids;
    auto global_matcher = server_database_->global_matcher();
    std::tie(status, ids, error_msg) = QueryParser::parse_select_meta_query(ptree, *global_matcher);
    if (status != common::Status::Ok()) {
      cur->set_error(status, error_msg.data());
      return;
    }
    std::vector<std::shared_ptr<Node>> nodes;
    std::tie(status, nodes, error_msg) = QueryParser::parse_processing_topology(ptree, cur, req);
    if (status != common::Status::Ok()) {
      cur->set_error(status, error_msg.data());
      return;
    }
    proc = std::make_shared<MetadataQueryProcessor>(nodes.front(), std::move(ids));
    if (proc->start()) {
      proc->stop();
    }
  } else {
    LOG(INFO) << "------SELECT";
    /*
    std::tie(status, error_msg) = parse_query(ptree, &req);
    if (status != common::Status::Ok()) {
      cur->set_error(status, error_msg.data());
      return;
    }
    std::vector<std::shared_ptr<Node>> nodes;
    std::tie(status, nodes, error_msg) = QueryParser::parse_processing_topology(ptree, cur, req);
    if (status != common::Status::Ok()) {
      cur->set_error(status, error_msg.data());
      return;
    }
    bool groupbytime = kind == QueryKind::GROUP_AGGREGATE;
    proc = std::make_shared<ScanQueryProcessor>(nodes, groupbytime);
    if (req.select.matcher) {
      session->set_series_matcher(req.select.matcher);
    } else {
      session->clear_series_matcher();
    }
    // Return error if no series was found
    if (req.select.columns.empty()) {
      cur->set_error(common::Status::QueryParsingError());
      return;
    }
    if (req.select.columns.at(0).ids.empty()) {
      cur->set_error(common::Status::NotFound());
      return;
    }
    std::unique_ptr<qp::IQueryPlan> query_plan;
    std::tie(status, query_plan) = qp::QueryPlanBuilder::create(req);
    if (status != common::Status::Ok()) {
      cur->set_error(status);
      return;
    }
    // TODO: log query plan if required
    if (proc->start()) {
      QueryPlanExecutor executor;
      executor.execute(*cstore_, std::move(query_plan), *proc);
      proc->stop();
    }
    */
  }
}

void StandaloneDatabase::suggest(StandaloneDatabaseSession* session, InternalCursor* cursor, const char* query) {

}

void StandaloneDatabase::search(StandaloneDatabaseSession* session, InternalCursor* cursor, const char* query) {

}

common::Status StandaloneDatabase::new_database(
    bool ismoving,
    const char* base_file_name,
    const char* metadata_path,
    const char* volumes_path,
    i32 num_volumes,
    u64 volume_size,
    bool allocate) {
  std::string server_metadata_path = std::string(metadata_path) + "/server";
  auto status = ServerDatabase::new_database(
      ismoving,
      base_file_name,
      server_metadata_path.c_str(),
      num_volumes == 0 ? "ExpandableFileStorage" : "FixedSizeFileStorage");
  if (!status.IsOk()) {
    return status;
  }

  std::string worker_metadata_path = std::string(metadata_path) + "/worker";
  status = WorkerDatabase::new_database(
      ismoving,
      base_file_name,
      worker_metadata_path.c_str(),
      volumes_path,
      num_volumes,
      volume_size,
      allocate);
  return status;
}

// Create new column store.
void StandaloneDatabase::recovery_create_new_column(ParamId id) {
  auto cstore = worker_database_->cstore();
  cstore->create_new_column(id);
}

// Update rescue points
void StandaloneDatabase::recovery_update_rescue_points(ParamId id, const std::vector<storage::LogicAddr>& addrs) {
  std::vector<storage::LogicAddr> copy_addrs = addrs;
  worker_database_->update_rescue_point(id, std::move(copy_addrs));
}

// Recovery write.
storage::NBTreeAppendResult StandaloneDatabase::recovery_write(Sample const& sample, bool allow_duplicates) {

}
  
SeriesMatcher* StandaloneDatabase::global_matcher() {
  return server_database_->global_matcher();
}

}  // namespace stdb

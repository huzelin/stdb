/*!
 * \file standalone_database_test.cc
 */
#include "stdb/core/standalone_database.h"

#include "gtest/gtest.h"

#include "stdb/common/apr_utils.h"

namespace stdb {

TEST(TestStandaloneDatabase, Test1) {
  initialize();

  // new database
  StandaloneDatabase::new_database(
      true,
      "test1",
      "/tmp/test_standalone_database/meta",
      "/tmp/test_standalone_database/volumes",
      20,
      1024 * 1024,
      true);
}

TEST(TestStandaloneDatabase, Test2) {
  FineTuneParams params;
  params.input_log_path = "/tmp/test_standalone_database/input_log/";
  std::shared_ptr<Synchronization> sync(new Synchronization());
  std::shared_ptr<SyncWaiter> sync_waiter(new SyncWaiter());

  StandaloneDatabase database(
      "/tmp/test_standalone_database/meta/server/test1.stdb",
      "/tmp/test_standalone_database/meta/worker/test1.stdb",
      params,
      sync,
      sync_waiter,
      true
      );
  database.initialize(params);

  database.close();
}

}  // namespace stdb

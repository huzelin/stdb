#include "stdb/query/plan/query_plan_builder.h"

#include "gtest/gtest.h"

#include "stdb/common/datetime.h"
#include "stdb/query/queryparser.h"

namespace stdb {
namespace qp {

static std::string make_scan_query(Timestamp begin, Timestamp end, OrderBy order) {
  std::stringstream str;
  str << "{ \"select\": \"test\", \"range\": { \"from\": " << "\"" << DateTimeUtil::to_iso_string(begin) << "\"";
  str << ", \"to\": " << "\"" << DateTimeUtil::to_iso_string(end) << "\"" << "},";
  str << "  \"order-by\": " << (order == OrderBy::SERIES ? "\"series\"," : "\"time\",");
  str << "  \"where\": " << "[ { \"tag1\" : \"1\" }, { \"tag1\": \"2\" } ],";
  str << "  \"filter\": " << " { \"test\": { \"gt\": 100, \"lt\": 200 } }";
  str << "}";
  return str.str();
}

static SeriesMatcher global_series_matcher;

void init_series_matcher() {
  static bool inited = false;
  if (inited) return;
  {
    const char* series = "test tag1=1";
    global_series_matcher.add(series , series + strlen(series));
  }
  {
    const char* series = "test tag1=2";
    global_series_matcher.add(series, series + strlen(series));
  }
  {
    const char* series = "test tag1=3";
    global_series_matcher.add(series, series + strlen(series));
  }
  inited = true;
}

TEST(TestQueryPlan, Test_make_scan_query) {
  init_series_matcher();

  std::string query_json = make_scan_query(1136214245999999999ul, 1136215245999999999ul, OrderBy::TIME); 
  common::Status status;
  boost::property_tree::ptree ptree;
  ErrorMsg error_msg;
  std::tie(status, ptree, error_msg) = QueryParser::parse_json(query_json.c_str());
  EXPECT_TRUE(status.IsOk());

  QueryKind query_kind;
  std::tie(status, query_kind, error_msg) = QueryParser::get_query_kind(ptree);
  EXPECT_TRUE(status.IsOk());
  EXPECT_EQ(qp::QueryKind::SELECT, query_kind);

  ReshapeRequest req;
  std::tie(status, req, error_msg) = QueryParser::parse_select_query(ptree, global_series_matcher);
  EXPECT_TRUE(status.IsOk());

  std::unique_ptr<qp::IQueryPlan> query_plan;
  std::tie(status, query_plan) = qp::QueryPlanBuilder::create(req);
  EXPECT_TRUE(status.IsOk());
  LOG(INFO) << "query plan debug_string:" << to_json(query_plan->debug_info());
}

TEST(TestQueryPlan, Test_2) {
  init_series_matcher();


}

}  // namespace qp
}  // namespace stdb

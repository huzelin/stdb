/*!
 * \file queryparser.h
 */
#ifndef STDB_QUERY_QUERYPARSER_H_
#define STDB_QUERY_QUERYPARSER_H_

#include <string>

#include "stdb/query/internal_cursor.h"
#include "stdb/query/queryprocessor_framework.h"
#include "stdb/index/seriesparser.h"
#include "stdb/index/stringpool.h"

namespace stdb {
namespace qp {

enum class QueryKind {
  SELECT,
  SELECT_META,
  JOIN,
  AGGREGATE,
  GROUP_AGGREGATE,
  GROUP_AGGREGATE_JOIN,
  SELECT_EVENTS,
};

extern std::string to_json(boost::property_tree::ptree const& ptree, bool pretty_print = true);

class SeriesRetreiver {
  std::vector<std::string> metric_;
  std::map<std::string, std::vector<std::string>> tags_;
  std::vector<std::string> series_;

 public:
  //! Matches all series names
  SeriesRetreiver();

  //! Matches all series from one metric
  SeriesRetreiver(std::vector<std::string> const& metric);

  //! Add tag-name and tag-value pair
  common::Status add_tag(std::string name, std::string value);

  //! Add tag name and set of possible values
  common::Status add_tags(std::string name, std::vector<std::string> values);

  //! Add full series name
  common::Status add_series_name(std::string name);

  //! Get results
  std::tuple<common::Status, std::vector<ParamId>> extract_ids(PlainSeriesMatcher const& matcher) const;

  std::tuple<common::Status, std::vector<ParamId>> extract_ids(SeriesMatcher const& matcher) const;

  std::tuple<common::Status, std::vector<ParamId>> fuzzy_match(PlainSeriesMatcher const& matcher) const;
};

using ErrorMsg = std::string;

struct QueryParser {

  static std::tuple<common::Status, boost::property_tree::ptree, ErrorMsg> parse_json(const char* query);

  /** Determain type of query.
  */
  static std::tuple<common::Status, QueryKind, ErrorMsg> get_query_kind(boost::property_tree::ptree const& ptree);

  /** Parse query and produce reshape request.
   * @param ptree contains query
   * @returns status and ReshapeRequest
   */
  static std::tuple<common::Status, ReshapeRequest, ErrorMsg> parse_select_query(boost::property_tree::ptree const& ptree, SeriesMatcher const& matcher);

  /** Parse select query (metadata query).
   * @param ptree is a property tree generated from query json
   * @param matcher is a global matcher
   */
  static std::tuple<common::Status, std::vector<ParamId>, ErrorMsg> parse_select_meta_query(boost::property_tree::ptree const& ptree, SeriesMatcher const& matcher);

  /** Parse search query.
   * @param ptree is a property tree generated from query json
   * @param matcher is a global matcher
   */
  static std::tuple<common::Status, std::vector<ParamId>, ErrorMsg> parse_search_query(boost::property_tree::ptree const& ptree, SeriesMatcher const& matcher);

  /** Parse events search query.
   * @param ptree is a property tree generated from query json
   * @param matcher is a global matcher
   */
  static std::tuple<common::Status, ReshapeRequest, ErrorMsg> parse_select_events_query(boost::property_tree::ptree const& ptree, const SeriesMatcher &matcher);

  /**
   * @brief Parse suggest query
   * @param ptree is a property tree generated from query json
   * @param matcher is a series matcher object
   */
  static std::tuple<common::Status, std::shared_ptr<PlainSeriesMatcher>, std::vector<ParamId>, ErrorMsg> parse_suggest_query(boost::property_tree::ptree const& ptree, SeriesMatcher const& matcher);

  /** Parse aggregate query and produce reshape request.
  */
  static std::tuple<common::Status, ReshapeRequest, ErrorMsg> parse_aggregate_query(
      boost::property_tree::ptree const& ptree,
      SeriesMatcher const& matcher);

  /** Parse join query and create `reshape` request for column-store.
  */
  static std::tuple<common::Status, ReshapeRequest, ErrorMsg> parse_join_query(
      boost::property_tree::ptree const& ptree,
      SeriesMatcher const& matcher);

  /**
   * Parse group-aggregate query
   * @param ptree is a json query
   * @param matcher is a series matcher
   * @return status and request object
   */
  static std::tuple<common::Status, ReshapeRequest, ErrorMsg> parse_group_aggregate_query(boost::property_tree::ptree const& ptree,
                                                                                          SeriesMatcher const& matcher);

  /**
   * Parse group-aggregate-join query
   * @param ptree is a json query
   * @param matcher is a series matcher
   * @return status and request object
   */
  static std::tuple<common::Status, ReshapeRequest, ErrorMsg> parse_group_aggregate_join_query(boost::property_tree::ptree const& ptree,
                                                                                               SeriesMatcher const& matcher);

  /** Parse stream processing pipeline.
   * @param ptree contains query
   * @returns vector of Nodes in proper order
   */
  static std::tuple<common::Status, std::vector<std::shared_ptr<Node> >, ErrorMsg> parse_processing_topology(
      boost::property_tree::ptree const& ptree,
      InternalCursor* cursor,
      const ReshapeRequest& req);
};

}  // namespace qp
}  // namespace stdb

#endif  // STDB_QUERY_QUERYPARSER_H_

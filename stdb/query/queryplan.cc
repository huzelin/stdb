/*!
 * \file queryplan.cc
 */
#include "stdb/query/queryplan.h"

#include "stdb/query/plan/aggregate_combiner.h"
#include "stdb/query/plan/aggregate.h"
#include "stdb/query/plan/aggregate_processing_step.h"
#include "stdb/query/plan/chain.h"
#include "stdb/query/plan/filter_processing_step.h"
#include "stdb/query/plan/group_aggregate_combiner.h"
#include "stdb/query/plan/group_aggregate_filter_processing_step.h"
#include "stdb/query/plan/group_aggregate_processing_step.h"
#include "stdb/query/plan/join.h"
#include "stdb/query/plan/materialization_step.h"
#include "stdb/query/plan/mergeby.h"
#include "stdb/query/plan/processing_prelude.h"
#include "stdb/query/plan/scan_events_processing_step.h"
#include "stdb/query/plan/scan_processing_step.h"
#include "stdb/query/plan/series_order_aggregate.h"
#include "stdb/query/plan/time_order_aggregate.h"

namespace stdb {
namespace qp {

struct TwoStepQueryPlan : IQueryPlan {
  std::unique_ptr<ProcessingPrelude> prelude_;
  std::unique_ptr<MaterializationStep> mater_;
  std::unique_ptr<ColumnMaterializer> column_;

  template<class T1, class T2>
  TwoStepQueryPlan(T1&& t1, T2&& t2)
        : prelude_(std::forward<T1>(t1))
        , mater_(std::forward<T2>(t2)) { }

  boost::property_tree::ptree debug_info() const override {
    boost::property_tree::ptree tree;
    tree.add("name", "TwoStepQueryPlan");
    tree.add_child("processing_prelude", prelude_->debug_info());
    tree.add_child("materialization_step", mater_->debug_info());
    return tree;
  }

  common::Status execute(const ColumnStore &cstore) {
    auto status = prelude_->apply(cstore);
    if (status != common::Status::Ok()) {
      return status;
    }
    status = mater_->apply(prelude_.get());
    if (status != common::Status::Ok()) {
      return status;
    }
    return mater_->extract_result(&column_);
  }

  std::tuple<common::Status, size_t> read(u8 *dest, size_t size) {
    if (!column_) {
      LOG(FATAL) << "Successful execute step required";
    }
    return column_->read(dest, size);
  }
};

// ----------- Query plan builder ------------ //
static bool filtering_enabled(const std::vector<Filter>& flt) {
  bool enabled = false;
  for (const auto& it: flt) {
    enabled |= it.enabled;
  }
  return enabled;
}

static std::tuple<common::Status, std::vector<ValueFilter>> convert_filters(const std::vector<Filter>& fltlist) {
  std::vector<ValueFilter> result;
  for (const auto& filter: fltlist) {
    ValueFilter flt;
    if (filter.flags & Filter::GT) {
      flt.greater_than(filter.gt);
    } else if (filter.flags & Filter::GE) {
      flt.greater_or_equal(filter.ge);
    }
    if (filter.flags & Filter::LT) {
      flt.less_than(filter.lt);
    } else if (filter.flags & Filter::LE) {
      flt.less_or_equal(filter.le);
    }
    if (!flt.validate()) {
      LOG(ERROR) << "Invalid filter";
      return std::make_tuple(common::Status::BadArg(), std::move(result));
    }
    result.push_back(flt);
  }
  return std::make_tuple(common::Status::Ok(), std::move(result));
}

static std::tuple<common::Status, AggregateFilter> convert_aggregate_filter(const Filter& filter, AggregationFunction fun) {
  AggregateFilter aggflt;
  if (filter.enabled) {
    ValueFilter flt;
    if (filter.flags&Filter::GT) {
      flt.greater_than(filter.gt);
    } else if (filter.flags&Filter::GE) {
      flt.greater_or_equal(filter.ge);
    }
    if (filter.flags&Filter::LT) {
      flt.less_than(filter.lt);
    } else if (filter.flags&Filter::LE) {
      flt.less_or_equal(filter.le);
    }
    if (!flt.validate()) {
      LOG(ERROR) << "Invalid filter";
      return std::make_tuple(common::Status::BadArg(), aggflt);
    }
    switch(fun) {
      case AggregationFunction::MIN:
        aggflt.set_filter(AggregateFilter::MIN, flt);
        break;
      case AggregationFunction::MAX:
        aggflt.set_filter(AggregateFilter::MAX, flt);
        break;
      case AggregationFunction::MEAN:
        aggflt.set_filter(AggregateFilter::AVG, flt);
        break;
      case AggregationFunction::SUM:
        LOG(ERROR) << "Aggregation function 'sum' can't be used with the filter";
        return std::make_tuple(common::Status::BadArg(), aggflt);
      case AggregationFunction::CNT:
        LOG(ERROR) << "Aggregation function 'cnt' can't be used with the filter";
        return std::make_tuple(common::Status::BadArg(), aggflt);
      case AggregationFunction::MIN_TIMESTAMP:
      case AggregationFunction::MAX_TIMESTAMP:
        LOG(ERROR) << "Aggregation function 'MIN(MAX)_TIMESTAMP' can't be used with the filter";
        return std::make_tuple(common::Status::BadArg(), aggflt);
      case AggregationFunction::FIRST_TIMESTAMP:
      case AggregationFunction::LAST_TIMESTAMP:
        LOG(ERROR) << "Aggregation function 'FIRST(LAST)_TIMESTAMP' can't be used with the filter";
        return std::make_tuple(common::Status::BadArg(), aggflt);
      case AggregationFunction::FIRST:
      case AggregationFunction::LAST:
        LOG(ERROR) << "Aggregation function 'FIRST(LAST)' can't be used with the filter";
        return std::make_tuple(common::Status::BadArg(), aggflt);
    };
  }
  return std::make_tuple(common::Status::Ok(), aggflt);
}

static std::tuple<common::Status, std::vector<AggregateFilter>> convert_aggregate_filters(const std::vector<Filter>& fltlist,
                                                                                          const std::vector<AggregationFunction>& funclst) {
  std::vector<AggregateFilter> result;
  if (fltlist.size() != funclst.size()) {
    LOG(ERROR) << "Number of filters doesn't match number of columns";
    return std::make_tuple(common::Status::BadArg(), std::move(result));
  }
  AggregateFilter aggflt;
  for (size_t ix = 0; ix < fltlist.size(); ix++) {
    const auto& filter = fltlist[ix];
    if (!filter.enabled) {
      continue;
    }
    AggregationFunction fun = funclst[ix];
    ValueFilter flt;
    if (filter.flags&Filter::GT) {
      flt.greater_than(filter.gt);
    } else if (filter.flags&Filter::GE) {
      flt.greater_or_equal(filter.ge);
    }
    if (filter.flags&Filter::LT) {
      flt.less_than(filter.lt);
    } else if (filter.flags&Filter::LE) {
      flt.less_or_equal(filter.le);
    }
    if (!flt.validate()) {
      LOG(ERROR) << "Invalid filter";
      return std::make_tuple(common::Status::BadArg(), std::move(result));
    }
    switch(fun) {
      case AggregationFunction::MIN:
        aggflt.set_filter(AggregateFilter::MIN, flt);
        continue;
      case AggregationFunction::MAX:
        aggflt.set_filter(AggregateFilter::MAX, flt);
        continue;
      case AggregationFunction::MEAN:
        aggflt.set_filter(AggregateFilter::AVG, flt);
        continue;
      case AggregationFunction::SUM:
        LOG(ERROR) << "Aggregation function 'sum' can't be used with the filter";
        break;
      case AggregationFunction::CNT:
        LOG(ERROR) << "Aggregation function 'cnt' can't be used with the filter";
        break;
      case AggregationFunction::MIN_TIMESTAMP:
      case AggregationFunction::MAX_TIMESTAMP:
        LOG(ERROR) << "Aggregation function 'MIN(MAX)_TIMESTAMP' can't be used with the filter";
        break;
      case AggregationFunction::FIRST_TIMESTAMP:
      case AggregationFunction::LAST_TIMESTAMP:
        LOG(ERROR) << "Aggregation function 'FIRST(LAST)_TIMESTAMP' can't be used with the filter";
        break;
      case AggregationFunction::FIRST:
      case AggregationFunction::LAST:
        LOG(ERROR) << "Aggregation function 'FIRST(LAST)' can't be used with the filter";
        break;
    };
    return std::make_tuple(common::Status::BadArg(), std::move(result));
  }
  result.push_back(aggflt);
  return std::make_tuple(common::Status::Ok(), std::move(result));
}

/**
 * @brief Layout filters to match columns/ids of the query
 * @param req is a reshape request
 * @return vector of filters and status
 */
static std::tuple<common::Status, std::vector<ValueFilter>> layout_filters(const ReshapeRequest& req) {
  std::vector<ValueFilter> result;

  common::Status s;
  std::vector<ValueFilter> flt;
  std::tie(s, flt) = convert_filters(req.select.filters);
  if (s != common::Status::Ok()) {
    // Bad filter in query
    return std::make_tuple(common::Status::BadArg(), std::move(result));
  }

  if (flt.empty()) {
    // Bad filter in query
    LOG(ERROR) << "Reshape request without filter supplied";
    return std::make_tuple(common::Status::BadArg(), std::move(result));
  }

  // We should duplicate the filters to match the layout of queried data
  for (size_t ixrow = 0; ixrow < req.select.columns.at(0).ids.size(); ixrow++) {
    for (size_t ixcol = 0; ixcol < req.select.columns.size(); ixcol++) {
      const auto& rowfilter = flt.at(ixcol);
      result.push_back(rowfilter);
    }
  }
  return std::make_tuple(common::Status::Ok(), std::move(result));
}

static std::tuple<common::Status, std::vector<AggregateFilter>> layout_aggregate_filters(const ReshapeRequest& req) {
  std::vector<AggregateFilter> result;
  AggregateFilter::Mode common_mode = req.select.filter_rule == FilterCombinationRule::ALL
      ? AggregateFilter::Mode::ALL
      : AggregateFilter::Mode::ANY;
  common::Status s;
  std::vector<AggregateFilter> flt;
  std::tie(s, flt) = convert_aggregate_filters(req.select.filters, req.agg.func);
  if (s != common::Status::Ok()) {
    // Bad filter in query
    return std::make_tuple(common::Status::BadArg(), std::move(result));
  }

  if (flt.empty()) {
    // Bad filter in query
    LOG(ERROR) << "Reshape request without filter supplied";
    return std::make_tuple(common::Status::BadArg(), std::move(result));
  }

  // We should duplicate the filters to match the layout of queried data
  for (size_t ixrow = 0; ixrow < req.select.columns.at(0).ids.size(); ixrow++) {
    for (size_t ixcol = 0; ixcol < req.select.columns.size(); ixcol++) {
      auto& colfilter = flt.at(ixcol);
      colfilter.mode = common_mode;
      result.push_back(colfilter);
    }
  }
  return std::make_tuple(common::Status::Ok(), std::move(result));
}

static std::tuple<common::Status, std::vector<AggregateFilter>> layout_aggregate_join_filters(const ReshapeRequest& req) {
  std::vector<AggregateFilter> result;
  AggregateFilter::Mode common_mode = req.select.filter_rule == FilterCombinationRule::ALL
      ? AggregateFilter::Mode::ALL
      : AggregateFilter::Mode::ANY;
  common::Status s;
  if (req.agg.func.size() != 1) {
    // TODO: support N:N downsampling
    return std::make_tuple(common::Status::BadArg(), std::move(result));
  }
  std::vector<AggregateFilter> colfilters;
  auto downsampling_fn = req.agg.func.front();
  for (auto it: req.select.filters) {
    AggregateFilter flt;
    std::tie(s, flt) = convert_aggregate_filter(it, downsampling_fn);
    if (s != common::Status::Ok()) {
      // Bad filter in query
      return std::make_tuple(common::Status::BadArg(), std::move(result));
    }
    flt.mode = common_mode;
    colfilters.push_back(std::move(flt));
  }

  if (colfilters.empty() || colfilters.size() != req.select.columns.size()) {
    // Bad filter in query
    LOG(ERROR) << "Reshape request without filter supplied";
    return std::make_tuple(common::Status::BadArg(), std::move(result));
  }

  // We should duplicate the filters to match the layout of queried data
  for (size_t ixrow = 0; ixrow < req.select.columns.at(0).ids.size(); ixrow++) {
    for (size_t ixcol = 0; ixcol < req.select.columns.size(); ixcol++) {
      auto& colfilter = colfilters.at(ixcol);
      result.push_back(colfilter);
    }
  }

  return std::make_tuple(common::Status::Ok(), std::move(result));
}

static std::tuple<common::Status, std::unique_ptr<IQueryPlan>> scan_query_plan(ReshapeRequest const& req) {
  // Hardwired query plan for scan query
  // Tier1
  // - List of range scan/filter operators
  //
  // Tier2
  // - If group-by is enabled:
  //   - Transform ids and matcher (generate new names)
  //   - Add merge materialization step (series or time order, depending on the
  //     order-by clause.
  // - Otherwise
  //   - If oreder-by is series add chain materialization step.
  //   - Otherwise add merge materializer.
  std::unique_ptr<IQueryPlan> result;

  if (req.agg.enabled || req.select.columns.size() != 1) {
    return std::make_tuple(common::Status::BadArg(), std::move(result));
  }

  std::unique_ptr<ProcessingPrelude> t1stage;
  if (filtering_enabled(req.select.filters)) {
    // Scan query can only have one filter
    common::Status s;
    std::vector<ValueFilter> flt;
    std::tie(s, flt) = layout_filters(req);
    if (s != common::Status::Ok()) {
      return std::make_tuple(common::Status::BadArg(), std::move(result));
    }
    t1stage.reset(new FilterProcessingStep(req.select.begin,
                                           req.select.end,
                                           flt,
                                           req.select.columns.at(0).ids));
  } else {
    t1stage.reset(new ScanProcessingStep  (req.select.begin,
                                           req.select.end,
                                           req.select.columns.at(0).ids));
  }

  std::unique_ptr<MaterializationStep> t2stage;
  if (req.group_by.enabled) {
    std::vector<ParamId> ids;
    for(auto id: req.select.columns.at(0).ids) {
      auto it = req.group_by.transient_map.find(id);
      if (it != req.group_by.transient_map.end()) {
        ids.push_back(it->second);
      }
    }
    if (req.order_by == OrderBy::SERIES) {
      t2stage.reset(new MergeBy<OrderBy::SERIES>(std::move(ids)));
    } else {
      t2stage.reset(new MergeBy<OrderBy::TIME>(std::move(ids)));
    }
  } else {
    auto ids = req.select.columns.at(0).ids;
    if (req.order_by == OrderBy::SERIES) {
      t2stage.reset(new Chain<>(std::move(ids)));
    } else {
      t2stage.reset(new MergeBy<OrderBy::TIME>(std::move(ids)));
    }
  }

  result.reset(new TwoStepQueryPlan(std::move(t1stage), std::move(t2stage)));
  return std::make_tuple(common::Status::Ok(), std::move(result));
}

static std::tuple<common::Status, std::unique_ptr<IQueryPlan>> scan_events_query_plan(ReshapeRequest const& req) {
  // Hardwired query plan for scan query
  // Tier1
  // - List of range scan/filter operators
  // Tier2
  // - If group-by is enabled:
  //   - Transform ids and matcher (generate new names)
  //   - Add merge materialization step (series or time order, depending on the
  //     order-by clause.
  // - Otherwise
  //   - If oreder-by is series add chain materialization step.
  //   - Otherwise add merge materializer.

  std::unique_ptr<IQueryPlan> result;

  if (req.agg.enabled || req.select.columns.size() != 1) {
    return std::make_tuple(common::Status::BadArg(), std::move(result));
  }

  std::unique_ptr<ProcessingPrelude> t1stage;
  if (req.select.event_body_regex.empty()) {
    // Regex filter is not set
    t1stage.reset(new ScanEventsProcessingStep  (req.select.begin,
                                                 req.select.end,
                                                 req.select.columns.at(0).ids));
  } else {
    t1stage.reset(new ScanEventsProcessingStep  (req.select.begin,
                                                 req.select.end,
                                                 req.select.event_body_regex,
                                                 req.select.columns.at(0).ids));
  }

  std::unique_ptr<MaterializationStep> t2stage;
  if (req.group_by.enabled) {
    std::vector<ParamId> ids;
    for(auto id: req.select.columns.at(0).ids) {
      auto it = req.group_by.transient_map.find(id);
      if (it != req.group_by.transient_map.end()) {
        ids.push_back(it->second);
      }
    }
    if (req.order_by == OrderBy::SERIES) {
      t2stage.reset(new MergeBy<OrderBy::SERIES, BinaryDataOperator>(std::move(ids)));
    } else {
      t2stage.reset(new MergeBy<OrderBy::TIME, BinaryDataOperator>(std::move(ids)));
    }
  } else {
    auto ids = req.select.columns.at(0).ids;
    if (req.order_by == OrderBy::SERIES) {
      t2stage.reset(new Chain<BinaryDataOperator>(std::move(ids)));
    } else {
      t2stage.reset(new MergeBy<OrderBy::TIME, BinaryDataOperator>(std::move(ids)));
    }
  }

  result.reset(new TwoStepQueryPlan(std::move(t1stage), std::move(t2stage)));
  return std::make_tuple(common::Status::Ok(), std::move(result));
}

static std::tuple<common::Status, std::unique_ptr<IQueryPlan>> aggregate_query_plan(ReshapeRequest const& req) {
  // Hardwired query plan for aggregate query
  // Tier1
  // - List of aggregate operators
  // Tier2
  // - If group-by is enabled:
  //   - Transform ids and matcher (generate new names)
  //   - Add merge materialization step (series or time order, depending on the
  //     order-by clause.
  // - Otherwise
  //   - If oreder-by is series add chain materialization step.
  //   - Otherwise add merge materializer.

  std::unique_ptr<IQueryPlan> result;

  if (req.order_by == OrderBy::TIME || req.agg.enabled == false || req.agg.step != 0) {
    return std::make_tuple(common::Status::BadArg(), std::move(result));
  }

  std::unique_ptr<ProcessingPrelude> t1stage;
  t1stage.reset(new AggregateProcessingStep(req.select.begin, req.select.end, req.select.columns.at(0).ids));

  std::unique_ptr<MaterializationStep> t2stage;
  if (req.group_by.enabled) {
    std::vector<ParamId> ids;
    for(auto id: req.select.columns.at(0).ids) {
      auto it = req.group_by.transient_map.find(id);
      if (it != req.group_by.transient_map.end()) {
        ids.push_back(it->second);
      }
    }
    t2stage.reset(new AggregateCombiner(std::move(ids), req.agg.func));
  } else {
    auto ids = req.select.columns.at(0).ids;
    t2stage.reset(new Aggregate(std::move(ids), req.agg.func));
  }

  result.reset(new TwoStepQueryPlan(std::move(t1stage), std::move(t2stage)));
  return std::make_tuple(common::Status::Ok(), std::move(result));
}

static std::tuple<common::Status, std::unique_ptr<IQueryPlan>> join_query_plan(ReshapeRequest const& req) {
  std::unique_ptr<IQueryPlan> result;

  // Group-by is not supported currently
  if (req.group_by.enabled || req.select.columns.size() < 2) {
    return std::make_tuple(common::Status::BadArg(), std::move(result));
  }

  if (req.agg.enabled == false) {
    std::unique_ptr<ProcessingPrelude> t1stage;
    std::vector<ParamId> t1ids;
    int cardinality = static_cast<int>(req.select.columns.size());
    for (size_t i = 0; i < req.select.columns.at(0).ids.size(); i++) {
      for (int c = 0; c < cardinality; c++) {
        t1ids.push_back(req.select.columns.at(static_cast<size_t>(c)).ids.at(i));
      }
    }
    if (filtering_enabled(req.select.filters)) {
      // Join query should have many filters (filter per metric)
      common::Status s;
      std::vector<ValueFilter> flt;
      std::tie(s, flt) = layout_filters(req);
      if (s != common::Status::Ok()) {
        return std::make_tuple(common::Status::BadArg(), std::move(result));
      }
      t1stage.reset(new FilterProcessingStep(req.select.begin,
                                             req.select.end,
                                             flt,
                                             std::move(t1ids)));
    } else {
      t1stage.reset(new ScanProcessingStep(req.select.begin, req.select.end, std::move(t1ids)));
    }

    std::unique_ptr<MaterializationStep> t2stage;

    if (req.group_by.enabled) {
      return std::make_tuple(common::Status::BadArg(), std::move(result));
    } else {
      t2stage.reset(new Join(req.select.columns.at(0).ids, cardinality, req.order_by, req.select.begin, req.select.end));
    }

    result.reset(new TwoStepQueryPlan(std::move(t1stage), std::move(t2stage)));
    return std::make_tuple(common::Status::Ok(), std::move(result));
  } else {
    std::unique_ptr<ProcessingPrelude> t1stage;
    std::vector<ParamId> t1ids;
    int cardinality = static_cast<int>(req.select.columns.size());
    for (size_t i = 0; i < req.select.columns.at(0).ids.size(); i++) {
      for (int c = 0; c < cardinality; c++) {
        t1ids.push_back(req.select.columns.at(static_cast<size_t>(c)).ids.at(i));
      }
    }
    if (filtering_enabled(req.select.filters)) {
      // Scan query can only have one filter
      common::Status s;
      std::vector<AggregateFilter> flt;
      std::tie(s, flt) = layout_aggregate_join_filters(req);
      if (s != common::Status::Ok()) {
        return std::make_tuple(common::Status::BadArg(), std::move(result));
      }
      t1stage.reset(new GroupAggregateFilterProcessingStep(req.select.begin,
                                                           req.select.end,
                                                           req.agg.step,
                                                           flt,
                                                           std::move(t1ids),
                                                           req.agg.func.front()));
    } else {
      t1stage.reset(new GroupAggregateProcessingStep(req.select.begin,
                                                     req.select.end,
                                                     req.agg.step,
                                                     std::move(t1ids),
                                                     req.agg.func.front()
                                                    ));
    }

    std::unique_ptr<MaterializationStep> t2stage;

    if (req.group_by.enabled) {
      return std::make_tuple(common::Status::BadArg(), std::move(result));
    } else {
      t2stage.reset(new Join(req.select.columns.at(0).ids, cardinality, req.order_by, req.select.begin, req.select.end));
    }

    result.reset(new TwoStepQueryPlan(std::move(t1stage), std::move(t2stage)));
    return std::make_tuple(common::Status::Ok(), std::move(result));
  }
}

static std::tuple<common::Status, std::unique_ptr<IQueryPlan>> group_aggregate_query_plan(ReshapeRequest const& req) {
  // Hardwired query plan for group aggregate query
  // Tier1
  // - List of group aggregate operators
  // Tier2
  // - If group-by is enabled:
  //   - Transform ids and matcher (generate new names)
  //   - Add merge materialization step (series or time order, depending on the
  //     order-by clause.
  // - Otherwise
  //   - If oreder-by is series add chain materialization step.
  //   - Otherwise add merge materializer.
  std::unique_ptr<IQueryPlan> result;

  if (!req.agg.enabled || req.agg.step == 0) {
    return std::make_tuple(common::Status::BadArg(), std::move(result));
  }

  std::unique_ptr<ProcessingPrelude> t1stage;
  if (filtering_enabled(req.select.filters)) {
    // Scan query can only have one filter
    common::Status s;
    std::vector<AggregateFilter> flt;
    std::tie(s, flt) = layout_aggregate_filters(req);
    if (s != common::Status::Ok()) {
      return std::make_tuple(common::Status::BadArg(), std::move(result));
    }
    t1stage.reset(new GroupAggregateFilterProcessingStep(req.select.begin,
                                                         req.select.end,
                                                         req.agg.step,
                                                         flt,
                                                         req.select.columns.at(0).ids));
  } else {
    t1stage.reset(new GroupAggregateProcessingStep(req.select.begin,
                                                   req.select.end,
                                                   req.agg.step,
                                                   req.select.columns.at(0).ids));
  }

  std::unique_ptr<MaterializationStep> t2stage;
  if (req.group_by.enabled) {
    std::vector<ParamId> ids;
    for(auto id: req.select.columns.at(0).ids) {
      auto it = req.group_by.transient_map.find(id);
      if (it != req.group_by.transient_map.end()) {
        ids.push_back(it->second);
      }
    }
    if (req.order_by == OrderBy::SERIES) {
      t2stage.reset(new GroupAggregateCombiner<OrderBy::SERIES>(std::move(ids), req.agg.func));
    } else {
      t2stage.reset(new GroupAggregateCombiner<OrderBy::TIME>(ids, req.agg.func));
    }
  } else {
    if (req.order_by == OrderBy::SERIES) {
      t2stage.reset(new SeriesOrderAggregate(req.select.columns.at(0).ids, req.agg.func));
    } else {
      t2stage.reset(new TimeOrderAggregate(req.select.columns.at(0).ids, req.agg.func));
    }
  }

  result.reset(new TwoStepQueryPlan(std::move(t1stage), std::move(t2stage)));
  return std::make_tuple(common::Status::Ok(), std::move(result));
}

std::tuple<common::Status, std::unique_ptr<IQueryPlan>> QueryPlanBuilder::create(const ReshapeRequest& req) {
  if (req.agg.enabled && req.agg.step == 0) {
    // Aggregate query
    return aggregate_query_plan(req);
  } else if (req.agg.enabled && req.agg.step != 0) {
    // Group aggregate query
    if (req.select.columns.size() == 1) {
      return group_aggregate_query_plan(req);
    } else {
      return join_query_plan(req);
    }
  } else if (req.agg.enabled == false && req.select.columns.size() > 1) {
    // Join query
    return join_query_plan(req);
  } else if (req.select.events) {
    // Select events
    return scan_events_query_plan(req);
  }
  // Select metrics
  return scan_query_plan(req);
}

void QueryPlanExecutor::execute(const storage::ColumnStore& cstore, std::unique_ptr<qp::IQueryPlan>&& iter, qp::IStreamProcessor& qproc) {
  common::Status status = iter->execute(cstore);
  if (status != common::Status::Ok()) {
    LOG(ERROR) << "Query plan error " << status.ToString();
    qproc.set_error(status);
    return;
  }
  const size_t dest_size = 0x1000;
  std::vector<u8> dest;
  dest.resize(dest_size);
  while (status == common::Status::Ok()) {
    size_t size;
    // This is OK because normal query (aggregate or select) will write fixed size samples with size = sizeof(Sample).
    //
    std::tie(status, size) = iter->read(reinterpret_cast<u8*>(dest.data()), dest_size);
    if (status != common::Status::Ok() && (status != common::Status::NoData() && status != common::Status::Unavailable())) {
      LOG(ERROR) << "Iteration error " << status.ToString();
      qproc.set_error(status);
      return;
    }

    size_t pos = 0;
    while (pos < size) {
      Sample const* sample = reinterpret_cast<Sample const*>(dest.data() + pos);
      if (!qproc.put(*sample)) {
        LOG(INFO) << "Iteration stopped by client";
        return;
      }
      pos += sample->payload.size;
    }
  }
}

}  // namespace qp
}  // namespaces stdb

package(default_visibility = ["//visibility:public"])

load("@rules_cc//cc:defs.bzl", "cc_library", "cc_proto_library", "cc_test")
load("@rules_proto//proto:defs.bzl", "proto_library")

cc_library(
  name = "query",
  srcs = [
    "queryparser.cc",
    "queryprocessor.cc",
    "plan/query_plan.cc",
    "plan/query_plan_builder.cc",
    "queryprocessor_framework.cc",
    "query_processing/absolute.cc", 
    "query_processing/eval.cc", 
    "query_processing/filterbyid.cc",
    "query_processing/limiter.cc",
    "query_processing/math.cc",
    "query_processing/rate.cc",
    "query_processing/scale.cc",
    "query_processing/sliding_window.cc",
    "query_processing/spacesaver.cc",
    "query_processing/top.cc",
  ],
  hdrs = [
    "queryparser.h",
    "external_cursor.h",
    "internal_cursor.h",
    "queryprocessor.h",
    "queryprocessor_framework.h",
    "query_processing/absolute.h", 
    "query_processing/eval.h",
    "query_processing/filterbyid.h",
    "query_processing/limiter.h",
    "query_processing/math.h",
    "query_processing/rate.h",
    "query_processing/scale.h",
    "query_processing/sliding_window.h",
    "query_processing/spacesaver.h",
    "query_processing/top.h",
    "plan/query_plan_builder.h",
    "plan/query_plan.h",
    "plan/two_step_query_plan.h",
    "steps/aggregate_combiner.h",
    "steps/aggregate.h",
    "steps/aggregate_processing_step.h",
    "steps/chain.h",
    "steps/filter_processing_step.h",
    "steps/group_aggregate_combiner.h",
    "steps/group_aggregate_filter_processing_step.h",
    "steps/group_aggregate_processing_step.h",
    "steps/join.h",
    "steps/materialization_step.h",
    "steps/mergeby.h",
    "steps/processing_prelude.h",
    "steps/scan_events_processing_step.h",
    "steps/scan_processing_step.h",
    "steps/series_order_aggregate.h",
    "steps/time_order_aggregate.h",
  ],
  alwayslink = 1,
  copts = [
    "-std=c++14",
    "-fopenmp",
  ],
  deps = [
    "//stdb/storage:storage",
    "//stdb/index:index",
    "@muparser//:libmuparser",
    "@com_github_boost_property_tree//:property_tree",
    "@com_github_boost_circular_buffer//:circular_buffer",
    "@com_github_boost_exception//:exception",
  ],
)

cc_test(
  name = "query_plan_builder_test",
  srcs = ["plan/query_plan_builder_test.cc"],
  deps = [
    "@gtest//:gtest",
    "@gtest//:gtest_main",
    "//stdb/query:query",
  ],
)

cc_test(
  name = "queryparser_test",
  srcs = ["queryparser_test.cc"],
  deps = [
    "@gtest//:gtest",
    "@gtest//:gtest_main",
    "//stdb/query:query",
  ],
)

cc_test(
  name = "queryprocessor_framework_test",
  srcs = ["queryprocessor_framework_test.cc"],
  deps = [
    "@gtest//:gtest",
    "@gtest//:gtest_main",
    "//stdb/query:query",
  ],
)

cc_test(
  name = "eval_test",
  srcs = ["query_processing/eval_test.cc"],
  deps = [
    "@gtest//:gtest",
    "@gtest//:gtest_main",
    "//stdb/query:query",
  ],
)

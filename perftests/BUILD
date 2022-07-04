package(default_visibility = ["//visibility:public"])

load("@rules_cc//cc:defs.bzl", "cc_library", "cc_proto_library", "cc_test")

cc_binary(
  name = "perf_rtree",
  srcs = [
    "perf_rtree.cc",
  ],
  copts = [
    "-std=c++14",
  ],
  deps = [
    "//stdb/index:index",
  ],
)


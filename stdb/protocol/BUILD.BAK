package(default_visibility = ["//visibility:public"])

load("@rules_cc//cc:defs.bzl", "cc_library", "cc_proto_library", "cc_test")

cc_library(
  name = "protocol",
  srcs = [
    "stream.cc",
    "resp.cc",
    "protocolparser.cc",
  ],
  hdrs = [
    "stream.h",
    "resp.h",
    "protocolparser.h",
  ],
  alwayslink = 1,
  copts = [
  ],
  deps = [
    "@com_github_boost_algorithm//:algorithm",
    "//stdb/common:common",
    "//stdb/core:core",
  ],
)

cc_test(
  name = "stream_test",
  srcs = ["stream_test.cc"],
  deps = [
    "@gtest//:gtest",
    "@gtest//:gtest_main",
    ":protocol",
  ],
)

cc_test(
  name = "resp_test",
  srcs = ["resp_test.cc"],
  deps = [
    "@gtest//:gtest",
    "@gtest//:gtest_main",
    ":protocol",
  ],
)

cc_test(
  name = "protocolparser_test",
  srcs = ["protocolparser_test.cc"],
  deps = [
    "@gtest//:gtest",
    "@gtest//:gtest_main",
    ":protocol",
  ],
)
  

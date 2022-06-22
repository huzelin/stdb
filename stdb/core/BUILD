package(default_visibility = ["//visibility:public"])

load("@rules_cc//cc:defs.bzl", "cc_library", "cc_proto_library", "cc_test")
load("@rules_proto//proto:defs.bzl", "proto_library")

cc_library(
  name = "core",
  srcs = [
    "stdb.cc",
    "cursor.cc",
    "metadatastorage.cc",
    "storage.cc",
  ],
  hdrs = [
    "stdb.h",
    "cursor.h",
    "metadatastorage.h",
    "storage.h",
  ],
  alwayslink = 1,
  copts = [
    "-std=c++14",
    "-fopenmp",
  ],
  linkopts = [
    "-ldl",
  ],
  deps = [
    "//stdb/storage:storage",
    "//stdb/index:index",
    "//stdb/query:query",
    "@com_github_boost_thread//:thread",
    "@sqlite//:libsqlite",
  ],
)

cc_test(
  name = "metadatastorage_test",
  srcs = ["metadatastorage_test.cc"],
  deps = [
    "@gtest//:gtest",
    "@gtest//:gtest_main",
    ":core",
  ],
)

cc_test(
  name = "storage_test",
  srcs = ["storage_test.cc"],
  deps = [
    "@gtest//:gtest",
    "@gtest//:gtest_main",
    ":core",
  ],
)

cc_test(
  name = "stdb_test",
  srcs = ["stdb_test.cc"],
  deps = [
    "@gtest//:gtest",
    "@gtest//:gtest_main",
    ":core",
  ],
)

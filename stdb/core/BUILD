package(default_visibility = ["//visibility:public"])

load("@rules_cc//cc:defs.bzl", "cc_library", "cc_proto_library", "cc_test")
load("@rules_proto//proto:defs.bzl", "proto_library")

cc_library(
  name = "core",
  srcs = [
    "controller.cc",
    "database.cc",
    "recovery_visitor.cc",
    "server_database.cc",
    "worker_database.cc",
    "standalone_database.cc",
    "standalone_database_session.cc",

    #"stdb.cc",
    "cursor.cc",
    "metadatastorage.cc",
    "storage.cc",
    "storage_api.cc",
  ],
  hdrs = [
    "controller.h",
    "database.h",
    "database_session.h",
    "recovery_visitor.h",
    "server_database.h",
    "worker_database.h",
    "standalone_database.h",
    "standalone_database_session.h",
    "session_waiter.h",

    #"stdb.h",
    "cursor.h",
    "metadatastorage.h",
    "storage.h",
    "storage_api.h",
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
    "//stdb/metastorage:metastorage",
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
  name = "standalone_database_test",
  srcs = ["standalone_database_test.cc"],
  deps = [
    "@gtest//:gtest",
    "@gtest//:gtest_main",
    ":core",
  ],
)

cc_test(
  name = "controller_test",
  srcs = ["controller_test.cc"],
  deps = [
    "@gtest//:gtest",
    "@gtest//:gtest_main",
    ":core",
  ],
)

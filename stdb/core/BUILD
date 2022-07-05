package(default_visibility = ["//visibility:public"])

load("@rules_cc//cc:defs.bzl", "cc_library", "cc_proto_library", "cc_test")
load("@rules_proto//proto:defs.bzl", "proto_library")

cc_library(
  name = "core",
  srcs = [
    "worker_meta_storage.cc",
    "server_meta_storage.cc",
    #"stdb.cc",
    "cursor.cc",
    "metadatastorage.cc",
    "storage.cc",
    "storage_api.cc",
  ],
  hdrs = [
    "synchronization.h",
    "worker_meta_storage.h",
    "server_meta_storage.h",
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
    "@com_github_boost_thread//:thread",
    "@sqlite//:libsqlite",
  ],
)

cc_test(
  name = "worker_meta_storage_test",
  srcs = ["worker_meta_storage_test.cc"],
  deps = [
    "@gtest//:gtest",
    "@gtest//:gtest_main",
    ":core",
  ],
)

cc_test(
  name = "server_meta_storage_test",
  srcs = ["server_meta_storage_test.cc"],
  deps = [
    "@gtest//:gtest",
    "@gtest//:gtest_main",
    ":core",
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

#cc_test(
#  name = "stdb_test",
#  srcs = ["stdb_test.cc"],
#  deps = [
#    "@gtest//:gtest",
#    "@gtest//:gtest_main",
#    ":core",
#  ],
#)

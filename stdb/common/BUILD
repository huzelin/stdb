package(default_visibility = ["//visibility:public"])

load("@rules_cc//cc:defs.bzl", "cc_library", "cc_proto_library", "cc_test")

cc_library(
  name = "common",
  srcs = [
    "crc32c.cc",
    "datetime.cc",
    "exception.cc",
    "hash.cc",
    "mmapfile.cc",
    "memorymappedfile.cc",
    "proto_configure.cc",
    "rwlock.cc",
    "signal_handler.cc",
    "strutil.cc",
  ],
  hdrs = [
    "basic.h",
    "blocking_queue.h",
    "crc32c.h",
    "datetime.h",
    "exception.h",
    "file_utils.h",
    "hash.h",
    "logging.h",
    "mmapfile.h",
    "memorymappedfile.h",
    "proto_configure.h",
    "rwlock.h",
    "status.h",
    "singleton.h",
    "thread_local.h",
    "timer.h",
    "signal_handler.h",
    "strutil.h",
  ],
  alwayslink = 1,
  copts = [
    "-DBOOST_DATE_TIME_POSIX_TIME_STD_CONFIG",
  ],
  deps = [
    "@sqlite//:libsqlite",
    "@expat//:libexpat",
    "@apr//:libapr",
    "@glog",
    "@com_google_protobuf//:protobuf",
    "@robin-map//:librobin-map",
    "@com_github_boost_date_time//:date_time",
  ],
)

cc_test(
  name = "status_test",
  srcs = ["status_test.cc"],
  deps = [
    "@gtest//:gtest",
    "@gtest//:gtest_main",
    ":common",
  ],
)

cc_test(
  name = "rwlock_test",
  srcs = ["rwlock_test.cc"],
  deps = [
    "@gtest//:gtest",
    "@gtest//:gtest_main",
    ":common",
  ],
)

cc_test(
  name = "hash_test",
  srcs = ["hash_test.cc"],
  deps = [
    "@gtest//:gtest",
    "@gtest//:gtest_main",
    ":common",
  ],
)

cc_test(
  name = "datetime_test",
  srcs = ["datetime_test.cc"],
  deps = [
    "@gtest//:gtest",
    "@gtest//:gtest_main",
    ":common",
  ],
)

cc_test(
  name = "memorymappedfile_test",
  srcs = ["memorymappedfile_test.cc"],
  deps = [
    "@gtest//:gtest",
    "@gtest//:gtest_main",
    ":common",
  ],
)

cc_test(
  name = "crc32c_test",
  srcs = ["crc32c_test.cc"],
  deps = [
    "@gtest//:gtest",
    "@gtest//:gtest_main",
    ":common",
  ],
)

cc_test(
  name = "strutil_test",
  srcs = ["strutil_test.cc"],
  deps = [
    "@gtest//:gtest",
    "@gtest//:gtest_main",
    ":common",
  ],
)


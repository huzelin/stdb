package(default_visibility = ["//visibility:public"])

load("@rules_cc//cc:defs.bzl", "cc_library", "cc_proto_library", "cc_test")

cc_library(
  name = "dataserver",
  srcs = [
    "tcp_server.cc",
  ],
  hdrs = [
    "tcp_server.h",
    "server.h",
  ],
  alwayslink = 1,
  copts = [
  ],
  deps = [
    "@com_github_boost_asio//:asio",
    "//stdb/core:core",
    "//stdb/protocol:protocol",
  ],
)

cc_binary(
  name = "stdb_data_server",
  srcs = ["main.cc"],
  deps = [":dataserver"],
) 

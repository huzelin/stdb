package(default_visibility = ["//visibility:public"])

load("@rules_cc//cc:defs.bzl", "cc_library", "cc_proto_library", "cc_test")
load("@rules_proto//proto:defs.bzl", "proto_library")

proto_library(
  name = "ds_config_proto",
  srcs = ["ds_config.proto"],
)

cc_proto_library(
   name = "ds_config_cc_proto",
   deps = [":ds_config_proto"],
)

cc_library(
  name = "dataserver",
  srcs = [
    "tcp_server.cc",
    "configer.cc",
  ],
  hdrs = [
    "tcp_server.h",
    "server.h",
    "configer.h",
  ],
  alwayslink = 1,
  copts = [
  ],
  deps = [
    "@com_github_boost_asio//:asio",
    "//stdb/core:core",
    "//stdb/protocol:protocol",
    "//stdb/common:common",
    ":ds_config_cc_proto",
  ],
)

cc_binary(
  name = "stdb_data_server",
  srcs = ["main.cc"],
  deps = [
    ":dataserver",
    "@com_github_boost_program_options//:program_options",
  ],
) 

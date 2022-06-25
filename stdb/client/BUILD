package(default_visibility = ["//visibility:public"])

load("@rules_cc//cc:defs.bzl", "cc_library", "cc_proto_library", "cc_test")

cc_binary(
  name = "stdb-cli",
  srcs = [
    "cli.cc",
  ],
  copts = [
  ],
  deps = [
    "//stdb/common:common",
    "//thirdparty/linenoise:linenoise",
    "@com_github_boost_program_options//:program_options",
  ],
)
  

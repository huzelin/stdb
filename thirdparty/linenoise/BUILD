package(default_visibility = ["//visibility:public"])

load("@rules_cc//cc:defs.bzl", "cc_library", "cc_proto_library", "cc_test")

cc_library(
  name = "linenoise",
  srcs = [
    "linenoise.c",
  ],
  hdrs = [
    "linenoise.h",
  ],
  alwayslink = 1,
  copts = [
  ],
  deps = [
  ],
)

cc_binary(
  name = "example",
  srcs = ["example.c"],
  deps = [
    ":linenoise",
  ],
) 

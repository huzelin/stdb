package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
  name = "type_index",
  includes = [
    "include/",
  ],
  hdrs = glob([
    "include/boost/**/*.hpp",
  ]),
  srcs = [
  ],
  deps = [
    "@com_github_boost_container_hash//:container_hash",
  ],
)

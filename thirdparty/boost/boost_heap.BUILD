package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
  name = "heap",
  includes = [
      "include/",
  ],
  hdrs = glob([
      "include/boost/**/*.hpp",
  ]),
  srcs = [
  ],
  deps = [
      "@com_github_boost_assert//:assert",
      "@com_github_boost_array//:array",
      "@com_github_boost_core//:core",
      "@com_github_boost_detail//:detail",
      "@com_github_boost_static_assert//:static_assert",
      "@com_github_boost_throw_exception//:throw_exception",
      "@com_github_boost_concept_check//:concept_check",
      "@com_github_boost_preprocessor//:preprocessor",
      "@com_github_boost_type_traits//:type_traits",
      "@com_github_boost_intrusive//:intrusive",
      "@com_github_boost_move//:move",
      "@com_github_boost_iterator//:iterator",
      "@com_github_boost_parameter//:parameter",
      "@com_github_boost_mp11//:mp11",
  ]
)

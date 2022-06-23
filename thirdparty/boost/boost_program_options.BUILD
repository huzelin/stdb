package(default_visibility = ["//visibility:public"])

licenses(["notice"])

cc_library(
  name = "program_options",
  includes = [
    "include/",
  ],
  hdrs = glob([
    "include/boost/**/*.hpp",
  ]),
  srcs = glob([
    "src/*.cpp",
  ]),
  deps = [
      "@com_github_boost_tokenizer//:tokenizer",
      "@com_github_boost_smart_ptr//:smart_ptr",
      "@com_github_boost_lexical_cast//:lexical_cast",
      "@com_github_boost_function//:function",
      "@com_github_boost_any//:any",
      "@com_github_boost_assert//:assert",
      "@com_github_boost_concept_check//:concept_check",
      "@com_github_boost_config//:config",
      "@com_github_boost_iterator//:iterator",
      "@com_github_boost_preprocessor//:preprocessor",
      "@com_github_boost_type_traits//:type_traits",
      "@com_github_boost_utility//:utility",
  ]
)

project(stdb)

cmake_minimum_required(VERSION 3.11)

option(test "Build all tests." ON)
enable_testing()
set(CMAKE_VERBOSE_MAKEFILE ON)

include_directories(
	${PROJECT_SOURCE_DIR}
)

add_subdirectory(thirdparty)
add_subdirectory(stdb)


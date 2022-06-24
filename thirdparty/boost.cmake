include(ExternalProject)

ExternalProject_Add(boost
	GIT_REPOSITORY https://github.com/boostorg/boost.git
	GIT_TAG boost-1.79.0
	CMAKE_ARGS "-DCMAKE_INSTALL_PREFIX=${EXTERNAL_INSTALL_LOCATION}"
	GIT_SHALLOW 1
	)



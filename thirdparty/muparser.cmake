include(ExternalProject)

ExternalProject_Add(muparser
	GIT_REPOSITORY https://github.com/beltoforion/muparser.git
	GIT_TAG v2.3.2
	CMAKE_ARGS "-DCMAKE_INSTALL_PREFIX=${EXTERNAL_INSTALL_LOCATION} -DBUILD_SHARED_LIBS=OFF -DENABLE_OPENMP=OFF"
	GIT_SHALLOW 1
	)



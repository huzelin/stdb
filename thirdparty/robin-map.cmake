include(ExternalProject)

ExternalProject_Add(robin-map
	GIT_REPOSITORY https://github.com/Tessil/robin-map.git
	GIT_TAG v1.0.1
	CMAKE_ARGS "-DCMAKE_INSTALL_PREFIX=${EXTERNAL_INSTALL_LOCATION}"
	GIT_SHALLOW 1
	)



include(ExternalProject)

ExternalProject_Add(CRoaring
	GIT_REPOSITORY https://github.com/RoaringBitmap/CRoaring.git
	GIT_TAG v0.5.0
	CMAKE_ARGS "-DCMAKE_INSTALL_PREFIX=${EXTERNAL_INSTALL_LOCATION} -DCMAKE_INSTALL_LIBDIR=${EXTERNAL_INSTALL_LOCATION}/lib64"
	GIT_SHALLOW 1
	)

ExternalProject_Add(boost
	GIT_REPOSITORY https://github.com/boostorg/boost.git
	GIT_TAG boost-1.79.0
	CMAKE_ARGS "-DCMAKE_INSTALL_PREFIX=${EXTERNAL_INSTALL_LOCATION}"
	GIT_SHALLOW 1
	)

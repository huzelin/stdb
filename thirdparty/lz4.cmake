include(ExternalProject)

ExternalProject_Add(lz4
	GIT_REPOSITORY https://github.com/lz4/lz4.git
	GIT_TAG v1.9.3
	CONFIGURE_COMMAND "mkdir -p ${EXTERNAL_INSTALL_LOCATION}/include & mkdir -p ${EXTERNAL_INSTALL_LOCATION}/lib64"
	BUILD_COMMAND make
	INSTALL_COMMAND "cp lib/*.h ${EXTERNAL_INSTALL_LOCATION}/include/ & cp lib/*.a ${EXTERNAL_INSTALL_LOCATION}/lib64" 
	GIT_SHALLOW 1
	)



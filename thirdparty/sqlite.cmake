include(ExternalProject)

ExternalProject_Add(sqlite
	GIT_REPOSITORY https://github.com/sqlite/sqlite.git
	GIT_TAG version-3.38.5
	CONFIGURE_COMMAND "./configure --prefix=${EXTERNAL_INSTALL_LOCATION}"
	BUILD_COMMAND make
	INSTALL_COMMAND make install
	GIT_SHALLOW 1
	)



include(ExternalProject)

ExternalProject_Add(apr
	GIT_REPOSITORY https://github.com/obstd/apr.git
	GIT_TAG trunk
	CONFIGURE_COMMAND "./buildconf -v & ./configure --prefix=${EXTERNAL_INSTALL_LOCATION} --with-expat=${EXTERNAL_INSTALL_LOCATION}/../expat/ --with-sqlite3=${EXTERNAL_INSTALL_LOCATION}/../sqlite --with-odbc=no --with-pgsql=no --enable-shared=false --disable-dso"
	BUILD_COMMAND make
	INSTALL_COMMAND make install
	GIT_SHALLOW 1
	)



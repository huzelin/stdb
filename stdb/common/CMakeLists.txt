set(SOURCE
	"crc32c.cc"
	"datetime.cc"
	"hash.cc"
	"memorymappedfile.cc"
	"mmapfile.cc"
	"proto_configure.cc"
	"rwlock.cc"
)
add_library(common ${SOURCE})



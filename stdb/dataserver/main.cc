/*!
 * \file main.cc
 */
#include <iostream>
#include <fstream>
#include <regex>
#include <thread>

#include "stdb/common/logging.h"
#include "stdb/common/signal_handler.h"
#include "stdb/core/storage_api.h"
#include "stdb/dataserver/database_manager.h"
#include "stdb/dataserver/server.h"

#include <boost/program_options.hpp>
#include <boost/format.hpp>

namespace po = boost::program_options;

static const char* CLI_HELP_MESSAGE = R"(`stdb_data_server` - spatial-temporal database daemon

**SYNOPSIS**
        stdb_data_server --help

        stdb_data_server --create

        stdb_data_server --delete

**DESCRIPTION**
        **stdb** is a spatial-temporal database daemon.
        All configuration can be done via `~/.stdb_ds.conf` configuration
        file.

**OPTIONS**
        **help**
            produce help message and exit

        **create**
            generate database files in `~/.stdb_ds.conf` folder, use with
            --allocate flag to actually allocate disk space

        **delete**
            delete database files in `~/.stdb_ds.conf` folder

        **(empty)**
           run server

)";

//! Format text for console. `plain_text` flag removes formatting.
static std::string cli_format(std::string dest) {
  bool plain_text = !isatty(STDOUT_FILENO);

  const char* BOLD = "\033[1m";
  const char* EMPH = "\033[3m";
  const char* UNDR = "\033[4m";
  const char* NORM = "\033[0m";

  auto format = [&](std::string& line, const char* pattern, const char* open) {
    size_t pos = 0;
    int token_num = 0;
    while (pos != std::string::npos) {
      pos = line.find(pattern, pos);
      if (pos != std::string::npos) {
        // match
        auto code = (token_num % 2) ? NORM : open;
        line.replace(pos, strlen(pattern), code);
        token_num++;
      }
    }
  };

  if (!plain_text) {
    format(dest, "**", BOLD);
    format(dest, "__", EMPH);
    format(dest, "`",  UNDR);
  } else {
    format(dest, "**", "");
    format(dest, "__", "");
    format(dest, "`",  "");
  }

  return dest;
}

static void rich_print(const char* msg) {
  std::stringstream stream(const_cast<char*>(msg));
  std::string dest;

  while (std::getline(stream, dest)) {
    std::cout << cli_format(dest) << std::endl;
  }
}

std::vector<std::shared_ptr<stdb::Server>> servers;

static void run_tcp_server(stdb::SignalHandler* signal_handler) {
  auto conn = stdb::DatabaseManager::Get()->get_connection("main");
  std::shared_ptr<stdb::ReadOperationBuilder> read_operation_builder;
  
  stdb::ServerSettings settings;
  settings.name = "TCP";
  auto addr = boost::asio::ip::address_v4::from_string("0.0.0.0");
  {
    boost::asio::ip::tcp::endpoint endpoint(addr, 6838);
    settings.protocols.push_back({ "RESP", endpoint });
  }
  {
    boost::asio::ip::tcp::endpoint endpoint(addr, 6839);
    settings.protocols.push_back({ "OpenTSDB", endpoint });
  }
  settings.nworkers = 2;
  
  LOG(INFO) << "start run tcp server";
  auto server = stdb::ServerFactory::instance().create(conn, read_operation_builder, settings);
  server->start(signal_handler, servers.size());
  servers.push_back(server);
}

static void run_rpc_server(stdb::SignalHandler* signal_handler) {
  // TODO: run rpc server
}

static void delete_database(const po::variables_map& vm) {
  bool force = false;
  if (vm.count("force")) {
    force = true;
  }
  boost::optional<std::string> db_name = vm["delete"].as<std::string>();
  stdb::DatabaseManager::Get()->delete_database(db_name.get().c_str(), force);

  stdb::DatabaseManager::Get()->clear_connection();
}

static void create_database(const po::variables_map& vm) {
  std::string db_name = vm["create"].as<std::string>();
  std::string metadata_path = stdb::common::GetHomeDir() + "/" + ".stdb/" + db_name + "/metadata/";
  std::string volumes_path =  stdb::common::GetHomeDir() + "/" + ".stdb/" + db_name + "/volumes/";
  std::string wal_path = stdb::common::GetHomeDir() + "/" + ".stdb/" + db_name + "/wal/";
  uint32_t num_volumes = 0;
  uint64_t volume_size = 1UL * 1024 * 1024 * 1024;
  bool allocate = false;

  if (vm.count("metadata_path")) {
    metadata_path = vm["metadata_path"].as<std::string>();
  }
  if (vm.count("volumes_path")) {
    volumes_path = vm["volumes_path"].as<std::string>();
  }
  if (vm.count("num_volumes")) {
    num_volumes = vm["num_volumes"].as<uint32_t>();
  }
  if (vm.count("volume_size")) {
    volume_size = vm["volume_size"].as<uint64_t>();
  }
  if (vm.count("allocate")) {
    allocate = true;
  }
  stdb::DatabaseManager::Get()->create_database_ex(
      db_name.c_str(), metadata_path.c_str(), volumes_path.c_str(),
      num_volumes, volume_size, allocate);

  if (vm.count("wal-path")) {
    wal_path = vm["wal_path"].as<std::string>();
  }
  stdb::DatabaseManager::Get()->set_wal(db_name, wal_path);

  stdb::DatabaseManager::Get()->clear_connection();
}

int main(int argc, char** argv) {
  std::locale::global(std::locale("C"));
  po::options_description cli_only_options;
  
  cli_only_options.add_options()
      ( "help", "Produce help message" )
      ( "version", "Print software version" )
      
      ( "delete", po::value<std::string>(), "Delete database")
      ( "force", "Force operation (can be used with --delete)" )
      
      ( "create", po::value<std::string>(), "Create database" )
      ( "metadata_path", po::value<std::string>(), "Metadata path (can be used with --create)" )
      ( "volumes_path",  po::value<std::string>(), "Volumes path (can bed used with --create)" )
      ( "num_volumes", po::value<uint32_t>(), "num volumes (can be used with --create)" )
      ( "volume_size", po::value<uint64_t>(), "volume size (can be used with --create)" )
      ( "allocate", "Preallocate disk space (can be used with --create)" )
      ( "wal-path", po::value<std::string>(), "WAL file dir (can be used with --create)" )
      
      ( "debug-dump", po::value<std::string>(), "Create debug dump")
      ( "debug-recovery-dump", po::value<std::string>(), "Create debug dump of the system after crash recovery")
      ;

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, cli_only_options), vm);
  po::notify(vm);

  if (vm.count("help")) {
    rich_print(CLI_HELP_MESSAGE);
    exit(EXIT_SUCCESS);
  }

  if (vm.count("version")) {
    std::cout << "stdb version: " << STDB_VERSION << std::endl;
    exit(EXIT_SUCCESS);
  }

  stdb::initialize();
  if (vm.count("delete")) {
    delete_database(vm);
    exit(EXIT_SUCCESS);
  }

  if (vm.count("create")) {
    create_database(vm);
    exit(EXIT_SUCCESS);
  }

  stdb::SignalHandler signal_handler;
  run_tcp_server(&signal_handler);
  run_rpc_server(&signal_handler);
  signal_handler.wait();
  stdb::DatabaseManager::Get()->clear_connection();

  return 0;
}

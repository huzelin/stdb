/*!
 * \file main.cc
 */
#include <iostream>
#include <fstream>
#include <regex>
#include <thread>

#include "stdb/dataserver/server.h"

#include <boost/program_options.hpp>
#include <boost/format.hpp>

namespace po = boost::program_options;

static const char* CLI_HELP_MESSAGE = R"(`stdb_data_server` - spatial-temporal database daemon

**SYNOPSIS**
        stdb_data_server --help

        stdb_data_server --init

        stdb_data_server --init-expandable

        stdb_data_server --create

        stdb_data_server --delete

**DESCRIPTION**
        **stdb** is a spatial-temporal database daemon.
        All configuration can be done via `~/.stdb_ds.conf` configuration
        file.

**OPTIONS**
        **help**
            produce help message and exit

        **init**
            create  configuration  file at `~/.stdb_ds.conf`  filled with
            default values and exit

        **init-expandable**
            create  configuration  file at `~/.stdb_ds.conf`  filled with
            default values and exit (sets nvolumes to 0)

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

int main(int argc, char** argv) {
  std::locale::global(std::locale("C"));
  po::options_description cli_only_options;
  
  cli_only_options.add_options()
      ("help", "Produce help message")
      ("config", po::value<std::string>(), "Path to configuration file")
      ("create", "Create database")
      ("allocate", "Preallocate disk space")
      ("delete", "Delete database")
      ("init", "Create default configuration")
      ("init-expandable", "Create configuration for expandable storage")
      ("disable-wal", "Disable WAL in generated configuration file (can be used with --init)")
      ("debug-dump", po::value<std::string>(), "Create debug dump")
      ("debug-recovery-dump", po::value<std::string>(), "Create debug dump of the system after crash recovery")
      ("version", "Print software version");

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, cli_only_options), vm);
  po::notify(vm);

  if (vm.count("help")) {
    rich_print(CLI_HELP_MESSAGE);
    exit(EXIT_SUCCESS);
  }
  
  boost::optional<std::string> cmd_config_path;
  if (vm.count("config")) {
    cmd_config_path = vm["config"].as<std::string>();
  }



  return 0;
}

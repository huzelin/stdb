/*!
 * \file cli.cc
 */
#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <sys/socket.h>

#include <cstddef>
#include <iostream>
#include <string>

#include <boost/program_options.hpp>

#include "stdb/common/basic.h"
#include "stdb/common/logging.h"
#include "stdb/common/strutil.h"
#include "thirdparty/linenoise/linenoise.h"

namespace po = boost::program_options;

std::string host = "127.0.0.1";
uint32_t port = 6838;
std::string historyPath = stdb::common::GetHomeDir() + "/.stdb_cli_history.txt"; 
int sockfd;
const size_t kBufCap = 1024;
size_t buf_size = 0;
char buf[kBufCap];

/* Linenoise completion callback. */
static void completionCallback(const char *buf, linenoiseCompletions *lc) {
  // if (buf[0] == 'h') {
  //  linenoiseAddCompletion(lc,"hello");
  //  linenoiseAddCompletion(lc,"hello there");
  // }
}

/* Linenoise hints callback. */
static char *hintsCallback(const char *buf, int *color, int *bold) {
  // if (!strcasecmp(buf,"hello")) {
  //  *color = 35;
  //  *bold = 0;
  //  return " World";
  // }
  return nullptr;
}

std::vector<std::tuple<const char*, uint32_t>> splitArgs(const char* line) {
  std::vector<std::tuple<const char*, uint32_t>> splits;
  auto success = stdb::common::StrUtil::split(line, strlen(line), splits);
  return splits;
}

static void preparePutBuf(const std::vector<std::tuple<const char*, uint32_t>>& args) {
  memset(buf, 0, kBufCap);
  buf_size = 0;

  for (auto& arg : args) {
    buf[buf_size++] = '+';
    
    memcpy(buf + buf_size, std::get<0>(arg), std::get<1>(arg));
    buf_size += std::get<1>(arg);

    buf[buf_size++] = '\r';
    buf[buf_size++] = '\n';
  }
}

static void writeBuf() {
  size_t offset = 0;
  while (offset != buf_size) {
    auto len = write(sockfd, buf + offset, buf_size - offset);
    if (len >= 0) {
      offset += len;
    } else {
      if (errno == EAGAIN) continue;
      else {
        printf("invalid connection, exiting");
        exit(0);
      }
    }
  }
}

static void processCommand(const std::vector<std::tuple<const char*, uint32_t>>& args) {
  if (args.size() == 3) {
    preparePutBuf(args);
    writeBuf();
  }
}

static void repl(const std::string& prompt) {
  linenoiseSetMultiLine(1);

  /* Set the completion callback. This will be called every time the
   * user uses the <tab> key. */
  linenoiseSetCompletionCallback(completionCallback);
  linenoiseSetHintsCallback(hintsCallback);

  /* Load history from file. The history file is just a plain text file
   * where entries are separated by newlines. */
  linenoiseHistoryLoad(historyPath.c_str()); /* Load the history at startup */

  char* line = nullptr;
  while ((line = linenoise(prompt.c_str())) != NULL) {
    if (line[0] != '\0') {
      auto args = splitArgs(line);
      processCommand(args);
      linenoiseHistoryAdd(line); /* Add to the history. */
      linenoiseHistorySave(historyPath.c_str()); /* Save the history on disk. */
    }
    free(line);
  }
}

static const char* CLI_HELP_MESSAGE = R"(`stdb_cli` - stdb client tool 

  **stdb_cli** --host 127.0.0.1 --port 6838

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

static void connectStdbServer() {
  struct sockaddr_in servaddr;
  sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd == -1) {
    printf("socket creation failed...\n");
    exit(0);
  }
  int flags = 1;
  if (setsockopt(sockfd, SOL_SOCKET, SO_KEEPALIVE, (void *)&flags, sizeof(flags))) {
    perror("ERROR: setsocketopt(), SO_KEEPALIVE");
    exit(0);
  };

  bzero(&servaddr, sizeof(servaddr));
  servaddr.sin_family = AF_INET;
  servaddr.sin_addr.s_addr = inet_addr(host.c_str());
  servaddr.sin_port = htons(port);

  if (connect(sockfd, (struct sockaddr*)&servaddr, sizeof(servaddr)) != 0) {
    printf("connection with the server failed...\n");
    exit(0);
  }
}

int main(int argc, char** argv) {
  std::locale::global(std::locale("C"));
  po::options_description cli_only_options;

  cli_only_options.add_options()
      ( "help", "Produce help message" )
      ( "host", po::value<std::string>(), "host of stdb server")
      ( "port", po::value<uint32_t>(), "port of stdb server" )
      ;

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, cli_only_options), vm);
  po::notify(vm);

  if (vm.count("help")) {
    rich_print(CLI_HELP_MESSAGE);
    exit(EXIT_SUCCESS);
  }

  if (vm.count("host")) {
    host = vm["host"].as<std::string>(); 
  }
  if (vm.count("port")) {
    port = vm["host"].as<uint32_t>();
  }

  connectStdbServer();
  std::string prompt = host + ":" + std::to_string(port) + "> ";
  repl(prompt);
  return 0;
}

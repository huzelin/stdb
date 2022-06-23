/*!
 * \file tcp_server.cc
 */
#include "stdb/dataserver/tcp_server.h"

#include <thread>
#include <atomic>

#include <boost/function.hpp>
#include <boost/exception/diagnostic_information.hpp>

namespace stdb {

std::string make_unique_session_name() {
  static std::atomic<int> counter = {0};
  std::stringstream str;
  str << "tcp-session-" << counter.fetch_add(1);
  return str.str();
}

/** Server session that handles RESP messages.
 *  Must be created in the heap.
 */
template <class ProtocolT>
class TelnetSession : public ProtocolSession, public std::enable_shared_from_this<TelnetSession<ProtocolT>> {
  // TODO: Unique session ID
  enum {
    BUFFER_SIZE = ProtocolT::RDBUF_SIZE,  //< Buffer size
  };
  const bool                      parallel_;
  IOServiceT*                     io_;
  SocketT                         socket_;
  StrandT                         strand_;
  std::shared_ptr<DbSession>      spout_;
  ProtocolT                       parser_;

 public:
  typedef Byte* BufferT;

  TelnetSession(IOServiceT *io, std::shared_ptr<DbSession> spout, bool parallel)
      : parallel_(parallel)
        , io_(io)
        , socket_(*io)
        , strand_(*io)
        , spout_(spout)
        , parser_(spout)
  {
    parser_.start();
  }

  ~TelnetSession() {
  }

  virtual SocketT& socket() {
    return socket_;
  }

  virtual void start() {
    BufferT buf;
    size_t buf_size;
    std::tie(buf, buf_size) = get_next_buffer();
    if (parallel_) {
      socket_.async_read_some(
          boost::asio::buffer(buf, buf_size),
          strand_.wrap(
              boost::bind(&TelnetSession<ProtocolT>::handle_read,
                          this->shared_from_this(),
                          buf,
                          boost::asio::placeholders::error,
                          boost::asio::placeholders::bytes_transferred)));
    } else {
      // Strand is not used here
      socket_.async_read_some(
          boost::asio::buffer(buf, buf_size),
          boost::bind(&TelnetSession<ProtocolT>::handle_read,
                      this->shared_from_this(),
                      buf,
                      boost::asio::placeholders::error,
                      boost::asio::placeholders::bytes_transferred));
    }
  }

  virtual ErrorCallback get_error_cb() {
    auto self = this->shared_from_this();
    auto weak = std::weak_ptr<TelnetSession>(self);
    auto fn = [weak](common::Status status, u64) {
      auto session = weak.lock();
      if (session) {
        LOG(ERROR) << status.ToString();
        boost::asio::streambuf stream;
        std::ostream os(&stream);
        os << session->parser_.error_repr(ProtocolT::DB, msg);
        boost::asio::async_write(session->socket_,
                                 stream,
                                 boost::bind(&TelnetSession<ProtocolT>::handle_write_error,
                                             session,
                                             boost::asio::placeholders::error));
      }
    };
    return ErrorCallback(fn);
  }

 private:
  /** Allocate new buffer.
  */
  std::tuple<BufferT, size_t> get_next_buffer() {
    Byte *buffer = parser_.get_next_buffer();
    return std::make_tuple(buffer, BUFFER_SIZE);
  }

  void handle_read(BufferT buffer,
                   boost::system::error_code error,
                   size_t nbytes)
  {
    if (error) {
      LOG(ERROR) << error.message();
      parser_.close();
    } else {
      try {
        auto response = parser_.parse_next(buffer, static_cast<u32>(nbytes));
        if(response.is_available()) {
          boost::asio::streambuf stream;
          std::ostream os(&stream);
          os << ProtocolT::PARSE, response.get_body();
          boost::asio::async_write(socket_, stream,
                                   boost::bind(&TelnetSession::handle_write,
                                               this->shared_from_this(),
                                               boost::asio::placeholders::error)
                                  );
        }
        start();
      } catch (StreamError const& stream_error) {
        // This error is related to client so we need to send it back
        logger_.error() << stream_error.what();
        boost::asio::streambuf stream;
        std::ostream os(&stream);
        os << parser_.error_repr(ProtocolT::PARSE, stream_error.what());
        boost::asio::async_write(socket_, stream,
                                 boost::bind(&TelnetSession::handle_write_error,
                                             this->shared_from_this(),
                                             boost::asio::placeholders::error)
                                );
      } catch (DatabaseError const& dberr) {
        // Database error
        logger_.error() << boost::current_exception_diagnostic_information();
        boost::asio::streambuf stream;
        std::ostream os(&stream);
        os << parser_.error_repr(ProtocolT::DB, dberr.what());
        boost::asio::async_write(socket_, stream,
                                 boost::bind(&TelnetSession::handle_write_error,
                                             this->shared_from_this(),
                                             boost::asio::placeholders::error)
                                );
      } catch (...) {
        // Unexpected error
        logger_.error() << boost::current_exception_diagnostic_information();
        boost::asio::streambuf stream;
        std::ostream os(&stream);
        os << parser_.error_repr(ProtocolT::ERR, boost::current_exception_diagnostic_information());
        boost::asio::async_write(socket_, stream,
                                 boost::bind(&TelnetSession::handle_write_error,
                                             this->shared_from_this(),
                                             boost::asio::placeholders::error)
                                );
      }
    }
  }


  void handle_write(boost::system::error_code error) {
    if (error) {
      LOG(ERROR) << "Error sending error message to client " << error.message();
      parser_.close();
    }
  }

  void handle_write_error(boost::system::error_code error) {
    if (!error) {
      LOG(ERROR) << "Clean shutdown";
      boost::system::error_code shutdownerr;
      socket_.shutdown(SocketT::shutdown_both, shutdownerr);
      if (shutdownerr) {
        LOG(ERROR) << "Shutdown error: " << shutdownerr.message();
      }
    } else {
      LOG(ERROR) << "Error sending error message to client";
      LOG(ERROR) << error.message();
      parser_.close();
    }
  }
};

typedef TelnetSession<RESPProtocolParser> RESPSession;
typedef TelnetSession<OpenTSDBProtocolParser> OpenTSDBSession;

struct RESPSessionBuilder : ProtocolSessionBuilder {
  bool parallel_;

  RESPSessionBuilder(bool parallel=true)
      : parallel_(parallel)
  {
  }

  virtual std::shared_ptr<ProtocolSession> create(IOServiceT *io, std::shared_ptr<DbSession> session) {
    std::shared_ptr<ProtocolSession> result;
    result.reset(new RESPSession(io, session, parallel_));
    return result;
  }

  virtual std::string name() const {
    return "RESP";
  }
};

struct OpenTSDBSessionBuilder : ProtocolSessionBuilder {
  bool parallel_;

  OpenTSDBSessionBuilder(bool parallel=true)
      : parallel_(parallel)
  {
  }

  virtual std::shared_ptr<ProtocolSession> create(IOServiceT *io, std::shared_ptr<DbSession> session) {
    std::shared_ptr<ProtocolSession> result;
    result.reset(new OpenTSDBSession(io, session, parallel_));
    return result;
  }

  virtual std::string name() const {
    return "OpenTSDB";
  }
};

std::unique_ptr<ProtocolSessionBuilder> ProtocolSessionBuilder::create_resp_builder(bool parallel) {
  std::unique_ptr<ProtocolSessionBuilder> res;
  res.reset(new RESPSessionBuilder(parallel));
  return res;
}

std::unique_ptr<ProtocolSessionBuilder> ProtocolSessionBuilder::create_opentsdb_builder(bool parallel) {
  std::unique_ptr<ProtocolSessionBuilder> res;
  res.reset(new OpenTSDBSessionBuilder(parallel));
  return res;
}

TcpAcceptor::TcpAcceptor(// Server parameters
                        std::vector<IOServiceT *> io, EndpointT endpoint,
                        // Storage & pipeline
                        std::shared_ptr<DbConnection> connection ,
                        bool parallel)
    //: parallel_(parallel)
    : acceptor_(own_io_, endpoint)
    , protocol_(ProtocolSessionBuilder::create_resp_builder(true))
    , sessions_io_(io)
    , connection_(connection)
    , io_index_{0}
    , start_barrier_(2)
    , stop_barrier_(2)
    , iothread_started_(false)
{
  LOG(INFO) << "Server created!";
  LOG(INFO) << "Endpoint: " << endpoint;

  // Blocking I/O services
  for (auto io: sessions_io_) {
    sessions_work_.emplace_back(*io);
  }
}

TcpAcceptor::TcpAcceptor(
        std::vector<IOServiceT*> io,
        EndpointT endpoint,
        std::unique_ptr<ProtocolSessionBuilder> protocol,
        std::shared_ptr<DbConnection> connection,
        bool parallel)
    //: parallel_(parallel)
    : acceptor_(own_io_, endpoint)
    , protocol_(std::move(protocol))
    , sessions_io_(io)
    , connection_(connection)
    , io_index_{0}
    , start_barrier_(2)
    , stop_barrier_(2)
    , iothread_started_(false)
{
  LOG(INFO) << "Server created!";
  LOG(INFO) << "Endpoint: " << endpoint;

  // Blocking I/O services
  for (auto io: sessions_io_) {
    sessions_work_.emplace_back(*io);
  }
}

TcpAcceptor::~TcpAcceptor() {
  LOG(INFO) << "TCP acceptor destroyed";
}

void TcpAcceptor::start() {
  WorkT work(own_io_);

  // Run detached thread for accepts
  auto self = shared_from_this();
  std::thread accept_thread([self]() {
#ifdef __gnu_linux__
    // Name the thread
    std::string thread_name = "TCP-accept-" + self->protocol_->name();
    auto thread = pthread_self();
    pthread_setname_np(thread, thread_name.c_str());
#endif
    LOG(INFO) << "Starting acceptor worker thread";
    self->start_barrier_.wait();
    LOG(INFO) << "Acceptor worker thread have started";

    try {
      self->own_io_.run();
    } catch (...) {
      LOG(ERROR) << "Error in acceptor worker thread: " << boost::current_exception_diagnostic_information();
      throw;
    }

    LOG(INFO) << "Stopping acceptor worker thread";
    self->stop_barrier_.wait();
    LOG(INFO) << "Acceptor worker thread have stopped";
  });
  accept_thread.detach();

  start_barrier_.wait();
  iothread_started_ = true;

  LOG(INFO) << "Start listening";
  _start();
}

void TcpAcceptor::_run_one() {
  own_io_.run_one();
}

void TcpAcceptor::_start() {
  std::shared_ptr<ProtocolSession> session;
  auto con = connection_.lock();
  if (con) {
    std::shared_ptr<DbSession> spout = con->create_session();
    IOServiceT* io = sessions_io_.at(static_cast<size_t>(io_index_++) % sessions_io_.size());
    session = protocol_->create(io, spout);
  } else {
    LOG(ERROR) << "Database was already closed";
  }
  // attach session to spout
  // run session
  acceptor_.async_accept(
      session->socket(),
      boost::bind(&TcpAcceptor::handle_accept,
                  this,
                  session,
                  boost::asio::placeholders::error)
      );
}

void TcpAcceptor::stop() {
  LOG(INFO) << "Stopping acceptor";
  acceptor_.close();
  own_io_.stop();
  sessions_work_.clear();
  LOG(INFO) << "Trying to stop acceptor";
  stop_barrier_.wait();
  LOG(INFO) << "Acceptor successfully stopped";
}

void TcpAcceptor::_stop() {
  LOG(INFO) << "Stopping acceptor (test runner)";
  acceptor_.close();
  if (!iothread_started_) {
    // If I/O thread wasn't started we have to spin the loop
    // to run pending I/O handlers to prevent memory leak.
    // This code path should only work in unit-tests.
    own_io_.poll();
  }
  own_io_.stop();
  sessions_work_.clear();
}

std::string TcpAcceptor::name() const {
  return protocol_->name();
}

void TcpAcceptor::handle_accept(std::shared_ptr<ProtocolSession> session, boost::system::error_code err) {
  if (LIKELY(!err)) {
    session->start();
    _start();
  } else {
    LOG(ERROR) << "Acceptor error " << err.message();
  }
}

TcpServer::TcpServer(std::shared_ptr<DbConnection> connection, int concurrency, EndpointT ep, TcpServer::Mode mode)
    : connection_(connection)
    , barrier(static_cast<u32>(concurrency) + 1)
    , stopped{0}
{
  LOG(INFO) << "TCP server created, concurrency: " << concurrency;
  if (mode == Mode::EVENT_LOOP_PER_THREAD) {
    for(int i = 0; i < concurrency; i++) {
      IOPtr ptr = IOPtr(new IOServiceT(1));
      iovec.push_back(ptr.get());
      ios_.push_back(std::move(ptr));
    }
  } else {
    IOPtr ptr = IOPtr(new IOServiceT(static_cast<size_t>(concurrency)));
    iovec.push_back(ptr.get());
    ios_.push_back(std::move(ptr));
  }
  bool parallel = mode == Mode::SHARED_EVENT_LOOP;
  auto con = connection_.lock();
  if (con) {
    auto serv = std::make_shared<TcpAcceptor>(iovec, ep, con, parallel);
    serv->start();
    acceptors_.push_back(serv);
  } else {
    LOG(ERROR) << "Can't start TCP server, database closed";
    std::runtime_error err("DB connection closed");
    throw err;
  }
}

TcpServer::TcpServer(std::shared_ptr<DbConnection> connection,
                     int concurrency,
                     std::map<EndpointT, std::unique_ptr<ProtocolSessionBuilder> > protocol_map,
                     TcpServer::Mode mode)
    : connection_(connection)
    , barrier(static_cast<u32>(concurrency) + 1)
    , stopped{0}
{
  LOG(INFO) << "TCP server created, concurrency: " << concurrency;
  if (mode == Mode::EVENT_LOOP_PER_THREAD) {
    for(int i = 0; i < concurrency; i++) {
      IOPtr ptr = IOPtr(new IOServiceT(1));
      iovec.push_back(ptr.get());
      ios_.push_back(std::move(ptr));
    }
  } else {
    IOPtr ptr = IOPtr(new IOServiceT(static_cast<size_t>(concurrency)));
    iovec.push_back(ptr.get());
    ios_.push_back(std::move(ptr));
  }

  bool parallel = mode == Mode::SHARED_EVENT_LOOP;
  auto con = connection_.lock();
  for (auto& kv: protocol_map) {
    EndpointT endpoint = kv.first;
    auto protocol = std::move(kv.second);
    LOG(INFO) << "Create acceptor for " << protocol->name() << ", endpoint: " << endpoint;
    if (con) {
      auto serv = std::make_shared<TcpAcceptor>(iovec, endpoint, std::move(protocol), con, parallel);
      serv->start();
      acceptors_.push_back(serv);
    } else {
      LOG(ERROR) << "Can't start TCP server, database closed";
      std::runtime_error err("DB connection closed");
      throw err;
    }
  }
}

TcpServer::~TcpServer() {
  LOG(INFO) << "TCP server destroyed";
}

void TcpServer::start(SignalHandler* sig, int id) {
  auto self = shared_from_this();
  sig->add_handler(boost::bind(&TcpServer::stop, self), id);

  auto iorun = [self](IOServiceT& io, int cnt) {
    auto fn = [self, &io, cnt]() {
#ifdef __gnu_linux__
            // Name the thread
      auto thread = pthread_self();
      pthread_setname_np(thread, "TCP-worker");
#endif
      try {
        LOG(INFO) << "Event loop " << cnt << " started";
        io.run();
        LOG(INFO) << "Event loop " << cnt << " stopped";
        self->barrier.wait();
      } catch (RESPError const& e) {
        LOG(ERROR) << e.what();
        throw;
      } catch (...) {
        LOG(ERROR) << "Error in event loop " << cnt << ": " << boost::current_exception_diagnostic_information();
        throw;
      }
      LOG(INFO) << "Worker thread " << cnt << " stopped";
    };
    return fn;
  };

  int cnt = 0;
  for (auto io: iovec) {
    std::thread iothread(iorun(*io, cnt++));
    iothread.detach();
  }
}

void TcpServer::stop() {
  if (stopped++ == 0) {
    for (auto serv: acceptors_) {
      serv->stop();
      LOG(INFO) << "TcpServer " << serv->name() << " stopped";
    }

    for(auto& svc: ios_) {
      svc->stop();
      LOG(INFO) << "I/O service stopped";
    }

    barrier.wait();
    LOG(INFO) << "I/O threads stopped";
  }
}

struct TcpServerBuilder {
  TcpServerBuilder() {
    ServerFactory::instance().register_type("TCP", *this);
  }

  std::shared_ptr<Server> operator () (std::shared_ptr<DbConnection> con,
                                       std::shared_ptr<ReadOperationBuilder>,
                                       const ServerSettings& settings) {
    auto nworkers = settings.nworkers;
    auto ncpus = std::thread::hardware_concurrency();
    if (ncpus <= 4) {
      nworkers = 1;
    } else if (ncpus <= 8) {
      nworkers = static_cast<int>(ncpus - 2);
    } else {
      nworkers = static_cast<int>(ncpus - 4);
    }
    if (nworkers >= MAX_THREADS) {
      nworkers = MAX_THREADS - 4;
    }
    std::map<EndpointT, std::unique_ptr<ProtocolSessionBuilder>> protocol_map;
    for (const auto& protocol: settings.protocols) {
      std::unique_ptr<ProtocolSessionBuilder> inst;
      if (protocol.name == "RESP") {
        inst = ProtocolSessionBuilder::create_resp_builder(true);
      } else if (protocol.name == "OpenTSDB") {
        inst = ProtocolSessionBuilder::create_opentsdb_builder(true);
      } else {
        LOG(ERROR) << "Unknown protocol " << protocol.name;
      }
      protocol_map[protocol.endpoint] = std::move(inst);
    }
    return std::make_shared<TcpServer>(con, nworkers, std::move(protocol_map));
  }
};

static TcpServerBuilder reg_type;

}  // namespace stdb


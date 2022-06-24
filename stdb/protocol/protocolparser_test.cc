/*!
 * \file protocolparser_test.cc 
 */
#include "stdb/protocol/protocolparser.h"

#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string/trim.hpp>

#include "stdb/common/logging.h"
#include "stdb/core/storage_api.h"
#include "stdb/protocol/resp.h"

#include "gtest/gtest.h"

namespace stdb {
namespace protocol {

struct ConsumerMock : DbSession {
  std::vector<ParamId>     param_;
  std::vector<Timestamp>   ts_;
  std::vector<double>          data_;
  std::vector<std::string>     event_;

  virtual ~ConsumerMock() {}

  virtual common::Status write(const Sample &sample) override {
    if (sample.payload.type == PAYLOAD_FLOAT) {
      param_.push_back(sample.paramid);
      ts_.push_back(sample.timestamp);
      data_.push_back(sample.payload.float64);
      return common::Status::Ok();
    } else if (sample.payload.type == PAYLOAD_EVENT) {
      param_.push_back(sample.paramid);
      ts_.push_back(sample.timestamp);
      int len = sample.payload.size - sizeof(Sample);
      event_.push_back(std::string(sample.payload.data, sample.payload.data + len));
      return common::Status::Ok();
    }
    return common::Status::BadArg();
  }

  virtual std::shared_ptr<DbCursor> query(const std::string&) override {
    throw "Not implemented";
  }

  virtual std::shared_ptr<DbCursor> suggest(const std::string&) override {
    throw "Not implemented";
  }

  virtual std::shared_ptr<DbCursor> search(const std::string&) override {
    throw "Not implemented";
  }

  virtual int param_id_to_series(ParamId id, char* buf, size_t sz) override {
    auto str = std::to_string(id);
    assert(str.size() <= sz);
    memcpy(buf, str.data(), str.size());
    return static_cast<int>(str.size());
  }

  virtual common::Status series_to_param_id(const char* begin, size_t sz, Sample* sample) override {
    int sign = 1;
    if (*begin == '!' && sz > 1) {
      // Events names start with !
      begin++;
      sz--;
      sign = -1;
    }
    std::string num(begin, begin + sz);
    boost::algorithm::trim(num);
    sample->paramid = sign * boost::lexical_cast<u64>(num);
    return common::Status::Ok();
  }

  virtual int name_to_param_id_list(const char* begin, const char* end, ParamId* ids, u32 cap) override {
    u32 nelem = std::count(begin, end, '|') + 1;
    if (nelem > cap) {
      return -1 * static_cast<int>(nelem);
    }
    const char* it_begin = begin;
    const char* it_end = begin;
    for (u32 i = 0; i < nelem; i++) {
      //move it_end
      while (*it_end != '|' && it_end < end) {
        it_end++;
      }
      int sign = 1;
      if (*it_begin == '!') {
        // Events names start with !
        it_begin++;
        sign = -1;
      }
      std::string val(it_begin, it_end);
      ids[i] = sign * boost::lexical_cast<u64>(val);
      it_end++;
      it_begin = it_end;
    }
    return static_cast<int>(nelem);
  }
};

void null_deleter(const char*) {}

std::shared_ptr<const char> buffer_from_static_string(const char* str) {
  return std::shared_ptr<const char>(str, &null_deleter);
}

TEST(TestProtocolParser, Test_protocol_parse_1) {
  const char *messages = "+1\r\n:2\r\n+34.5\r\n+6\r\n:7\r\n+8.9\r\n";
  std::shared_ptr<ConsumerMock> cons(new ConsumerMock());
  RESPProtocolParser parser(cons);
  auto buf = parser.get_next_buffer();
  memcpy(buf, messages, 29);
  parser.start();
  parser.parse_next(buf, 29);
  parser.close();

  EXPECT_EQ(cons->param_[0], 1);
  EXPECT_EQ(cons->param_[1], 6);
  EXPECT_EQ(cons->ts_[0], 2);
  EXPECT_EQ(cons->ts_[1], 7);
  EXPECT_EQ(cons->data_[0], 34.5);
  EXPECT_EQ(cons->data_[1], 8.9);
}

TEST(TestProtocolParser, Test_protocol_parser_bulk_1) {
  const char *messages = "+1|2\r\n:3\r\n*2\r\n+45.6\r\n+7.89\r\n+10|11|12\r\n:13\r\n*3\r\n+1.4\r\n+15\r\n+1.6\r\n";
  std::shared_ptr<ConsumerMock> cons(new ConsumerMock());
  RESPProtocolParser parser(cons);
  auto buf = parser.get_next_buffer();
  memcpy(buf, messages, strlen(messages));
  parser.start();
  parser.parse_next(buf, static_cast<u32>(strlen(messages)));
  parser.close();

  EXPECT_EQ(cons->param_[0], 1);
  EXPECT_EQ(cons->param_[1], 2);
  EXPECT_EQ(cons->param_[2], 10);
  EXPECT_EQ(cons->param_[3], 11);
  EXPECT_EQ(cons->param_[4], 12);
  EXPECT_EQ(cons->ts_[0], 3);
  EXPECT_EQ(cons->ts_[1], 3);
  EXPECT_EQ(cons->ts_[2], 13);
  EXPECT_EQ(cons->ts_[3], 13);
  EXPECT_EQ(cons->ts_[4], 13);
  EXPECT_EQ(cons->data_[0], 45.6);
  EXPECT_EQ(cons->data_[1], 7.89);
  EXPECT_EQ(cons->data_[2], 1.4);
  EXPECT_EQ(cons->data_[3], 15.0);
  EXPECT_EQ(cons->data_[4], 1.6);
}

TEST(TestProtocolParser, Test_protocol_parse_2) {
  const char *message1 = "+1\r\n:2\r\n+34.5\r\n+6\r\n:7\r\n+8.9";
  const char *message2 = "\r\n+10\r\n:11\r\n+12.13\r\n+14\r\n:15\r\n+16.7\r\n";

  std::shared_ptr<ConsumerMock> cons(new ConsumerMock);
  RESPProtocolParser parser(cons);
  parser.start();

  auto buf = parser.get_next_buffer();
  memcpy(buf, message1, 27);
  parser.parse_next(buf, 27);

  EXPECT_EQ(cons->param_.size(), 1);
  // 0
  EXPECT_EQ(cons->param_[0], 1);
  EXPECT_EQ(cons->ts_[0], 2);
  EXPECT_EQ(cons->data_[0], 34.5);

  buf = parser.get_next_buffer();
  memcpy(buf, message2, 37);
  parser.parse_next(buf, 37);

  EXPECT_EQ(cons->param_.size(), 4);
  // 1
  EXPECT_EQ(cons->param_[1], 6);
  EXPECT_EQ(cons->ts_[1], 7);
  EXPECT_EQ(cons->data_[1], 8.9);
  // 2
  EXPECT_EQ(cons->param_[2], 10);
  EXPECT_EQ(cons->ts_[2], 11);
  EXPECT_EQ(cons->data_[2], 12.13);
  // 3
  EXPECT_EQ(cons->param_[3], 14);
  EXPECT_EQ(cons->ts_[3], 15);
  EXPECT_EQ(cons->data_[3], 16.7);
  parser.close();
}

TEST(TestProtocolParser, Test_protocol_parse_error_format) {
  const char *messages = "+1\r\n:2\r\n+34.5\r\n+2\r\n:d\r\n+8.9\r\n";
  std::shared_ptr<ConsumerMock> cons(new ConsumerMock);
  RESPProtocolParser parser(cons);
  parser.start();
  auto buf = parser.get_next_buffer();
  memcpy(buf, messages, 29);
  EXPECT_THROW(parser.parse_next(buf, 29), RESPError);
}

TEST(TestProtocolParser, Test_protocol_parse_larget_integer) {
  std::string message = "+1\r\n:18446744073709551615\r\n+34.5\r\n";
  std::shared_ptr<ConsumerMock> cons(new ConsumerMock);
  RESPProtocolParser parser(cons);
  parser.start();
  auto buf = parser.get_next_buffer();
  memcpy(buf, message.data(), message.length());
  EXPECT_NO_THROW(parser.parse_next(buf, static_cast<u32>(message.length())));
}

TEST(TestProtocolParser, Test_protocol_parse_error_integer) {
  std::string message = "+1\r\n:20000000000000000000\r\n+34.5\r\n";
  std::shared_ptr<ConsumerMock> cons(new ConsumerMock);
  RESPProtocolParser parser(cons);
  parser.start();
  auto buf = parser.get_next_buffer();
  memcpy(buf, message.data(), message.length());
  EXPECT_THROW(parser.parse_next(buf, static_cast<u32>(message.length())), RESPError);
}

TEST(TestProtocolParser, Test_protocol_parse_dictionary_error_format) {
  {
    const char *message = "*1\r\n";
    std::shared_ptr<ConsumerMock> cons(new ConsumerMock);
    RESPProtocolParser parser(cons);
    parser.start();
    auto buf = parser.get_next_buffer();
    memcpy(buf, message, strlen(message));
    EXPECT_THROW(parser.parse_next(buf, strlen(message)), ProtocolParserError);
  }
  {
    const char *message = "*2\r\n:1\r\n:2\r\n";
    std::shared_ptr<ConsumerMock> cons(new ConsumerMock);
    RESPProtocolParser parser(cons);
    parser.start();
    auto buf = parser.get_next_buffer();
    memcpy(buf, message, strlen(message));
    EXPECT_THROW(parser.parse_next(buf, strlen(message)), ProtocolParserError);
  }
  {
    const char *message = "*2\r\n+1\r\n+2\r\n";
    std::shared_ptr<ConsumerMock> cons(new ConsumerMock);
    RESPProtocolParser parser(cons);
    parser.start();
    auto buf = parser.get_next_buffer();
    memcpy(buf, message, strlen(message));
    EXPECT_THROW(parser.parse_next(buf, strlen(message)), ProtocolParserError);
  }
}

template<class Protocol, class Pred, class Mock>
void find_framing_issues(const char* message, size_t msglen, size_t pivot1, size_t pivot2, Pred const& pred, std::shared_ptr<Mock> cons) {
  auto buffer1 = buffer_from_static_string(message);
  auto buffer2 = buffer_from_static_string(message + pivot1);
  auto buffer3 = buffer_from_static_string(message + pivot2);

  PDU pdu1 = {
    buffer1,
    static_cast<u32>(pivot1),
    0u
  };
  PDU pdu2 = {
    buffer2,
    static_cast<u32>(pivot2 - pivot1),
    0u
  };
  PDU pdu3 = {
    buffer3,
    static_cast<u32>(msglen - pivot2),
    0u
  };

  Protocol parser(cons);
  parser.start();
  auto buf = parser.get_next_buffer();
  memcpy(buf, message, pivot1);
  parser.parse_next(buf, pivot1);

  buf = parser.get_next_buffer();
  memcpy(buf, message + pivot1, pivot2 - pivot1);
  parser.parse_next(buf, pivot2 - pivot1);

  buf = parser.get_next_buffer();
  memcpy(buf, message + pivot2, msglen - pivot2);
  parser.parse_next(buf, msglen - pivot2);

  parser.close();

  pred(cons);
}

/**
 * This test is created to find nontrivial framing issues in protocol parser.
 * Everything works fine when PDU contains entire record (series, timestamp and value)
 * but in the real world scenario this envariant can be broken and each record can be
 * scattered between many PDUs.
 */
TEST(TestProtocolParser, Test_protocol_parser_framing) {
  const char *message = "+1\r\n:2\r\n+34.5\r\n"
      "+6\r\n:7\r\n+8.9\r\n"
      "+10\r\n:11\r\n+12.13\r\n"
      "+14\r\n:15\r\n+16.7\r\n";

  auto pred = [] (std::shared_ptr<ConsumerMock> cons) {

    EXPECT_EQ(cons->param_.size(), 4);
    // 0
    EXPECT_EQ(cons->param_[0], 1);
    EXPECT_EQ(cons->ts_[0], 2);
    EXPECT_TRUE(fabs(cons->data_[0] - 34.5) <= 1e-9);
    // 1
    EXPECT_EQ(cons->param_[1], 6);
    EXPECT_EQ(cons->ts_[1], 7);
    EXPECT_TRUE(fabs(cons->data_[1] - 8.9) <= 1e-9);
    // 2
    EXPECT_EQ(cons->param_[2], 10);
    EXPECT_EQ(cons->ts_[2], 11);
    EXPECT_TRUE(fabs(cons->data_[2] - 12.13) <= 1e-9);
    // 3
    EXPECT_EQ(cons->param_[3], 14);
    EXPECT_EQ(cons->ts_[3], 15);
    EXPECT_TRUE(fabs(cons->data_[3] - 16.7) <= 1e-9);
  };

  size_t msglen = strlen(message);

  for (int i = 0; i < 100; i++) {
    size_t pivot1 = 1 + static_cast<size_t>(rand()) % (msglen / 2);
    size_t pivot2 = 1+ static_cast<size_t>(rand()) % (msglen - pivot1 - 2) + pivot1;
    std::shared_ptr<ConsumerMock> cons(new ConsumerMock);
    find_framing_issues<RESPProtocolParser>(message, msglen, pivot1, pivot2, pred, cons);
  }
}

TEST(TestProtocolParser, Test_protocol_parser_framing_bulk) {
  const char *message = "+1|6\r\n:2\r\n*2\r\n+34.5\r\n+8.9\r\n"
      "+10|14|15\r\n:11\r\n*3\r\n+12.13\r\n+16.17\r\n+18.19\r\n";

  auto pred = [] (std::shared_ptr<ConsumerMock> cons) {

    EXPECT_EQ(cons->param_.size(), 5);
    // 0
    EXPECT_EQ(cons->param_[0], 1);
    EXPECT_EQ(cons->ts_[0], 2);
    EXPECT_TRUE(fabs(cons->data_[0] - 34.5) <= 1e-9);
    // 1
    EXPECT_EQ(cons->param_[1], 6);
    EXPECT_EQ(cons->ts_[1], 2);
    EXPECT_TRUE(fabs(cons->data_[1] - 8.9) <= 1e-9);
    // 2
    EXPECT_EQ(cons->param_[2], 10);
    EXPECT_EQ(cons->ts_[2], 11);
    EXPECT_TRUE(fabs(cons->data_[2] - 12.13) <= 1e-9);
    // 3
    EXPECT_EQ(cons->param_[3], 14);
    EXPECT_EQ(cons->ts_[3], 11);
    EXPECT_TRUE(fabs(cons->data_[3] - 16.17) <= 1e-9);
    // 4
    EXPECT_EQ(cons->param_[4], 15);
    EXPECT_EQ(cons->ts_[4], 11);
    EXPECT_TRUE(fabs(cons->data_[4] - 18.19) <= 1e-9);
  };

  size_t msglen = strlen(message);

  for (int i = 0; i < 100; i++) {
    size_t pivot1 = 1 + static_cast<size_t>(rand()) % (msglen / 2);
    size_t pivot2 = 1+ static_cast<size_t>(rand()) % (msglen - pivot1 - 2) + pivot1;
    std::shared_ptr<ConsumerMock> cons(new ConsumerMock);
    find_framing_issues<RESPProtocolParser>(message, msglen, pivot1, pivot2, pred, cons);
  }
}

TEST(TestProtocolParser, Test_protocol_parser_framing_dict) {
  const char *message =
      // Dictionary
      "*2\r\n+111\r\n:1\r\n"
      "*2\r\n+222\r\n:2\r\n"
      // The actual messages
      ":1\r\n:42\r\n+200\r\n"
      ":2\r\n:53\r\n+300\r\n"
      ":1\r\n:43\r\n+201\r\n"
      ":2\r\n:54\r\n+301\r\n"
      ;

  auto pred = [] (std::shared_ptr<ConsumerMock> cons) {

    EXPECT_EQ(cons->param_.size(), 4);
    // 0
    EXPECT_EQ(cons->param_[0], 111);
    EXPECT_EQ(cons->ts_[0], 42);
    EXPECT_FLOAT_EQ(cons->data_[0], 200);
    // 1
    EXPECT_EQ(cons->param_[1], 222);
    EXPECT_EQ(cons->ts_[1], 53);
    EXPECT_FLOAT_EQ(cons->data_[1], 300);
    // 2
    EXPECT_EQ(cons->param_[2], 111);
    EXPECT_EQ(cons->ts_[2], 43);
    EXPECT_FLOAT_EQ(cons->data_[2], 201);
    // 3
    EXPECT_EQ(cons->param_[3], 222);
    EXPECT_EQ(cons->ts_[3], 54);
    EXPECT_FLOAT_EQ(cons->data_[3], 301);
  };

  size_t msglen = strlen(message);

  for (int i = 0; i < 100; i++) {
    size_t pivot1 = 1 + static_cast<size_t>(rand()) % (msglen / 2);
    size_t pivot2 = 1+ static_cast<size_t>(rand()) % (msglen - pivot1 - 2) + pivot1;
    std::shared_ptr<ConsumerMock> cons(new ConsumerMock);
    find_framing_issues<RESPProtocolParser>(message, msglen, pivot1, pivot2, pred, cons);
  }
}

TEST(TestProtocolParser, Test_protocol_parser_framing_event) {
  const char *message = "+!1\r\n:2\r\n+event1\r\n"
      "+!2\r\n:7\r\n+event2\r\n"
      "+!3\r\n:11\r\n+event3\r\n"
      "+!4\r\n:15\r\n+event4\r\n";

  auto pred = [] (std::shared_ptr<ConsumerMock> cons) {

    EXPECT_EQ(cons->param_.size(), 4);
    // 0
    EXPECT_EQ(cons->param_[0], static_cast<u64>(-1));
    EXPECT_EQ(cons->ts_[0], 2);
    EXPECT_EQ(cons->event_[0], "event1");
    // 1
    EXPECT_EQ(cons->param_[1], static_cast<u64>(-2));
    EXPECT_EQ(cons->ts_[1], 7);
    EXPECT_EQ(cons->event_[1], "event2");
    // 2
    EXPECT_EQ(cons->param_[2], static_cast<u64>(-3));
    EXPECT_EQ(cons->ts_[2], 11);
    EXPECT_EQ(cons->event_[2], "event3");
    // 3
    EXPECT_EQ(cons->param_[3], static_cast<u64>(-4));
    EXPECT_EQ(cons->ts_[3], 15);
    EXPECT_EQ(cons->event_[3], "event4");
  };

  size_t msglen = strlen(message);

  for (int i = 0; i < 100; i++) {
    size_t pivot1 = 1 + static_cast<size_t>(rand()) % (msglen / 2);
    size_t pivot2 = 1+ static_cast<size_t>(rand()) % (msglen - pivot1 - 2) + pivot1;
    std::shared_ptr<ConsumerMock> cons(new ConsumerMock);
    find_framing_issues<RESPProtocolParser>(message, msglen, pivot1, pivot2, pred, cons);
  }
}

TEST(TestProtocolParser, Test_protocol_parser_framing_event_bulk) {
  const char *message = "+!1|!2|!3|!4\r\n"
      ":9\r\n"
      "*4\r\n"
      "+event1\r\n"
      "+event2\r\n"
      "+event3\r\n"
      "+event4\r\n";

  auto pred = [] (std::shared_ptr<ConsumerMock> cons) {

    EXPECT_EQ(cons->param_.size(), 4);
    // 0
    EXPECT_EQ(cons->param_[0], static_cast<u64>(-1));
    EXPECT_EQ(cons->ts_[0], 9);
    EXPECT_EQ(cons->event_[0], "event1");
    // 1
    EXPECT_EQ(cons->param_[1], static_cast<u64>(-2));
    EXPECT_EQ(cons->ts_[1], 9);
    EXPECT_EQ(cons->event_[1], "event2");
    // 2
    EXPECT_EQ(cons->param_[2], static_cast<u64>(-3));
    EXPECT_EQ(cons->ts_[2], 9);
    EXPECT_EQ(cons->event_[2], "event3");
    // 3
    EXPECT_EQ(cons->param_[3], static_cast<u64>(-4));
    EXPECT_EQ(cons->ts_[3], 9);
    EXPECT_EQ(cons->event_[3], "event4");
  };

  size_t msglen = strlen(message);

  for (int i = 0; i < 100; i++) {
    size_t pivot1 = 1 + static_cast<size_t>(rand()) % (msglen / 2);
    size_t pivot2 = 1+ static_cast<size_t>(rand()) % (msglen - pivot1 - 2) + pivot1;
    std::shared_ptr<ConsumerMock> cons(new ConsumerMock);
    find_framing_issues<RESPProtocolParser>(message, msglen, pivot1, pivot2, pred, cons);
  }
}

struct NameCheckingConsumer : DbSession {
  enum { ID = 101 };

  int called_;
  int num_calls_expected_;

  std::map<u64, std::string> series;
  std::map<std::string, u64> index;
  std::vector<ParamId>   ids;
  std::vector<Timestamp> ts;
  std::vector<double>        xs;


  NameCheckingConsumer(std::string expected, int ncalls_expected)
      : called_(0)
        , num_calls_expected_(ncalls_expected) {
    series[ID] = expected;
    index[expected] = ID;
  }

  NameCheckingConsumer(std::vector<std::string> expected, int ncalls_expected)
      : called_(0)
        , num_calls_expected_(ncalls_expected) {
    ParamId id = ID;
    for (const auto& name: expected) {
      series[id] = name;
      index[name] = id;
      id++;
    }
  }

  virtual ~NameCheckingConsumer() {
    if (num_calls_expected_ >= 0 && called_ != num_calls_expected_) {
      LOG(FATAL) << "Test wasn't called";
    }
  }

  virtual common::Status write(const Sample &sample) override {
    called_++;
    ids.push_back(sample.paramid);
    ts.push_back(sample.timestamp);
    xs.push_back(sample.payload.type == PAYLOAD_FLOAT ? sample.payload.float64 : static_cast<double>(INFINITY));
    return common::Status::Ok();
  }

  virtual std::shared_ptr<DbCursor> query(const std::string&) override {
    throw "Not implemented";
  }

  virtual std::shared_ptr<DbCursor> suggest(const std::string&) override {
    throw "Not implemented";
  }

  virtual std::shared_ptr<DbCursor> search(const std::string&) override {
    throw "Not implemented";
  }

  virtual int param_id_to_series(ParamId id, char* buf, size_t sz) override {
    if (series.count(id)) {
      std::string expected = series[id];
      size_t bytes_copied = std::min(sz, expected.size());
      memcpy(buf, expected.data(), bytes_copied);
      return static_cast<int>(bytes_copied);
    }
    return 0;
  }

  virtual common::Status series_to_param_id(const char* begin, size_t sz, Sample* sample) override {
    std::string name(begin, begin + sz);
    if (index.count(name)) {
      sample->paramid = index[name];
      return common::Status::Ok();
    }
    LOG(FATAL) << "Invalid series name";
    return common::Status::Ok();
  }

  virtual int name_to_param_id_list(const char* begin, const char* end, ParamId* ids, u32) override {
    std::string name(begin, end);
    if (index.count(name)) {
      ids[0] = index[name];
      return 1;
    }
    return 0;
  }
};

void test_series_name_parsing(const char* messages, const char* expected_tags, int n) {
  std::shared_ptr<NameCheckingConsumer> cons(new NameCheckingConsumer(expected_tags, n));
  RESPProtocolParser parser(cons);
  parser.start();
  auto buf = parser.get_next_buffer();
  size_t buflen = strlen(messages);
  memcpy(buf, messages, buflen);
  parser.parse_next(buf, buflen);
}

TEST(TestProtocolParser, Test_protocol_parse_series_name_error_with_carriage_return) {
  const char *messages = "+test tag1=value1 tag2=value2\r\n:2000\n+34.5\r\n+test tag1=value1 tag2=value2\r\n:3000\r\n+8.9\r\n";
  test_series_name_parsing(messages, "test tag1=value1 tag2=value2", 2);
}

TEST(TestProtocolParser, Test_protocol_parse_series_name_error_no_carriage_return) {
  const char *messages = "+test tag1=value1 tag2=value2\n:2000\n+34.5\n+test tag1=value1 tag2=value2\n:3000\n+8.9\n";
  test_series_name_parsing(messages, "test tag1=value1 tag2=value2", 2);
}

TEST(TestProtocolParser, Test_protocol_parse_series_name_error_no_carriage_return_2) {
  const char *messages = "+trialrank2 tag1=hello tag2=check\n:1418224205000000000\n:31\n";
  test_series_name_parsing(messages, "trialrank2 tag1=hello tag2=check", 1);
}

//   OpenTSDB protocol parser tests   //
static const u64 NANOSECONDS = 1000000000;

TEST(TestProtocolParser, Test_opentsdb_protocol_parse_1) {
  std::string messages = "put test 2 12.3 tag1=value1 tag2=value2\n";
  std::string expected_tag = "test tag1=value1 tag2=value2";
  std::shared_ptr<NameCheckingConsumer> cons(new NameCheckingConsumer(expected_tag, 1));
  OpenTSDBProtocolParser parser(cons);
  auto buf = parser.get_next_buffer();
  memcpy(buf, messages.data(), messages.size());
  parser.start();
  parser.parse_next(buf, static_cast<u32>(messages.size()));
  parser.close();

  EXPECT_EQ(cons->ids.size(), 1);
  EXPECT_EQ(cons->ids.at(0), cons->index[expected_tag]);
  EXPECT_EQ(cons->ts.at(0),  2*NANOSECONDS);
  EXPECT_EQ(cons->xs.at(0), 12.3);
}

TEST(TestProtocolParser, Test_opentsdb_protocol_parse_2) {
  std::string messages =
      "put test 2 34.5 tag=1\n"
      "put test 7 89.0 tag=2\n"
      "put  test 10 11.1 tag=3\n"
      "put test  13 14.5 tag=4\n"
      "put test 16  17.1 tag=5\n"
      "put test 19 20.2  tag=6\n"
      "put test 22 23.2 tag=7 \n";
  std::vector<std::string> expected_names = {
    "test tag=1", "test tag=2",
    "test tag=3", "test  tag=4",  // for actual series parser "test  tag=4" and "test tag=4" is the same
    "test tag=5", "test tag=6",
    "test tag=7"
  };
  std::vector<Timestamp> expected_ts = {
    2, 7, 10, 13, 16, 19, 22
  };
  std::vector<double> expected_values = {
    34.5, 89.0, 11.1, 14.5, 17.1, 20.2, 23.2
  };
  std::shared_ptr<NameCheckingConsumer> cons(new NameCheckingConsumer(expected_names, -1));
  OpenTSDBProtocolParser parser(cons);
  auto buf = parser.get_next_buffer();
  memcpy(buf, messages.data(), messages.size());
  parser.start();
  parser.parse_next(buf, static_cast<u32>(messages.size()));
  parser.close();

  EXPECT_EQ(cons->ids.size(), 7);
  for (int i = 0; i < 7; i++) {
    EXPECT_EQ(cons->ids.at(i),  cons->index[expected_names[i]]);
    EXPECT_EQ(cons->ts.at(i),  expected_ts.at(i) * NANOSECONDS);
    EXPECT_EQ(cons->xs.at(i), expected_values.at(i));
  }
}

TEST(TestProtocolParser, Test_open_tsdb_protocol_parser_framing) {
  const char *message = "put test 10001 34.57 tag1=1 tag2=1\n"
      "put test 10002 81.09 tag1=2 tag2=2\n"
      "put test 10003 12.13 tag1=3 tag2=3\n"
      "put test 10004 16.71 tag1=1 tag2=1\n";

  std::vector<std::string> expected = {
    "test tag1=1 tag2=1",
    "test tag1=2 tag2=2",
    "test tag1=3 tag2=3",
  };

  auto pred = [&] (std::shared_ptr<NameCheckingConsumer> cons) {

    EXPECT_EQ(cons->ids.size(), 4);
    // 0
    EXPECT_EQ(cons->ids[0], cons->index[expected.at(0)]);
    EXPECT_EQ(cons->ts[0], 10001*NANOSECONDS);
    EXPECT_FLOAT_EQ(cons->xs[0], 34.57);
    // 1
    EXPECT_EQ(cons->ids[1], cons->index[expected.at(1)]);
    EXPECT_EQ(cons->ts[1], 10002*NANOSECONDS);
    EXPECT_FLOAT_EQ(cons->xs[1], 81.09);
    // 2
    EXPECT_EQ(cons->ids[2], cons->index[expected.at(2)]);
    EXPECT_EQ(cons->ts[2], 10003*NANOSECONDS);
    EXPECT_FLOAT_EQ(cons->xs[2], 12.13);
    // 3
    EXPECT_EQ(cons->ids[3], cons->index[expected.at(0)]);
    EXPECT_EQ(cons->ts[3], 10004*NANOSECONDS);
    EXPECT_FLOAT_EQ(cons->xs[3], 16.71);
  };

  size_t msglen = strlen(message);

  for (int i = 0; i < 100; i++) {
    size_t pivot1 = 1 + static_cast<size_t>(rand()) % (msglen / 2);
    size_t pivot2 = 1 + static_cast<size_t>(rand()) % (msglen - pivot1 - 2) + pivot1;
    std::shared_ptr<NameCheckingConsumer> cons = std::make_shared<NameCheckingConsumer>(expected, -1);
    find_framing_issues<OpenTSDBProtocolParser>(message, msglen, pivot1, pivot2, pred, cons);
  }
}

}  // namespace protocol
}  // namespace stdb

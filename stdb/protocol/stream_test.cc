/*!
 * \file stream_test.cc
 */
#include "stdb/protocol/stream.h"

#include "gtest/gtest.h"

namespace stdb {
namespace protocol {

TEST(TestStream, Test_stream_1) {
  std::string expected = "hello world";
  MemStreamReader stream_reader(expected.data(), expected.size());
  Byte buffer[1024] = {};
  EXPECT_TRUE(!stream_reader.is_eof());
  auto bytes_read = stream_reader.read(buffer, 1024);
  EXPECT_EQ(static_cast<size_t>(bytes_read), expected.size());
  EXPECT_EQ(std::string(buffer), expected);
  EXPECT_TRUE(stream_reader.is_eof());
}

TEST(TestStream, Test_stream_2) {
  std::string expected = "hello world";
  MemStreamReader stream_reader(expected.data(), expected.size());
  Byte buffer[1024] = {};
  stream_reader.close();
  auto bytes_read = stream_reader.read(buffer, 1024);
  EXPECT_EQ(static_cast<size_t>(bytes_read), 0);
}

TEST(TestStream, Test_stream_3) {
  std::string expected = "abcde";
  MemStreamReader stream_reader(expected.data(), expected.size());
  EXPECT_EQ(stream_reader.pick(), 'a');
  EXPECT_EQ(stream_reader.get(),  'a');
  EXPECT_EQ(stream_reader.get(),  'b');
  EXPECT_EQ(stream_reader.get(),  'c');
  EXPECT_EQ(stream_reader.pick(), 'd');
  EXPECT_EQ(stream_reader.get(),  'd');
}

}  // namespace protocol
}  // namespace stdb

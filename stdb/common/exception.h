/*
 * \file exception.h 
 * \brief The stdb exception.
 */
#ifndef STDB_COMMON_EXCEPTION_H_
#define STDB_COMMON_EXCEPTION_H_

#include <functional>
#include <sstream>
#include <vector>

namespace stdb {

inline void MakeStringInternal(std::stringstream& /*ss*/) {}

template <typename T>
inline void MakeStringInternal(std::stringstream& ss, const T& t) {
  ss << t;
}

template <typename T, typename... Args>
inline void MakeStringInternal(std::stringstream& ss, const T& t, const Args&... args) {
  MakeStringInternal(ss, t);
  MakeStringInternal(ss, args...);
}

template <typename... Args>
std::string MakeString(const Args&... args) {
  std::stringstream ss;
  MakeStringInternal(ss, args...);
  return std::string(ss.str());
}

extern void SetStackTraceFetcher(std::function<std::string(void)> fetcher); 

class Exception : public std::exception {
 public:
  Exception(const char* file,
            const int line,
            const char* condition,
            const std::string& msg,
            const void* caller = nullptr);
  
  void AppendMessage(const std::string& msg);
  std::string msg() const;
  inline const std::vector<std::string>& msg_stack() const {
    return msg_stack_;
  }

  const char* what() const noexcept override;
  const void* caller() const noexcept;

 protected:
  std::vector<std::string> msg_stack_;
  std::string full_msg_;
  std::string stack_trace_;
  const void* caller_;
};

#ifndef STDB_CONDITION_THROW
#define STDB_CONDITION_THROW(condition, ...)            \
    do { \
      if (!(condition)) { \
        throw stdb::Exception(__FILE__, __LINE__, #condition, MakeString(__VA_ARGS__)); \
      } \
    } while (false)
#endif

#ifndef STDB_THROW
#define STDB_THROW(...) \
    throw stdb::Exception(__FILE__, __LINE__, "", MakeString(__VA_ARGS__))
#endif

}  // namespace stdb

#endif  // STDB_COMMON_EXCEPTION_H_

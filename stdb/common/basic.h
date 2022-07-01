/*!
 * \file basic.h
 */
#ifndef STDB_COMMON_BASIC_H_
#define STDB_COMMON_BASIC_H_

#include <stdint.h>

#include <string>

namespace stdb {

// Some type definitions.
#ifdef __GNUC__
#define LIKELY(x)   __builtin_expect((x), 1)
#define UNLIKELY(x) __builtin_expect((x), 0)
#else
#define LIKELY(x)   (x)
#define UNLIKELY(x) (x)
#endif

#define PACKED __attribute__((__packed__))
#define UNUSED(x) (void)(x)
#define LIMITS_MAX_SNAME 0x1000
#define LIMITS_MAX_TAGS 32
#define MAX_THREADS 1024

#define STDB_VERSION 101

#define STDB_MIN_TIMESTAMP 0ull
#define STDB_MAX_TIMESTAMP (~0ull)
#define STDB_LIMITS_MAX_EVENT_LEN 4096
#define STDB_LIMITS_MAX_ROW_WIDTH 0x100

typedef uint64_t u64;
typedef int64_t  i64;
typedef uint32_t u32;
typedef int32_t  i32;
typedef uint16_t u16;
typedef int16_t  i16;
typedef uint8_t  u8;
typedef int8_t   i8;

typedef u64 Timestamp;  //< Timestamp
typedef u64 ParamId;    //< Parameter (or sequence) id
typedef float SpatialType; //< Spatial type, such as lon, lat

//! Payload data
typedef struct {
  //------------------------------------------//
  //       Normal payload (float value)       //
  //------------------------------------------//
  //! Value
  double float64;

  /** Payload size (payload can be variably sized)
   *  size = 0 means size = sizeof(Sample)
   */
  u16 size;

  //! Data element flags
  enum {
    REGULLAR         = 1 << 8,  /** indicates that the sample is a part of regullar time-series */
    PARAMID_BIT      = 1,       /** indicates that the param id is set */
    TIMESTAMP_BIT    = 1 << 1,  /** indicates that the timestamp is set */
    SPATIAL_BIT      = 1 << 2,  /** indicates that the spatial is set */
    CUSTOM_TIMESTAMP = 1 << 3,  /** indicates that timestamp shouldn't be formatted during output */
    FLOAT_BIT        = 1 << 4,  /** scalar type */
    TUPLE_BIT        = 1 << 5,  /** tuple type */
    EVENT_BIT        = 1 << 6,  /** event type */
    SAX_WORD         = 1 << 10, /** indicates that SAX word is stored in extra payload */
  };
  u16 type;

  //---------------------------//
  //       Extra payload       //
  //---------------------------//

  //! Extra payload data
  char data[0];
} PData;

#define PAYLOAD_FLOAT         (PData::PARAMID_BIT | PData::TIMESTAMP_BIT | PData::FLOAT_BIT)
#define PAYLOAD_SPATIAL_FLOAT (PData::PARAMID_BIT | PData::TIMESTAMP_BIT | PData::FLOAT_BIT | PData::SPATIAL_BIT)
#define PAYLOAD_TUPLE         (PData::PARAMID_BIT | PData::TIMESTAMP_BIT | PData::TUPLE_BIT)
#define PAYLOAD_EVENT         (PData::PARAMID_BIT | PData::TIMESTAMP_BIT | PData::EVENT_BIT)
#define PAYLOAD_NONE          (PData::PARAMID_BIT | PData::TIMESTAMP_BIT)

//! Cursor result type
typedef struct {
  Timestamp timestamp;
  SpatialType lon;
  SpatialType lat;
  ParamId   paramid;
  PData     payload;
} Sample;

inline bool same_value(double a, double b) {
  union Bits {
    double d;
    u64 u;
  };
  Bits ba = {};
  ba.d = a;
  Bits bb = {};
  bb.d = b;
  return ba.u == bb.u;
}

/**
 * configuration.
 */
typedef struct {
  //! Max size of the input-log volume
  u64 input_log_volume_size = 1UL * 1024 * 1024 * 1024;

  //! Number of volumes to keep
  u64 input_log_volume_numb = 4;

  //! Input log max concurrency
  u32 input_log_concurrency = 2;

  //! Path to input log root directory
  const char* input_log_path = "/data/input_log";

} FineTuneParams;

namespace common {

inline std::string GetHomeDir() {
  auto ptr = getenv("HOME");
  if (ptr) {
    return std::string(ptr);
  } else {
    return "/home/stdb/";
  }
}

}  // namespace common
}  // namespace stdb

#endif  // STDB_COMMON_BASIC_H_

/*!
 * \file types.h
 */
#ifndef STDB_COMMON_TYPES_H_
#define STDB_COMMON_TYPES_H_

#include <stdint.h>

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

#endif  // STDB_COMMON_TYPES_H_

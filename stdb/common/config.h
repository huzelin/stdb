/*!
 * \file config.h
 */
#ifndef STDB_COMMON_CONFIG_H_
#define STDB_COMMON_CONFIG_H_

#include "stdb/common/types.h"

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

#endif  // STDB_COMMON_CONFIG_H_

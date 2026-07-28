#ifndef DELTA_SHARING_NATIVE_H
#define DELTA_SHARING_NATIVE_H

#include <stddef.h>
#include <stdint.h>

typedef struct ArrowArrayStream ArrowArrayStream;

typedef struct {
  uint32_t abi_version;
  int32_t kernel_smoke_ok;
  const char *delta_kernel_version;
  const char *arrow_rs_version;
  const char *ffi_backend;
  const char *kernel_smoke_message;
  uint64_t active_streams;
  uint64_t cancelled_streams;
  uint64_t emitted_batches;
} DeltaSharingNativeInfo;

int32_t delta_sharing_native_populate_test_stream(
    ArrowArrayStream *destination,
    int32_t batches,
    int32_t rows_per_batch,
    int32_t error_after,
    int32_t panic_after,
    char *error_buffer,
    size_t error_capacity);

int32_t delta_sharing_native_info(
    DeltaSharingNativeInfo *output,
    char *error_buffer,
    size_t error_capacity);

#endif

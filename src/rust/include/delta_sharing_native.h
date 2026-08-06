#ifndef DELTA_SHARING_NATIVE_H
#define DELTA_SHARING_NATIVE_H

#include <stddef.h>
#include <stdint.h>

typedef struct ArrowArrayStream ArrowArrayStream;

int32_t delta_sharing_native_populate_test_stream(
    ArrowArrayStream *destination,
    int32_t batches,
    int32_t rows_per_batch,
    int32_t error_after,
    int32_t panic_after,
    char *error_buffer,
    size_t error_capacity);

int32_t delta_sharing_native_populate_snapshot_stream(
    ArrowArrayStream *destination,
    const char *table_location,
    const char *cleanup_root,
    const char *const *columns,
    size_t column_count,
    int32_t has_limit,
    uint64_t limit,
    uint32_t batch_size,
    char *error_buffer,
    size_t error_capacity);

int32_t delta_sharing_native_populate_cdf_stream(
    ArrowArrayStream *destination,
    const char *table_location,
    const char *cleanup_root,
    const char *const *columns,
    size_t column_count,
    uint64_t start_version,
    uint64_t end_version,
    uint32_t batch_size,
    char *error_buffer,
    size_t error_capacity);

int32_t delta_sharing_native_reap_pending(
    uint64_t *pending,
    char *error_buffer,
    size_t error_capacity);

#endif

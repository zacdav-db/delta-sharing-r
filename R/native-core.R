# Internal native lifecycle proof. Public reads remain routed through the
# execution interface until the compact Kernel invocation is implemented.

.native_test_stream <- function(batches = 1L,
                                rows_per_batch = 3L,
                                error_after = -1L,
                                panic_after = -1L) {
  stream <- nanoarrow::nanoarrow_allocate_array_stream()
  .Call(
    C_delta_sharing_stream_from_test_data,
    stream,
    as.integer(batches),
    as.integer(rows_per_batch),
    as.integer(error_after),
    as.integer(panic_after)
  )
  stream
}

.native_diagnostics <- function() {
  .Call(C_delta_sharing_native_diagnostics)
}

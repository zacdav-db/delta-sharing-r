#include <R.h>
#include <R_ext/Rdynload.h>
#include <R_ext/Utils.h>
#include <R_ext/Visibility.h>
#include <Rinternals.h>

#include <errno.h>
#include <limits.h>
#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#else
#include <pthread.h>
#endif

#include "rust/include/delta_sharing_native.h"

#define DELTA_SHARING_ERROR_CAPACITY 1024
#define DELTA_SHARING_INTERRUPT_MESSAGE "delta-sharing stream interrupted"

/*
 * Stable Arrow C Stream ABI. Keeping this small definition local avoids
 * coupling the registered R shim to either nanoarrow's or arrow-rs' headers.
 */
struct ArrowSchema;
struct ArrowArray;
struct ArrowArrayStream {
  int (*get_schema)(struct ArrowArrayStream *, struct ArrowSchema *);
  int (*get_next)(struct ArrowArrayStream *, struct ArrowArray *);
  const char *(*get_last_error)(struct ArrowArrayStream *);
  void (*release)(struct ArrowArrayStream *);
  void *private_data;
};

#ifdef _WIN32
typedef DWORD DeltaSharingThreadId;

static DeltaSharingThreadId delta_sharing_current_thread(void) {
  return GetCurrentThreadId();
}

static int delta_sharing_same_thread(DeltaSharingThreadId left,
                                     DeltaSharingThreadId right) {
  return left == right;
}
#else
typedef pthread_t DeltaSharingThreadId;

static DeltaSharingThreadId delta_sharing_current_thread(void) {
  return pthread_self();
}

static int delta_sharing_same_thread(DeltaSharingThreadId left,
                                     DeltaSharingThreadId right) {
  return pthread_equal(left, right) != 0;
}
#endif

typedef struct {
  ArrowArrayStream inner;
  DeltaSharingThreadId owner_thread;
  int inner_released;
  int interrupted;
} DeltaSharingInterruptStream;

static void delta_sharing_check_interrupt(void *data) {
  (void)data;
  R_CheckUserInterrupt();
}

static int delta_sharing_interrupt_pending(void) {
  return R_ToplevelExec(delta_sharing_check_interrupt, NULL) == FALSE;
}

static void delta_sharing_release_inner(DeltaSharingInterruptStream *state) {
  if (state == NULL || state->inner_released) {
    return;
  }
  state->inner_released = 1;
  if (state->inner.release != NULL) {
    state->inner.release(&state->inner);
  }
}

static int delta_sharing_interrupt_get_schema(ArrowArrayStream *stream,
                                              struct ArrowSchema *schema) {
  DeltaSharingInterruptStream *state =
      stream == NULL ? NULL : stream->private_data;
  if (state == NULL || state->inner_released ||
      state->inner.get_schema == NULL) {
    return EINVAL;
  }
  return state->inner.get_schema(&state->inner, schema);
}

static int delta_sharing_interrupt_get_next(ArrowArrayStream *stream,
                                            struct ArrowArray *array) {
  DeltaSharingInterruptStream *state =
      stream == NULL ? NULL : stream->private_data;
  if (state == NULL) {
    return EINVAL;
  }
  if (state->interrupted) {
    return EINTR;
  }

  /*
   * Imported consumers may pull from their own worker threads. Only the exact
   * R thread that constructed this stream may touch R's interrupt machinery.
   */
  if (delta_sharing_same_thread(
          state->owner_thread, delta_sharing_current_thread()) &&
      delta_sharing_interrupt_pending()) {
    state->interrupted = 1;
    delta_sharing_release_inner(state);
    return EINTR;
  }

  if (state->inner_released || state->inner.get_next == NULL) {
    return EINVAL;
  }
  return state->inner.get_next(&state->inner, array);
}

static const char *delta_sharing_interrupt_get_last_error(
    ArrowArrayStream *stream) {
  DeltaSharingInterruptStream *state =
      stream == NULL ? NULL : stream->private_data;
  if (state == NULL) {
    return "delta-sharing stream state is unavailable";
  }
  if (state->interrupted) {
    return DELTA_SHARING_INTERRUPT_MESSAGE;
  }
  if (!state->inner_released && state->inner.get_last_error != NULL) {
    return state->inner.get_last_error(&state->inner);
  }
  return NULL;
}

static void delta_sharing_interrupt_release(ArrowArrayStream *stream) {
  if (stream == NULL || stream->release == NULL) {
    return;
  }

  DeltaSharingInterruptStream *state = stream->private_data;
  /*
   * Mark the outer stream released before invoking the inner callback so
   * repeated or re-entrant release cannot cancel native ownership twice.
   */
  stream->release = NULL;
  stream->private_data = NULL;
  if (state != NULL) {
    delta_sharing_release_inner(state);
    free(state);
  }
  stream->get_schema = NULL;
  stream->get_next = NULL;
  stream->get_last_error = NULL;
}

static int delta_sharing_install_interrupt_wrapper(ArrowArrayStream *stream) {
  if (stream == NULL || stream->release == NULL ||
      stream->get_schema == NULL || stream->get_next == NULL) {
    return EINVAL;
  }

  DeltaSharingInterruptStream *state =
      (DeltaSharingInterruptStream *)calloc(1, sizeof(*state));
  if (state == NULL) {
    stream->release(stream);
    return ENOMEM;
  }
  state->inner = *stream;
  state->owner_thread = delta_sharing_current_thread();

  stream->get_schema = delta_sharing_interrupt_get_schema;
  stream->get_next = delta_sharing_interrupt_get_next;
  stream->get_last_error = delta_sharing_interrupt_get_last_error;
  stream->release = delta_sharing_interrupt_release;
  stream->private_data = state;
  return 0;
}

static void install_interrupt_wrapper_or_error(ArrowArrayStream *stream) {
  const int status = delta_sharing_install_interrupt_wrapper(stream);
  if (status != 0) {
    Rf_error(
        "Native operation failed (status %d): interruptible Arrow stream "
        "setup failed.",
        status);
  }
}

static int32_t scalar_int32(SEXP value, const char *name) {
  if (TYPEOF(value) != INTSXP || XLENGTH(value) != 1 ||
      INTEGER(value)[0] == NA_INTEGER) {
    Rf_error("`%s` must be one non-missing integer.", name);
  }

  return (int32_t)INTEGER(value)[0];
}

static const char *scalar_utf8(SEXP value, const char *name) {
  if (TYPEOF(value) != STRSXP || XLENGTH(value) != 1 ||
      STRING_ELT(value, 0) == NA_STRING) {
    Rf_error("`%s` must be one non-missing string.", name);
  }

  const char *result = Rf_translateCharUTF8(STRING_ELT(value, 0));
  if (result[0] == '\0') {
    Rf_error("`%s` must not be empty.", name);
  }
  return result;
}

static uint64_t optional_limit(SEXP value, int32_t *has_limit) {
  if (value == R_NilValue) {
    *has_limit = 0;
    return 0;
  }

  double result;
  if (TYPEOF(value) == INTSXP && XLENGTH(value) == 1 &&
      INTEGER(value)[0] != NA_INTEGER) {
    result = (double)INTEGER(value)[0];
  } else if (TYPEOF(value) == REALSXP && XLENGTH(value) == 1 &&
             !ISNA(REAL(value)[0])) {
    result = REAL(value)[0];
  } else {
    Rf_error("`limit` must be NULL or one non-missing number.");
  }

  if (!R_FINITE(result) || result < 0 || result > 9007199254740992.0 ||
      floor(result) != result) {
    Rf_error(
        "`limit` must be a whole number between 0 and 2^53, or NULL.");
  }

  *has_limit = 1;
  return (uint64_t)result;
}

static uint64_t required_version(SEXP value, const char *name) {
  double result;
  if (TYPEOF(value) == INTSXP && XLENGTH(value) == 1 &&
      INTEGER(value)[0] != NA_INTEGER) {
    result = (double)INTEGER(value)[0];
  } else if (TYPEOF(value) == REALSXP && XLENGTH(value) == 1 &&
             !ISNA(REAL(value)[0])) {
    result = REAL(value)[0];
  } else {
    Rf_error("`%s` must be one non-missing number.", name);
  }
  if (!R_FINITE(result) || result < 0 || result > 9007199254740992.0 ||
      floor(result) != result) {
    Rf_error("`%s` must be a whole number between 0 and 2^53.", name);
  }
  return (uint64_t)result;
}

static void raise_native_error(int32_t status, const char *message) {
  const char *safe_message = message;
  if (safe_message == NULL || safe_message[0] == '\0') {
    safe_message = "Native operation failed without a diagnostic message.";
  }

  Rf_error("Native operation failed (status %d): %s", status, safe_message);
}

static SEXP delta_sharing_stream_from_test_data(
    SEXP stream_xptr,
    SEXP batches,
    SEXP rows_per_batch,
    SEXP error_after,
    SEXP panic_after) {
  if (TYPEOF(stream_xptr) != EXTPTRSXP) {
    Rf_error("`stream_xptr` must be an R external pointer.");
  }
  if (!Rf_inherits(stream_xptr, "nanoarrow_array_stream")) {
    Rf_error("`stream_xptr` must inherit from 'nanoarrow_array_stream'.");
  }

  ArrowArrayStream *stream =
      (ArrowArrayStream *)R_ExternalPtrAddr(stream_xptr);
  if (stream == NULL) {
    Rf_error("nanoarrow stream pointer is NULL.");
  }

  const int32_t batches_value = scalar_int32(batches, "batches");
  const int32_t rows_value = scalar_int32(rows_per_batch, "rows_per_batch");
  const int32_t error_value = scalar_int32(error_after, "error_after");
  const int32_t panic_value = scalar_int32(panic_after, "panic_after");

  char error[DELTA_SHARING_ERROR_CAPACITY] = {0};
  const int32_t status = delta_sharing_native_populate_test_stream(
      stream,
      batches_value,
      rows_value,
      error_value,
      panic_value,
      error,
      sizeof(error));

  if (status != 0) {
    raise_native_error(status, error);
  }
  install_interrupt_wrapper_or_error(stream);

  return R_NilValue;
}

static SEXP delta_sharing_stream_from_snapshot(
    SEXP stream_xptr,
    SEXP table_location,
    SEXP cleanup_root,
    SEXP columns,
    SEXP limit,
    SEXP batch_size) {
  if (TYPEOF(stream_xptr) != EXTPTRSXP) {
    Rf_error("`stream_xptr` must be an R external pointer.");
  }
  if (!Rf_inherits(stream_xptr, "nanoarrow_array_stream")) {
    Rf_error("`stream_xptr` must inherit from 'nanoarrow_array_stream'.");
  }

  ArrowArrayStream *stream =
      (ArrowArrayStream *)R_ExternalPtrAddr(stream_xptr);
  if (stream == NULL) {
    Rf_error("nanoarrow stream pointer is NULL.");
  }

  const int32_t batch_size_value = scalar_int32(batch_size, "batch_size");
  if (batch_size_value < 1 || batch_size_value > 1000000) {
    Rf_error("`batch_size` must be between 1 and 1000000.");
  }

  if (columns != R_NilValue && TYPEOF(columns) != STRSXP) {
    Rf_error("`columns` must be NULL or a character vector.");
  }
  const R_xlen_t column_count =
      columns == R_NilValue ? 0 : XLENGTH(columns);
  if (columns != R_NilValue && column_count == 0) {
    Rf_error("`columns` must be NULL or contain at least one name.");
  }
  if (column_count > 10000) {
    Rf_error("`columns` must contain at most 10000 names.");
  }

  const char **column_values = NULL;
  if (column_count > 0) {
    column_values =
        (const char **)R_alloc((size_t)column_count, sizeof(const char *));
    for (R_xlen_t index = 0; index < column_count; ++index) {
      if (STRING_ELT(columns, index) == NA_STRING) {
        Rf_error("`columns` must not contain missing names.");
      }
      column_values[index] = Rf_translateCharUTF8(STRING_ELT(columns, index));
      if (column_values[index][0] == '\0') {
        Rf_error("`columns` must not contain empty names.");
      }
    }
  }

  int32_t has_limit = 0;
  const uint64_t limit_value = optional_limit(limit, &has_limit);
  const char *table_location_value =
      scalar_utf8(table_location, "table_location");
  const char *cleanup_root_value = NULL;
  if (cleanup_root != R_NilValue) {
    cleanup_root_value = scalar_utf8(cleanup_root, "cleanup_root");
  }

  char error[DELTA_SHARING_ERROR_CAPACITY] = {0};
  const int32_t status = delta_sharing_native_populate_snapshot_stream(
      stream,
      table_location_value,
      cleanup_root_value,
      column_values,
      (size_t)column_count,
      has_limit,
      limit_value,
      (uint32_t)batch_size_value,
      error,
      sizeof(error));

  if (status != 0) {
    raise_native_error(status, error);
  }
  install_interrupt_wrapper_or_error(stream);

  return R_NilValue;
}

static SEXP delta_sharing_stream_from_cdf(
    SEXP stream_xptr,
    SEXP table_location,
    SEXP cleanup_root,
    SEXP columns,
    SEXP start_version,
    SEXP end_version,
    SEXP batch_size) {
  if (TYPEOF(stream_xptr) != EXTPTRSXP) {
    Rf_error("`stream_xptr` must be an R external pointer.");
  }
  if (!Rf_inherits(stream_xptr, "nanoarrow_array_stream")) {
    Rf_error("`stream_xptr` must inherit from 'nanoarrow_array_stream'.");
  }
  ArrowArrayStream *stream =
      (ArrowArrayStream *)R_ExternalPtrAddr(stream_xptr);
  if (stream == NULL) {
    Rf_error("nanoarrow stream pointer is NULL.");
  }

  const int32_t batch_size_value = scalar_int32(batch_size, "batch_size");
  if (batch_size_value < 1 || batch_size_value > 1000000) {
    Rf_error("`batch_size` must be between 1 and 1000000.");
  }
  if (columns != R_NilValue && TYPEOF(columns) != STRSXP) {
    Rf_error("`columns` must be NULL or a character vector.");
  }
  const R_xlen_t column_count =
      columns == R_NilValue ? 0 : XLENGTH(columns);
  if (columns != R_NilValue && column_count == 0) {
    Rf_error("`columns` must be NULL or contain at least one name.");
  }
  if (column_count > 10000) {
    Rf_error("`columns` must contain at most 10000 names.");
  }
  const char **column_values = NULL;
  if (column_count > 0) {
    column_values =
        (const char **)R_alloc((size_t)column_count, sizeof(const char *));
    for (R_xlen_t index = 0; index < column_count; ++index) {
      if (STRING_ELT(columns, index) == NA_STRING) {
        Rf_error("`columns` must not contain missing names.");
      }
      column_values[index] = Rf_translateCharUTF8(STRING_ELT(columns, index));
      if (column_values[index][0] == '\0') {
        Rf_error("`columns` must not contain empty names.");
      }
    }
  }

  const char *table_location_value =
      scalar_utf8(table_location, "table_location");
  const char *cleanup_root_value = NULL;
  if (cleanup_root != R_NilValue) {
    cleanup_root_value = scalar_utf8(cleanup_root, "cleanup_root");
  }
  const uint64_t start_version_value =
      required_version(start_version, "start_version");
  const uint64_t end_version_value =
      required_version(end_version, "end_version");

  char error[DELTA_SHARING_ERROR_CAPACITY] = {0};
  const int32_t status = delta_sharing_native_populate_cdf_stream(
      stream,
      table_location_value,
      cleanup_root_value,
      column_values,
      (size_t)column_count,
      start_version_value,
      end_version_value,
      (uint32_t)batch_size_value,
      error,
      sizeof(error));
  if (status != 0) {
    raise_native_error(status, error);
  }
  install_interrupt_wrapper_or_error(stream);
  return R_NilValue;
}

static SEXP delta_sharing_native_diagnostics(void) {
  DeltaSharingNativeInfo info;
  memset(&info, 0, sizeof(info));
  char error[DELTA_SHARING_ERROR_CAPACITY] = {0};

  const int32_t status =
      delta_sharing_native_info(&info, error, sizeof(error));
  if (status != 0) {
    raise_native_error(status, error);
  }

  const int count = 10;
  SEXP result = PROTECT(Rf_allocVector(VECSXP, count));
  SEXP names = PROTECT(Rf_allocVector(STRSXP, count));

  SET_STRING_ELT(names, 0, Rf_mkChar("abi_version"));
  SET_STRING_ELT(names, 1, Rf_mkChar("delta_kernel_version"));
  SET_STRING_ELT(names, 2, Rf_mkChar("arrow_rs_version"));
  SET_STRING_ELT(names, 3, Rf_mkChar("ffi_backend"));
  SET_STRING_ELT(names, 4, Rf_mkChar("kernel_smoke_ok"));
  SET_STRING_ELT(names, 5, Rf_mkChar("kernel_smoke_message"));
  SET_STRING_ELT(names, 6, Rf_mkChar("active_streams"));
  SET_STRING_ELT(names, 7, Rf_mkChar("cancelled_streams"));
  SET_STRING_ELT(names, 8, Rf_mkChar("emitted_batches"));
  SET_STRING_ELT(names, 9, Rf_mkChar("pending_cleanups"));

  SET_VECTOR_ELT(result, 0, Rf_ScalarInteger((int)info.abi_version));
  SET_VECTOR_ELT(result, 1, Rf_mkString(info.delta_kernel_version));
  SET_VECTOR_ELT(result, 2, Rf_mkString(info.arrow_rs_version));
  SET_VECTOR_ELT(result, 3, Rf_mkString(info.ffi_backend));
  SET_VECTOR_ELT(result, 4, Rf_ScalarLogical(info.kernel_smoke_ok));
  SET_VECTOR_ELT(result, 5, Rf_mkString(info.kernel_smoke_message));
  SET_VECTOR_ELT(result, 6, Rf_ScalarReal((double)info.active_streams));
  SET_VECTOR_ELT(result, 7, Rf_ScalarReal((double)info.cancelled_streams));
  SET_VECTOR_ELT(result, 8, Rf_ScalarReal((double)info.emitted_batches));
  SET_VECTOR_ELT(result, 9, Rf_ScalarReal((double)info.pending_cleanups));
  Rf_setAttrib(result, R_NamesSymbol, names);

  UNPROTECT(2);
  return result;
}

static SEXP delta_sharing_reap_pending_cleanups(void) {
  uint64_t pending = 0;
  char error[DELTA_SHARING_ERROR_CAPACITY] = {0};
  const int32_t status =
      delta_sharing_native_reap_pending(&pending, error, sizeof(error));
  if (status != 0) {
    raise_native_error(status, error);
  }
  return Rf_ScalarReal((double)pending);
}

static const R_CallMethodDef call_methods[] = {
    {"delta_sharing_stream_from_test_data",
     (DL_FUNC)&delta_sharing_stream_from_test_data,
     5},
    {"delta_sharing_stream_from_snapshot",
     (DL_FUNC)&delta_sharing_stream_from_snapshot,
     6},
    {"delta_sharing_stream_from_cdf",
     (DL_FUNC)&delta_sharing_stream_from_cdf,
     7},
    {"delta_sharing_native_diagnostics",
     (DL_FUNC)&delta_sharing_native_diagnostics,
     0},
    {"delta_sharing_reap_pending_cleanups",
     (DL_FUNC)&delta_sharing_reap_pending_cleanups,
     0},
    {NULL, NULL, 0}};

void attribute_visible R_init_delta_sharing(DllInfo *dll) {
  R_registerRoutines(dll, NULL, call_methods, NULL, NULL);
  R_useDynamicSymbols(dll, FALSE);
  R_forceSymbols(dll, TRUE);
}

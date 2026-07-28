#include <R.h>
#include <R_ext/Rdynload.h>
#include <R_ext/Visibility.h>
#include <Rinternals.h>

#include <limits.h>
#include <stdint.h>
#include <string.h>

#include "rust/include/delta_sharing_native.h"

#define DELTA_SHARING_ERROR_CAPACITY 1024

static int32_t scalar_int32(SEXP value, const char *name) {
  if (TYPEOF(value) != INTSXP || XLENGTH(value) != 1 ||
      INTEGER(value)[0] == NA_INTEGER) {
    Rf_error("`%s` must be one non-missing integer.", name);
  }

  return (int32_t)INTEGER(value)[0];
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

  const int count = 9;
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

  SET_VECTOR_ELT(result, 0, Rf_ScalarInteger((int)info.abi_version));
  SET_VECTOR_ELT(result, 1, Rf_mkString(info.delta_kernel_version));
  SET_VECTOR_ELT(result, 2, Rf_mkString(info.arrow_rs_version));
  SET_VECTOR_ELT(result, 3, Rf_mkString(info.ffi_backend));
  SET_VECTOR_ELT(result, 4, Rf_ScalarLogical(info.kernel_smoke_ok));
  SET_VECTOR_ELT(result, 5, Rf_mkString(info.kernel_smoke_message));
  SET_VECTOR_ELT(result, 6, Rf_ScalarReal((double)info.active_streams));
  SET_VECTOR_ELT(result, 7, Rf_ScalarReal((double)info.cancelled_streams));
  SET_VECTOR_ELT(result, 8, Rf_ScalarReal((double)info.emitted_batches));
  Rf_setAttrib(result, R_NamesSymbol, names);

  UNPROTECT(2);
  return result;
}

static const R_CallMethodDef call_methods[] = {
    {"delta_sharing_stream_from_test_data",
     (DL_FUNC)&delta_sharing_stream_from_test_data,
     5},
    {"delta_sharing_native_diagnostics",
     (DL_FUNC)&delta_sharing_native_diagnostics,
     0},
    {NULL, NULL, 0}};

void attribute_visible R_init_delta_sharing(DllInfo *dll) {
  R_registerRoutines(dll, NULL, call_methods, NULL, NULL);
  R_useDynamicSymbols(dll, FALSE);
  R_forceSymbols(dll, TRUE);
}

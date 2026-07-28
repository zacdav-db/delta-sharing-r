#!/bin/sh
set -eu

usage() {
    echo "usage: DELTA_SHARING_LINUX_IMAGE=<sha256:id|image@sha256:digest> $0 <source.tar.gz>" >&2
}

if test "$#" -ne 1; then
    usage
    exit 2
fi

engine=${CONTAINER_ENGINE:-podman}
image=${DELTA_SHARING_LINUX_IMAGE:-}
archive=$1

case "$image" in
    sha256:*)
        digest=${image#sha256:}
        ;;
    *@sha256:*)
        digest=${image##*@sha256:}
        ;;
    *)
        digest=
        ;;
esac
if test "${#digest}" -ne 64 ||
    printf '%s' "$digest" | grep -Eq '[^0-9a-f]'; then
    echo "error: DELTA_SHARING_LINUX_IMAGE must use an exact sha256 digest" >&2
    exit 2
fi

if ! command -v "$engine" >/dev/null 2>&1; then
    echo "error: container engine not found: $engine" >&2
    exit 2
fi
if ! test -f "$archive"; then
    echo "error: source archive not found: $archive" >&2
    exit 2
fi

archive_directory=$(CDPATH= cd -- "$(dirname -- "$archive")" && pwd)
archive_path=$archive_directory/$(basename -- "$archive")
archive_contents=$(tar -tzf "$archive_path")

for required in src/rust/vendor.tar.xz src/rust/vendor-config.toml; do
    if ! printf '%s\n' "$archive_contents" | grep -Eq "/${required}$"; then
        echo "error: source archive is missing ${required}" >&2
        exit 2
    fi
done
if printf '%s\n' "$archive_contents" | grep -Eq '/src/vendor/'; then
    echo "error: source archive contains an unpacked vendor directory" >&2
    exit 2
fi

exec "$engine" run \
    --rm \
    --network none \
    --read-only \
    --tmpfs /tmp:rw,exec,nosuid,nodev,size=6g \
    --mount "type=bind,source=${archive_path},destination=/proof/package.tar.gz,readonly" \
    "$image" \
    /bin/sh -ceu '
        for command in R Rscript cargo rustc cmake cc; do
            command -v "$command" >/dev/null
        done
        test "$(rustc --version | sed -n "s/^rustc \([0-9][0-9.]*\).*/\1/p")" = "1.88.0"

        export CARGO_HOME=/tmp/cargo-home
        export CARGO_NET_OFFLINE=true
        export HTTP_PROXY=http://127.0.0.1:9
        export HTTPS_PROXY=http://127.0.0.1:9
        export ALL_PROXY=http://127.0.0.1:9
        export R_LIBS_USER=/tmp/r-library
        mkdir -p "$CARGO_HOME" "$R_LIBS_USER"

        R CMD INSTALL --preclean --library="$R_LIBS_USER" /proof/package.tar.gz
        Rscript -e "
            .libPaths(c(Sys.getenv(\"R_LIBS_USER\"), .libPaths()))
            library(delta.sharing)
            info <- delta.sharing:::.native_diagnostics()
            stopifnot(
                identical(info\$delta_kernel_version, \"0.22.0\"),
                identical(info\$arrow_rs_version, \"57.3.0\"),
                isTRUE(info\$kernel_smoke_ok),
                info\$active_streams == 0
            )
        "

        if find "$R_LIBS_USER/delta.sharing" -type d \
            \( -name .cargo -o -name target -o -name vendor \) \
            | grep -q .; then
            echo "error: installed package retained a native build tree" >&2
            exit 1
        fi
    '

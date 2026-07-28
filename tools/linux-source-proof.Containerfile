ARG BASE_IMAGE
FROM ${BASE_IMAGE}

ARG RUST_VERSION=1.88.0

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        cmake \
        curl \
        libcurl4-openssl-dev \
        libssl-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

RUN curl --proto '=https' --tlsv1.2 --fail --silent --show-error \
        --output /tmp/rustup-init.sh \
        https://sh.rustup.rs \
    && sh /tmp/rustup-init.sh \
        -y --profile minimal --default-toolchain "${RUST_VERSION}" \
    && rm /tmp/rustup-init.sh

ENV PATH="/root/.cargo/bin:${PATH}"

RUN Rscript -e \
    'install.packages(c("httr2", "jsonlite", "nanoarrow", "openssl", "S7"), repos = "https://cloud.r-project.org", Ncpus = 2L)'

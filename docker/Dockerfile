# Download base image ubuntu 22.04
FROM ubuntu:22.04

RUN apt-get update \
    && DEBIAN_FRONTEND="noninteractive" apt-get install --no-install-recommends -y \
    build-essential \
    clang \
    cmake \
    ca-certificates \
    wget \
    curl \
    libprotobuf-dev \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

RUN wget https://github.com/TA-Lib/ta-lib/releases/download/v0.6.3/ta-lib-0.6.3-src.tar.gz \
    && tar -xzf ta-lib-0.6.3-src.tar.gz \
    && cd ta-lib-0.6.3 \
    && ./configure --prefix=/usr \
    && make \
    && make install \
    && cd .. \
    && rm -rf ta-lib-0.6.3 

RUN apt-get update \
    && DEBIAN_FRONTEND="noninteractive" apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH \
    RUST_VERSION=1.81

RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
    sh -s -- -y --default-toolchain ${RUST_VERSION}

COPY . .
RUN pip3 install --no-cache-dir -r requirements.txt

RUN cargo build --release
CMD ["launch.sh"]


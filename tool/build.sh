#!/bin/bash

set -e

cd "$(dirname "$0")/../turbo_mysql_core"

CRATE_NAME="turbo_mysql_core"
OS_TYPE="$(uname -s)"
ARCH_TYPE="$(uname -m)"

if [[ "$OS_TYPE" == "Darwin" ]]; then
  rustup target add aarch64-apple-darwin x86_64-apple-darwin
  cargo build --release --target aarch64-apple-darwin
  cargo build --release --target x86_64-apple-darwin

  UNIVERSAL_DIR="../turbo_mysql/native/macos-universal"
  mkdir -p "$UNIVERSAL_DIR"

  lipo -create \
    target/aarch64-apple-darwin/release/lib${CRATE_NAME}.dylib \
    target/x86_64-apple-darwin/release/lib${CRATE_NAME}.dylib \
    -output "$UNIVERSAL_DIR/lib${CRATE_NAME}.dylib"
  exit 0
fi

if [[ "$OS_TYPE" == Linux* ]]; then
  if command -v aarch64-linux-gnu-gcc >/dev/null 2>&1; then
    rustup target add x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu
    export CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc
    cargo build --release --target x86_64-unknown-linux-gnu
    cargo build --release --target aarch64-unknown-linux-gnu

    mkdir -p ../turbo_mysql/native/linux-x64
    mkdir -p ../turbo_mysql/native/linux-arm-64

    cp target/x86_64-unknown-linux-gnu/release/lib${CRATE_NAME}.so ../turbo_mysql/native/linux-x64/lib${CRATE_NAME}.so
    cp target/aarch64-unknown-linux-gnu/release/lib${CRATE_NAME}.so ../turbo_mysql/native/linux-arm-64/lib${CRATE_NAME}.so
    exit 0
  fi
fi

cargo build --release

case "${OS_TYPE}" in
    Linux*)
        LIB_NAME="lib${CRATE_NAME}.so"
        if [ "${ARCH_TYPE}" = "x86_64" ]; then
            TARGET_FOLDER="linux-x64"
        else
            TARGET_FOLDER="linux-arm-64"
        fi
        ;;
    MINGW*|MSYS*|CYGWIN*)
        LIB_NAME="${CRATE_NAME}.dll"
        TARGET_FOLDER="windows-x64"
        ;;
    *)
        exit 1
        ;;
esac

DEST_DIR="../turbo_mysql/native/${TARGET_FOLDER}"
mkdir -p "${DEST_DIR}"
cp "target/release/${LIB_NAME}" "${DEST_DIR}/${LIB_NAME}"
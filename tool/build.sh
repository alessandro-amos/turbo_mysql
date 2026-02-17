#!/bin/bash

set -e

cd "$(dirname "$0")/../turbo_mysql_core"

cargo build --release

CRATE_NAME="turbo_mysql_core"
OS_TYPE="$(uname -s)"
ARCH_TYPE="$(uname -m)"

case "${OS_TYPE}" in
    Linux*)
        LIB_NAME="lib${CRATE_NAME}.so"
        if [ "${ARCH_TYPE}" = "x86_64" ]; then
            TARGET_FOLDER="linux-x64"
        else
            TARGET_FOLDER="linux-arm-64"
        fi
        ;;
    Darwin*)
        LIB_NAME="lib${CRATE_NAME}.dylib"
        if [ "${ARCH_TYPE}" = "arm64" ]; then
            TARGET_FOLDER="macos-arm-64"
        else
            TARGET_FOLDER="macos-x64"
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
#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "${SCRIPT_DIR}/../.." && pwd)

OUTPUT_DIR=${OUTPUT_DIR:-"${REPO_ROOT}/target/swift-xcframework"}
ARTIFACT_NAME=${ARTIFACT_NAME:-NanoGraphFFI}
HEADER_DIR="${REPO_ROOT}/crates/nanograph-ffi/include"
ARM_TARGET=${ARM_TARGET:-aarch64-apple-darwin}
X86_TARGET=${X86_TARGET:-x86_64-apple-darwin}
INCLUDE_X86_64=${INCLUDE_X86_64:-0}

mkdir -p "${OUTPUT_DIR}"

echo "Building nanograph-ffi for ${ARM_TARGET}"
cargo build -p nanograph-ffi --release --target "${ARM_TARGET}"

ARM_LIB="${REPO_ROOT}/target/${ARM_TARGET}/release/libnanograph_ffi.a"

if [[ ! -f "${ARM_LIB}" ]]; then
  echo "missing expected library: ${ARM_LIB}" >&2
  exit 1
fi

XCFRAMEWORK_DIR="${OUTPUT_DIR}/${ARTIFACT_NAME}.xcframework"
ZIP_PATH="${OUTPUT_DIR}/${ARTIFACT_NAME}.xcframework.zip"
SHA_PATH="${OUTPUT_DIR}/${ARTIFACT_NAME}.xcframework.sha256"

XCFRAMEWORK_ARGS=(
  -library "${ARM_LIB}"
  -headers "${HEADER_DIR}"
)

if [[ "${INCLUDE_X86_64}" == "1" ]]; then
  echo "Building nanograph-ffi for ${X86_TARGET}"
  cargo build -p nanograph-ffi --release --target "${X86_TARGET}"

  X86_LIB="${REPO_ROOT}/target/${X86_TARGET}/release/libnanograph_ffi.a"
  if [[ ! -f "${X86_LIB}" ]]; then
    echo "missing expected library: ${X86_LIB}" >&2
    exit 1
  fi

  XCFRAMEWORK_ARGS+=(
    -library "${X86_LIB}"
    -headers "${HEADER_DIR}"
  )
else
  echo "Skipping ${X86_TARGET} build (set INCLUDE_X86_64=1 to opt in)"
fi

rm -f "${ZIP_PATH}" "${SHA_PATH}"
rm -rf "${XCFRAMEWORK_DIR}"

echo "Packaging XCFramework"
xcodebuild -create-xcframework \
  "${XCFRAMEWORK_ARGS[@]}" \
  -output "${XCFRAMEWORK_DIR}"

echo "Zipping XCFramework"
ditto -c -k --sequesterRsrc --keepParent "${XCFRAMEWORK_DIR}" "${ZIP_PATH}"

echo "Computing checksum"
shasum -a 256 "${ZIP_PATH}" | awk '{print $1}' > "${SHA_PATH}"

echo "Swift XCFramework ready:"
echo "  xcframework: ${XCFRAMEWORK_DIR}"
echo "  zip:         ${ZIP_PATH}"
echo "  checksum:    $(cat "${SHA_PATH}")"

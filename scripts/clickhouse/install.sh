#!/usr/bin/env bash
# Installs a pinned ClickHouse static binary under the repo-local .cache tree.
# Idempotent: skips download when the binary already reports the pinned version.
set -euo pipefail

# --- pin: change these here and in README / up.sh when bumping ---
# CLICKHOUSE_VERSION is the install-root directory and the major.minor match
# prefix used by version_matches_pin. CLICKHOUSE_BUILD is the exact LTS patch
# used in download URLs (packages.clickhouse.com and GitHub Releases).
CLICKHOUSE_VERSION="25.8"
CLICKHOUSE_BUILD="25.8.29.51"
CLICKHOUSE_RELEASE_TAG="v${CLICKHOUSE_BUILD}-lts"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
INSTALL_ROOT="${REPO_ROOT}/.cache/clickhouse/${CLICKHOUSE_VERSION}"

if [ "$(id -u)" -eq 0 ]; then
  echo "refusing to run as root — install is repo-local only" >&2
  exit 1
fi

clickhouse_bin="${INSTALL_ROOT}/clickhouse"

installed_version() {
  if [ ! -x "$clickhouse_bin" ]; then
    return 1
  fi
  "$clickhouse_bin" --version 2>&1 | head -1
}

version_matches_pin() {
  local reported="$1"
  case "$reported" in
    *" ${CLICKHOUSE_VERSION}."*) return 0 ;;
    *" ${CLICKHOUSE_VERSION} "*) return 0 ;;
    *) return 1 ;;
  esac
}

if reported="$(installed_version 2>/dev/null || true)" && [ -n "$reported" ]; then
  if version_matches_pin "$reported"; then
    echo "clickhouse ${CLICKHOUSE_VERSION} already installed at ${clickhouse_bin}"
    exit 0
  fi
fi

os="$(uname -s)"
arch="$(uname -m)"

case "${os}:${arch}" in
  Darwin:arm64) platform="macos-aarch64" ;;
  Darwin:x86_64) platform="macos-amd64" ;;
  Linux:aarch64|Linux:arm64) platform="linux-aarch64" ;;
  Linux:x86_64|Linux:amd64) platform="linux-amd64" ;;
  *)
    echo "unsupported platform: ${os} ${arch}" >&2
    exit 1
    ;;
esac

# Linux: versioned tarballs on packages.clickhouse.com (arch suffix is amd64/arm64).
# macOS: bare binaries on GitHub Releases (no tgz on the packages CDN).
case "$platform" in
  linux-amd64)
    artifact_kind="tarball"
    artifact_name="clickhouse-common-static-${CLICKHOUSE_BUILD}-amd64.tgz"
    download_url="https://packages.clickhouse.com/tgz/stable/${artifact_name}"
    ;;
  linux-aarch64)
    artifact_kind="tarball"
    artifact_name="clickhouse-common-static-${CLICKHOUSE_BUILD}-arm64.tgz"
    download_url="https://packages.clickhouse.com/tgz/stable/${artifact_name}"
    ;;
  macos-aarch64)
    artifact_kind="binary"
    artifact_name="clickhouse-macos-aarch64"
    download_url="https://github.com/ClickHouse/ClickHouse/releases/download/${CLICKHOUSE_RELEASE_TAG}/${artifact_name}"
    ;;
  macos-amd64)
    artifact_kind="binary"
    artifact_name="clickhouse-macos"
    download_url="https://github.com/ClickHouse/ClickHouse/releases/download/${CLICKHOUSE_RELEASE_TAG}/${artifact_name}"
    ;;
esac

mkdir -p "$INSTALL_ROOT"
tmpdir="$(mktemp -d "${INSTALL_ROOT}/.download.XXXXXX")"
trap 'rm -rf "$tmpdir"' EXIT

archive="${tmpdir}/${artifact_name}"

echo "==> downloading clickhouse ${CLICKHOUSE_BUILD} for ${platform}"
echo "    ${download_url}"

if command -v curl >/dev/null 2>&1; then
  curl -fsSL -o "$archive" "$download_url"
elif command -v wget >/dev/null 2>&1; then
  wget -q -O "$archive" "$download_url"
else
  echo "need curl or wget to download" >&2
  exit 1
fi

if [ -n "${CLICKHOUSE_SHA256:-}" ]; then
  if command -v shasum >/dev/null 2>&1; then
    echo "${CLICKHOUSE_SHA256}  ${archive}" | shasum -a 256 -c -
  elif command -v sha256sum >/dev/null 2>&1; then
    echo "${CLICKHOUSE_SHA256}  ${archive}" | sha256sum -c -
  else
    echo "CLICKHOUSE_SHA256 is set but no shasum/sha256sum found" >&2
    exit 1
  fi
else
  echo "warning: CLICKHOUSE_SHA256 is unset — download is unverified" >&2
fi

case "$artifact_kind" in
  binary)
    cp "$archive" "$clickhouse_bin"
    ;;
  tarball)
    extract_dir="${tmpdir}/extract"
    mkdir -p "$extract_dir"
    tar -xzf "$archive" -C "$extract_dir"

    found="$(find "$extract_dir" -type f -name clickhouse ! -path '*/.*' 2>/dev/null | head -1 || true)"
    if [ -z "$found" ]; then
      echo "could not locate clickhouse binary inside ${artifact_name}" >&2
      echo "inspect ${extract_dir} and adjust install.sh if the layout differs" >&2
      exit 1
    fi
    cp "$found" "$clickhouse_bin"
    ;;
esac

chmod +x "$clickhouse_bin"

echo "==> installed ${clickhouse_bin}"

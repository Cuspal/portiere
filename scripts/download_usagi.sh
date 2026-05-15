#!/usr/bin/env bash
# Fetch a SHA-pinned USAGI JAR into ./vendor/usagi.jar.
#
# Used by .github/workflows/usagi-baseline.yml (tag-push only).
# Not used by default CI — USAGI tests are marked @pytest.mark.slow.
#
# USAGI v1.4.4 is the latest stable JAR with batch CLI support
# (https://github.com/OHDSI/Usagi/releases). If you bump the version,
# bump USAGI_JAR_SHA256 too — without that, this script does NOT verify
# the download and will exit 2.

set -euo pipefail

USAGI_VERSION="${USAGI_VERSION:-1.4.4}"
USAGI_JAR_URL="${USAGI_JAR_URL:-https://github.com/OHDSI/Usagi/releases/download/v${USAGI_VERSION}/Usagi-${USAGI_VERSION}.jar}"
USAGI_JAR_SHA256="${USAGI_JAR_SHA256:-}"

VENDOR_DIR="${VENDOR_DIR:-./vendor}"
OUT="${VENDOR_DIR}/usagi.jar"

if [[ -z "${USAGI_JAR_SHA256}" ]]; then
    echo "Error: USAGI_JAR_SHA256 must be set (refusing to download unverified JAR)." >&2
    echo "Hint: pin the SHA from https://github.com/OHDSI/Usagi/releases" >&2
    exit 2
fi

mkdir -p "${VENDOR_DIR}"

echo "Fetching USAGI ${USAGI_VERSION} from ${USAGI_JAR_URL}..."
curl --fail --location --silent --show-error --output "${OUT}" "${USAGI_JAR_URL}"

actual_sha=$(shasum -a 256 "${OUT}" | awk '{print $1}')
if [[ "${actual_sha}" != "${USAGI_JAR_SHA256}" ]]; then
    echo "Error: USAGI JAR SHA256 mismatch" >&2
    echo "  expected: ${USAGI_JAR_SHA256}" >&2
    echo "  actual:   ${actual_sha}" >&2
    rm -f "${OUT}"
    exit 3
fi

echo "OK: ${OUT} ($(stat -f%z "${OUT}" 2>/dev/null || stat -c%s "${OUT}") bytes)"

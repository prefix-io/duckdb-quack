#!/usr/bin/env bash
# Build (and optionally publish) release artifacts for the Prefix quack fork.
#
#   scripts/release.sh                       # build both arches from HEAD into out/
#   scripts/release.sh --arch arm64          # one arch only
#   scripts/release.sh --publish TAG         # build both arches, create GitHub
#                                            # release TAG with assets + SHA256SUMS
#
# The commit must be pushed: artifacts are built from a clean clone of the
# remote, never from the working tree. amd64 builds run emulated on arm64
# hosts (slow but correct); expect ~15 min native, 1h+ emulated.
set -euo pipefail
cd "$(dirname "$0")/.."

ARCHES=(amd64 arm64)
PUBLISH_TAG=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --arch) ARCHES=("$2"); shift 2 ;;
    --publish) PUBLISH_TAG="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 1 ;;
  esac
done

COMMIT=$(git rev-parse HEAD)
if ! git branch -r --contains "$COMMIT" | grep -q .; then
  echo "HEAD ($COMMIT) is not pushed to any remote branch; push first." >&2
  exit 1
fi
if [[ -n $(git status --porcelain -- src test scripts) ]]; then
  echo "WARNING: working tree differs from HEAD; the artifact is built from the pushed commit only." >&2
fi

mkdir -p out
for arch in "${ARCHES[@]}"; do
  rm -rf "out/linux_${arch}" "out/quack-duckdb_v1.5.5-linux_${arch}.duckdb_extension"
  echo "=== building linux/${arch} from ${COMMIT} ==="
  docker buildx build \
    --platform "linux/${arch}" \
    --build-arg "QUACK_COMMIT=${COMMIT}" \
    --target artifact \
    --output "type=local,dest=out/linux_${arch}" \
    -f scripts/artifact.Dockerfile \
    scripts
  mv "out/linux_${arch}/quack.duckdb_extension" "out/quack-duckdb_v1.5.5-linux_${arch}.duckdb_extension"
  cat "out/linux_${arch}/PROVENANCE"
done

(cd out && sha256sum ./*.duckdb_extension > SHA256SUMS && cat SHA256SUMS)

if [[ -n "$PUBLISH_TAG" ]]; then
  gh release create "$PUBLISH_TAG" \
    --target "$COMMIT" \
    --title "$PUBLISH_TAG" \
    --notes "Prefix quack fork artifacts built from ${COMMIT}. See HACKING.md for validation steps run before release." \
    out/*.duckdb_extension out/SHA256SUMS
  echo "Published release $PUBLISH_TAG. Update prefix/common/data_warehouse/duckdb/quack_pin.json in mono."
fi

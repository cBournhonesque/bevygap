#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

container_runtime="${CONTAINER_RUNTIME:-}"
if [[ -z "$container_runtime" ]]; then
  if command -v podman >/dev/null 2>&1; then
    container_runtime=podman
  else
    container_runtime=docker
  fi
fi

if ! command -v "$container_runtime" >/dev/null 2>&1; then
  echo "gen-edgegap-client: missing container runtime '$container_runtime'" >&2
  exit 1
fi

spec_url="${EDGEGAP_OPENAPI_SPEC_URL:-https://api.edgegap.com/swagger.json}"
generator_image="${EDGEGAP_OPENAPI_GENERATOR_IMAGE:-openapitools/openapi-generator-cli:v7.11.0}"
spec_path="${EDGEGAP_OPENAPI_SPEC_PATH:-target/edgegap-openapi/swagger.json}"

mkdir -p "$(dirname "$spec_path")"
curl -fsSL "$spec_url" -o "$spec_path"

"$container_runtime" run \
  --rm \
  -v "$repo_root:/local:Z" \
  "$generator_image" generate \
  -i "/local/$spec_path" \
  -g rust \
  -o /local/edgegap_async/ \
  --additional-properties=packageName=edgegap_async,packageVersion=0.1.0

# Auto-generated Cargo metadata is not quite what we publish/check in.
# Keep this post-processing small and explicit; do not hand-edit generated src files.
tmp_file="$(mktemp)"
sed \
  -e 's/^description =.*$/description = "Auto-generated client library for the Edgegap API (async), used by bevygap"/' \
  -e '/^description.*/a \
repository = "https://github.com/RJ/bevygap/"' \
  edgegap_async/Cargo.toml > "$tmp_file"
mv "$tmp_file" edgegap_async/Cargo.toml

cargo fmt -p edgegap_async

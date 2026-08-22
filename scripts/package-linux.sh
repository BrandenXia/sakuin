#!/usr/bin/env bash
set -euo pipefail

[[ "$(uname -s)" == "Linux" ]] || {
  printf 'error: Linux release bundles must be packaged on Linux\n' >&2
  exit 1
}

script_directory="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
project_directory="$(cd -- "${script_directory}/.." && pwd)"
machine="$(uname -m)"
case "${machine}" in
x86_64)
  release_architecture=amd64
  ;;
aarch64|arm64)
  release_architecture=arm64
  ;;
*)
  printf 'error: unsupported Linux architecture: %s\n' "${machine}" >&2
  exit 1
  ;;
esac

binary_directory="${1:-}"
if [[ -z "${binary_directory}" ]]; then
  discovered_binary="$(find "${project_directory}/build/linux" \
    -path '*/release/sakuin' -type f -perm -u+x -print -quit 2>/dev/null || true)"
  [[ -n "${discovered_binary}" ]] || {
    printf 'error: no prebuilt Linux sakuin binary found beneath %s/build/linux\n' \
      "${project_directory}" >&2
    exit 1
  }
  binary_directory="$(dirname -- "${discovered_binary}")"
fi
output_directory="${2:-${project_directory}/dist}"
for binary in sakuin sakuin-api-key; do
  [[ -x "${binary_directory}/${binary}" ]] || {
    printf 'error: missing executable: %s/%s\n' "${binary_directory}" "${binary}" >&2
    exit 1
  }
done

staging_directory="$(mktemp -d)"
trap 'rm -rf -- "${staging_directory}"' EXIT
install -d "${staging_directory}/bin" "${staging_directory}/lib" "${output_directory}"
install -m 0755 "${binary_directory}/sakuin" \
  "${binary_directory}/sakuin-api-key" "${staging_directory}/bin/"

while IFS= read -r dependency; do
  [[ -n "${dependency}" ]] || continue
  case "$(basename -- "${dependency}")" in
  ld-linux*|libc.so.*|libdl.so.*|libm.so.*|libpthread.so.*|libresolv.so.*|librt.so.*)
    continue
    ;;
  esac
  cp -L -- "${dependency}" "${staging_directory}/lib/"
done < <(ldd "${binary_directory}/sakuin" \
  "${binary_directory}/sakuin-api-key" | \
  awk '/=> \/[^ ]+/ { print $3 } /^\// && $1 !~ /:$/ { print $1 }' | sort -u)

if [[ -z "$(find "${staging_directory}/lib" -mindepth 1 -print -quit)" ]]; then
  rmdir "${staging_directory}/lib"
fi

archive="sakuin-linux-${release_architecture}.tar.gz"
tar --create --gzip --file "${output_directory}/${archive}" \
  --directory "${staging_directory}" .
(
  cd "${output_directory}"
  sha256sum "${archive}" >"${archive}.sha256"
)
printf 'Created %s and %s.sha256\n' \
  "${output_directory}/${archive}" "${output_directory}/${archive}"

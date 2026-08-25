#!/usr/bin/env bash
set -euo pipefail

script_directory="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
project_directory="$(cd -- "${script_directory}/.." && pwd)"
cd "${project_directory}"

compose=(docker compose --project-directory "${project_directory}")
credential_directory=/var/lib/sakuin/operational/api

fail() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

require_docker() {
  command -v docker >/dev/null 2>&1 || fail "Docker is not installed"
  docker compose version >/dev/null 2>&1 ||
    fail "The Docker Compose plugin is not installed"
  docker info >/dev/null 2>&1 || fail "The Docker daemon is not available"
}

env_value() {
  local name="$1"
  local value=""
  if [[ -f .env ]]; then
    value="$(sed -n "s/^${name}=//p" .env | tail -n 1)"
    value="${value%\"}"
    value="${value#\"}"
    value="${value%\'}"
    value="${value#\'}"
  fi
  printf '%s' "${value}"
}

credential_command() {
  "${compose[@]}" run --rm --no-deps --entrypoint sakuin-api-key sakuin \
    --state-dir "${credential_directory}" "$@"
}

initialize_credentials() {
  local listing=""
  if ! listing="$(credential_command list 2>/dev/null)"; then
    credential_command init
    listing="$(credential_command list)"
  fi
  if ! grep -q '^reader[[:space:]]' <<<"${listing}"; then
    printf 'One-time reader API token:\n'
    credential_command create --id reader --permissions search
  fi
  if ! grep -q '^operator[[:space:]]' <<<"${listing}"; then
    printf 'One-time operator API token:\n'
    credential_command create --id operator --permissions admin
  fi
}

wait_for_ready() {
  local attempt
  for ((attempt = 1; attempt <= 30; ++attempt)); do
    if "${compose[@]}" exec -T sakuin \
      curl --fail --silent http://127.0.0.1:8080/v1/ready >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  "${compose[@]}" logs --tail=100 sakuin >&2 || true
  fail "Sakuin did not become ready within 30 seconds"
}

fetch_metrics() {
  local token="$1"
  "${compose[@]}" exec -T sakuin curl --fail --silent --show-error \
    -H "Authorization: Bearer ${token}" \
    http://127.0.0.1:8080/metrics
}

sample_activity() (
  local token="$1"
  local window_seconds="$2"
  [[ "${window_seconds}" =~ ^[0-9]+$ ]] &&
    ((window_seconds >= 1 && window_seconds <= 3600)) ||
    fail "activity window must be an integer from 1 to 3600 seconds"

  local sample_directory
  sample_directory="$(mktemp -d)"
  trap 'rm -rf -- "${sample_directory}"' EXIT
  fetch_metrics "${token}" >"${sample_directory}/before.prom"
  printf 'Sampling crawler activity for %s seconds...\n' "${window_seconds}" >&2
  sleep "${window_seconds}"
  fetch_metrics "${token}" >"${sample_directory}/after.prom"
  awk -v window_seconds="${window_seconds}" \
    -f "${script_directory}/metrics-delta.awk" \
    "${sample_directory}/before.prom" "${sample_directory}/after.prom"
)

deploy() {
  local api_port="${SAKUIN_API_PORT:-}"
  "${compose[@]}" config --quiet
  "${compose[@]}" pull
  initialize_credentials
  "${compose[@]}" up --detach
  wait_for_ready
  if [[ -z "${api_port}" ]]; then
    api_port="$(env_value SAKUIN_API_PORT)"
  fi
  printf 'Sakuin is ready at http://127.0.0.1:%s\n' "${api_port:-8080}"
}

command="${1:-up}"
require_docker

case "${command}" in
up)
  deploy
  ;;
down)
  "${compose[@]}" down
  ;;
logs)
  "${compose[@]}" logs --follow --tail=100 sakuin
  ;;
status)
  "${compose[@]}" ps
  if [[ -n "${2:-}" ]]; then
    "${compose[@]}" exec -T sakuin curl --fail --silent --show-error \
      -H "Authorization: Bearer ${2}" \
      http://127.0.0.1:8080/v1/status
    printf '\n'
  fi
  ;;
metrics)
  [[ -n "${2:-}" ]] || fail "usage: $0 metrics OPERATOR_TOKEN"
  fetch_metrics "${2}"
  ;;
activity)
  [[ -n "${2:-}" ]] ||
    fail "usage: $0 activity OPERATOR_TOKEN [WINDOW_SECONDS]"
  sample_activity "${2}" "${3:-10}"
  ;;
maintenance)
  [[ -n "${2:-}" ]] || fail "usage: $0 maintenance OPERATOR_TOKEN [verify]"
  maintenance_url=http://127.0.0.1:8080/v1/operations/storage-maintenance
  if [[ "${3:-}" == "verify" ]]; then
    maintenance_url="${maintenance_url}?verify=true"
  elif [[ -n "${3:-}" ]]; then
    fail "usage: $0 maintenance OPERATOR_TOKEN [verify]"
  fi
  "${compose[@]}" exec -T sakuin curl --fail --silent --show-error \
    --request POST -H "Authorization: Bearer ${2}" "${maintenance_url}"
  printf '\n'
  ;;
verify)
  "${compose[@]}" exec -T sakuin sakuin admin verify
  ;;
key)
  key_id="${2:-}"
  permissions="${3:-search}"
  [[ -n "${key_id}" ]] || fail "usage: $0 key KEY_ID [search|admin|search,admin]"
  credential_command create --id "${key_id}" --permissions "${permissions}"
  "${compose[@]}" kill --signal SIGHUP sakuin >/dev/null 2>&1 || true
  ;;
*)
  fail "usage: $0 [up|down|logs|status [OPERATOR_TOKEN]|metrics OPERATOR_TOKEN|activity OPERATOR_TOKEN [WINDOW_SECONDS]|maintenance OPERATOR_TOKEN [verify]|verify|key KEY_ID [PERMISSIONS]]"
  ;;
esac

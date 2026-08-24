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

wait_for_health() {
  local attempt
  for ((attempt = 1; attempt <= 30; ++attempt)); do
    if "${compose[@]}" exec -T sakuin \
      curl --fail --silent http://127.0.0.1:8080/v1/health >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  "${compose[@]}" logs --tail=100 sakuin >&2 || true
  fail "Sakuin did not become healthy within 30 seconds"
}

deploy() {
  local api_port="${SAKUIN_API_PORT:-}"
  "${compose[@]}" config --quiet
  "${compose[@]}" pull
  initialize_credentials
  "${compose[@]}" up --detach
  wait_for_health
  if [[ -z "${api_port}" ]]; then
    api_port="$(env_value SAKUIN_API_PORT)"
  fi
  printf 'Sakuin is healthy at http://127.0.0.1:%s\n' "${api_port:-8080}"
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
  "${compose[@]}" exec -T sakuin curl --fail --silent --show-error \
    -H "Authorization: Bearer ${2}" \
    http://127.0.0.1:8080/metrics
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
  fail "usage: $0 [up|down|logs|status [OPERATOR_TOKEN]|metrics OPERATOR_TOKEN|verify|key KEY_ID [PERMISSIONS]]"
  ;;
esac

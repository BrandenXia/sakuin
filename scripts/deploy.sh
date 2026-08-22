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

require_bootstrap() {
  local bootstrap="${SAKUIN_DHT_BOOTSTRAP:-}"
  if [[ -z "${bootstrap}" ]]; then
    bootstrap="$(env_value SAKUIN_DHT_BOOTSTRAP)"
  fi
  if [[ -z "${bootstrap}" && "${SAKUIN_ALLOW_PASSIVE_DHT:-0}" != "1" ]]; then
    fail "set SAKUIN_DHT_BOOTSTRAP in .env to trusted host:port contacts (or set SAKUIN_ALLOW_PASSIVE_DHT=1)"
  fi
}

credential_command() {
  "${compose[@]}" run --rm --no-deps --entrypoint sakuin-api-key sakuin \
    --state-dir="${credential_directory}" "$@"
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
  local release_version="${SAKUIN_VERSION:-}"
  require_bootstrap
  "${compose[@]}" config --quiet
  if [[ -z "${release_version}" ]]; then
    release_version="$(env_value SAKUIN_VERSION)"
  fi
  if [[ "${release_version:-latest}" == "latest" ]]; then
    # A stable Docker build argument would otherwise keep an older cached
    # `latest` bundle after a new GitHub release is published.
    "${compose[@]}" build --pull --no-cache
  else
    "${compose[@]}" build --pull
  fi
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
  ;;
verify)
  "${compose[@]}" exec -T sakuin sakuin admin verify \
    --config=/etc/sakuin/sakuin.toml
  ;;
key)
  key_id="${2:-}"
  permissions="${3:-search}"
  [[ -n "${key_id}" ]] || fail "usage: $0 key KEY_ID [search|admin|search,admin]"
  credential_command create --id "${key_id}" --permissions "${permissions}"
  "${compose[@]}" kill --signal SIGHUP sakuin >/dev/null 2>&1 || true
  ;;
*)
  fail "usage: $0 [up|down|logs|status|verify|key KEY_ID [PERMISSIONS]]"
  ;;
esac

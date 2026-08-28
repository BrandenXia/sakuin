#!/usr/bin/env bash
set -euo pipefail

script_directory="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
project_directory="$(cd -- "${script_directory}/.." && pwd)"

base_url="${SAKUIN_BENCH_URL:-http://127.0.0.1:8080}"
token="${SAKUIN_BENCH_TOKEN:-}"
token_file=""
requests=20
concurrency=4
warmup=2
timeout_seconds=30
memory_budget_mib="${SAKUIN_BENCH_MEMORY_BUDGET_MIB:-200}"
soak_seconds=0
selected_target=all
search_path='/v1/search?q=linux&limit=20'
container="${SAKUIN_BENCH_CONTAINER:-}"
docker_enabled=true
monitor_pid=""
temporary_directory=""
benchmark_failed=false
soak_start_us=""

usage() {
  cat <<'EOF'
Usage: benchmark-deployment.sh [OPTIONS]

Run a read-only benchmark against an already deployed Sakuin service.

Options:
  --url URL             API base URL (default: http://127.0.0.1:8080)
  --token-file PATH     Read the operator token from PATH
  --requests COUNT      Measured requests per endpoint (default: 20)
  --concurrency COUNT   Concurrent requests per endpoint (default: 4)
  --warmup COUNT        Warm-up requests per endpoint (default: 2)
  --timeout SECONDS     Per-request timeout (default: 30)
  --memory-budget MIB   Container-memory comparison budget (default: 200)
  --soak-seconds COUNT  Observe background container-memory growth after the
                        request benchmark (default: 0; requires Docker)
  --only TARGET         all, health, search, torznab, status, or metrics
  --search-path PATH    Native search path and encoded query string
  --container NAME      Docker container name or ID to monitor
  --no-docker           Skip Docker CPU and memory sampling
  -h, --help            Show this help

The operator token is read from --token-file, SAKUIN_BENCH_TOKEN, or a hidden
interactive prompt, in that order. Benchmark results are printed to stdout;
progress and diagnostics go to stderr, so the report can be redirected safely.
EOF
}

fail() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "$1 is required"
}

positive_integer() {
  local name="$1"
  local value="$2"
  local maximum="$3"
  if [[ ! "${value}" =~ ^[0-9]+$ || ${#value} -gt 7 ]]; then
    fail "${name} must be an integer from 1 to ${maximum}"
  fi
  value="$((10#${value}))"
  if ((value < 1 || value > maximum)); then
    fail "${name} must be an integer from 1 to ${maximum}"
  fi
  printf '%s' "${value}"
}

nonnegative_integer() {
  local name="$1"
  local value="$2"
  local maximum="$3"
  if [[ ! "${value}" =~ ^[0-9]+$ || ${#value} -gt 7 ]]; then
    fail "${name} must be an integer from 0 to ${maximum}"
  fi
  value="$((10#${value}))"
  if ((value > maximum)); then
    fail "${name} must be an integer from 0 to ${maximum}"
  fi
  printf '%s' "${value}"
}

while (($# > 0)); do
  case "$1" in
  --url)
    (($# >= 2)) || fail "--url requires a value"
    base_url="$2"
    shift 2
    ;;
  --token-file)
    (($# >= 2)) || fail "--token-file requires a value"
    token_file="$2"
    shift 2
    ;;
  --requests)
    (($# >= 2)) || fail "--requests requires a value"
    requests="$2"
    shift 2
    ;;
  --concurrency)
    (($# >= 2)) || fail "--concurrency requires a value"
    concurrency="$2"
    shift 2
    ;;
  --warmup)
    (($# >= 2)) || fail "--warmup requires a value"
    warmup="$2"
    shift 2
    ;;
  --timeout)
    (($# >= 2)) || fail "--timeout requires a value"
    timeout_seconds="$2"
    shift 2
    ;;
  --memory-budget)
    (($# >= 2)) || fail "--memory-budget requires a value"
    memory_budget_mib="$2"
    shift 2
    ;;
  --soak-seconds)
    (($# >= 2)) || fail "--soak-seconds requires a value"
    soak_seconds="$2"
    shift 2
    ;;
  --only)
    (($# >= 2)) || fail "--only requires a value"
    selected_target="$2"
    shift 2
    ;;
  --search-path)
    (($# >= 2)) || fail "--search-path requires a value"
    search_path="$2"
    shift 2
    ;;
  --container)
    (($# >= 2)) || fail "--container requires a value"
    container="$2"
    shift 2
    ;;
  --no-docker)
    docker_enabled=false
    shift
    ;;
  -h | --help)
    usage
    exit 0
    ;;
  *)
    fail "unknown option: $1"
    ;;
  esac
done

requests="$(positive_integer requests "${requests}" 100000)"
concurrency="$(positive_integer concurrency "${concurrency}" 256)"
warmup="$(nonnegative_integer warmup "${warmup}" 1000)"
timeout_seconds="$(positive_integer timeout "${timeout_seconds}" 3600)"
memory_budget_mib="$(positive_integer memory-budget "${memory_budget_mib}" 1048576)"
soak_seconds="$(nonnegative_integer soak-seconds "${soak_seconds}" 604800)"
if ((concurrency > requests)); then
  concurrency="${requests}"
fi

case "${selected_target}" in
all | health | search | torznab | status | metrics) ;;
*) fail "--only must be all, health, search, torznab, status, or metrics" ;;
esac

base_url="${base_url%/}"
if [[ ! "${base_url}" =~ ^https?:// || "${base_url}" =~ [[:space:]\"\\] ]]; then
  fail "--url must be an HTTP(S) URL without whitespace, quotes, or backslashes"
fi
if [[ "${search_path}" != /* || "${search_path}" =~ [[:space:]\"\\\|] ]]; then
  fail "--search-path must begin with / and contain no whitespace, quotes, pipes, or backslashes"
fi

require_command curl
require_command awk
require_command sort
require_command mktemp
require_command date
curl --help all 2>/dev/null | grep -q -- '--parallel' ||
  fail "curl must support parallel transfers"

if [[ -n "${token_file}" ]]; then
  [[ -r "${token_file}" ]] || fail "token file is not readable: ${token_file}"
  token="$(<"${token_file}")"
elif [[ -z "${token}" && -t 0 ]]; then
  read -r -s -p 'Sakuin operator token: ' token
  printf '\n' >&2
fi
[[ -n "${token}" ]] ||
  fail "provide an operator token with --token-file or SAKUIN_BENCH_TOKEN"
if [[ "${token}" == *$'\n'* || "${token}" == *$'\r'* ||
  "${token}" == *\"* || "${token}" == *\\* ]]; then
  fail "operator token contains unsupported characters"
fi

temporary_directory="$(mktemp -d)"
chmod 700 "${temporary_directory}"

cleanup() {
  if [[ -n "${monitor_pid}" ]]; then
    kill "${monitor_pid}" >/dev/null 2>&1 || true
    wait "${monitor_pid}" >/dev/null 2>&1 || true
  fi
  if [[ -n "${temporary_directory}" && -d "${temporary_directory}" ]]; then
    rm -rf -- "${temporary_directory}"
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

curl_config="${temporary_directory}/curl.conf"
{
  printf 'silent\n'
  printf 'show-error\n'
  printf 'connect-timeout = "%s"\n' "${timeout_seconds}"
  printf 'max-time = "%s"\n' "${timeout_seconds}"
  printf 'header = "Authorization: Bearer %s"\n' "${token}"
} >"${curl_config}"
chmod 600 "${curl_config}"

fetch() {
  curl --config "${curl_config}" --fail "$@"
}

printf 'Checking readiness and operator credentials...\n' >&2
fetch --output /dev/null "${base_url}/v1/ready" ||
  fail "Sakuin is not ready at ${base_url}"
fetch "${base_url}/metrics" >"${temporary_directory}/metrics-before.prom" ||
  fail "the supplied token does not have operator access"

if [[ "${selected_target}" == all ]]; then
  authenticated_requests=$(((requests + warmup) * 4 + 2))
elif [[ "${selected_target}" == health ]]; then
  authenticated_requests=2
else
  authenticated_requests=$((requests + warmup + 2))
fi
if ((authenticated_requests > 100)); then
  printf 'warning: this run plans %s authenticated requests; the default deployment permits 120 per minute, so HTTP 429 responses may measure rate limiting rather than service capacity\n' \
    "${authenticated_requests}" >&2
fi

if [[ "${docker_enabled}" == true && -z "${container}" ]] &&
  command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1 &&
  docker compose version >/dev/null 2>&1; then
  container="$(docker compose --project-directory "${project_directory}" ps -q sakuin 2>/dev/null || true)"
fi
if [[ "${docker_enabled}" == true && -n "${container}" ]]; then
  command -v docker >/dev/null 2>&1 || fail "docker is required for container sampling"
  docker inspect "${container}" >/dev/null 2>&1 ||
    fail "Docker container was not found: ${container}"
elif [[ "${docker_enabled}" == true ]]; then
  printf 'Docker container not detected; API benchmarks will run without container sampling. Use --container NAME to select it explicitly.\n' >&2
fi
if ((soak_seconds > 0)) &&
  { [[ "${docker_enabled}" != true ]] || [[ -z "${container}" ]]; }; then
  fail "--soak-seconds requires a detected container or --container NAME"
fi

now_microseconds() {
  local candidate=""
  local seconds=""
  local fraction=""
  if [[ "${EPOCHREALTIME:-}" =~ ^([0-9]+)\.([0-9]+)$ ]]; then
    seconds="${BASH_REMATCH[1]}"
    fraction="${BASH_REMATCH[2]}000000"
    printf '%s%s' "${seconds}" "${fraction:0:6}"
    return
  fi
  candidate="$(date +%s%N 2>/dev/null || true)"
  if [[ "${candidate}" =~ ^[0-9]{16,}$ ]]; then
    printf '%s' "${candidate:0:${#candidate}-3}"
    return
  fi
  if command -v perl >/dev/null 2>&1; then
    perl -MTime::HiRes=time -e 'printf "%.0f", time() * 1000000'
    return
  fi
  printf '%s000000' "$(date +%s)"
}

sample_container() {
  local sample=""
  sample="$(docker stats --no-stream \
    --format '{{.MemUsage}}|{{.MemPerc}}|{{.CPUPerc}}|{{.PIDs}}' \
    "${container}" 2>/dev/null || true)"
  if [[ -n "${sample}" ]]; then
    printf '%s|%s\n' "$(now_microseconds)" "${sample}" \
      >>"${temporary_directory}/container.samples"
  fi
}

monitor_container() {
  while true; do
    sample_container
    sleep 1
  done
}

if [[ "${docker_enabled}" == true && -n "${container}" ]]; then
  monitor_container &
  monitor_pid=$!
fi

printf 'endpoint\trequests\tsuccessful\tfailed\twall_seconds\tsuccess_rps\tmean_ms\tp50_ms\tp95_ms\tp99_ms\tmax_ms\tavg_bytes\n'

run_target() {
  local label="$1"
  local path="$2"
  local url="${base_url}${path}"
  local sample_file="${temporary_directory}/${label}.samples"
  local latency_file="${temporary_directory}/${label}.latencies"
  local workload_file="${temporary_directory}/${label}.curl"
  local start_us=""
  local end_us=""
  local elapsed_us=""
  local elapsed_seconds=""
  local curl_status=0
  local completed=0
  local successful=0
  local failed=0
  local success_rps=0
  local latency_summary='0 0 0 0 0'
  local mean_ms=0
  local p50_ms=0
  local p95_ms=0
  local p99_ms=0
  local max_ms=0
  local average_bytes=0

  printf 'Warming %s (%s requests)...\n' "${label}" "${warmup}" >&2
  for ((request = 0; request < warmup; ++request)); do
    fetch --output /dev/null "${url}" ||
      fail "warm-up request failed for ${label}"
  done

  {
    printf 'parallel\n'
    printf 'parallel-max = "%s"\n' "${concurrency}"
    printf 'write-out = "%%{http_code}\\t%%{time_total}\\t%%{size_download}\\n"\n'
    for ((request = 0; request < requests; ++request)); do
      # curl associates --output with one URL, so repeat it for every parallel
      # transfer to keep response bodies out of the timing records.
      printf 'output = "/dev/null"\n'
      printf 'url = "%s"\n' "${url}"
    done
  } >"${workload_file}"
  chmod 600 "${workload_file}"

  printf 'Benchmarking %s (%s requests, concurrency %s)...\n' \
    "${label}" "${requests}" "${concurrency}" >&2
  start_us="$(now_microseconds)"
  curl --config "${curl_config}" --config "${workload_file}" >"${sample_file}" ||
    curl_status=$?
  end_us="$(now_microseconds)"
  elapsed_us=$((end_us - start_us))
  if ((elapsed_us < 1)); then
    elapsed_us=1
  fi
  elapsed_seconds="$(awk -v value="${elapsed_us}" 'BEGIN { printf "%.6f", value / 1000000 }')"

  completed="$(awk 'NF >= 3 { count++ } END { print count + 0 }' "${sample_file}")"
  successful="$(awk 'NF >= 3 && $1 >= 200 && $1 < 300 { count++ } END { print count + 0 }' "${sample_file}")"
  failed=$((requests - successful))
  if ((failed < 0)); then
    failed=0
  fi
  awk 'NF >= 3 { print $2 + 0 }' "${sample_file}" | sort -n >"${latency_file}"
  if ((completed > 0)); then
    latency_summary="$(awk '
      { values[++count] = $1 * 1000; sum += values[count] }
      END {
        p50 = int(count * 0.50 + 0.999999)
        p95 = int(count * 0.95 + 0.999999)
        p99 = int(count * 0.99 + 0.999999)
        printf "%.3f %.3f %.3f %.3f %.3f", sum / count, values[p50],
               values[p95], values[p99], values[count]
      }' "${latency_file}")"
    read -r mean_ms p50_ms p95_ms p99_ms max_ms <<<"${latency_summary}"
  fi
  success_rps="$(awk -v count="${successful}" -v elapsed="${elapsed_us}" \
    'BEGIN { printf "%.3f", count * 1000000 / elapsed }')"
  average_bytes="$(awk '
    NF >= 3 && $1 >= 200 && $1 < 300 { bytes += $3; count++ }
    END { printf "%.0f", count ? bytes / count : 0 }' "${sample_file}")"

  printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
    "${label}" "${requests}" "${successful}" "${failed}" \
    "${elapsed_seconds}" "${success_rps}" "${mean_ms}" "${p50_ms}" \
    "${p95_ms}" "${p99_ms}" "${max_ms}" "${average_bytes}"

  if ((curl_status != 0 || successful != requests)); then
    benchmark_failed=true
    printf '%s response codes: ' "${label}" >&2
    awk 'NF >= 1 { codes[$1]++ } END {
      separator = ""
      for (code in codes) {
        printf "%s%s=%s", separator, code, codes[code]
        separator = ", "
      }
      print ""
    }' "${sample_file}" >&2
  fi
}

run_selected() {
  local label="$1"
  local path="$2"
  if [[ "${selected_target}" == all || "${selected_target}" == "${label}" ]]; then
    run_target "${label}" "${path}"
  fi
}

run_selected health '/v1/health'
run_selected search "${search_path}"
run_selected torznab "/torznab/api?t=search&q=linux&limit=20&apikey=${token}"
run_selected status '/v1/status'
run_selected metrics '/metrics'

if ((soak_seconds > 0)); then
  printf 'Observing background container memory for %s seconds...\n' \
    "${soak_seconds}" >&2
  soak_start_us="$(now_microseconds)"
  sample_container
  for ((elapsed = 0; elapsed < soak_seconds; ++elapsed)); do
    sleep 1
    if (((elapsed + 1) % 60 == 0 && elapsed + 1 < soak_seconds)); then
      printf 'Memory observation: %s/%s seconds...\n' "$((elapsed + 1))" \
        "${soak_seconds}" >&2
    fi
  done
  sample_container
fi

if [[ -n "${monitor_pid}" ]]; then
  kill "${monitor_pid}" >/dev/null 2>&1 || true
  wait "${monitor_pid}" >/dev/null 2>&1 || true
  monitor_pid=""
  sample_container
fi

fetch "${base_url}/metrics" >"${temporary_directory}/metrics-after.prom" ||
  fail "could not collect final operator metrics"

metric_value() {
  local metric="$1"
  local file="$2"
  awk -v expected="${metric}" '$1 == expected { print $2; exit }' "${file}"
}

searchable_records() {
  awk '$1 ~ /^sakuin_classification_records\{/ { total += $2 }
       END { print total + 0 }' "$1"
}

before_projection="$(metric_value sakuin_search_index_estimated_memory_bytes \
  "${temporary_directory}/metrics-before.prom")"
after_projection="$(metric_value sakuin_search_index_estimated_memory_bytes \
  "${temporary_directory}/metrics-after.prom")"
before_records="$(searchable_records "${temporary_directory}/metrics-before.prom")"
after_records="$(searchable_records "${temporary_directory}/metrics-after.prom")"

printf '\nservice_metric\tbefore\tafter\tdelta\n'
awk -v before="${before_projection:-0}" -v after="${after_projection:-0}" \
  'BEGIN { printf "search_projection_mib\t%.3f\t%.3f\t%.3f\n",
                  before / 1048576, after / 1048576,
                  (after - before) / 1048576 }'
printf 'searchable_records\t%s\t%s\t%s\n' "${before_records}" \
  "${after_records}" "$((after_records - before_records))"
awk -v bytes="${after_projection:-0}" -v records="${after_records}" \
  'BEGIN { printf "search_projection_bytes_per_record\t-\t%.1f\t-\n",
                  records ? bytes / records : 0 }'

if [[ "${docker_enabled}" == true && -n "${container}" &&
  -s "${temporary_directory}/container.samples" ]]; then
  printf '\ncontainer\tmemory_start_mib\tmemory_peak_mib\tmemory_end_mib\tmax_memory_percent\tmax_cpu_percent\tmax_pids\tmemory_budget_mib\twithin_budget\n'
  awk -F '|' -v name="${container}" -v budget="${memory_budget_mib}" '
    function memory_mib(value, parts, number, unit) {
      split(value, parts, " ")
      match(parts[1], /^[0-9.]+/)
      number = substr(parts[1], RSTART, RLENGTH) + 0
      unit = substr(parts[1], RLENGTH + 1)
      if (unit == "B") return number / 1048576
      if (unit == "kB" || unit == "KB" || unit == "KiB") return number / 1024
      if (unit == "MB" || unit == "MiB") return number
      if (unit == "GB" || unit == "GiB") return number * 1024
      if (unit == "TB" || unit == "TiB") return number * 1048576
      return number / 1048576
    }
    function percent(value) { gsub(/%/, "", value); return value + 0 }
    {
      memory = memory_mib($2)
      if (count++ == 0) start = memory
      end = memory
      if (memory > peak) peak = memory
      memory_percent = percent($3)
      cpu_percent = percent($4)
      if (memory_percent > max_memory_percent) max_memory_percent = memory_percent
      if (cpu_percent > max_cpu_percent) max_cpu_percent = cpu_percent
      if ($5 + 0 > max_pids) max_pids = $5 + 0
    }
    END {
      printf "%s\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%d\t%d\t%s\n", name,
             start, peak, end, max_memory_percent, max_cpu_percent, max_pids,
             budget, (peak <= budget ? "yes" : "no")
    }' "${temporary_directory}/container.samples"
else
  printf '\ncontainer\tunavailable\n'
fi

if [[ -n "${soak_start_us}" && -s "${temporary_directory}/container.samples" ]]; then
  printf '\nmemory_soak\tsamples\tobserved_seconds\tmemory_start_mib\tmemory_end_mib\tmemory_growth_mib\tmemory_peak_mib\tslope_mib_per_hour\n'
  awk -F '|' -v name="${container}" -v start_us="${soak_start_us}" '
    function memory_mib(value, parts, number, unit) {
      split(value, parts, " ")
      match(parts[1], /^[0-9.]+/)
      number = substr(parts[1], RSTART, RLENGTH) + 0
      unit = substr(parts[1], RLENGTH + 1)
      if (unit == "B") return number / 1048576
      if (unit == "kB" || unit == "KB" || unit == "KiB") return number / 1024
      if (unit == "MB" || unit == "MiB") return number
      if (unit == "GB" || unit == "GiB") return number * 1024
      if (unit == "TB" || unit == "TiB") return number * 1048576
      return number / 1048576
    }
    $1 + 0 >= start_us + 0 {
      timestamp = ($1 - start_us) / 1000000
      memory = memory_mib($2)
      if (count++ == 0) {
        first_timestamp = timestamp
        first_memory = memory
        peak = memory
      }
      last_timestamp = timestamp
      last_memory = memory
      if (memory > peak) peak = memory
      sum_x += timestamp
      sum_y += memory
      sum_xx += timestamp * timestamp
      sum_xy += timestamp * memory
    }
    END {
      denominator = count * sum_xx - sum_x * sum_x
      slope = denominator == 0 ? 0 : (count * sum_xy - sum_x * sum_y) / denominator * 3600
      observed = count ? last_timestamp - first_timestamp : 0
      growth = count ? last_memory - first_memory : 0
      printf "%s\t%d\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\n",
             name, count, observed, first_memory, last_memory, growth, peak,
             slope
    }' "${temporary_directory}/container.samples"
fi

if [[ "${benchmark_failed}" == true ]]; then
  fail "one or more benchmark requests failed; see response-code diagnostics"
fi

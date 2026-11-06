#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
# shellcheck source=bench/kind/common.sh
source bench/kind/common.sh
phases="${BENCH_PHASES:-half:3m:0.5,full:3m:1,burst:4m:3,idle:4m:0}"
results="${BENCH_RESULTS:-bench/results/$(date +%Y-%m-%d)}"
profile="${BENCH_PROFILE:-fixed}"
if [[ "$profile" == changing ]]; then
  phases="${BENCH_PHASES:-load:6m:1.1,idle:1m:0}"
  results="${BENCH_RESULTS:-bench/results/$(date +%Y-%m-%d)-changing}"
  rpm_windows="${BENCH_RPM_WINDOWS:-600,600,180,180,180,180,600}"
  passes="${BENCH_PASSES:-3}"
  if [[ -e "$results" ]]; then
    echo "choose a new BENCH_RESULTS directory to preserve $results" >&2
    exit 1
  fi
  if [[ ! "$phases" =~ ^[[:alnum:]_.:,+-]+$ ]]; then
    echo "invalid changing-quota phase schedule" >&2
    exit 1
  fi
  if [[ ! "$rpm_windows" =~ ^[1-9][0-9]*(,[1-9][0-9]*)+$ ]]; then
    echo "BENCH_RPM_WINDOWS must list at least two positive RPM limits, comma separated" >&2
    exit 1
  fi
  if [[ ! "$passes" =~ ^[1-9][0-9]*$ ]]; then
    echo "BENCH_PASSES must be a positive integer" >&2
    exit 1
  fi
elif [[ "$profile" != fixed ]]; then
  echo "unknown BENCH_PROFILE: $profile" >&2
  exit 1
fi
concurrency=2
step=15
local_port=19095
commit=$(git rev-parse HEAD)
dirty=false
if [ -n "$(git status --porcelain)" ]; then dirty=true; fi
rpm=$(grep -o '"-rpm", "[0-9]*"' bench/kind/manifests/10-fakeprovider.yaml | grep -o '[0-9]*')

ensure_cluster
build_and_load worker producer
install_keda
deploy_base
if [[ "$profile" == changing ]]; then
  kubectl -n "$namespace" patch deploy fakeprovider --type=json \
    -p='[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"-openai-window-rpm='"$rpm_windows"'"}]' >/dev/null
fi

log "deploying Prometheus"
kubectl apply -f bench/prometheus/prometheus.yaml >/dev/null
kubectl -n "$namespace" rollout status deploy/prometheus --timeout 120s >/dev/null

duration_seconds() {
  local rest=$1 total=0
  while [[ $rest =~ ^([0-9]+)([hms])(.*)$ ]]; do
    case "${BASH_REMATCH[2]}" in
      h) total=$(( total + BASH_REMATCH[1] * 3600 )) ;;
      m) total=$(( total + BASH_REMATCH[1] * 60 )) ;;
      s) total=$(( total + BASH_REMATCH[1] )) ;;
    esac
    rest=${BASH_REMATCH[3]}
  done
  if [ -n "$rest" ] || [ -z "$1" ]; then
    echo "unsupported duration $1" >&2
    exit 1
  fi
  echo "$total"
}

phase_seconds() {
  local total=0 seconds
  IFS=',' read -ra items <<<"$1"
  for item in "${items[@]}"; do
    IFS=':' read -r _ duration _ <<<"$item"
    seconds=$(duration_seconds "$duration") || exit 1
    total=$(( total + seconds ))
  done
  echo "$total"
}
schedule_seconds=$(phase_seconds "$phases")

forward_pid=""
stop_forward() {
  if [ -n "$forward_pid" ]; then
    kill "$forward_pid" 2>/dev/null || true
    wait "$forward_pid" 2>/dev/null || true
    forward_pid=""
  fi
}
trap stop_forward EXIT

export_series() {
  local dir=$1 start=$2 end=$3 window=$4 label=$5 adaptive=$6
  kubectl -n "$namespace" port-forward svc/prometheus "$local_port:9090" >/dev/null 2>&1 &
  forward_pid=$!
  local ready=false
  for _ in $(seq 1 30); do
    if curl -sf "http://localhost:$local_port/-/ready" >/dev/null; then
      ready=true
      break
    fi
    sleep 1
  done
  if [ "$ready" != true ]; then
    echo "the Prometheus port-forward never became ready" >&2
    exit 1
  fi
  range() {
    curl -sf "http://localhost:$local_port/api/v1/query_range" \
      --data-urlencode "query=$2" --data-urlencode "start=$start" --data-urlencode "end=$end" --data-urlencode "step=$step" \
      > "$dir/$1.json"
  }
  instant() {
    curl -sf "http://localhost:$local_port/api/v1/query" --data-urlencode "query=$2" --data-urlencode "time=$end" > "$dir/$1.json"
  }
  range replicas 'count(up{job="workers"} == 1)'
  if [[ "$profile" == changing ]]; then
    range provider_rpm 'max(quotaware_limit_requests{model="gpt-4.1-mini"})'
  fi
  range inflight 'sum(worker_calls_inflight)'
  range rate_limited_per_minute '(sum(rate(worker_calls_total{status="429"}[1m])) * 60) or vector(0)'
  range retries_per_minute '(sum(rate(worker_retries_total[1m])) * 60) or vector(0)'
  range completed_per_minute 'sum(rate(worker_jobs_completed_total[1m])) * 60'
  range queue_length 'producer_queue_length'
  range multiplier 'producer_load_multiplier'
  range ceiling 'sum(quotaware_ceiling{key="combined"})'
  range latency_p95_over_time 'histogram_quantile(0.95, sum(rate(worker_job_latency_seconds_bucket[2m])) by (le))'
  instant jobs_total 'max_over_time(producer_jobs_total['"$window"'s])'
  instant completed_total 'sum(max_over_time(worker_jobs_completed_total['"$window"'s]))'
  instant failed_total 'sum(max_over_time(worker_jobs_failed_total['"$window"'s]))'
  instant abandoned_total 'sum(max_over_time(worker_jobs_abandoned_total['"$window"'s]))'
  instant requeued_total 'sum(max_over_time(worker_jobs_requeued_total['"$window"'s]))'
  instant rate_limited_total 'sum(max_over_time(worker_calls_total{status="429"}['"$window"'s]))'
  instant retries_total 'sum(max_over_time(worker_retries_total['"$window"'s]))'
  instant backoff_seconds_total 'sum(max_over_time(worker_backoff_seconds_total['"$window"'s]))'
  instant latency_p50 'histogram_quantile(0.5, sum(max_over_time(worker_job_latency_seconds_bucket['"$window"'s])) by (le))'
  instant latency_p95 'histogram_quantile(0.95, sum(max_over_time(worker_job_latency_seconds_bucket['"$window"'s])) by (le))'
  if [[ "$profile" == changing ]]; then
    printf '{"start":%s,"end":%s,"label":"%s","commit":"%s","dirty":%s,"profile":"changing","phases":"%s","rpm_windows":[%s],"adaptive":%s}\n' \
      "$start" "$end" "$label" "$commit" "$dirty" "$phases" "$rpm_windows" "$adaptive" > "$dir/run.json"
  else
    printf '{"start":%s,"end":%s,"label":"%s","commit":"%s","dirty":%s}\n' "$start" "$end" "$label" "$commit" "$dirty" > "$dir/run.json"
  fi
  stop_forward
}

run_variant() {
  local name=$1 manifest=$2 label=$3
  local dir="${4:-$results}/$name"
  mkdir -p "$dir"
  log "run $name: $manifest, schedule ${schedule_seconds}s"
  kubectl -n "$namespace" delete job producer --ignore-not-found --wait=true >/dev/null
  kubectl -n "$namespace" delete scaledobject llm-workers --ignore-not-found --wait=true >/dev/null
  kubectl -n "$namespace" delete deploy llm-workers --ignore-not-found --wait=true >/dev/null
  kubectl -n "$namespace" exec deploy/redis -- redis-cli del jobs >/dev/null
  if [[ "$profile" == changing ]]; then
    kubectl -n "$namespace" rollout restart deploy/fakeprovider >/dev/null
    kubectl -n "$namespace" rollout status deploy/fakeprovider --timeout 120s >/dev/null
  fi
  kubectl -n "$namespace" rollout restart deploy/quotaware >/dev/null
  kubectl -n "$namespace" rollout status deploy/quotaware --timeout 120s >/dev/null
  kubectl apply -f bench/manifests/workers.yaml >/dev/null
  kubectl -n "$namespace" rollout status deploy/llm-workers --timeout 120s >/dev/null
  sed "s/WORKERS_CREDENTIAL/$(credential_hash sk-bench)/" "$manifest" | kubectl apply -f - >/dev/null
  sleep 20
  local start end
  start=$(date -u +%s)
  sed "s/BENCH_PHASES/$phases/" bench/manifests/producer.yaml | kubectl apply -f - >/dev/null
  kubectl -n "$namespace" wait --for=condition=complete job/producer --timeout "$(( schedule_seconds + 1200 ))s" >/dev/null
  end=$(date -u +%s)
  log "run $name finished after $(( end - start ))s, exporting series"
  local adaptive=false
  if [[ "$manifest" == *scaledobject-b.yaml || "$manifest" == *scaledobject-c.yaml ]]; then adaptive=true; fi
  export_series "$dir" "$start" "$end" "$(( end - start ))" "$label" "$adaptive"
}

if [[ "$profile" == changing ]]; then
  orders=("A B C" "C B A" "B C A")
  for pass in $(seq 1 "$passes"); do
    read -ra order <<<"${orders[$(( (pass - 1) % ${#orders[@]} ))]}"
    for arm in "${order[@]}"; do
      case "$arm" in
        A) manifest=scaledobject-a; label="queue only" ;;
        B) manifest=scaledobject-static; label="static maximum of 10" ;;
        C) manifest=scaledobject-c; label="quotaware, safety 1" ;;
      esac
      run_variant "$arm" "bench/manifests/$manifest.yaml" "$label" "$results/pass-$pass"
    done
    go run ./bench/cmd/plot -results "$results/pass-$pass" -concurrency "$concurrency"
  done
  exit 0
fi

run_variant A bench/manifests/scaledobject-a.yaml "queue trigger"
run_variant B bench/manifests/scaledobject-b.yaml "queue trigger + quotaware, safety 0.85"
run_variant C bench/manifests/scaledobject-c.yaml "queue trigger + quotaware, safety 1"

log "plotting"
go run ./bench/cmd/plot -results "$results" -concurrency "$concurrency" >/dev/null
{
  echo "# quotaware benchmark $(basename "$results")"
  echo
  echo "Schedule \`$phases\` against a fake provider limited to $rpm requests per minute, two calls in flight per worker pod, the same job schedule and prompt sizes for every run (producer seed 1). Built from commit \`$commit\`$([ "$dirty" = true ] && echo ' with uncommitted changes')."
  echo
  echo "- A: the KEDA Redis list trigger alone, ten jobs per replica, 1 to 30 pods."
  echo "- B: the same trigger capped by the quotaware ceiling at the default safety factor 0.85."
  echo "- C: the same cap with \`safety: \"1\"\`, so the ceiling is the full quota."
  echo
  echo "![replicas](replicas.svg)"
  echo
  echo "![retries per minute, each after a 429 in these runs](retries.svg)"
  echo
  echo "![queue](queue.svg)"
  echo
  cat "$results/summary.md"
} > "$results/README.md"
cat "$results/summary.md"
log "results in $results; run 'make demo-down' to delete the cluster"

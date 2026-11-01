#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
# shellcheck source=bench/kind/common.sh
source bench/kind/common.sh
phases="${BENCH_PHASES:-half:3m:0.5,full:3m:1,burst:4m:3,idle:4m:0}"
results="${BENCH_RESULTS:-bench/results/$(date +%Y-%m-%d)}"
concurrency=2
step=15
local_port=19095

ensure_cluster
build_and_load worker producer
install_keda
deploy_base

log "deploying Prometheus"
kubectl apply -f bench/prometheus/prometheus.yaml >/dev/null
kubectl -n "$namespace" rollout status deploy/prometheus --timeout 120s >/dev/null

phase_seconds() {
  local total=0
  IFS=',' read -ra items <<<"$1"
  for item in "${items[@]}"; do
    IFS=':' read -r _ duration _ <<<"$item"
    case "$duration" in
      *h) total=$(( total + ${duration%h} * 3600 )) ;;
      *m) total=$(( total + ${duration%m} * 60 )) ;;
      *s) total=$(( total + ${duration%s} )) ;;
      *) echo "unsupported duration $duration" >&2; exit 1 ;;
    esac
  done
  echo "$total"
}
schedule_seconds=$(phase_seconds "$phases")

export_series() {
  local dir=$1 start=$2 end=$3 window=$4
  local forward_pid
  kubectl -n "$namespace" port-forward svc/prometheus "$local_port:9090" >/dev/null 2>&1 &
  forward_pid=$!
  trap 'kill $forward_pid 2>/dev/null || true' RETURN
  for i in $(seq 1 30); do
    curl -sf "http://localhost:$local_port/-/ready" >/dev/null && break
    sleep 1
  done
  range() {
    curl -sf "http://localhost:$local_port/api/v1/query_range" \
      --data-urlencode "query=$2" --data-urlencode "start=$start" --data-urlencode "end=$end" --data-urlencode "step=$step" \
      > "$dir/$1.json"
  }
  instant() {
    curl -sf "http://localhost:$local_port/api/v1/query" --data-urlencode "query=$2" --data-urlencode "time=$end" > "$dir/$1.json"
  }
  range replicas 'count(up{job="workers"} == 1)'
  range inflight 'sum(worker_calls_inflight)'
  range rate_limited_per_minute 'sum(rate(worker_calls_total{status="429"}[1m])) * 60'
  range retries_per_minute 'sum(rate(worker_retries_total[1m])) * 60'
  range completed_per_minute 'sum(rate(worker_jobs_completed_total[1m])) * 60'
  range queue_length 'producer_queue_length'
  range multiplier 'producer_load_multiplier'
  range ceiling 'sum(quotaware_ceiling)'
  range latency_p95_over_time 'histogram_quantile(0.95, sum(rate(worker_job_latency_seconds_bucket[2m])) by (le))'
  instant jobs_total 'max_over_time(producer_jobs_total['"$window"'s])'
  instant completed_total 'sum(increase(worker_jobs_completed_total['"$window"'s]))'
  instant failed_total 'sum(increase(worker_jobs_failed_total['"$window"'s]))'
  instant rate_limited_total 'sum(increase(worker_calls_total{status="429"}['"$window"'s]))'
  instant retries_total 'sum(increase(worker_retries_total['"$window"'s]))'
  instant backoff_seconds_total 'sum(increase(worker_backoff_seconds_total['"$window"'s]))'
  instant latency_p50 'histogram_quantile(0.5, sum(increase(worker_job_latency_seconds_bucket['"$window"'s])) by (le))'
  instant latency_p95 'histogram_quantile(0.95, sum(increase(worker_job_latency_seconds_bucket['"$window"'s])) by (le))'
  printf '{"start":%s,"end":%s}\n' "$start" "$end" > "$dir/run.json"
}

run_variant() {
  local name=$1 manifest=$2
  local dir="$results/$name"
  mkdir -p "$dir"
  log "run $name: $manifest, schedule ${schedule_seconds}s"
  kubectl -n "$namespace" delete job producer --ignore-not-found --wait=true >/dev/null
  kubectl -n "$namespace" delete scaledobject llm-workers --ignore-not-found --wait=true >/dev/null
  kubectl -n "$namespace" delete deploy llm-workers --ignore-not-found --wait=true >/dev/null
  kubectl -n "$namespace" exec deploy/redis -- redis-cli del jobs >/dev/null
  kubectl -n "$namespace" rollout restart deploy/quotaware >/dev/null
  kubectl -n "$namespace" rollout status deploy/quotaware --timeout 120s >/dev/null
  kubectl apply -f bench/manifests/workers.yaml >/dev/null
  kubectl -n "$namespace" rollout status deploy/llm-workers --timeout 120s >/dev/null
  sed "s/WORKERS_CREDENTIAL/$(credential_hash sk-bench)/" "$manifest" | kubectl apply -f - >/dev/null
  sleep 20
  local start end
  start=$(date -u +%s)
  sed "s/BENCH_PHASES/$phases/" bench/manifests/producer.yaml | kubectl apply -f - >/dev/null
  kubectl -n "$namespace" wait --for=condition=complete job/producer --timeout "$(( schedule_seconds + 300 ))s" >/dev/null
  for i in $(seq 1 60); do
    [ "$(kubectl -n "$namespace" exec deploy/redis -- redis-cli llen jobs)" = "0" ] && break
    sleep 10
  done
  sleep 30
  end=$(date -u +%s)
  log "run $name finished after $(( end - start ))s, exporting series"
  export_series "$dir" "$start" "$end" "$(( end - start ))"
}

run_variant A bench/manifests/scaledobject-a.yaml
run_variant B bench/manifests/scaledobject-b.yaml

log "plotting"
go run ./bench/cmd/plot -results "$results" -concurrency "$concurrency" >/dev/null
{
  echo "# quotaware benchmark $(basename "$results")"
  echo
  echo "Schedule \`$phases\` against a fake provider limited to 600 requests per minute, two calls in flight per worker pod, the same seed for both runs."
  echo
  echo "![replicas](replicas.svg)"
  echo
  echo "![429 per minute](rate_limits.svg)"
  echo
  echo "![queue](queue.svg)"
  echo
  cat "$results/summary.md"
} > "$results/README.md"
cat "$results/summary.md"
log "results in $results; run 'make demo-down' to delete the cluster"

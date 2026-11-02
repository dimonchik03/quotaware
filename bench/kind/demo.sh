#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."
cluster=quotaware
namespace=quotaware-demo
watch_seconds="${WATCH_SECONDS:-300}"
burst_after="${BURST_AFTER_SECONDS:-90}"
export KUBECONFIG="$PWD/bench/kind/.kubeconfig"

log() { printf '\n==> %s\n' "$*"; }

if ! kind get clusters 2>/dev/null | grep -qx "$cluster"; then
  log "creating kind cluster $cluster (kubeconfig in $KUBECONFIG, your default context is untouched)"
  kind create cluster --config bench/kind/kind.yaml --wait 120s
else
  kind export kubeconfig --name "$cluster" >/dev/null
fi

log "building images"
docker build -q -t quotaware:dev .
docker build -q -t quotaware-fakeprovider:dev --build-arg CMD=./bench/cmd/fakeprovider .
kind load docker-image --name "$cluster" quotaware:dev quotaware-fakeprovider:dev

log "installing KEDA"
helm repo add kedacore https://kedacore.github.io/charts --force-update >/dev/null
helm repo update kedacore >/dev/null
helm upgrade --install keda kedacore/keda --version 2.21.0 --namespace keda --create-namespace --wait --timeout 5m >/dev/null

log "resetting the demo namespace"
kubectl delete namespace "$namespace" --ignore-not-found --wait=true >/dev/null
kubectl apply -f bench/kind/manifests/00-namespace.yaml >/dev/null

log "deploying the fake provider, redis and the workers"
kubectl apply -f bench/kind/manifests/10-fakeprovider.yaml -f bench/kind/manifests/20-redis.yaml -f bench/kind/manifests/30-workers.yaml >/dev/null
kubectl -n "$namespace" rollout status deploy/fakeprovider deploy/redis deploy/llm-workers --timeout 120s >/dev/null

log "installing quotaware"
helm upgrade --install quotaware deploy/helm/quotaware --namespace "$namespace" \
  --set image.repository=quotaware --set image.tag=dev --set image.pullPolicy=Never \
  --set upstreams.openai=http://fakeprovider:8081 --set upstreams.anthropic=http://fakeprovider:8081 \
  --wait --timeout 2m >/dev/null
kubectl -n "$namespace" rollout restart deploy/llm-workers >/dev/null
kubectl -n "$namespace" rollout status deploy/llm-workers --timeout 120s >/dev/null
kubectl -n "$namespace" run grpc-probe --rm -i --restart=Never --image=busybox:1.36 --quiet -- \
  sh -c 'for i in $(seq 1 30); do nc -z quotaware 9090 && exit 0; sleep 1; done; echo "quotaware:9090 unreachable" >&2; exit 1' >/dev/null

log "seeding the queue and creating the ScaledObject"
kubectl apply -f bench/kind/manifests/50-load.yaml >/dev/null
kubectl -n "$namespace" wait --for=condition=complete job/seed-queue --timeout 60s >/dev/null
credential=$(printf 'Bearer sk-workers' | shasum -a 256 | cut -c1-12)
sed "s/WORKERS_CREDENTIAL/$credential/" bench/kind/manifests/40-scaledobject.yaml | kubectl apply -f - >/dev/null

log "watching for ${watch_seconds}s: demand is 20 (200 jobs / 10), replicas follow the quota ceiling; a burst from another key starts at ${burst_after}s"
printf '%-9s %-9s %-8s %-6s %s\n' time replicas desired 429/15s "ceiling (quotaware log)"
started=$(date +%s)
burst_started=false
while [ $(( $(date +%s) - started )) -lt "$watch_seconds" ]; do
  elapsed=$(( $(date +%s) - started ))
  if [ "$burst_started" = false ] && [ "$elapsed" -ge "$burst_after" ]; then
    kubectl apply -f bench/kind/manifests/60-burst.yaml >/dev/null
    burst_started=true
    printf '%-9s %s\n' "$(date +%H:%M:%S)" "burst job started: 30 loops with another credential for 60s"
  fi
  read -r current desired <<<"$(kubectl -n "$namespace" get hpa keda-hpa-llm-workers -o jsonpath='{.status.currentReplicas} {.status.desiredReplicas}' 2>/dev/null || echo "- -")"
  limited=$(kubectl -n "$namespace" logs deploy/llm-workers --all-pods=true --since=15s 2>/dev/null | grep -cE '(^| )429$' || true)
  ceiling=$(kubectl -n "$namespace" logs deploy/quotaware --since=15s 2>/dev/null | grep 'ceiling changed' | tail -1 | sed -E 's/.*from=([0-9]+) to=([0-9]+) reason=([a-z_]+).*/\1 -> \2 (\3)/' || true)
  printf '%-9s %-9s %-8s %-6s %s\n' "$(date +%H:%M:%S)" "${current:--}" "${desired:--}" "${limited:-0}" "${ceiling}"
  sleep 15
done

log "KEDA operator log lines for the ScaledObject"
kubectl -n keda logs deploy/keda-operator --since="${watch_seconds}s" | grep -i 'llm-workers' | grep -vi 'reconciling' | tail -12 || true

log "quotaware ceiling changes"
kubectl -n "$namespace" logs deploy/quotaware --since="${watch_seconds}s" | grep 'ceiling changed' | sed -E 's/^time=([^ ]+) level=INFO msg="ceiling changed" //' | tail -30 || true

log "done; run 'make demo-down' to delete the cluster"

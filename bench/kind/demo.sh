#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."
cluster=quotaware
namespace=quotaware-demo
watch_seconds="${WATCH_SECONDS:-240}"
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
helm repo add kedacore https://kedacore.github.io/charts >/dev/null 2>&1 || true
helm repo update kedacore >/dev/null
helm upgrade --install keda kedacore/keda --version 2.21.0 --namespace keda --create-namespace --wait --timeout 5m >/dev/null

log "deploying the fake provider, redis and the worker deployment"
kubectl apply -f bench/kind/manifests/00-namespace.yaml
kubectl apply -f bench/kind/manifests/10-fakeprovider.yaml -f bench/kind/manifests/20-redis.yaml -f bench/kind/manifests/30-workers.yaml
kubectl -n "$namespace" rollout status deploy/fakeprovider deploy/redis deploy/llm-workers --timeout 120s >/dev/null

log "installing quotaware"
helm upgrade --install quotaware deploy/helm/quotaware --namespace "$namespace" \
  --set image.repository=quotaware --set image.tag=dev --set image.pullPolicy=Never \
  --set upstreams.openai=http://fakeprovider:8081 --set upstreams.anthropic=http://fakeprovider:8081 \
  --wait --timeout 2m >/dev/null

log "creating the ScaledObject, seeding the queue and starting the load"
kubectl delete job -n "$namespace" seed-queue load --ignore-not-found >/dev/null
kubectl apply -f bench/kind/manifests/40-scaledobject.yaml -f bench/kind/manifests/50-load.yaml
kubectl -n "$namespace" wait --for=condition=complete job/seed-queue --timeout 60s >/dev/null

log "watching the HPA for ${watch_seconds}s (replicas follow the quota ceiling, demand stays at 20)"
printf '%-9s %-10s %-9s %s\n' time replicas desired "ceiling (quotaware log)"
end=$(( $(date +%s) + watch_seconds ))
while [ "$(date +%s)" -lt "$end" ]; do
  hpa=$(kubectl -n "$namespace" get hpa keda-hpa-llm-workers -o jsonpath='{.status.currentReplicas} {.status.desiredReplicas}' 2>/dev/null || echo "- -")
  ceiling=$(kubectl -n "$namespace" logs deploy/quotaware --since=15s 2>/dev/null | grep 'ceiling changed' | tail -1 | sed -E 's/.*from=([0-9]+) to=([0-9]+) reason=([a-z_]+).*/\1 -> \2 (\3)/' || true)
  # shellcheck disable=SC2086
  printf '%-9s %-10s %-9s %s\n' "$(date +%H:%M:%S)" ${hpa} "${ceiling}"
  sleep 15
done

log "KEDA operator log lines for the ScaledObject"
kubectl -n keda logs deploy/keda-operator --since="${watch_seconds}s" | grep -i 'llm-workers' | tail -15 || true

log "quotaware ceiling changes"
kubectl -n "$namespace" logs deploy/quotaware --since="${watch_seconds}s" | grep 'ceiling changed' | tail -25 || true

log "done; run 'make demo-down' to delete the cluster"

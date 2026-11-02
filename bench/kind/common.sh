#!/usr/bin/env bash

cluster=quotaware
namespace=quotaware-demo

log() { printf '\n==> %s\n' "$*"; }

ensure_cluster() {
  export KUBECONFIG="$PWD/bench/kind/.kubeconfig"
  if ! kind get clusters 2>/dev/null | grep -qx "$cluster"; then
    log "creating kind cluster $cluster (kubeconfig in $KUBECONFIG, your default context is untouched)"
    kind create cluster --config bench/kind/kind.yaml --wait 120s
  else
    kind export kubeconfig --name "$cluster" >/dev/null
  fi
}

build_and_load() {
  log "building images"
  docker build -q -t quotaware:dev .
  docker build -q -t quotaware-fakeprovider:dev --build-arg CMD=./bench/cmd/fakeprovider .
  local images="quotaware:dev quotaware-fakeprovider:dev"
  for extra in "$@"; do
    docker build -q -t "quotaware-$extra:dev" --build-arg "CMD=./bench/cmd/$extra" .
    images="$images quotaware-$extra:dev"
  done
  # shellcheck disable=SC2086
  kind load docker-image --name "$cluster" $images
}

install_keda() {
  log "installing KEDA"
  helm repo add kedacore https://kedacore.github.io/charts --force-update >/dev/null
  helm repo update kedacore >/dev/null
  helm upgrade --install keda kedacore/keda --version 2.21.0 --namespace keda --create-namespace --wait --timeout 5m >/dev/null
}

deploy_base() {
  log "resetting the $namespace namespace and deploying the fake provider, redis and quotaware"
  kubectl delete namespace "$namespace" --ignore-not-found --wait=true >/dev/null
  kubectl apply -f bench/kind/manifests/00-namespace.yaml >/dev/null
  kubectl apply -f bench/kind/manifests/10-fakeprovider.yaml -f bench/kind/manifests/20-redis.yaml >/dev/null
  kubectl -n "$namespace" rollout status deploy/fakeprovider deploy/redis --timeout 120s >/dev/null
  helm upgrade --install quotaware deploy/helm/quotaware --namespace "$namespace" \
    --set image.repository=quotaware --set image.tag=dev --set image.pullPolicy=Never \
    --set upstreams.openai=http://fakeprovider:8081 --set upstreams.anthropic=http://fakeprovider:8081 \
    --set observe.insecure=true \
    --wait --timeout 2m >/dev/null
  kubectl -n "$namespace" run grpc-probe --rm -i --restart=Never --image=busybox:1.36 --quiet -- \
    sh -c 'for i in $(seq 1 30); do nc -z quotaware 9090 && exit 0; sleep 1; done; echo "quotaware:9090 unreachable" >&2; exit 1' >/dev/null
}

credential_hash() {
  printf 'Bearer %s' "$1" | shasum -a 256 | cut -c1-12
}

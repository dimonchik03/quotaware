#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."
export KUBECONFIG="$PWD/bench/kind/.kubeconfig"
kind delete cluster --name quotaware
rm -f "$KUBECONFIG"

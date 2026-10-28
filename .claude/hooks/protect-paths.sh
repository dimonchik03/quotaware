#!/usr/bin/env bash
set -euo pipefail

input="$(cat)"
file_path="$(printf '%s' "$input" | jq -r '.tool_input.file_path // empty' 2>/dev/null || true)"

[ -z "$file_path" ] && exit 0

case "$file_path" in
  *.env.example)
    exit 0
    ;;
  *.env|*.env.*|*.pem|*.key|*/secrets/*|*package-lock.json|*pnpm-lock.yaml|*yarn.lock|*go.sum|*poetry.lock|*uv.lock|*Cargo.lock|*/node_modules/*|*/dist/*|*/coverage/*)
    printf 'Blocked: %s is a protected path. Secrets, lockfiles and generated output are managed by tooling, not edited directly.\n' "$file_path" >&2
    exit 2
    ;;
esac

exit 0

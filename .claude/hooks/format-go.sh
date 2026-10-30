#!/usr/bin/env bash
set -euo pipefail

input="$(cat)"
file_path="$(printf '%s' "$input" | jq -r '.tool_input.file_path // empty' 2>/dev/null || true)"

case "$file_path" in
  *.go)
    gofmt -l -w "$file_path" >/dev/null
    ;;
esac

exit 0

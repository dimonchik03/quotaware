# Release checklist

For v0.1.0 and every release after it.

1. `main` is green: `go test -race -p 2 ./...`, `golangci-lint run`, `make e2e`, and
   the Python collector tests (`python -m pytest -q collector/python`).
2. `make demo` ran on the release commit and the HPA followed the ceiling through the
   burst; `make bench` results for the commit are under `bench/results/<date>/` and
   `docs/benchmark.md` points at them.
3. `README.md` describes every subcommand, flag and trigger metadata key that exists,
   and the Helm chart `appVersion` equals the version being tagged.
4. Every accepted ADR matches the code; a change of mind got a new record.
5. `CHANGELOG` is the goreleaser changelog: commit messages since the last tag follow
   Conventional Commits, so check `git log --oneline <last-tag>..` reads well.
6. Tag and push: `git tag -a v0.1.0 -m "v0.1.0" && git push origin v0.1.0`.
7. The `release` workflow runs the tests again, publishes archives and checksums on
   the GitHub release and pushes `ghcr.io/dimonchik03/quotaware:<version>`; pull the
   image and run `quotaware version` from it.
8. Flip the README's "no image is published yet" paragraph to the published image
   and drop the `--set image.*` flags from the Helm example, together with the chart
   `appVersion` bump for the next release.
9. Install the chart from the tag against a kind cluster with the published image
   and run the demo ScaledObject once more.
10. Announce with the benchmark charts and the numbers table, nothing else.

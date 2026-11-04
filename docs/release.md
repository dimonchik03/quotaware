# Release checklist

For every release, `vX.Y.Z` below.

1. `main` is green: `go test -race -p 2 ./...`, `golangci-lint run`, `make e2e`, and
   the Python collector tests (`python -m pytest -q collector/python`).
2. `make demo` ran on the release commit and the HPA followed the ceiling through the
   burst; `docs/benchmark.md` points at the `bench/results/<date>/` it quotes and
   names the commit they were built from.
3. `README.md` describes every subcommand, flag and trigger metadata key that exists.
4. The Helm chart `version` and `appVersion` and the Python package `version` in
   `collector/python/pyproject.toml` equal `X.Y.Z`. The chart passes flags the binary
   must know, so a chart whose `appVersion` points at an older image crash-loops; the
   `release` workflow refuses a tag any of the three versions disagrees with.
5. Every accepted ADR matches the code; a change of mind got a new record.
6. `CHANGELOG` is the goreleaser changelog: commit messages since the last tag follow
   Conventional Commits, so check `git log --oneline <last-tag>..` reads well.
7. Tag and push: `git tag -a vX.Y.Z -m "vX.Y.Z" && git push origin vX.Y.Z`.
8. The `release` workflow runs the tests again, publishes archives and checksums on
   the GitHub release and pushes `ghcr.io/dimonchik03/quotaware:X.Y.Z`; pull the
   image and check that `quotaware version` prints `X.Y.Z`.
9. Install the chart from the tag against a kind cluster with the published image
   and run the demo ScaledObject once more.
10. Update the README "Status" section and the `git clone --branch` line of the
    Helm install to the new release, with an upgrade note when defaults changed.

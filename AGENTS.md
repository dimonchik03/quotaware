# Working agreement

This file is the single source of truth for every agent and every human working in this repository. Tool-specific entry points such as `CLAUDE.md` import it and add only what is specific to that tool.

## Principles

- Correctness over speed. A change is done when it is tested, reviewed and documented, not when it compiles.
- Small, reversible steps. Several focused commits beat one large one.
- Explicit over implicit. Names, types and errors carry the meaning; comments do not.
- Code documents what the system does. Architecture decision records document why.

## Workflow

1. Understand: read the relevant code and the records in `docs/adr` before proposing a change.
2. Plan: anything that touches architecture gets an ADR first (`/adr`).
3. Test first: write the failing test, make it pass, refactor (`/tdd`). User-facing behaviour gets an end-to-end scenario (`/e2e`).
4. Verify: run the narrowest check first, the full suite before opening a pull request.
5. Review: run `/review` on your own diff before asking anyone else to read it.
6. Ship: `/ship` opens the pull request with the template filled in.

## Definition of done

- Tests cover the new behaviour and the whole suite passes.
- No new lint or type errors.
- Public behaviour changes are reflected in `README.md`.
- Architectural changes have an accepted ADR.
- Commit messages follow Conventional Commits.

## Conventions

Detailed rules live in `.claude/rules`. The short version:

- English everywhere: code, identifiers, commits, documentation.
- No code comments. If something needs explaining, rename it, extract it or write an ADR.
- Fail loudly: no swallowed errors, no silent fallbacks.
- Dependencies point inward: the domain never imports infrastructure.

## Commands

- Build: `go build ./...`
- Unit tests, narrow first: `go test ./internal/headroom/`, then `go test -p 2 ./...`
- Race check before a pull request: `go test -race -p 2 ./...`
- Lint: `golangci-lint run`
- Format: `gofmt -l -w .` (the Claude Code hook applies it on every edited Go file)
- End to end through Docker Compose: `make e2e` (builds both images, runs the scenario in `e2e/`, tears down)
- Demo and benchmark: `make demo`, `make bench` (available from milestones 3 and 4)

Never run two heavy commands (tests, lint, kind) at the same time on this machine.

## Layout

    cmd/quotaware/      entry point and subcommands: proxy, scaler, all, simulate
    internal/headroom/  domain: the ceiling algorithm, pure functions
    internal/aimd/      domain: congestion-control fallback, pure functions
    internal/state/     application: per-bucket state, EWMA, staleness
    internal/headers/   adapter: provider header parsers with fixtures
    internal/proxy/     adapter: transparent reverse proxy and usage extraction
    internal/scaler/    adapter: KEDA ExternalScaler over gRPC
    internal/metrics/   adapter: Prometheus registry
    deploy/             Helm chart, ScaledObject examples, Grafana dashboard
    bench/              fakeprovider, producer and worker libraries, binaries under bench/cmd, kind and Prometheus configs, plots, results
    e2e/                Docker Compose scenario against the built images, behind the e2e build tag
    docs/               SPEC.md, design.md, adr/, benchmark.md

Dependencies point inward: domain imports nothing from the repository, application
imports the domain, adapters import application and domain and never each other.
Full description and milestones: `docs/SPEC.md`.

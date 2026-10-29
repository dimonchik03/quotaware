# 0001. Record architecture decisions

Date: 2026-10-29

## Status

Accepted

## Context

Decisions about storage, protocols, public API shape and concurrency are expensive to reverse and easy to forget. Code shows what was built; it does not show why, or what was rejected. Both humans and coding agents working in this repository need that reasoning to avoid relitigating settled questions or breaking invariants nobody wrote down.

## Decision

We record every hard-to-reverse decision as a short Architecture Decision Record in `docs/adr`, numbered sequentially, written from `template.md`, and landed in the same pull request as the first code that depends on it. Records are immutable once accepted; a change of mind produces a new record that supersedes the old one.

## Alternatives considered

- Design documents in a wiki: they drift from the code and are invisible to agents reading the repository.
- Explanatory comments in code: they violate the no-comments rule and scatter reasoning across files.
- Nothing: the default, and the reason this record exists.

## Consequences

Every architectural pull request carries one more file. In exchange, reviewers and agents can read the decision history in order, and `/adr` turns writing one into a two-minute task.

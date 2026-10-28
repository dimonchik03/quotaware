# Architecture

- Layers point inward: adapters depend on application, application depends on domain. The domain has no dependency on frameworks, databases or transports.
- Every external system sits behind an interface owned by the layer that uses it.
- Decisions that are hard to reverse (storage, protocols, public API shape, concurrency model) are recorded in `docs/adr` before the code lands.
- Prefer composition over inheritance and explicit wiring over dependency-injection magic.
- Configuration is read once at the edge and passed down as values. No global mutable state.
- The public surface is versioned and documented. Breaking changes require an ADR and a major version bump.

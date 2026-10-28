# Testing

- Every behaviour change ships with a test in the same commit.
- Follow the pyramid: many fast unit tests, fewer integration tests, a small set of end-to-end scenarios covering the critical user journeys.
- Name tests as sentences describing behaviour: `rejects a request when the quota is exhausted`, never `test1`.
- Arrange, act, assert. One behaviour per test. No conditional logic inside a test.
- Test through public interfaces. Never mock the module under test or its internals; mock only external boundaries such as network, clock and filesystem.
- End-to-end tests run against the real wiring of the application with external systems replaced by test doubles at the process boundary.
- A flaky test is a bug. Fix it or delete it; never retry it into passing.
- Run the narrowest check first (one file, one package), the full suite before a pull request.

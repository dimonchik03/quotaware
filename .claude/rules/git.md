# Git

- Conventional Commits: `type(scope): summary`. Types: feat, fix, refactor, test, docs, chore, build, ci, perf.
- Summary in imperative mood, lower case, no trailing period, at most 72 characters.
- One logical change per commit. Tests travel with the code they cover.
- Branches: `feat/<short-slug>`, `fix/<short-slug>`, `chore/<short-slug>`, `docs/<short-slug>`. Bots such as Dependabot keep their own prefixes.
- Never rewrite history on `main`. Never force-push a shared branch.
- Never commit secrets, local settings or generated artefacts.

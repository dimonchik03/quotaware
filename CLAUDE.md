@AGENTS.md

## Claude Code

- Skills: `/tdd`, `/e2e`, `/adr`, `/review`, `/ship`. Definitions live in `.claude/skills`.
- Subagents: `architect` plans, `test-engineer` writes tests, `reviewer` reads diffs and never edits. Definitions live in `.claude/agents`.
- Rules in `.claude/rules` load automatically. Path-scoped rules apply only to matching files.
- The guard hook in `.claude/hooks` blocks direct edits to secrets, lockfiles and generated output.
- The format hook in `.claude/hooks` runs `gofmt` on every Go file after an edit.
- Local overrides go in `CLAUDE.local.md` and `.claude/settings.local.json`; both are gitignored.

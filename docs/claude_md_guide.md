# CLAUDE.md — A Practical Guide

> Goal: make **Claude Code** fast, accurate, and aligned with your repo & team by giving it crisp, living instructions it auto-loads.

---

## TL;DR

* **Keep it short, specific, and human-readable.** Treat it like a high-leverage prompt that loads on every run. ([anthropic.com][1])
* **Put files where Claude will find them:** project root, parents, relevant children, and (optionally) your home dir. ([anthropic.com][1])
* **Generate a starter file with `/init` and iterate.** Use `#` to append memories; tune wording for adherence. ([anthropic.com][1], [docs.anthropic.com][2])
* **Scale sanely:** use **imports** (document references) to split long content into focused docs. ([docs.anthropic.com][2], [Reddit][3])
* **Teams report better results when workflows/tools are documented in CLAUDE.md.**&#x20;

---

## 1) Placement & Scope

Claude automatically loads **CLAUDE.md** from multiple locations:

* **Project root** (checked into git)
* **Any parent dir** of your current working directory (great for monorepos)
* **Relevant child dirs** (auto-loaded on demand when working there)
* **User/global:** `~/.claude/CLAUDE.md` to apply defaults across all projects
  Tip: `/init` can scaffold a starter. ([anthropic.com][1])

For organization policy / enterprise rollouts, there are **system-level** memory files; **project**, **user**, and legacy **local** memories are supported in a hierarchy (local is now deprecated—prefer imports). ([docs.anthropic.com][2])

---

## 2) What to Put In (and What to Leave Out)

**High-impact sections (keep concise):**

* **Common commands** (build, test, lint, one-offs)
* **Code style & conventions** (imports, naming, formatting, error handling)
* **Testing guidance** (how to run single tests fast; when to run full suite)
* **Repo etiquette & workflows** (branch naming, PR rules, release notes)
* **Env specifics & quirks** (supported compilers, pyenv/node versions, gotchas)
  These are exactly the categories Anthropic highlights for CLAUDE.md. ([anthropic.com][1])

**Leave out** secrets and bulky historical docs. Put detailed architecture, schemas, runbooks, etc. **in separate files** and **import** them (see next section). ([docs.anthropic.com][2])

---

## 3) Keep It Maintainable: “Document References” (Imports)

Instead of a 500-line monster, **split** content and **import** what matters:

* In `CLAUDE.md`, add lines like:
  `See @README for overview and @docs/git-instructions.md for workflow.`
* Imports support relative/absolute paths and can be **recursive** (max depth).
* Prefer imports over the deprecated `CLAUDE.local.md` for personal tweaks. ([docs.anthropic.com][2])

This **document-references** pattern is widely recommended by the community to keep CLAUDE.md lean while pointing Claude to deeper docs when needed. ([Reddit][3])

---

## 4) Editing & Evolving Your CLAUDE.md

* **Iterate like a prompt.** Small wording changes (e.g., “**YOU MUST**”, “**IMPORTANT**”) can materially improve adherence. ([anthropic.com][1])
* **Append memories quickly**: start a line with `#` during a session; pick the target memory file. Use `/memory` to open & edit. ([docs.anthropic.com][2])
* **Back it by practice.** Anthropic teams report better outcomes (onboarding speed, routine tasks) when workflows/tools are documented in CLAUDE.md.&#x20;

---

## 5) Minimal, Battle-Tested Template

```markdown
# CLAUDE.md — Project Guide

## 1) Overview
- Purpose: one sentence on what this repo does.
- Tech stack: e.g., Python 3.11 + FastAPI + Postgres.

## 2) Fast Commands
- Build: `make build`
- Run: `make dev`
- Tests (single file): `pytest -q tests/test_api.py::TestAuth::test_login`
- Lint/format: `ruff check . && ruff format .`

## 3) Code Style
- Imports: prefer absolute; no wildcard `*`.
- Errors: use `Result[...]` helpers; never swallow exceptions.
- Types: mypy must pass; no `type: ignore` without reason.

## 4) Git Workflow
- Branches: `feat/<scope>-<slug>`, `fix/<scope>-<slug>`
- Commits: Conventional Commits (e.g., `feat(api): add token refresh`)
- PRs: link issue, checklist, tests required.

## 5) Tests
- Unit tests colocated in `tests/`, fixtures in `tests/conftest.py`
- Run the smallest scope that proves changes.

## 6) Quirks & Warnings
- IMPORTANT: do NOT regenerate `schema.sql` manually; run `make db/migrate`.

## 7) Key Docs (imports)
See @README for project overview, @docs/ARCHITECTURE.md for system diagram,
and @docs/DEPLOYMENT.md for release steps.
```

---

## 6) Monorepo Add-On

At monorepo root, keep **global** standards; in each package (`/api`, `/web`), add a **package-specific** `CLAUDE.md` with commands and rules. Claude loads parents and relevant children automatically as you work in those areas. ([anthropic.com][1])

---

## 7) Team vs Personal Guidance

* **Team-shared**: `./CLAUDE.md` (checked in).
* **Personal defaults** (language, editor quirks, local paths): import a file in `~/.claude/` from the project CLAUDE.md (so it stays out of git). ([docs.anthropic.com][2])

---

## 8) Common Pitfalls (and Fixes)

* **Bloat** → split with imports; keep the top file skim-friendly. ([docs.anthropic.com][2], [Reddit][3])
* **Vague rules** → replace with precise, runnable commands (“Run `pytest -q tests/foo.py::TestBar`”). ([anthropic.com][1])
* **Stale guidance** → review monthly; prune aggressively. ([docs.anthropic.com][2])

---

## 9) Nice Extras (when you’re ready)

* Use `/init` to bootstrap; then refine wording & sections. ([anthropic.com][1])
* If your team uses GitHub heavily, include the **exact** `gh` CLI flows you expect (open PR, label, etc.) so Claude follows them. ([anthropic.com][1])

---

### Sources (selected)

* Anthropic engineering: **“Claude Code: Best practices for agentic coding”** — what to include, where to place CLAUDE.md, `/init`, tuning, permissions. ([anthropic.com][1])
* Anthropic docs: **“Manage Claude’s memory”** — memory hierarchy, **imports** (`@path/to`), recursion, `#` & `/memory`. ([docs.anthropic.com][2])
* Anthropic case study (PDF): **How Anthropic teams use Claude Code** — writing detailed CLAUDE.md improves outcomes.&#x20;
* Community pattern: **Document references for large CLAUDE.md** — break up content, keep top file lean. ([Reddit][3])
* Field tips (EN): **Builder.io** — notes on hierarchical CLAUDE.md and practical UX tips. ([Builder.io][4])

[1]: https://www.anthropic.com/engineering/claude-code-best-practices "Claude Code Best Practices \ Anthropic"
[2]: https://docs.anthropic.com/en/docs/claude-code/memory "Manage Claude's memory - Anthropic"
[3]: https://www.reddit.com/r/ClaudeAI/comments/1lr6occ/tip_managing_large_claudemd_files_with_document/?utm_source=chatgpt.com "Tip: Managing Large CLAUDE.md Files with Document ..."
[4]: https://www.builder.io/blog/claude-code "How I use Claude Code (+ my best tips)"

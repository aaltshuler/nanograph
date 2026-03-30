---
title: Skills
slug: skills
---

# Skills

Skills are reusable capabilities that teach AI agents how to work with nanograph. Instead of pasting instructions into every prompt, install a skill once and your agent — Claude Code, Codex, Cursor, or any compatible tool — gains structured knowledge about nanograph operations, migrations, and workflows.

Skills are distributed through the [Vercel Agent Skills marketplace](https://skills.sh/) and installed with a single command.

## Installation

```bash
npx skills add nanograph/nanograph-skills
```

This installs all official nanograph skills into your project. Your AI agent will automatically pick them up on the next session.

## Available Skills

| Skill | What it teaches |
|-------|-----------------|
| **nanograph-ops** | Day-to-day database management — schema design, mutations, querying, search, aliases, output formats, and maintenance workflows |
| **nanograph-lance** | Safe migration of nanograph databases from Lance storage format v2 to v2.2, with full backup/verify/swap procedure |

### nanograph-ops

The operational skill covers the most common agent mistakes and how to avoid them:

- **Mutations over reingestion** — use `insert`/`update`/`delete` queries instead of export-edit-reload cycles that destroy CDC history
- **Schema design** — `@key` on every node type, `@description` and `@instruction` annotations for agent discoverability
- **Query aliases** — define short positional commands in `nanograph.toml` so agents don't build fragile multi-flag invocations
- **Search** — lexical predicates (`search`, `fuzzy`, `match_text`), semantic search with `nearest(...)`, hybrid ranking with `bm25` and `rrf`, and graph-constrained search patterns
- **Blobs and media** — `@media_uri(mime)` for external media, JSONL media formats (`@file:`, `@base64:`, `@uri:`), media-root storage, and media-node modeling
- **Embedding providers** — OpenAI text embeddings, Gemini multimodal embeddings, provider configuration, auto-detection, dimensionality reduction, and provider switching with re-embed
- **Post-change workflow** — the correct sequence after editing `.pg` or `.gq` files: check → migrate → embed → smoke test
- **Output formats** — `--format json`, `--format jsonl`, or `--format kv` for agents; `--quiet` to suppress human-readable output

### nanograph-lance

The migration skill guides agents through a safe export-rebuild-verify-swap procedure when upgrading Lance storage format from 2.0 to 2.2. It covers why 2.2 is worth upgrading to (better compression for text, enums, dates, and vectors), what is preserved vs lost, pre-flight checks, data validation, embedding regeneration, rollback steps, and a completion checklist.

## How Skills Work

Each skill is a markdown file (`SKILL.md`) with YAML frontmatter that tells the agent when to activate. When your agent encounters nanograph files — `.pg` schemas, `.gq` queries, `nanograph.toml`, or `*.nano/` databases — the relevant skill triggers automatically and provides the right procedures.

Skills follow the [agent skills specification](https://agentskills.io/specification). The file structure is:

```
skills/<skill-name>/
├── SKILL.md              # Skill definition with trigger rules and instructions
└── references/           # Optional detailed reference docs
```

## Supported Agents

Skills work with any agent that supports the skills specification, including:

- Claude Code
- OpenAI Codex
- OpenCode
- GitHub Copilot
- Cursor
- OpenClaw
- Google Gemini CLI
- Cline
- Warp
- Factory Droid
- AMP

See [skills.sh](https://skills.sh/) for the full compatibility list.

## Writing Custom Skills

You can write your own nanograph skills for domain-specific workflows — for example, a skill that teaches agents your team's schema conventions or query patterns. Follow the [agent skills specification](https://agentskills.io/specification) and place them in a `skills/` directory in your project or publish them to a separate repository.

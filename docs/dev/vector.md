---
audience: dev
status: superseded
updated: 2026-02-17
superseded_by: docs/dev/search-plan.md
---

# Vector Search Design (Superseded)

This document is retained only as historical context.

The canonical implementation plan for search and vector work is:

- `docs/dev/search-plan.md`

## Why Superseded

- Earlier drafts in this file mixed multiple phases and contained outdated examples.
- The active plan now separates v0 correctness work from ANN optimization and embedding rollout.
- Keeping a single source of truth avoids drift between design notes and implementation.

## Current Source of Truth

For all new implementation and review decisions, use:

- `docs/dev/search-plan.md`

If historical rationale is needed, use Git history for this file.

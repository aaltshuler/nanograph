# Property Graph Schema — Omni Context Graph

> **Status**: v1.1 (Expert-reviewed)
> **Target DB**: nanograph (strongly-typed)
> **Created**: 2026-02-14
> **Updated**: 2026-02-14

## Purpose

Capture the **physics of execution and value creation** — not just what happened, but who decided, why, what rules were screened against, what precedent it sets, and what value it drives. AI agents traverse backward from any outcome to reconstruct the full decision trace, and forward from intelligence to surface and capture opportunities.

---

## Graph Principles

### Node Classes

Every node belongs to one of two classes. This is a **hard constraint**, not a guideline.

| Class | Mutability | Nodes | Rationale |
|-------|-----------|-------|-----------|
| **Pointer** (mutable) | Update in place | `Actor`, `Client`, `Record`, `Opportunity`, `Project`, `ActionItem` | Represent the current state of a thing in the world. Identity is stable. |
| **Claims & Events** (append-only) | Never overwritten | `Decision`, `Signal`, `Policy`, `Action` | Assert something happened or is true. History must be unbroken. |

| Scenario | Strategy | Reason |
|----------|----------|--------|
| New decision | Append | Decisions are immutable events |
| Policy changes | Append new + `SUPERSEDES` old | Preserves the rule that was active when past decisions were made |
| Signal updated (new info) | Append new + `SUPERSEDES` old | Original signal may have informed past decisions |
| Client/Actor properties change | **Update in place** | Identity is stable |
| Opportunity stage advances | **Update in place** | Stage is a live property, not a historical claim |
| Project status changes | **Update in place** | Status is a live property |
| ActionItem completed | **Update in place** | Status is a live property |
| Record URIs change | **Update in place** | URIs are pointers, not claims |
| Fixing an error | Append corrected + `SUPERSEDES` erroneous | Never delete the error — it may have informed decisions |

### Node Properties by Class

**All nodes** carry:

| Property | Type | Description |
|----------|------|-------------|
| `slug` | String | **Primary key.** Hash-generated, globally unique across the graph. |
| `createdAt` | DateTime | When the node was written to the graph (system time). |

**Pointer nodes** additionally carry:

| Property | Type | Description |
|----------|------|-------------|
| `updatedAt` | DateTime | Last modification timestamp. |
| `notes` | List[String] | Freeform annotations. May be empty `[]`. |

**Claims & Events nodes** do **not** carry `updatedAt` or `notes` — they are immutable and are not living entities.

### Bi-Temporal Time Model

Every Claims & Events node has two time dimensions:
- **`createdAt`** = when the node was written to the graph (system/ingestion time).
- **Domain event time** (`decidedAt`, `observedAt`, `executedAt`, `effectiveFrom`) = when it actually happened in the world.

These may differ significantly — e.g., a meeting decision made Tuesday may not be ingested until Thursday. The AI uses domain time for historical reasoning and `createdAt` for debugging ingestion latency.

### Fact vs Assumption

Claims & Events nodes that make assertions carry an `assertion` property:

| Value | Meaning | Confidence | Example |
|-------|---------|------------|---------|
| `fact` | Verified, treated as in effect / true | Not required | "Client paid $5,000 on Feb 3" |
| `assumption` | Not fully verified; inferred or estimated | Required: `low`, `medium`, `high` | "Client likely frustrated based on tone" |

- **Pointer nodes** do not carry `assertion` — they reference things that exist, they don't make claims.
- `Policy` and `Action` are implicit facts — they don't carry `assertion` (a policy either exists or it doesn't; an action either happened or it didn't).
- `Decision` and `Signal` carry `assertion` + optional `confidence`.

### Enums Over Ranges

Continuous ranges (like 0.0–1.0) are replaced with semantic 3–4 value enums. This forces decisive categorization and makes graph queries mathematically predictable.

### Enum Casing

All enum values are **lowercase** throughout the schema. No mixed casing.

### Contestation is Derived

No node stores a `contested` property. A node is contested when it has an incoming or outgoing `CONTRADICTS` edge with `resolution = pending`. Compute this at query time.

---

## Node Types

### Pointer Nodes (Mutable)

All pointer nodes carry `slug`, `createdAt`, `updatedAt`, and `notes` (List[String]) in addition to the fields listed below.

#### Opportunity

The central value node. Everything in the graph ultimately connects to opportunities — Signals surface them, Decisions advance them, ActionItems organize the work, Actions record execution.

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `title` | String | Yes | What the opportunity is |
| `description` | String | No | Full context and scope |
| `type` | Enum | Yes | `net_new`, `expansion`, `upsell`, `retention` |
| `stage` | Enum | Yes | `identified`, `qualifying`, `proposal`, `active`, `won`, `lost`, `hold` |
| `priority` | Enum | Yes | `low`, `medium`, `high`, `critical` |
| `risk` | Enum | No | `low`, `medium`, `high`, `critical` |
| `amount` | Number | No | Deal value |
| `currency` | String | No | Currency code (e.g., `USD`) |
| `amountPaid` | Number | No | Total received to date |
| `discount` | Number | No | Discount percentage |
| `expectedClose` | DateTime | No | Anticipated close date |
| `closedAt` | DateTime | No | Actual close date |
| `isRecurring` | Boolean | No | Whether this is a recurring engagement |
| `recurringInterval` | String | No | Recurrence period (e.g., `monthly`, `quarterly`) |

#### Project

An active client engagement or deliverable. Promoted from Record because projects carry operational state beyond a simple pointer.

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `name` | String | Yes | Project name |
| `description` | String | No | Scope and context |
| `status` | Enum | Yes | `active`, `hold`, `support`, `completed` |
| `type` | String | No | Engagement kind (e.g., `dev contract`, `1:1 consulting`) |
| `startDate` | DateTime | No | When work began |
| `expectedCompletion` | DateTime | No | Target completion |

#### ActionItem

Pending human or agent work. Distinct from `Action` (which is a past execution log).

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `title` | String | Yes | What needs to be done |
| `description` | String | No | Full context for the task |
| `status` | Enum | Yes | `open`, `in_progress`, `blocked`, `completed`, `cancelled` |
| `priority` | Enum | Yes | `low`, `medium`, `high`, `critical` |
| `dueDate` | DateTime | No | When it's due |

#### Client

The human or organizational subject of decisions.

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `name` | String | Yes | Full name or company |
| `status` | Enum | Yes | `healthy`, `nurture`, `cold`, `dormant`, `focus` |
| `tags` | List[String] | Yes | Roles and labels (e.g., `client`, `champion`, `star`). Empty list `[]` is valid for new/unknown contacts. |
| `company` | String | No | Organization |
| `email` | String | No | Primary email |

#### Actor

Who or what makes decisions or owns tasks.

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `name` | String | Yes | Actor name |
| `type` | Enum | Yes | `human`, `agent` |
| `defaultChannel` | String | No | Primary interaction channel |

#### Record

Pointer to an external artifact. Concrete, addressable things in external systems.

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `platform` | Enum | Yes | `stripe`, `mercury`, `gmail`, `slack`, `github`, `internal`, `other` |
| `recordType` | String | Yes | Platform-specific type (`invoice`, `thread`, `pr`, `document`) |
| `externalId` | String | No | Native ID in the source system (for dedupe) |
| `uri` | String | No | Deep link to the record |
| `title` | String | No | Human-readable label |

**Dedupe rule:** For records where `platform != internal`, uniqueness is enforced on `(platform, externalId)` when `externalId` is present. This prevents duplicate Gmail threads, Stripe invoices, etc.

---

### Claims & Events Nodes (Append-Only)

All Claims & Events nodes carry `slug` and `createdAt` in addition to the fields listed below. They do **not** carry `updatedAt` (they are immutable) or `notes` (they are not living entities).

#### Decision

The atomic core of intent. Every business action traces back to a decision.

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `decidedAt` | DateTime | Yes | When the decision was made |
| `intent` | String | Yes | What was decided |
| `status` | Enum | Yes | `proposed`, `approved`, `rejected`, `cancelled` |
| `domain` | Enum | Yes | `dev`, `finance`, `sales`, `compliance`, `content` |
| `assertion` | Enum | Yes | `fact` or `assumption` |
| `confidence` | Enum | When assumption | `low`, `medium`, `high` |

A Decision is considered **executed** when it has at least one `RESULTED_IN` edge pointing to an `Action` with `success = true`. No `executed` status value — execution is proven by Actions.

#### Signal

Observed intelligence that informs decisions. The "I noticed something" node — a detected cue from the world that may trigger action.

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `observedAt` | DateTime | Yes | When the signal was captured |
| `summary` | String | Yes | What the signal conveys |
| `sentiment` | Enum | No | `positive`, `neutral`, `negative`, `mixed` |
| `urgency` | Enum | No | `low`, `medium`, `high`, `critical` |
| `sourceType` | Enum | Yes | `meeting`, `email`, `message`, `slack`, `call`, `observation` |
| `assertion` | Enum | Yes | `fact` or `assumption` |
| `confidence` | Enum | When assumption | `low`, `medium`, `high` |

#### Policy

Codified business rules that decisions are screened against. Versioned — new versions supersede old. Agents use policies to generate enrichment artifacts (via `DERIVED_FROM`) that build the case for passing the screen.

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `key` | String | Yes | Stable identifier across versions |
| `name` | String | Yes | Rule name |
| `effectiveFrom` | DateTime | Yes | When rule took effect |
| `effectiveTo` | DateTime | No | **Only set when known at creation** (e.g., a Q4 promo with a hard end date). Otherwise, effective end is derived from the newer policy that `SUPERSEDES` this one (i.e., the node whose `SUPERSEDES` edge points here). Never mutate this field after creation. |
| `description` | String | Yes | Full rule text |
| `tags` | List[String] | No | Categorization labels |

#### Action

The physical state change resulting from a decision. The indisputable machine log of what executed.

| Property | Type | Required | Description |
|----------|------|----------|-------------|
| `executedAt` | DateTime | Yes | When the action executed |
| `operation` | Enum | Yes | `create`, `update`, `delete`, `send` |
| `payloadDiffJson` | Json | No | Before/after state (explicitly unstructured) |
| `success` | Boolean | Yes | Did it succeed? |
| `errorMessage` | String | No | Failure reason when `success = false` |
| `resultSummary` | String | No | Qualitative outcome description (e.g., "Invoice sent, client acknowledged") |

---

## Edge Types

### Core Edges

The decision spine — standardize these hard. They form the canonical trace path.

| Edge | From → To | Properties | Description |
|------|-----------|------------|-------------|
| `MADE_BY` | Decision → Actor | *(none)* | Who made this decision |
| `AFFECTS` | Decision / Signal → Client | *(none)* | Who is impacted or who this intelligence is about |
| `INFORMED_BY` | Decision → Signal | `influence`: `primary`, `supporting`, `minor` | What signals drove the decision |
| `SCREENED_BY` | Decision → Policy | `outcome`: `passed`, `failed`, `overridden`, `not_applicable`; `overrideReason`: String (required when outcome = `overridden`) | Screening gate — did the decision pass the policy check? |
| `RESULTED_IN` | Decision → Action | *(none)* | What happened as a result |
| `TOUCHED` | Action → **any Pointer node** | `action`: `created`, `updated`, `deleted`, `sent`, `mentioned`; `mutatedFields`: List[String] | Links execution to the exact entity and fields it altered. Not limited to Records — Actions can touch Opportunities, Projects, ActionItems, Clients, etc. |
| `SOURCED_FROM` | Signal → Record | optional `extractionMethod`: `llm`, `manual`, `rule` | Where the signal was extracted from |
| `HAS_EVIDENCE` | (Decision / Signal / Policy / Action) → Record | *(none)* | Supporting artifact |

### Value Loop Edges

Connect intelligence to revenue and task execution.

| Edge | From → To | Properties | Description |
|------|-----------|------------|-------------|
| `SURFACED` | Signal → Opportunity | `confidence`: `low`, `medium`, `high` | Intelligence reveals a new opportunity |
| `TARGETS` | Decision → Opportunity | *(none)* | A decision advances or captures this opportunity |
| `GENERATES` | Decision / Opportunity → ActionItem | *(none)* | A decision or deal spawns pending work |
| `ASSIGNED_TO` | ActionItem → Actor | *(none)* | Who is responsible for the work |
| `RESOLVES` | Action → ActionItem | *(none)* | Execution log completes the pending task |

### Versioning & Conflict Edges

| Edge | From → To | Properties | Description |
|------|-----------|------------|-------------|
| `SUPERSEDES` | (Policy / Signal / Decision) → same type | optional `reason`: String | New version replaces old. Only between Claims & Events nodes of the same type. |
| `CONTRADICTS` | Claims & Events ↔ Claims & Events | `resolution`: `pending`, `source_invalidated`, `target_invalidated`, `both_invalidated` | Flags incompatible assertions between claim/event nodes |

### Structural & Ownership Edges

Ownership and containment edges (`OWNS`, `CONTAINS`) follow **strict top-down directionality** — parent points to child. No bottom-up edges. Provenance and learning edges (`DERIVED_FROM`, `BASED_ON_PRECEDENT`) are directional but not ownership.

| Edge | From → To | Properties | Description |
|------|-----------|------------|-------------|
| `OWNS` | Client → Opportunity / Project / Record | *(none)* | Client ownership of a deal, project, or artifact |
| `OWNS` | Actor → Opportunity / Project | *(none)* | Internal actor ownership of a deal or project |
| `CONTAINS` | Opportunity → Project | *(none)* | Opportunity spawned or includes this project |
| `CONTAINS` | Project → ActionItem | *(none)* | Project contains this task |
| `CONTAINS` | Record → Record | *(none)* | Record hierarchy (e.g., thread contains messages) |
| `DERIVED_FROM` | Record → Policy | *(none)* | Record was generated to satisfy a policy's requirements (e.g., enrichment profile built from screening criteria) |
| `BASED_ON_PRECEDENT` | Decision → Decision | `similarity`: `strong`, `moderate`, `weak` | Learning loop — links to past decisions that informed this one |

---

## Screening Outcome Enum

The `SCREENED_BY` edge's `outcome` property records whether a decision passed the policy gate — and if not, why it was allowed through anyway.

| Value | Meaning | Example |
|-------|---------|---------|
| `passed` | Decision meets the policy criteria | "Enrichment profile satisfies screening policy — cleared" |
| `failed` | Decision does not meet the policy; blocked or rejected | "Screening criteria not met — deal not pursued" |
| `overridden` | Decision failed the screen but was consciously pushed through (requires `overrideReason`) | "Screening failed but approved for strategic logo" |
| `not_applicable` | Policy exists but doesn't apply to this decision | "Screening policy doesn't apply to internal projects" |

---

## Standardized Enums Reference

All enum values are **lowercase**.

### Universal Qualifiers
- **Urgency, Priority & Risk**: `low` | `medium` | `high` | `critical`
- **Confidence**: `low` | `medium` | `high` (required when `assertion = assumption`)

### Lifecycle Statuses
- **Opportunity Stage**: `identified` | `qualifying` | `proposal` | `active` | `won` | `lost` | `hold`
- **Project Status**: `active` | `hold` | `support` | `completed`
- **ActionItem Status**: `open` | `in_progress` | `blocked` | `completed` | `cancelled`
- **Decision Status**: `proposed` | `approved` | `rejected` | `cancelled`

### Logic Enums
- **Screening Outcome**: `passed` | `failed` | `overridden` (requires `overrideReason`) | `not_applicable`
- **Resolution**: `pending` | `source_invalidated` | `target_invalidated` | `both_invalidated`

---

## Canonical Trace Example

Starting from an entity that was changed, answer "why did this happen?":

```
Opportunity (or any Pointer node)
  ⬅ TOUCHED (action: updated, mutatedFields: ["stage"])
Action (what changed, success?, resultSummary)
  ⬅ RESULTED_IN
Decision (intent, domain)
  → MADE_BY → Actor (who decided)
  → AFFECTS → Client (who is impacted)
  → TARGETS → Opportunity (what deal this advances)
  → INFORMED_BY → Signal (intelligence, influence: primary/supporting)
  → SCREENED_BY → Policy (outcome: passed/overridden)
    Policy ⬅ DERIVED_FROM ⬅ Record (enrichment profile built from policy criteria)
  → HAS_EVIDENCE → Record (supporting artifacts)
```

Starting from intelligence, answer "what value did this surface?":

```
Signal (summary, urgency)
  → AFFECTS → Client (who this intelligence is about)
  → SURFACED → Opportunity (stage, priority)
    ⬅ CONTAINS
    Project (status)
      ⬅ CONTAINS
      ActionItem (status, dueDate)
        → ASSIGNED_TO → Actor
    ⬅ TARGETS
    Decision (intent)
      → RESULTED_IN → Action (success?)
        → RESOLVES → ActionItem
        → TOUCHED → Opportunity (mutatedFields: ["stage"])
```

---

## Design Rules

1. **Pointer vs Claims & Events is enforced.** Pointer nodes: update in place. Claims & Events nodes: append-only; corrections use `SUPERSEDES`.
2. **Execution is proven by Actions.** A Decision is "executed" if it has at least one `RESULTED_IN → Action(success=true)`. Failed execution is `success=false`. No `executed` status on Decision.
3. **Contestation is derived, not stored.** No `contested` field on any node. Compute from `CONTRADICTS` edges where `resolution = pending`.
4. **Reference edges are standardized.** Proof = `HAS_EVIDENCE`. Source extraction = `SOURCED_FROM`. Execution touched = `TOUCHED`.
5. **Enums over floats.** All continuous ranges replaced with semantic 3–4 value enums.
6. **Bi-temporal timestamps.** `createdAt` = graph-write-time. Domain event fields (`decidedAt`, `executedAt`, `observedAt`, `effectiveFrom`) = world-time. One domain timestamp name per node type.
7. **`slug` is the universal key.** Every node gets a hash-generated `slug` as its primary identifier, globally unique across the graph.
8. **All pointer nodes carry `notes`.** `notes: List[String]` for freeform annotations on all mutable entities.
9. **All enums are lowercase.** No mixed casing anywhere in the schema.
10. **Ownership edges are top-down.** `OWNS` and `CONTAINS` always point from parent to child. Provenance (`DERIVED_FROM`) and learning (`BASED_ON_PRECEDENT`) edges are directional but not ownership.
11. **`TOUCHED` targets any Pointer node.** Actions don't just touch Records — they can touch Opportunities, Projects, ActionItems, Clients, etc. The `action` property describes what was done; `mutatedFields` specifies what changed.
12. **Policy `effectiveTo` is creation-only.** Set only for pre-planned hard expirations. Otherwise, effective end is derived from the `SUPERSEDES` edge.
13. **Record dedupe on `(platform, externalId)`.** For records where `platform != internal`, uniqueness is enforced when `externalId` is present.

---

## Migration Path from MongoDB

| Phase | What | Source |
|-------|------|--------|
| 1 | Seed **Client** nodes | `crm` collection |
| 2 | Seed **Opportunity** nodes (with financial fields, dates, recurring) | `opportunities` collection |
| 3 | Seed **Project** nodes | `projects` collection |
| 4 | Seed **ActionItem** nodes | `tasks` collection |
| 5 | Seed **Record** nodes (`recordType: document`) | `docs` collection |
| 6 | Build **Decision + Action** pairs from event history | `crm_events` collection |
| 7 | Wire edges using existing foreign keys | `clientId`, `projectId`, `opportunityId` references |

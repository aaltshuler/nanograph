# NanoGraph — Use Cases

## 1. Personal CRM & The Physics of Execution

### The Graph (v1.1 Complete Trace)

Context graph with decision traces that allows agents to reason through specific business scenarios and make autonomous decisions.

#### Diagram 1 — Intelligence: Signal surfaces an Opportunity

Jamie's coffee chat produces a Signal. The Signal links to who it's about and what deal it reveals.

```text
  👤 Jamie Lee (Client)
     tags: ['connector']
        │
        └── OWNS ──▶ ☕ Coffee Chat (Record)
                        platform: 'internal'
                        recordType: 'meeting'
                           ▲
                           │ SOURCED_FROM
                           │
                     🧠 "Hates vendor" (Signal)
                        urgency: 'high'
                        sourceType: 'meeting'
                           │
                           ├── AFFECTS ──▶ 👤 Priya Shah (Client)
                           │                  company: 'Stripe'
                           │
                           └── SURFACED ──▶ 🎯 Stripe Migration (Opportunity)
                                              stage: 'identified'

  👤 Priya Shah (Client) ── OWNS ──▶ 🎯 Stripe Migration (Opportunity)
```

#### Diagram 2 — Enrichment & Screening: Agent builds the case, then passes the gate

The agent reads the Screening Policy, generates an Enrichment Profile based on its criteria, then the Decision is screened and passes.

```text
  ⚖️ Make Proposal (Decision)
     intent: 'Make proposal for Stripe migration'
     domain: 'sales'
        │
        ├── MADE_BY ──────────▶ 👤 Andrew (Actor, type: 'human')
        │
        ├── INFORMED_BY ──────▶ 🧠 "Hates vendor" (Signal)
        │   (influence: 'primary')
        │
        ├── AFFECTS ──────────▶ 👤 Priya Shah (Client)
        │
        ├── TARGETS ──────────▶ 🎯 Stripe Migration (Opportunity)
        │
        ├── SCREENED_BY ──────▶ 📜 Screening Policy (Policy)
        │   (outcome: 'passed')    key: 'client-screen'
        │                          description: TBD
        │                             │
        │                             └── SUPERSEDES ──▶ 📜 Screening Policy v1
        │                                                   (Policy, original)
        │
        └── RESULTED_IN ─────▶ ⚙️ Build Profile (Action)
                                  operation: 'create'
                                  success: true
                                     │
                                     └── TOUCHED ──▶ 📊 Enrichment Profile (Record)
                                         (action: 'created')
                                                        │
                                                        └── DERIVED_FROM ──▶ 📜 Screening Policy
```

#### Diagram 3 — Execution: Work is assigned, completed, and proven

The Decision generates work. The agent executes it, creating the proposal and advancing the deal. The Opportunity spawns a Project.

```text
  ⚖️ Make Proposal (Decision)
        │
        ├── GENERATES ────▶ 📋 Draft Proposal (ActionItem)
        │                     status: 'completed'
        │                     priority: 'high'
        │                        │
        │                        └── ASSIGNED_TO ──▶ 🤖 OmniBot (Actor, type: 'agent')
        │
        └── RESULTED_IN ──▶ ⚙️ Sent Proposal (Action)
                               operation: 'send'
                               success: true
                                  │
                                  ├── RESOLVES ──▶ 📋 Draft Proposal (ActionItem)
                                  │
                                  ├── TOUCHED ───▶ 📄 Proposal.pdf (Record)
                                  │   (action: 'created')
                                  │
                                  └── TOUCHED ───▶ 🎯 Stripe Migration (Opportunity)
                                      (action: 'updated',
                                       mutatedFields: ['stage'])

  🎯 Stripe Migration (Opportunity)
        │
        └── CONTAINS ──▶ 🏗️ Data Pipeline (Project)
                            status: 'active'
```

### Entity Mapping (All 10 Node Types Utilized)

| Class | Node | Real-World Representation in this Trace |
| --- | --- | --- |
| **Pointer** | **`Client`** | Jamie (the connector who introduced) and Priya (the buyer at Stripe). |
| **Pointer** | **`Record`** | The coffee chat meeting notes (source), the agent-built Enrichment Profile (screening input), & the generated Proposal PDF (output). |
| **Pointer** | **`Opportunity`** | The Stripe Migration deal surfaced from the chat. |
| **Pointer** | **`Project`** | The active delivery (Data Pipeline) spawned after the deal was won. |
| **Pointer** | **`Actor`** | Andrew (human, makes the decision) and OmniBot (agent, executes the work). |
| **Pointer** | **`ActionItem`** | The pending task ("Draft Proposal") generated by the decision. |
| **Event** | **`Signal`** | The high-urgency intelligence detected: "Priya hates her vendor." |
| **Event** | **`Decision`** | "Make Proposal" — Andrew decides to pursue the Stripe migration. |
| **Event** | **`Policy`** | The Screening Policy gate — the deal must pass before the agent proceeds. Current Screening Policy `SUPERSEDES` v1. |
| **Event** | **`Action`** | Two Actions: (1) "Build Profile" — agent creates the Enrichment Profile from the policy criteria; (2) "Sent Proposal" — the proposal is delivered and the deal advances. |

### Key Patterns Demonstrated

**1. Agent-Driven Enrichment (building the case from the policy)**

When the Signal surfaces an opportunity, the agent reads the Screening Policy and generates an **Enrichment Profile** (Record, `recordType: 'enrichment'`) based on its criteria. The Enrichment Profile is `DERIVED_FROM` the Policy — the policy defines what the agent needs to gather, and the profile is the result. This is the agent doing research to satisfy the gate, not just collecting random data.

**2. Screening Gate (Policy as a pass/fail checkpoint)**

The Decision "Make Proposal" is screened against the Screening Policy. Because the agent built an Enrichment Profile that satisfies the policy criteria, the outcome is `passed`. If the enrichment had been insufficient, the outcome would be `failed` and the agent would escalate to a human.

**3. Policy Versioning (SUPERSEDES)**

The current "Screening Policy" `SUPERSEDES` "Screening Policy v1". Both share the same `key: 'client-screen'`, so the system knows they're versions of the same rule. Past decisions screened against v1 retain their links — history is never rewritten.

**4. Complete Decision Spine**

The Decision connects to everything required by the canonical trace:
- `MADE_BY` → Actor (who decided — Andrew)
- `INFORMED_BY` → Signal (what intelligence drove it)
- `SCREENED_BY` → Policy (did it pass the gate?)
- `TARGETS` → Opportunity (what deal it advances)
- `GENERATES` → ActionItem (what work it spawns)
- `AFFECTS` → Client (who is impacted)
- `RESULTED_IN` → Action (proof of execution)

---

### Rich AI Queries (Cypher Syntax)

**1. "Which deals passed screening, and what enrichment did the agent build?"**

```cypher
MATCH (dec:Decision)-[screen:SCREENED_BY {outcome: 'passed'}]->(pol:Policy)
MATCH (dec)-[:TARGETS]->(opp:Opportunity)
MATCH (profile:Record {recordType: 'enrichment'})-[:DERIVED_FROM]->(pol)
MATCH (act:Action {operation: 'create'})-[:TOUCHED {action: 'created'}]->(profile)
MATCH (dec)-[:RESULTED_IN]->(act)
RETURN 
    opp.title AS Deal,
    pol.name AS ScreenPassed,
    profile.title AS EnrichmentProfile,
    act.resultSummary AS AgentWork
```

```text
┌──────────────────┬──────────────────┬──────────────────────┬──────────────────┐
│ Deal             │ ScreenPassed     │ EnrichmentProfile    │ AgentWork        │
├──────────────────┼──────────────────┼──────────────────────┼──────────────────┤
│ Stripe Migration │ Screening Policy │ Stripe - Enrichment  │ Company data     │
│                  │                  │                      │ compiled, fit    │
│                  │                  │                      │ score calculated │
└──────────────────┴──────────────────┴──────────────────────┴──────────────────┘
```

**2. "What is the true ROI of my AI agents (measured by closed pipeline)?"**

```cypher
MATCH (agent:Actor {type: 'agent'})<-[:ASSIGNED_TO]-(task:ActionItem)
MATCH (task)<-[:GENERATES]-(dec:Decision)-[:TARGETS]->(opp:Opportunity {stage: 'won'})
MATCH (act:Action {success: true})-[:RESOLVES]->(task)
RETURN 
    agent.name AS Agent,
    COUNT(task) AS TasksCompleted,
    SUM(opp.amount) AS RevenueInfluenced
ORDER BY RevenueInfluenced DESC
```

```text
┌───────────────┬────────────────┬───────────────────┐
│ Agent         │ TasksCompleted │ RevenueInfluenced │
├───────────────┼────────────────┼───────────────────┤
│ OmniBot       │ 14             │ 320000            │
│ Research Agent│ 8              │ 115000            │
└───────────────┴────────────────┴───────────────────┘
```

**3. "How did we land the Stripe project, and who actually did the work?"**

```cypher
MATCH (opp:Opportunity {title: 'Stripe Migration'})
MATCH (sig:Signal)-[:SURFACED]->(opp)
MATCH (sig)-[:AFFECTS]->(buyer:Client)
MATCH (dec:Decision)-[:TARGETS]->(opp)
MATCH (dec)-[:INFORMED_BY]->(sig)
MATCH (dec)-[:MADE_BY]->(decider:Actor)
MATCH (dec)-[:GENERATES]->(task:ActionItem)-[:ASSIGNED_TO]->(worker:Actor)
RETURN 
    buyer.name AS Client,
    sig.summary AS Intelligence,
    decider.name AS DecidedBy,
    dec.intent AS Strategy,
    worker.name AS ExecutedBy
```

```text
┌────────────┬────────────────────────┬───────────┬────────────────┬────────────┐
│ Client     │ Intelligence           │ DecidedBy │ Strategy       │ ExecutedBy │
├────────────┼────────────────────────┼───────────┼────────────────┼────────────┤
│ Priya Shah │ Priya hates vendor...  │ Andrew    │ Make Proposal  │ OmniBot    │
└────────────┴────────────────────────┴───────────┴────────────────┴────────────┘
```

---

### Landing Page Narrative

**Panel 1 — What your current stack sees:**

> *Salesforce:* A deal for Stripe.
> *Asana:* A checked-off task: "Draft proposal."
> *Contacts App:* Jamie Lee - 415-555-0142.
> *Notion:* A forgotten screening policy document.

**Panel 2 — What NanoGraph knows:**

> The complete anatomy of the win. Jamie's coffee chat generated a Signal: Priya hated her vendor. That Signal surfaced the Stripe Migration opportunity. OmniBot read your Screening Policy, built an Enrichment Profile from its criteria, and the deal passed screening. Andrew decided to make a proposal. OmniBot drafted and sent it. The Action log proves the proposal was delivered, the deal advanced to Won, and an active delivery project was spawned.

**Panel 3 — Six months from now:**

> You ask your AI: *"Which deals passed screening only because the agent built enrichment profiles?"*
> NanoGraph traverses the `Policy` → `Record (enrichment)` → `Action` → `Decision` loop and replies: *"12 deals. The agent-built profiles had a 90% screening pass rate vs. 60% for deals without enrichment. Those 12 deals generated $480K in pipeline."*

**Tagline:** *"CRMs track what happened. Task managers track what's next. NanoGraph remembers exactly how, why, and who made it happen."*

---

### NanoGraph Implementation Mapping

The diagrams above use conceptual edge names for readability. The NanoGraph implementation adapts these to its type system constraints.

#### Property Renames

NanoGraph reserves certain identifiers. These properties are renamed from the conceptual spec:

| Spec Name | NanoGraph Name | Node | Reason |
|-----------|---------------|------|--------|
| `type` | `actorType` | Actor | Reserved identifier |
| `type` | `dealType` | Opportunity | Reserved identifier |
| `type` | `engagementType` | Project | Reserved identifier |
| `key` | `policyKey` | Policy | Reserved identifier |
| `payloadDiffJson` | `payloadDiff` | Action | No Json type; stored as String |

#### Edge Splits

NanoGraph requires fixed endpoint types per edge. Polymorphic edges are split:

| Spec Edge | NanoGraph Edges |
|-----------|----------------|
| `AFFECTS` | `DecisionAffects: Decision -> Client`, `SignalAffects: Signal -> Client` |
| `TOUCHED` | `TouchedRecord: Action -> Record`, `TouchedOpportunity: Action -> Opportunity` |
| `OWNS` | `ClientOwnsRecord: Client -> Record`, `ClientOwnsOpportunity: Client -> Opportunity`, `ClientOwnsProject: Client -> Project` |
| `GENERATES` | `DecisionGenerates: Decision -> ActionItem` |

The `TOUCHED` edge property `action` is renamed to `touchAction` to avoid keyword conflicts.

#### Deferred Edges

These spec edges are not yet implemented (add via `nanograph migrate` when needed):

| Edge | Reason |
|------|--------|
| `HAS_EVIDENCE` | Would require 4 typed variants. Add when needed. |
| `CONTRADICTS` | Complex conflict resolution. Add when contestation queries are needed. |
| `SUPERSEDES` for Signal/Decision | Only `Policy -> Policy` implemented. |
| `OWNS` for Actor | Actor -> Opportunity/Project ownership not yet modeled. |
| `CONTAINS` for Project/Record | Project -> ActionItem and Record -> Record hierarchies. Add when needed. |
| `TOUCHED` for other Pointer nodes | Only Record and Opportunity variants. Add when needed. |

#### Implementation Files

The working NanoGraph implementation lives in [`examples/revops/`](../../examples/revops/):

| File | Description |
|------|-------------|
| `revops.pg` | Schema: 10 node types, 21 edge types |
| `revops.gq` | 24 queries: lookups, traces, aggregation, filtering, mutations |
| `revops.jsonl` | Seed data: 14 nodes, 22 edges (Stripe Migration trace) |

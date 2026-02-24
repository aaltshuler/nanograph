---
title: Search
slug: search
---

# Search Guide

NanoGraph supports text search, vector (semantic) search, hybrid search, and graph-constrained search — all from the same query language. This guide covers each type with real examples using a Star Wars character graph.

## Setup

The examples use the `search-test/` dataset in the repo. For full schema syntax, see [schema.md](schema.md).

The schema:

```graphql
node Character {
    slug: String @key
    name: String
    alignment: enum(light, dark, neutral)
    era: enum(prequel, original, sequel, imperial)
    bio: String
    embedding: Vector(1536) @embed(bio) @index
}

node Film {
    slug: String @key
    title: String
    release_year: I32
}

edge HasParent: Character -> Character
edge SiblingOf: Character -> Character
edge MarriedTo: Character -> Character
edge HasMentor: Character -> Character
edge DebutsIn: Character -> Film
```

Key schema details:
- `embedding: Vector(1536)` — stores a 1536-dimensional float vector (matches OpenAI `text-embedding-3-small`)
- `@embed(bio)` — auto-generates the embedding from the `bio` String property at load time. For `@embed` annotation details, see [schema.md](schema.md#annotations).
- `@index` — creates a vector index for efficient similarity search

## Text search

Text search predicates go in the `match` block and filter rows. They don't return scores — a row either matches or it doesn't.

### `search()` — token-based keyword match

```graphql
query keyword_search($q: String) {
    match {
        $c: Character
        search($c.bio, $q)
    }
    return { $c.slug, $c.name }
    order { $c.slug asc }
}
```

```bash
nanograph run --db sw.nano --query search-test/queries.gq --name keyword_search --param 'q=dark side'
```

Token-based keyword match: query and field are tokenized, lowercased, and all query tokens must be present in the field. Fast — no embeddings needed.

**When to use**: filtering by known terms, keyword matching, multi-token must-match search.

### `fuzzy()` — approximate match

```graphql
query fuzzy_search($q: String) {
    match {
        $c: Character
        fuzzy($c.bio, $q)
    }
    return { $c.slug, $c.name }
    order { $c.slug asc }
}
```

```bash
nanograph run --db sw.nano --query search-test/queries.gq --name fuzzy_search --param 'q=Skywalker'
```

Matches even if there are small spelling differences (edit distance). Useful when queries may contain typos.

**When to use**: user-facing search where input quality varies.

### `match_text()` — contiguous token match

```graphql
query match_text_search($q: String) {
    match {
        $c: Character
        match_text($c.bio, $q)
    }
    return { $c.slug, $c.name }
    order { $c.slug asc }
}
```

```bash
nanograph run --db sw.nano --query search-test/queries.gq --name match_text_search --param 'q=Jedi Master'
```

Matches contiguous token sequences. The query is tokenized and must appear as consecutive tokens in the text.

**When to use**: phrase matching where word order matters.

## Ranked text search

### `bm25()` — relevance scoring

Unlike the filter predicates above, `bm25()` returns a numeric score. It can be used in both `return` (to project the score) and `order` (to rank by relevance).

```graphql
query bm25_search($q: String) {
    match {
        $c: Character
    }
    return { $c.slug, $c.name, bm25($c.bio, $q) as score }
    order { bm25($c.bio, $q) desc }
    limit 5
}
```

```bash
nanograph run --db sw.nano --query search-test/queries.gq --name bm25_search --param 'q=clone wars lightsaber'
```

BM25 is a standard information retrieval ranking function. Higher score = more relevant, so use `desc` ordering. All rows are scanned; `limit` controls how many top results are returned.

**When to use**: ranked keyword search where you want the most relevant results, not just matches.

## Vector (semantic) search

Vector search finds nodes by meaning similarity, not keyword matching. The query string is embedded at runtime (OpenAI by default, or deterministic mock mode) and compared against stored embeddings using cosine distance.

### `nearest()` — semantic similarity

```graphql
query semantic_search($q: String) {
    match { $c: Character }
    return {
        $c.slug as slug,
        $c.name as name,
        nearest($c.embedding, $q) as score
    }
    order { nearest($c.embedding, $q) }
    limit 5
}
```

```bash
nanograph run --db sw.nano --query search-test/queries.gq --name semantic_search --param 'q=who turned evil'
```

Example results:
```
| slug       | name             | score              |
|------------|------------------|--------------------|
| anakin     | Anakin Skywalker | 0.6674947996036102 |
| maul       | Maul             | 0.6721895395861276 |
| luke       | Luke Skywalker   | 0.6865860774370471 |
| din-djarin | Din Djarin       | 0.7283290565980479 |
| obi-wan    | Obi-Wan Kenobi   | 0.7593273313045857 |
```

**Important**: `nearest()` returns **cosine distance** (lower = closer/better). Default ordering is ascending, so the most similar result comes first. This is the opposite of BM25 where higher = better.

- Cosine distance 0.0 = identical vectors
- Cosine distance 1.0 = orthogonal (unrelated)
- Cosine distance 2.0 = opposite

The query `"who turned evil"` doesn't contain the word "Anakin" or "Skywalker", yet Anakin ranks first because the meaning of his bio (fall to the dark side) is semantically closest to the query.

**When to use**: natural language queries, conceptual search, when users don't know the exact terms.

## Hybrid search

Hybrid search combines vector and text signals for better results than either alone.

### `rrf()` — Reciprocal Rank Fusion

RRF merges two ranked lists by combining `1/(k + rank)` from each signal. It doesn't require the scores to be on the same scale.

```graphql
query hybrid_search($q: String) {
    match {
        $c: Character
    }
    return {
        $c.slug as slug,
        $c.name as name,
        nearest($c.embedding, $q) as semantic_distance,
        bm25($c.bio, $q) as lexical_score,
        rrf(nearest($c.embedding, $q), bm25($c.bio, $q)) as hybrid_score
    }
    order { rrf(nearest($c.embedding, $q), bm25($c.bio, $q)) desc }
    limit 5
}
```

```bash
nanograph run --db sw.nano --query search-test/queries.gq --name hybrid_search --param 'q=father and son conflict'
```

**Important**: RRF score is **higher = better** (unlike `nearest()` which is lower = better). Always use `desc` ordering with `rrf()`.

Projecting all three scores (`semantic_distance`, `lexical_score`, `hybrid_score`) is useful for debugging ranking behavior.

**When to use**: production search where you want robustness — vector search catches semantic matches while BM25 catches exact keyword hits.

## Graph-constrained search

This is where NanoGraph's graph structure shines. You can combine edge traversal with any search predicate to scope results to a subgraph.

### Traversal + semantic ranking

Find Luke's parents ranked by semantic similarity to a query:

```graphql
query family_semantic($slug: String, $q: String) {
    match {
        $c: Character { slug: $slug }
        $c hasParent $p
    }
    return { $p.slug, $p.name }
    order { nearest($p.embedding, $q) }
    limit 3
}
```

```bash
# Who among Luke's parents matches "chosen one prophecy"?
nanograph run --db sw.nano --query search-test/queries.gq --name family_semantic \
  --param 'slug=luke' --param 'q=chosen one prophecy'
# → Anakin (his bio mentions the Chosen One prophecy)

# Who among Luke's parents matches "politician"?
nanograph run --db sw.nano --query search-test/queries.gq --name family_semantic \
  --param 'slug=luke' --param 'q=politician'
# → Padmé (senator, diplomat, Galactic Senate)
```

The graph traversal (`hasParent`) narrows the candidate set first, then `nearest()` ranks within that set. This is fundamentally different from searching all characters — you're searching within a relationship context.

**Why this matters**: In a large graph with thousands of characters, you don't want to rank everyone. You want to ask "among this character's family/colleagues/mentors, who is most relevant to X?" Graph edges define the context; search ranks within it.

### Traversal + BM25 ranking

Find students of a mentor, ranked by bio relevance:

```graphql
query students_search($mentor: String, $q: String) {
    match {
        $m: Character { slug: $mentor }
        $s hasMentor $m
    }
    return { $s.slug, $s.name, bm25($s.bio, $q) as score }
    order { bm25($s.bio, $q) desc }
    limit 5
}
```

```bash
nanograph run --db sw.nano --query search-test/queries.gq --name students_search \
  --param 'mentor=obi-wan' --param 'q=dark side'
```

### Pure graph traversal

No search predicates — just follow edges:

```graphql
query mentors_of($slug: String) {
    match {
        $c: Character { slug: $slug }
        $c hasMentor $m
    }
    return { $m.slug, $m.name }
    order { $m.slug asc }
}
```

```bash
nanograph run --db sw.nano --query search-test/queries.gq --name mentors_of --param 'slug=luke'
# → Obi-Wan, Yoda
```

### Cross-type traversal

Traverse from characters to films:

```graphql
query debut_film($slug: String) {
    match {
        $c: Character { slug: $slug }
        $c debutsIn $f
    }
    return { $f.slug, $f.title, $f.release_year }
}
```

```bash
nanograph run --db sw.nano --query search-test/queries.gq --name debut_film --param 'slug=anakin'
# → The Phantom Menace, 1999
```

### Reverse traversal

Find all characters who debuted in a given film:

```graphql
query same_debut($film: String) {
    match {
        $c: Character
        $c debutsIn $f
        $f: Film { slug: $film }
    }
    return { $c.slug, $c.name }
    order { $c.slug asc }
}
```

```bash
nanograph run --db sw.nano --query search-test/queries.gq --name same_debut --param 'film=a-new-hope'
# → Han, Leia, Luke, Obi-Wan
```

## Embedding workflow

### How `@embed` works

1. Define a `Vector(dim)` property with `@embed(source_prop)` on a node type
2. At `nanograph load` time, the loader reads each row's source property (e.g. `bio`)
3. Sends the text to OpenAI's embedding API (default: `text-embedding-3-small`)
4. Stores the resulting vector in the `embedding` column
5. Vectors are cached in `_embedding_cache.jsonl` — unchanged text is not re-embedded

Mutations (`insert`, `update`) flow through the same embedding materialization pipeline. In current behavior, embeddings are generated when the target vector is missing/null. Updating only the source text does not automatically overwrite an existing vector unless the target embedding field is also cleared or reassigned.

### Chunking for long text

When `NANOGRAPH_EMBED_CHUNK_CHARS` > 0 (e.g. 1500), text longer than that limit is split into overlapping segments:

```
Text (4500 chars), chunk_chars=1500, overlap=200, step=1300:

|---- chunk 1: chars 0–1499 ----|
                    |---- chunk 2: chars 1300–2799 ----|
                                        |---- chunk 3: chars 2600–4099 ----|
                                                            |---- chunk 4: 3900–4500 --|
```

Each chunk is embedded separately, then all chunk vectors are averaged and L2-normalized into a single vector stored on the node.

**Tradeoff**: Chunking is a safety net for text exceeding the model's token limit (~8K tokens for `text-embedding-3-small`). For text under the limit, embedding the full text in one shot produces a better vector than chunk-and-average. Set `NANOGRAPH_EMBED_CHUNK_CHARS=0` (default) unless your data contains very long documents.

### Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `OPENAI_API_KEY` | (unset) | OpenAI API key (required unless mock mode is enabled) |
| `NANOGRAPH_EMBEDDINGS_MOCK` | `0` | Use deterministic local mock embeddings (`1`/`true` disables external API calls) |
| `NANOGRAPH_EMBED_MODEL` | `text-embedding-3-small` | Embedding model |
| `NANOGRAPH_EMBED_BATCH_SIZE` | `64` | Texts per API batch |
| `NANOGRAPH_EMBED_CHUNK_CHARS` | `0` (disabled) | Chunk size for long text |
| `NANOGRAPH_EMBED_CHUNK_OVERLAP_CHARS` | `128` | Overlap between chunks |
| `NANOGRAPH_EMBED_CACHE_MAX_ENTRIES` | `50000` | Max records retained in `_embedding_cache.jsonl` |
| `NANOGRAPH_EMBED_CACHE_LOCK_STALE_SECS` | `60` | Stale-lock timeout for cache file lock recovery |

### Choosing dimensions

Match the `Vector(dim)` to the model's output:
- `text-embedding-3-small` → `Vector(1536)`
- `text-embedding-3-large` → `Vector(3072)`

## Score interpretation cheat sheet

| Function | Range | Ordering | Meaning |
|----------|-------|----------|---------|
| `nearest()` | 0.0 – 2.0 | ascending (lower = better) | Cosine distance |
| `bm25()` | 0.0+ | **descending** (higher = better) | Term relevance score |
| `rrf()` | 0.0 – ~0.03 | **descending** (higher = better) | Fused rank score |
| `search()` | boolean | filter only | Token-based all-terms match (case-insensitive) |
| `fuzzy()` | boolean | filter only | Approximate match |
| `match_text()` | boolean | filter only | Contiguous token match |

## Design patterns

### Filter then rank

Use text predicates in `match` to narrow candidates, then rank with `nearest()` or `bm25()`:

```graphql
query dark_side_semantic($q: String) {
    match {
        $c: Character
        search($c.bio, "dark side")
    }
    return { $c.slug, $c.name }
    order { nearest($c.embedding, $q) }
    limit 5
}
```

### Graph scope then rank

Use edge traversal to define the candidate set, then rank with search:

```graphql
query mentor_students_ranked($mentor: String, $q: String) {
    match {
        $m: Character { slug: $mentor }
        $s hasMentor $m
    }
    return { $s.slug, $s.name }
    order { nearest($s.embedding, $q) }
    limit 5
}
```

This is the key pattern for graph-constrained search: **edges define context, search ranks within it.**

### Debug ranking

Project all scores to understand why results are ranked the way they are:

```graphql
return {
    $c.slug,
    nearest($c.embedding, $q) as vec_dist,
    bm25($c.bio, $q) as bm25,
    rrf(nearest($c.embedding, $q), bm25($c.bio, $q)) as hybrid
}
```

## See also

- [Star Wars Example](starwars-example.md) — worked example with real search output
- [Context Graph Example](context-graph-example.md) — CRM context graph with search
- [Schema Language Reference](schema.md) — `@embed`, `Vector(dim)`, and other annotations
- [Query Language Reference](queries.md) — full query syntax reference

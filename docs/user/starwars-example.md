# Star Wars Example

A worked example using the `search-test/` dataset — 10 characters, 8 films, 26 edges. Demonstrates text search, vector search, hybrid search, and graph-constrained queries with real output.

For the full Star Wars graph (66 nodes, 146 edges, 30 queries), see `examples/starwars/` in the repo.

## Schema

The schema defines two node types and five edge types:

```
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

Key features:
- `@key` on `slug` for merge identity and edge resolution
- `@embed(bio)` auto-generates embeddings from the `bio` text at load time
- `enum()` for constrained categorical values
- Multiple relationship types between Characters

## Data

Each character has a short biography used for text and semantic search. Sample JSONL lines:

```jsonl
{"type": "Character", "data": {"slug": "luke", "name": "Luke Skywalker", "alignment": "light", "era": "original", "bio": "Farm boy from Tatooine who became a Jedi Knight..."}}
{"type": "Film", "data": {"slug": "a-new-hope", "title": "A New Hope", "release_year": 1977}}
{"edge": "HasMentor", "from": "luke", "to": "obi-wan"}
{"edge": "DebutsIn", "from": "luke", "to": "a-new-hope"}
```

Full data: `search-test/data.jsonl`

## Setup

```bash
# Initialize and load
nanograph init sw.nano --schema search-test/schema.pg
nanograph load sw.nano --data search-test/data.jsonl --mode overwrite

# Typecheck queries
nanograph check --db sw.nano --query search-test/queries.gq
```

Embeddings require `OPENAI_API_KEY` for real vectors, or `NANOGRAPH_EMBEDDINGS_MOCK=1` for deterministic mock embeddings (CI/testing).

## Query walkthrough

### Text search: keyword match

Find characters whose bio contains tokens matching "dark side":

```
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

```
+---------+------------------+
| slug    | name             |
+---------+------------------+
| anakin  | Anakin Skywalker |
| han     | Han Solo         |
| leia    | Leia Organa      |
| luke    | Luke Skywalker   |
| maul    | Maul             |
| obi-wan | Obi-Wan Kenobi   |
+---------+------------------+
```

### Text search: fuzzy match

Tolerates typos — matches "Skywalker" even with slight variations:

```bash
nanograph run --db sw.nano --query search-test/queries.gq --name fuzzy_search --param 'q=Skywalker'
```

```
+------------+------------------+
| slug       | name             |
+------------+------------------+
| ahsoka     | Ahsoka Tano      |
| anakin     | Anakin Skywalker |
| din-djarin | Din Djarin       |
| han        | Han Solo         |
| leia       | Leia Organa      |
| luke       | Luke Skywalker   |
| obi-wan    | Obi-Wan Kenobi   |
| padme      | Padmé Amidala    |
| yoda       | Yoda             |
+------------+------------------+
```

### Ranked text search: BM25

BM25 scores rank by term relevance (higher = more relevant):

```bash
nanograph run --db sw.nano --query search-test/queries.gq --name bm25_search --param 'q=clone wars lightsaber'
```

```
+---------+------------------+---------------------+
| slug    | name             | score               |
+---------+------------------+---------------------+
| maul    | Maul             | 2.0280410401645574  |
| yoda    | Yoda             | 0.23748817133389355 |
| ahsoka  | Ahsoka Tano      | 0.22663750001748606 |
| obi-wan | Obi-Wan Kenobi   | 0.22309213435432632 |
| anakin  | Anakin Skywalker | 0.19633594688216696 |
+---------+------------------+---------------------+
```

### Vector search: semantic similarity

`nearest()` returns cosine distance (lower = closer). This query surfaces semantically related characters even when exact keyword overlap is weak:

```bash
nanograph run --db sw.nano --query search-test/queries.gq --name semantic_search --param 'q=who turned evil'
```

```
+------------+----------------+--------------------+
| slug       | name           | score              |
+------------+----------------+--------------------+
| han        | Han Solo       | 0.9980415368698449 |
| ahsoka     | Ahsoka Tano    | 1.0046345156154508 |
| din-djarin | Din Djarin     | 1.0240105612658987 |
| maul       | Maul           | 1.0241794143160183 |
| luke       | Luke Skywalker | 1.027392048768229  |
+------------+----------------+--------------------+
```

### Hybrid search: RRF fusion

Combines vector and BM25 signals. RRF score is higher = better:

```bash
nanograph run --db sw.nano --query search-test/queries.gq --name hybrid_search --param 'q=father and son conflict'
```

```
+------------+------------------+--------------------+--------------------+----------------------+
| slug       | name             | semantic_distance  | lexical_score      | hybrid_score         |
+------------+------------------+--------------------+--------------------+----------------------+
| luke       | Luke Skywalker   | 0.9602065235745608 | 2.525812644333232  | 0.032266458495966696 |
| din-djarin | Din Djarin       | 0.9478181841294104 | 0.9380762831416056 | 0.031754032258064516 |
| anakin     | Anakin Skywalker | 0.9732102863093259 | 1.7514723677174175 | 0.031754032258064516 |
| ahsoka     | Ahsoka Tano      | 0.9444194482165815 | 0.1682143690866992 | 0.03177805800756621  |
| leia       | Leia Organa      | 0.989305766517369  | 0.9542918865678294 | 0.03125763125763126  |
+------------+------------------+--------------------+--------------------+----------------------+
```

Projecting all three scores is useful for debugging ranking behavior.

### Graph traversal: mentors

Pure edge traversal — no search predicates:

```bash
nanograph run --db sw.nano --query search-test/queries.gq --name mentors_of --param 'slug=luke'
```

```
+---------+----------------+
| slug    | name           |
+---------+----------------+
| obi-wan | Obi-Wan Kenobi |
| yoda    | Yoda           |
+---------+----------------+
```

### Graph + vector search: family semantic

Traverse `hasParent` to narrow candidates, then rank by semantic similarity. The graph defines the context; search ranks within it:

```bash
nanograph run --db sw.nano --query search-test/queries.gq --name family_semantic \
  --param 'slug=luke' --param 'q=chosen one prophecy'
```

```
+--------+------------------+
| slug   | name             |
+--------+------------------+
| anakin | Anakin Skywalker |
| padme  | Padmé Amidala    |
+--------+------------------+
```

Anakin ranks first because his bio mentions the Chosen One prophecy.

### Graph + BM25: student search

Mentor's students ranked by bio relevance to "dark side":

```bash
nanograph run --db sw.nano --query search-test/queries.gq --name students_search \
  --param 'mentor=obi-wan' --param 'q=dark side'
```

```
+--------+------------------+--------------------+
| slug   | name             | score              |
+--------+------------------+--------------------+
| anakin | Anakin Skywalker | 0.6406223369077882 |
| luke   | Luke Skywalker   | 0.6343308271593584 |
+--------+------------------+--------------------+
```

### Cross-type traversal: debut film

Traverse from Character to Film:

```bash
nanograph run --db sw.nano --query search-test/queries.gq --name debut_film --param 'slug=anakin'
```

```
+----------------+--------------------+--------------+
| slug           | title              | release_year |
+----------------+--------------------+--------------+
| phantom-menace | The Phantom Menace | 1999         |
+----------------+--------------------+--------------+
```

### Reverse traversal: same debut

Find all characters who debuted in a given film:

```bash
nanograph run --db sw.nano --query search-test/queries.gq --name same_debut --param 'film=a-new-hope'
```

```
+---------+----------------+
| slug    | name           |
+---------+----------------+
| han     | Han Solo       |
| leia    | Leia Organa    |
| luke    | Luke Skywalker |
| obi-wan | Obi-Wan Kenobi |
+---------+----------------+
```

## Key takeaways

- **Graph scope then rank**: Edge traversal narrows the candidate set, then search ranks within it. This is fundamentally different from searching all nodes — you search within a relationship context.
- **Score interpretation**: `nearest()` returns cosine distance (lower = closer, 0.0–2.0). `bm25()` returns term relevance (higher = better). `rrf()` fuses both signals (higher = better).
- **Semantic vs lexical**: Semantic search catches meaning-based matches even without keyword overlap. BM25 catches exact term hits. Hybrid combines both for robustness.

## See also

- [Search Guide](search.md) — full search syntax, embedding workflow, score interpretation
- [Schema Language Reference](schema.md) — types, annotations, `@embed`
- [Query Language Reference](queries.md) — full query syntax

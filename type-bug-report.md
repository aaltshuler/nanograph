# Bug: `check` does not resolve Vector properties

## Summary

`nanograph check` fails to typecheck queries that reference `Vector` fields declared in the schema. The typechecker reports "type X has no property `embedding`" even though the field is defined with `Vector(1536)? @embed(...) @index`.

## Reproduction

```bash
ngdev check --query spec/revops.gq --db omni.nano
```

```
ERROR: query `search_opportunities`: type error: T15: type `Opportunity` has no property `embedding`
ERROR: query `search_signals`: type error: T15: type `Signal` has no property `embedding`
ERROR: query `search_projects`: type error: T15: type `Project` has no property `embedding`
ERROR: query `search_tasks`: type error: T15: type `ActionItem` has no property `embedding`
ERROR: query `search_decisions`: type error: T15: type `Decision` has no property `embedding`
```

## Schema

All five types declare the field correctly in `spec/schema.pg`:

```
node Opportunity {
    ...
    embedding: Vector(1536)? @embed(description) @index
}

node Signal {
    ...
    embedding: Vector(1536)? @embed(summary) @index
}

node Project {
    ...
    embedding: Vector(1536)? @embed(description) @index
}

node ActionItem {
    ...
    embedding: Vector(1536)? @embed(title) @index
}

node Decision {
    ...
    embedding: Vector(1536)? @embed(intent) @index
}
```

## Query syntax

```gq
query search_opportunities($q: String) {
    match { $o: Opportunity }
    return {
        $o.slug
        $o.title
        nearest($o.embedding, $q) as score
    }
    order { nearest($o.embedding, $q) }
    limit 10
}
```

## Behavior

- **`nanograph run`** (stable): queries execute correctly at runtime
- **`ngdev check`** (dev): typechecker does not include `Vector` fields in the type's property map, so `$o.embedding` fails resolution

## Expected

`check` should recognize `Vector(N)` fields as valid properties when used inside `nearest()`.

## Workaround

None — the errors are harmless since runtime execution works. All 32 non-search queries pass typecheck cleanly.

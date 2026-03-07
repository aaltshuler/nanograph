# nanograph export: edges use internal IDs instead of slugs

## Version
nanograph 0.9.0 (dev build)

## Problem
`nanograph export --format jsonl` emits edge records with numeric internal IDs for `src`/`dst`/`from`/`to` fields instead of slug-based references.

Exported edge example:
```json
{"data":{},"dst":41,"edge":"MadeBy","from":"116","id":0,"src":116,"to":"Andrew"}
```

Expected (portable) format:
```json
{"edge":"MadeBy","from":"dec-build-mcp","to":"act-andrew"}
```

## Impact
Exported JSONL cannot be loaded into a different database — `nanograph load` fails with:
```
storage error: node not found by @key: ActionItem.slug=98
```
because internal IDs are database-specific and don't match across instances.

## Workaround
Manually convert edges to slug-based references using the node ID→slug mapping from the same export. See `scripts/nano-convert.py`.

## Also noted
- Edge properties require a `"data": {}` wrapper in 0.9.0 (not needed in 0.8.x)
- Node records also require a `"data": {}` wrapper in 0.9.0

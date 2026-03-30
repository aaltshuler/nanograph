---
title: Blobs and Media Nodes
slug: blobs
---

# Blobs and Media Nodes

nanograph does not store raw blob bytes inside `.nano/`.

Instead, media is modeled as normal nodes with external URIs. The database stores:

- the media URI
- the mime type
- optional derived metadata
- optional vector fields that you generate through nanograph or manage externally

This keeps `.nano/` small and Git-friendly while still allowing graph traversal from media nodes and optional vector search when you have vectors available.

## Storage model

Use media nodes, not `Blob` properties.

Example:

```graphql
node Product {
    slug: String @key
    name: String
}

node PhotoAsset {
    slug: String @key
    uri: String @media_uri(mime)
    mime: String
    width: I32?
    height: I32?
}

edge HasPhoto: Product -> PhotoAsset
```

In this model:

- `uri` points to the external media asset
- `mime` stores the media type
- edges connect domain entities to media assets

nanograph never writes media bytes into `nodes/*` or `edges/*` Lance datasets.

## How nanograph stores media

`@media_uri(mime_prop)` marks a `String` property as an external media URI and names the sibling mime field.

Example:

```graphql
uri: String @media_uri(mime)
mime: String
```

Rules:

- `@media_uri(...)` is valid only on `String` or `String?`
- the annotation argument must name a sibling `String` or `String?` property
- plain strings are rejected during load for `@media_uri` fields
- the value must come from one of nanograph's media input formats

Supported load formats:

- `@file:path/to/image.jpg`
- `@base64:...`
- `@uri:file:///absolute/path/image.jpg`
- `@uri:https://example.com/image.jpg`
- `@uri:s3://bucket/path/image.jpg`

Behavior:

- `@file:` reads a file and imports it into nanograph's media root
- `@base64:` decodes bytes and imports them into the media root
- `@uri:` stores the URI directly without copying

When nanograph imports bytes from `@file:` or `@base64:`, it writes them outside the database under the media root and stores the final `file://...` URI in the node.

By default the media root is:

```text
<db-parent>/media/
```

You can override it with:

```bash
NANOGRAPH_MEDIA_ROOT=/absolute/path/to/media
```

Imported files are content-addressed and grouped by node type, for example:

```text
media/
  photoasset/
    4c8f...e2a1.jpg
```

Relative `@file:` paths are only supported when loading from a file on disk, because nanograph resolves them relative to the source JSONL file.

## Vectors on media nodes

Schema allows `@embed(source_prop)` to reference a `@media_uri(...)` property, but storage support and embedding support are different things.

If you add a vector field to a media node, you have two options:

- let nanograph populate it through `@embed(uri)` when Gemini is configured
- manage the vector externally and load it like any other `Vector(dim)` property

Example:

```graphql
node PhotoAsset {
    slug: String @key
    uri: String @media_uri(mime)
    mime: String
    embedding: Vector(768)? @embed(uri) @index
}
```

Provider behavior, media limits, and `nanograph embed` guidance live in [embeddings.md](embeddings.md).

## Supported media storage formats

nanograph can normalize and store references for more media types than it can embed. The built-in media loader recognizes common extensions and byte signatures including:

- images: PNG, JPEG, GIF, WEBP
- documents: PDF
- audio: MP3, WAV
- video: MP4, MOV

Important implications:

- `@file:` and `@base64:` work well because nanograph imports them into local `file://` assets
- stored `file://...` and `https://...` media nodes work normally for storage and traversal
- plain `s3://...` URIs can be stored, but they are just references
- if you need vector search on media with OpenAI or with remote URI schemes your provider cannot fetch directly, precompute vectors outside nanograph and load them into a normal `Vector(dim)` property

## Loading media

Example JSONL:

```jsonl
{"type":"PhotoAsset","data":{"slug":"space","uri":"@file:photos/space.jpg"}}
{"type":"PhotoAsset","data":{"slug":"beach","uri":"@uri:https://example.com/beach.jpg","mime":"image/jpeg"}}
{"type":"Product","data":{"slug":"rocket","name":"Rocket Poster"}}
{"type":"HasPhoto","from":"rocket","to":"space"}
```

Overwrite load:

```bash
nanograph load --db app.nano --data data.jsonl --mode overwrite
```

After load:

- the `PhotoAsset.uri` value is a durable external URI
- `PhotoAsset.mime` is filled automatically if possible
- any explicit `PhotoAsset.embedding` value you supplied is stored like any other vector

## Querying media

Because media is modeled as nodes, you query it like any other node type.

If media nodes already have vectors, nearest-neighbor search works like any other vector field:

```graphql
query image_search($q: String) {
    match { $img: PhotoAsset }
    return {
        $img.slug as slug,
        $img.uri as uri,
        $img.mime as mime,
        nearest($img.embedding, $q) as score
    }
    order { nearest($img.embedding, $q) }
    limit 5
}
```

Traverse from matched images to related graph entities:

```graphql
query products_from_image_search($q: String) {
    match {
        $product: Product
        $product hasPhoto $img
    }
    return {
        $product.slug as product,
        $product.name as name,
        $img.slug as image,
        $img.uri as uri
    }
    order { nearest($img.embedding, $q) }
    limit 5
}
```

This is the intended media-node workflow in nanograph today:

1. store media as nodes with external URIs
2. optionally populate vectors through [embeddings.md](embeddings.md) or an external pipeline
3. issue a text query or vector query
4. rank media nodes with `nearest(...)`
5. traverse from those media nodes into the rest of the graph

## Operational notes

- nanograph stores references and derived vectors, not blob payloads, in `.nano/`
- imported media files live under the media root, not under `nodes/` or `edges/`
- if you keep the media root inside your repo, add it to `.gitignore`
- `nanograph export` preserves URIs by default; it does not silently copy external assets

## Working with external media vectors

External vectors are still useful when:

- you want OpenAI embeddings for text but a separate pipeline for media
- your media assets are too large or use a URI scheme you do not want nanograph to pass through directly
- you already have an existing multimodal embedding pipeline

In those cases:

1. store media as `@media_uri(...)` nodes
2. generate media vectors in an external pipeline
3. load those vectors into a normal `Vector(dim)` property
4. query with `nearest(...)` as usual

Normal re-embed guidance still applies: switching embedding providers or models requires recomputing existing vectors so they remain comparable.

## See also

- [schema.md](schema.md)
- [search.md](search.md)
- [embeddings.md](embeddings.md)
- [config.md](config.md)

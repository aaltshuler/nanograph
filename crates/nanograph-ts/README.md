# nanograph TypeScript SDK

`nanograph-db` is the first-party Node/TypeScript SDK for nanograph.

## Media nodes and external assets

nanograph stores media as external URIs, not blob bytes inside `.nano/`.

The SDK supports media-node storage helpers today. OpenAI embeddings remain text-only, while Gemini can embed text and media sources through nanograph's `@embed(...)` workflow.

Typical schema:

```graphql
node PhotoAsset {
  slug: String @key
  uri: String @media_uri(mime)
  mime: String
}

node Product {
  slug: String @key
  name: String
}

edge HasPhoto: Product -> PhotoAsset
```

## Example

```ts
import { Database, mediaFile, mediaUri } from "nanograph-db";

const schema = `
node PhotoAsset {
  slug: String @key
  uri: String @media_uri(mime)
  mime: String
}

node Product {
  slug: String @key
  name: String
}

edge HasPhoto: Product -> PhotoAsset
`;

const queries = `
query products_from_image_search($q: String) {
  match {
    $product: Product
    $product hasPhoto $img
  }
  return { $product.slug as product, $img.slug as image, $img.uri as uri }
}
`;

const db = await Database.init("app.nano", schema);

await db.loadRows(
  [
    {
      type: "PhotoAsset",
      data: {
        slug: "space",
        uri: mediaFile("/absolute/path/space.jpg", "image/jpeg"),
      },
    },
    {
      type: "Product",
      data: { slug: "rocket", name: "Rocket Poster" },
    },
    {
      edge: "HasPhoto",
      from: "rocket",
      to: "space",
    },
  ],
  "overwrite",
);

const rows = await db.run(queries, "products_from_image_search", { q: "space scene" });
```

## Notes

- `loadRows()` is a helper over normal JSONL load semantics
- `mediaFile(...)`, `mediaBase64(...)`, and `mediaUri(...)` serialize to nanograph's media source forms
- `describe()` includes `mediaMimeProp` for `@media_uri(...)` properties
- `embed()` uses the same provider/env setup as the CLI
- `changes()` returns the lineage-native CDC contract for new `NamespaceLineage` graphs
- OpenAI is text-only; use Gemini for built-in media embeddings

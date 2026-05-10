# Vector + filter + embedder demo

A 5-scene walkthrough of bw's vector search using Turkish food and city
notes (`pastırmalı kuru fasulye`, `Hatay biberli ekmek`, `baklava`,
`Hatay`, `Antalya`, çay kültürü). Shows everything the markdown demo
doesn't:

- the `vector` struct tag,
- a `WithEmbedder` hook that fills in vectors automatically,
- a `query.Query` filter combined with vector search,
- updates flowing through embedder + HNSW graph re-wiring,
- backup / restore portability across directories.

> ## A note on the embedder
>
> This demo uses a deliberately **fake** embedder: a 64-dim hash-of-
> tokens "bag of words" projection. It is deterministic and has no
> external dependency, which keeps the example self-contained, but it
> has no semantic understanding. So `pastırma` and `pastırmalı` land
> in different hash buckets, `et` and `kuru fasulye` are unrelated
> from the model's point of view, and the rankings reflect token
> overlap rather than meaning.
>
> The point of this example is the bw plumbing — `WithEmbedder`, the
> `vector` tag, atomic update via HNSW re-wiring, portable Backup /
> Restore. Plug in a real embedding model (Ollama, OpenAI, Cohere,
> a local ONNX model, …) and the same code produces semantically
> meaningful rankings.

## Run

From the repo root:

```bash
go run ./example/vector
```

## Scenes

1. **Bulk insert.** Six Turkish-language markdown notes are loaded and
   inserted with `Embed` left empty. The `WithEmbedder` hook produces
   a 64-dim hash-of-tokens "bag of words" vector per record. The
   embedder is fake but deterministic; in a real app you'd swap it
   for an OpenAI / Cohere / local model call inside the same hook.
2. **Pure vector search.** A query about Turkish food ranks `baklava`
   first, with `Hatay biberli ekmek` close behind. The two city notes
   land lower because at this stage they describe geography and
   history rather than recipes.
3. **Filter.** A `query.Query` for `category=sehir` restricts results
   to city notes. The vector pass still scores them — the filter
   only narrows the candidate set, so `Antalya` and `Hatay` are
   ranked by how well each matches the query (Mediterranean / antique
   ruins / historical streets).
4. **Rewrite + re-insert.** `sehir-hatay.md` is rewritten to emphasise
   Hatay's food scene (`pastırma, baklava, fıstık, biberli ekmek,
   künefe, oruk, humus, kâğıt kebabı`). After re-insert the same
   scene-2 query promotes `Hatay` from fourth place to second.
   Behind the scenes the embedder produced a new vector, the HNSW
   graph dropped the old neighbour edges and built new ones, all
   inside a single Badger transaction.
5. **Backup → fresh dir → Restore.** A backup is taken, a brand-new
   database is opened in a fresh temp directory, the backup is
   restored, and the same query runs against it. Top hits are
   identical to scene 4 — the vector index, HNSW graph and FTS
   posting lists are all just Badger keys, so nothing has to be
   rebuilt on the receiving side.

## Plugging in a real embedder

`WithEmbedder` takes any `func(ctx, *T) ([]float32, error)`. For an
OpenAI embedding it might be:

```go
bw.WithEmbedder[Note](func(ctx context.Context, n *Note) ([]float32, error) {
    resp, err := openaiClient.Embeddings.New(ctx, openai.EmbeddingNewParams{
        Model: "text-embedding-3-small",
        Input: openai.F[openai.EmbeddingNewParamsInputUnion](
            shared.UnionString(n.Title + "\n" + n.Body),
        ),
    })
    if err != nil {
        return nil, err
    }
    return floatToFloat32(resp.Data[0].Embedding), nil
})
```

The vector tag's `dim=` must match what the model returns
(`text-embedding-3-small` → 1536, Cohere `embed-english-v3.0` →
1024, local `all-MiniLM-L6-v2` → 384). bw locks the dimension into
the bucket manifest on the first insert so a mistake later raises
`ErrDimMismatch` instead of silently corrupting the index.

## Why filter is fast

`Category` is tagged `index`, so the bw query planner uses the
secondary index to gather matching pks before any vector distance
is computed. Vector search runs only over the filter's survivors —
the cost is dominated by the filter selectivity, not the corpus
size.

# atlas

2D semantic map of the document index. each document is a point on a canvas, positioned by semantic similarity and colored by platform.

**live:** [pub-search.waow.tech/atlas](https://pub-search.waow.tech/atlas)

## data pipeline

`scripts/build-atlas` is a batch python script (uv inline dependencies) that:

1. **exports vectors** from turbopuffer — paginated query with `rank_by: ["id", "asc"]`, 10k rows/page, fetches all vectors + metadata. the live counts are rendered in the atlas footer; don't hardcode them here.
2. **PCA 1024 → 50** — denoising pass (~50% variance explained at current corpus size)
3. **UMAP 50 → 2** — cosine metric, `n_neighbors=15`, `min_dist=0.1`, `random_state=42`; coords normalized to [-1, 1]. **display only.**
4. **UMAP 50 → 10** — a second, separate fit (`n_neighbors=30`, `min_dist=0`) used **only** for clustering
5. **HDBSCAN** at two granularities, fit on the **10D** space:
   - coarse: `min_cluster_size=50`, `min_samples=10` (zoomed-out "regions")
   - fine: `min_cluster_size=20`, `min_samples=3` (zoomed-in clusters)
   - both thresholds are absolute, not corpus-scaled — cluster granularity therefore drifts as the corpus grows
   - outliers snapped to the nearest **10D** centroid; label centroids are then recomputed in 2D, since that's where labels are drawn
   - each fine cluster is mapped to a parent coarse cluster by majority vote

   > **why 10D and not the 2D coords** — clustering in the display projection turns UMAP's own artifacts (tearing, crowding) into cluster boundaries. Scored against PCA-50 cosine space, which neither projection was fit in, the old 2D clustering gave silhouette **+0.004 coarse / −0.067 fine** — the fine tier was structured *worse than chance*. The 10D space gives **+0.033 / +0.017**. `cluster_selection_method="leaf"` and a corpus-scaled `min_cluster_size` were tested at the same time and both made it worse at our thresholds; neither was adopted.
6. **labels** — c-TF-IDF over document titles per cluster → 3-term keyword seed, then refined into 2-4 word topic names by `claude-haiku-4-5`. **evidence is core members only** — the ~40% of points HDBSCAN calls noise are snapped to a cluster for *display*, but including their titles blurs the vocabulary the label is drawn from (median 36% coarse / 42% fine of each cluster's titles). core-only evidence changes 61% of coarse and 70% of fine labels. a cluster whose core members all have empty titles falls back to full membership (`llm_refine_labels`, batched + async, both tiers). Requires `ANTHROPIC_API_KEY`; without it the c-TF-IDF keywords ship as-is, and any cluster the LLM fails to name falls back to its keyword label.
7. **publication centroids** — documents grouped by `basePath` (2+ docs), enriched from turso with name/coverImage, plus author avatars and leaflet theme colors. Both use on-disk caches in `site/` (`atlas-avatar-cache.json`, `atlas-theme-cache.json`) that deploy alongside `atlas.json` — the prefect flow clones fresh each run, so the deployed copy is what it reads on cold start.
8. **outputs** `site/atlas.json` (~15MB at ~53k docs, gitignored)

dependencies: `umap-learn`, `hdbscan`, `scikit-learn`, `httpx`, `numpy`, `pydantic-settings`, `anthropic`. Pinned to `numpy<2.2` and python `>=3.12,<3.14` — umap's transitive numba/llvmlite have no wheels outside that window.

```bash
./scripts/build-atlas              # writes site/atlas.json
./scripts/build-atlas -o out.json  # custom output path
```

## frontend

`site/atlas.html` + `site/atlas.js` + `site/atlas.css`

- **canvas 2D** renderer — no libraries, sprite-based (pre-rendered offscreen canvas per platform); WebGL only for the rotating document planets at deep zoom
- **pan/zoom** via wheel, drag, touch/pinch (max 500×; documents become rotating planets past ~45×, then flat cards)
- **semantic zoom**: coarse labels → fine labels → document titles as you zoom in
- **cluster nebulae as lanterns**: one smooth-falloff glow per fine cluster at the weighted center of its members, sized by RMS spread with a bounded peak opacity — wide, translucent, and smooth at every zoom (coarse regions use the same sprite, fading out by ~2.8×)
- **label economy**: all text competes in one collision pass, placed in priority order — cluster labels (bold landmarks), then document titles ranked by real recommend counts (`/recommended` boost on `popScore`), then publication names with whatever room is left; per-layer caps live in `ATLAS_TUNE.labels`
- **hover/selection card** with title, publication, platform; **click** opens the document
- **theme support**: dark (default), light, system — synced with the rest of the site

## recomputing

automated: the `leaflet-atlas` prefect deployment (`my-prefect-server/flows/atlas.py:rebuild_atlas`) runs **every 6 hours** on heavypad — clones the repo, runs `build-atlas`, runs `build-facts` (best-effort; a failure deploys the committed `facts.json` rather than blocking), and deploys `site/` to Cloudflare Pages. Pinned to python 3.13 and single-threaded BLAS/numba, since the build OOM'd the pod as the corpus grew.

trigger a rebuild now (against prefect-server.waow.tech, tailnet):

```bash
prefect deployment run 'leaflet-atlas/leaflet-atlas' --watch
```

### ⚠️ the flow deploys data, NOT frontend changes

`flows/atlas.py:deploy_to_pages` calls `wrangler pages deploy` **directly** — it does not regenerate the workbox service worker. That's fine for `atlas.json`, which `workbox-config.cjs` serves via a `StaleWhileRevalidate` runtime cache rather than precaching. But `*.js` **is** precached with a content hash baked into `sw.js`, so:

- changed **`atlas.json` only** → the flow is sufficient
- changed **`atlas.js` / any `site/*.js`** → you must run `site/deploy.sh` (regenerates `sw.js`), or returning visitors keep the old script forever

`deploy.sh` ships whatever `site/atlas.json` is on disk, and that file is gitignored — so pull the live one down first or you'll overwrite good data with a stale local build:

```bash
curl -sfo site/atlas.json https://pub-search.waow.tech/atlas.json
cd site && ./deploy.sh
```

## future work

- **outlier fraction**: HDBSCAN calls ~40% of points noise and `assign_outliers` snaps them to a region for display. A `min_samples` sweep (1→25) shows the noise rate never leaves 36-44%, so it's real structure, not a tuning artifact — core points carry median membership probability 0.97-0.99. Labels no longer use these points, but two consumers still do: per-cluster `count` in `atlas.json` is inflated by the snap rate, and the fine-cluster lantern centers/spreads are computed from post-snap membership. Emitting a core/snapped bit per point would let the frontend fix both.
- **exemplar-seeded labels**: feed the LLM the N documents nearest each centroid instead of c-TF-IDF keywords (sembleverse does this) — untested here
- **hierarchical clustering**: replace the two-strata (coarse/fine) approach with a proper hierarchy (Ward linkage on HDBSCAN centroids + `cut_tree` at multiple levels) for smooth fractal zoom
- **event-driven rebuild**: trigger off significant index changes instead of the fixed 6h cron

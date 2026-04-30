# CLAUDE.md

## Verification Over Recall

- Never rely on remembered knowledge for libraries, APIs, or frameworks —
  check installed versions and docs before writing code or making claims
- If you lack verified information, acknowledge uncertainty and investigate
  first rather than speculate

## Collaboration Style

- Be objective, not agreeable — act as a partner, not a sycophant
- Push back when you disagree, flag tradeoffs honestly, don't sugarcoat problems
- Keep explanations brief and to the point
- Accuracy over speed

## Commands

```bash
npm run dev       # Start dev server (localhost:4321)
npm run build     # Production build
npm run preview   # Preview production build
npm run check     # Astro + TypeScript type check
```

## Architecture

**Edge Extender** is a browser-only geospatial tool that extends polygon boundaries using Voronoi diagrams. All processing runs client-side via WebAssembly — no data leaves the browser.

**Stack:** Astro 6 (static site) + Svelte 5 (interactive islands) + DuckDB WASM (spatial SQL engine) + MapLibre GL (map rendering)

### Data flow

```
File drop → format detection + ZIP extraction (DropZone)
  → DuckDB registers file buffer (loader.ts)
  → 5-stage SQL pipeline (src/lib/pipeline/):
      layer_01: load & normalize geometry (MakeValid, Transform to EPSG:4326, Force2D)
      layer_02: extract boundaries (ST_Boundary, ST_LineMerge, minus neighbor overlap)
      layer_03: interpolate points along boundaries (ST_LineInterpolatePoints, no endpoints)
      layer_04: generate Voronoi diagram (ST_VoronoiDiagram, ST_Intersects point→cell assignment)
      layer_05: merge cells (ST_Node all boundaries, ST_Polygonize, point-in-polygon reassignment)
  → export GeoJSON → MapLibre renders result
```

### Key design decisions

- **All geospatial logic is SQL.** Complex operations (Voronoi, ST_Node, ST_Polygonize, RTREE index) run inside DuckDB's spatial extension, not JavaScript.
- **DuckDB WASM constraints:** single-threaded (`SET threads = 1`), vite optimization excluded. The `duckdb.svelte.ts` singleton initializes the spatial extension and handles connection lifecycle.
- **Retry on failure:** `pipeline/index.ts` automatically retries with doubled point-spacing distance when Voronoi generation fails (memory/precision issues).
- **COEP/COOP headers required:** `SharedArrayBuffer` (needed by DuckDB) requires `Cross-Origin-Opener-Policy: same-origin` + `Cross-Origin-Embedder-Policy: require-corp`. These are set in `astro.config.mjs` (dev server) and `public/_headers` (Netlify/Cloudflare deploy).
- **Svelte 5 runes:** Uses `$state()`, `$effect()`, and `untrack()` — not legacy Svelte reactivity.
- **Path alias:** `$lib` resolves to `src/lib/` (configured in `astro.config.mjs` vite alias).

### Reference Docs

- `docs/performance.md` — WASM memory model, SPATIAL_JOIN behaviour differences vs. Linux, lines stage evolution, connection settings, pipeline phase memory profile

### Supported input formats

GeoJSON, GeoParquet, GeoPackage, Shapefile (zip), KML, GML, GPX. Loader detects format, extracts from ZIPs via `fflate`, and registers buffers with DuckDB. GeoParquet is loaded without `ST_Read` (no geometry tag); all others use `ST_Read`.

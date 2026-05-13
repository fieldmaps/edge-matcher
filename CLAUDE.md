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

## Solution Choice

- Optimise for the best long-term answer, not the smallest delta from the
  current state. "Minimal change" is expedient, not principled — don't default
  to it. If you're proposing it for scope reasons, say so explicitly.
- When two clean endpoints exist (all-A or all-B), lead with the better one.
  Don't propose hybrids that split the difference unless the hybrid is
  genuinely better than both endpoints — mixed states usually carry the
  costs of both options without the full benefits of either.
- When recommending an approach, name the endpoint you'd actually pick and
  why, before listing alternatives.

## Commands

```bash
npm run dev       # Start dev server (localhost:4321)
npm run build     # Production build
npm run preview   # Preview production build
npm run check     # Astro + TypeScript type check
```

## UI Verification

**Always verify UI changes in a real browser before reporting them done.** Type-checking and build success do not prove that a page renders, that an island hydrates, or that user flows work. Use `playwright-cli` (installed at `/opt/homebrew/bin/playwright-cli`) — a terminal wrapper around the Playwright MCP server, drivable from Bash.

Minimal flow:

```bash
npm run dev > /tmp/dev.log 2>&1 &      # background dev server
sleep 4 && grep -E "Local" /tmp/dev.log # find the port (may not be 4321 if busy)
playwright-cli open http://localhost:<port>/
playwright-cli snapshot                  # accessibility-tree snapshot of current page
playwright-cli eval "() => ({ ... })"    # arbitrary JS in page context
playwright-cli click <ref>               # ref comes from snapshot
playwright-cli dialog-accept             # or dialog-dismiss for window.confirm()
playwright-cli close                     # always close at the end
```

Notes:

- `playwright-cli snapshot` writes a YAML accessibility tree to `.playwright-cli/page-*.yml`. The tree includes element `ref` IDs (e.g. `ref=e9`) you pass to `click`/`hover`/`fill`. Trust `eval` output over older snapshot files — snapshots can be stale relative to the live DOM.
- `playwright-cli` uses one persistent browser per session. Use `attach`/`detach` only if you need to share a browser across processes; for our verification flows, `open` + `close` is enough.
- Do NOT write standalone `.spec.ts` files for this — drive Playwright from Bash via `playwright-cli`. (Reason: the project does not have a Playwright test runner configured, and per-test setup duplicates the dev-server lifecycle.)
- For PWA / offline behaviour, `caches.keys()`, `navigator.serviceWorker.getRegistrations()`, and `localStorage` are all reachable from `eval` for direct state inspection.

## Architecture

**Topology Tools** is a browser-only suite of geospatial topology utilities. Each tool runs client-side via WebAssembly — no data leaves the browser. The root `/` is a landing page that lists tools; today there is one — Edge Extender at `/extend`, which extends polygon boundaries using Voronoi diagrams.

**Stack:** Astro 6 (static site) + Svelte 5 (interactive islands) + DuckDB WASM (spatial SQL engine) + MapLibre GL (map rendering)

### Data flow

```
File drop → format detection + ZIP extraction (DropZone)
  → DuckDB registers file buffer (loader.ts)
  → 5-stage SQL pipeline (src/lib/tools/edge-extender/pipeline/):
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
- **Retry on failure:** `src/lib/tools/edge-extender/pipeline/index.ts` automatically retries with doubled point-spacing distance when Voronoi generation fails (memory/precision issues).
- **Tool layout convention:** Each tool lives at `src/lib/tools/<slug>/` (its `App.svelte` + a `pipeline/` directory if it has one) and has a route at `src/pages/<slug>.astro`. Shared infrastructure stays in `src/lib/db/` (DuckDB singleton + loader + export) and `src/lib/components/` (DropZone, MapView, DownloadMenu, OfflineToggle, ToolCard). Adding a tool = new folder under `tools/`, new entry in `src/lib/tools.ts`, new page, optionally an icon under `public/icons/tools/`. No infrastructure changes.
- **No COEP/COOP.** The mvp/eh DuckDB variants are single-threaded and don't need SharedArrayBuffer (and we exclude the coi variant anyway — it breaks OPFS FSAH clones). Dropping COEP `require-corp` is what lets us fetch the spatial extension cross-origin from `extensions.duckdb.org` (which doesn't send CORP). Don't re-add COEP unless you've audited the cross-origin fetches it would block.
- **Svelte 5 runes:** Uses `$state()`, `$effect()`, and `untrack()` — not legacy Svelte reactivity.
- **Path alias:** `$lib` resolves to `src/lib/` (configured in both the Vite alias in `astro.config.mjs` and the `paths` map in `tsconfig.json` so Astro/TS check sees it too).

### Reference Docs

- `docs/performance.md` — WASM memory model, SPATIAL_JOIN behaviour differences vs. Linux, lines stage evolution, connection settings, pipeline phase memory profile

### Supported input formats

GeoJSON, GeoParquet, GeoPackage, Shapefile (zip), KML, GML, GPX. Loader detects format, extracts from ZIPs via `fflate`, and registers buffers with DuckDB. GeoParquet is loaded without `ST_Read` (no geometry tag); all others use `ST_Read`.

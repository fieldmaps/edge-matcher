# Performance Notes

Memory constraints and findings for DuckDB-WASM in the browser. The sister Python pipeline (`edge-extender`) has its own server-side notes; this file covers behaviour that differs in the WASM context. Algorithmic structure and SQL are ported verbatim from edge-extender — see that repo's `docs/performance.md` for profiling history.

---

## WASM memory model vs. Linux

DuckDB on Linux uses virtual memory. The SPATIAL_JOIN operator pre-allocates ~1× physical RAM as a spill reservation — a virtual address claim with no physical pages mapped. Setting `memory_limit = '999GB'` exceeds the reservation threshold and lets the query proceed cheaply.

**In WASM this does not work.** WebAssembly has no virtual memory overcommit. Every `memory.grow()` call allocates real physical pages immediately. Setting `memory_limit = '999GB'` causes DuckDB to attempt a real multi-gigabyte allocation, which fails with `"Allocation failure"` rather than the budgeted `"failed to allocate data of size X MiB (Y GiB/Y GiB used)"`.

### Reading error messages

| Message | Meaning |
| ------- | ------- |
| `"failed to allocate data of size X MiB (Y GiB/Y GiB used)"` | DuckDB's budget manager rejected the allocation. Y equals `memory_limit` exactly. Actual data in memory may be much smaller — this is often the SPATIAL_JOIN reservation bug, not real data pressure. |
| `"Allocation failure"` | WASM `memory.grow()` failed. The physical heap is genuinely exhausted. No budget trick helps; the query must use less memory. |

---

## SPATIAL_JOIN operator

**What triggers it:** any `ST_Intersects`, `ST_Within`, or `ST_Contains` predicate in a JOIN ON clause — including LATERAL subqueries and correlated `EXISTS` / `NOT EXISTS` subqueries with a spatial predicate. DuckDB's optimiser rewrites these to `SPATIAL_JOIN`.

**WASM consequence:** because the reservation maps real pages, queries with SPATIAL_JOIN OOM immediately on memory-constrained devices, even when the actual data being joined is tiny.

### Patterns that work in WASM

- **Aggregate over one table** with no join (`ST_Union_Agg`, `ST_Node`, `ST_VoronoiDiagram`).
- **`CROSS JOIN` against a single-row intermediate table** — the optimiser sees it as a nested loop, not a spatial join. Useful for "subtract this one global geometry from each row" patterns.
- **Per-row scalar spatial functions** against a scalar geometry value (`ST_Intersection`, `ST_Difference`, `ST_Boundary`).
- **Bbox-prefiltered self-join.** Replace `JOIN b ON ST_Intersects(a.geom, b.geom)` with explicit scalar bbox-overlap predicates: `ST_XMax(b) >= ST_XMin(a) AND ST_XMin(b) <= ST_XMax(a) AND ST_YMax(b) >= ST_YMin(a) AND ST_YMin(b) <= ST_YMax(a)`. DuckDB plans this as `PIECEWISE_MERGE_JOIN` (range join), not `SPATIAL_JOIN`.
- **Bbox-prefiltered point-in-polygon.** For `JOIN ... ON ST_Within(p.pt, c.geom)`, add `ST_X(p.pt) >= ST_XMin(c.geom) AND ... AND ST_Within(p.pt, c.geom)`. The bbox predicates are necessary conditions for `ST_Within`, so semantics are preserved; the planner uses them as the join keys and `ST_Within` becomes a residual `FILTER`. Same for `NOT EXISTS` correlated subqueries.

These bbox-prefilter patterns are what make most of the pipeline WASM-safe without `memory_limit` overrides. They were profiled in edge-extender as identical-output and faster than the LATERAL+`ST_Intersects` forms they replaced.

---

## The remaining WASM-only adaptation: `voronoi.ts` `_04_tmp2`

The Voronoi cell-to-fid assignment uses `JOIN ... ON ST_Intersects(a.geom, b.geom)` (point × cell) and cannot use a bbox prefilter without changing semantics — generators that land exactly on a cell boundary must intersect both adjacent cells, and the bbox prefilter would still trigger `SPATIAL_JOIN` because the predicate is `ST_Intersects` rather than `ST_Within`.

The mitigation: wrap the join with `SET memory_limit = '999GB'` + an RTREE index, then restore the original limit. This is the one place the WASM SPATIAL_JOIN reservation cannot be avoided structurally. It works because the joined tables are small (bounded by `MAX_POINTS = 10M` generators × roughly equal cell count) and the reservation never triggers a real allocation past the working set.

The 999GB override is **not** applied anywhere else in the pipeline. `lines.ts` (bbox self-join), `merge.ts` `_05_tmp1` (bbox-prefiltered NOT EXISTS), and `merge.ts` `_05` (bbox-prefiltered LEFT JOIN) all plan as `PIECEWISE_MERGE_JOIN` or `HASH_JOIN` and stay safely within the WASM heap.

---

## Connection settings (`duckdb.svelte.ts`)

| Setting | Effect |
| ------- | ------- |
| `SET threads = 1` | Primary memory dial. In WASM, DuckDB is single-threaded anyway; this makes it explicit and prevents unexpected parallel allocations. |
| `SET preserve_insertion_order = false` | Free win. Removes sequence-tracking overhead from every intermediate buffer and eliminates the reorder pass after aggregations. No correctness impact. |
| `SET geometry_always_xy = true` | Correctness: forces (lon, lat) coordinate order regardless of CRS definition. Required for correct EPSG:4326 output. |
| `memory_limit` | Left at default (80% of device RAM). The only override is the targeted `999GB` workaround in `voronoi.ts` described above. |

---

## Pipeline phase memory profile

| Phase | Module | Memory concern | Notes |
| ----- | ------ | -------------- | ----- |
| Load | `loader.ts` | Low | File buffer registered directly; no copy |
| Lines | `lines.ts` | Medium | Bbox-self-join materializes per-polygon neighbor unions (3–10 geoms each). No global aggregate. |
| Points | `points.ts` | Low–medium | Proportional to interpolated point count. `MAX_POINTS = 10M` enforces a hard cap with retry-and-double-distance fallback. |
| **Voronoi** | `voronoi.ts` | **High** | `ST_VoronoiDiagram(ST_Collect(list(geom)))` materialises entire point cloud in GEOS. `_04_tmp2` join uses the `999GB` override. Retry mechanism doubles spacing until it fits. |
| **Merge** | `merge.ts` | **High** | `ST_Node(ST_Collect(...))` collects all interior + extension boundaries. The corner-snap step (`_05_tmp2`) snaps to a discrete corner set rather than nearest segment — see `merge.ts` for the rationale. Bbox-prefiltered `_05_tmp1` and `_05` joins avoid SPATIAL_JOIN. |
| Export | `index.ts` | Medium | `ST_AsGeoJSON` per row, then JS string concat. Validation checks (overlap / gap / row count) run first; `runValidation` warnings go to console. |

The retry loop in `pipeline/index.ts` (up to 10 attempts, doubling distance each time) is the safety valve for Points and Voronoi OOMs. It only covers stages 3–4; an OOM at lines (stage 2) or merge (stage 5) propagates as an unrecoverable error — the user is expected to fall back to the Python `edge-extender` for inputs that don't fit.

# Performance Notes

Memory constraints and findings for DuckDB-WASM in the browser. The sister Python pipeline (`edge-extender`) has server-side notes; this file covers behaviour that differs in the WASM context.

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

**What triggers it:** any `ST_Intersects`, `ST_Within`, or `ST_Contains` predicate in a JOIN — including LATERAL subqueries. DuckDB's optimiser always rewrites these to `SPATIAL_JOIN`.

**WASM consequence:** because the reservation maps real pages, queries with SPATIAL_JOIN OOM immediately on memory-constrained devices, even when the actual data being joined is tiny.

**Fix:** restructure queries to avoid SPATIAL_JOIN entirely. Patterns that work in WASM:

- Aggregate over one table (`ST_Union_Agg`, `ST_Node`) with no join.
- `CROSS JOIN` against a single-row intermediate table — the optimiser sees it as a nested loop, not a spatial join.
- Per-row scalar spatial functions (`ST_Intersection`, `ST_Difference`, `ST_Boundary`) against a scalar geometry value.

The `memory_limit = '999GB'` workaround from `edge-extender` is applied in `voronoi.ts` and `merge.ts` where the joined tables are small (points against polygons). It should **not** be applied where the joined tables are large, as it removes the only safety valve against genuine WASM heap exhaustion.

---

## Lines stage evolution (`pipeline/lines.ts`)

The lines stage extracts exterior-facing boundary segments per polygon (edges not shared with any neighbour). Three approaches were tried before reaching the current implementation.

### Attempt 1 — LATERAL join (original)

```sql
LEFT JOIN LATERAL (
  SELECT ST_Union_Agg(b.geom) AS neighbor_union
  FROM layer_02_tmp AS b
  WHERE b.fid != a.fid AND ST_Intersects(a.geom, b.geom)
) AS sub ON true
```

**Failure:** `ST_Intersects` in LATERAL triggers SPATIAL_JOIN → `"Allocation failure"` on large datasets.

### Attempt 2 — Bulk pairwise spatial join

```sql
SELECT ST_Intersection(a.geom, b.geom) AS geom
FROM layer_02_tmp AS a
JOIN layer_02_tmp AS b ON a.fid < b.fid AND ST_Intersects(a.geom, b.geom)
```

Still uses `ST_Intersects` in a JOIN → still triggers SPATIAL_JOIN → same failure.

### Attempt 3 — Global union approach

```sql
SELECT ST_Boundary(ST_Union_Agg(geom)) AS geom FROM layer_01
```

No spatial join. But `ST_Union_Agg` on a large complex dataset (e.g. Chile admin3) materialises the entire polygon collection into GEOS at once → `"Allocation failure"`. `ST_CoverageUnion_Agg` has the same problem for large inputs and additionally crashes on invalid geometry.

### Current implementation — plain boundary extraction

```sql
SELECT fid, UNNEST(ST_Dump(ST_Boundary(geom))).geom AS geom FROM layer_01
```

No aggregates, no joins, no GEOS multi-geometry operations. Processes one polygon at a time. Memory usage is O(max single polygon size), not O(dataset size).

**Trade-off:** shared boundary edges appear for both neighbouring polygons, so Voronoi seeds are placed on interior edges too. This is acceptable because:

1. GEOS `ST_VoronoiDiagram` handles coincident seeds (same location, different fid) without error.
2. Both fids intersect the resulting Voronoi cell, so the assignment check (`assignedCount >= ptCount`) passes.
3. In `merge.ts`, original polygon boundaries take priority over Voronoi cells, so interior shared-edge seeds only affect the extension space very close to where a shared boundary meets the exterior — a minor inaccuracy for most datasets.

---

## Connection settings (`duckdb.svelte.ts`)

| Setting | Effect |
| ------- | ------- |
| `SET threads = 1` | Primary memory dial. In WASM, DuckDB is single-threaded anyway; this makes it explicit and prevents unexpected parallel allocations. |
| `SET preserve_insertion_order = false` | Free win. Removes sequence-tracking overhead from every intermediate buffer and eliminates the reorder pass after aggregations. No correctness impact. |
| `SET geometry_always_xy = true` | Correctness: forces (lon, lat) coordinate order regardless of CRS definition. Required for correct EPSG:4326 output. |
| `memory_limit` | Left at default (80% of device RAM). Explicit override only used as a workaround around the SPATIAL_JOIN reservation bug in `voronoi.ts` and `merge.ts`. |

---

## Pipeline phase memory profile

| Phase | Module | Memory concern | Notes |
| ----- | ------ | -------------- | ----- |
| Load | `loader.ts` | Low | File buffer registered directly; no copy |
| Lines | `lines.ts` | **Previously high** | Fixed: now O(single polygon). See above. |
| Points | `points.ts` | Low–medium | Proportional to number of interpolated points |
| **Voronoi** | `voronoi.ts` | **High** | `ST_VoronoiDiagram(ST_Collect(list(geom)))` materialises entire point cloud in GEOS. Retry mechanism doubles spacing until it fits. |
| **Merge** | `merge.ts` | **High** | `ST_Node(ST_Collect(...))` has the same collect-everything pattern for all boundaries. |
| Export | `index.ts` | Medium | `ST_AsGeoJSON` per row, then JS string concat |

The retry loop in `pipeline/index.ts` (up to 10 attempts, doubling distance each time) is the safety valve for Voronoi and Merge OOMs. It only covers stages 3–4; a genuine OOM at merge (stage 5) propagates as an unrecoverable error.

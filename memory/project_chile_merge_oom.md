---
name: Chile merge OOM investigation
description: ST_Node heap exhaustion at stageMerge for large-coastline datasets; root cause analysis and remaining ideas
type: project
---

## Problem

Processing Chile (large, complex coastline) fails with `"Out of Memory Error: Allocation failure"` at the **merge stage** (`stageMerge` in `src/lib/pipeline/merge.ts`). This is a WASM `memory.grow()` failure — physical heap genuinely exhausted, no budget trick (`memory_limit='999GB'`) can help.

The user must give Chile a huge point spacing to get past Voronoi (the retry loop handles that), but then OOMs at merge regardless.

## Root cause

`ST_Node(ST_Collect(list(all_boundaries)))` in `layer_05_pieces` is the failure point. `ST_Node` is a **global operation** — it must load every boundary vertex from both `layer_01` (Chile's complex original polygons) and `layer_04` (Voronoi cells) into GEOS simultaneously to find all intersections. Chile's coastline has an enormous vertex count; even after multiple rounds of simplification this still exhausts the WASM heap.

## What was already changed (commits in session)

1. **Drop `layer_02` and `layer_03` before `stageMerge`** (`src/lib/pipeline/index.ts`) — frees boundary-line and interpolated-point data before the expensive merge. Was wasted memory but not the bottleneck.

2. **Eliminate `ST_Union_Agg` from the noding CTE** (`merge.ts`) — the original query ran `ST_Union_Agg(ST_Boundary(geom))` on both `layer_01` and `layer_04` before feeding results into `ST_Node`. That was three sequential GEOS materializations. Replaced with a single direct `ST_Collect` of raw boundaries → saves two intermediate GEOS allocations but `ST_Node` itself still OOMs.

3. **Escalating simplification fallback on `layer_01` boundaries** (`merge.ts`) — on OOM, retries noding with `ST_SimplifyPreserveTopology` at tolerances `[0, 1e-5, 1e-4, 1e-3]` degrees. Spatial joins afterward still use full `layer_01`/`layer_04` geometry. All four levels still OOM for Chile.

**Why:** - `ST_Node` cannot be chunked. It needs all segments at once to detect every crossing. Simplification helps somewhat but Chile's coastline is so complex that even 1e-3° (~100 m) tolerance produces too many vertices.

## Current state of memory at merge time

After fixes above, tables alive when `stageMerge` runs:

- `layer_01` — full-resolution Chile polygons (large, unavoidable)
- `layer_04` — Voronoi cells, one per fid (simpler geometry)
- `layer_02` / `layer_03` — **now dropped** (no longer wasted)

## Remaining ideas not yet tried

### A — Use `layer_04` directly as fallback output

If `stageMerge` throws, catch in `index.ts` and create `layer_05` from `layer_04` instead:

```sql
CREATE TABLE layer_05 AS SELECT fid, ST_Multi(geom) AS geom FROM layer_04
```

Downside: original polygon boundary shapes aren't preserved exactly (Voronoi boundaries approximate them). But the result is usable and avoids a hard crash.

### B — Per-fid extension without ST_Node

Alternative merge formula:

```
result_fid = ST_Union(orig_polygon, ST_Difference(voronoi_cell, all_orig_union))
```

This avoids ST_Node + ST_Polygonize entirely. The blocker: `all_orig_union = ST_Union_Agg(layer_01)` also OOMs for Chile (same reason lines.ts falls back). Could try chunked progressive union (union first half, union second half, union those two results) — each chunk is smaller, but the final step might still OOM.

### C — Simplify `layer_01` in-place early in the pipeline

Instead of simplifying only for noding (idea 3 above, which we tried), simplify `layer_01` itself after loading, before `stageLines`. Then ALL downstream stages (lines, points, voronoi, merge) use the simplified geometry. Downside: output polygon shapes are visibly simplified.

### D — Geographic subsetting (clip before process)

The clip feature already exists (`src/lib/pipeline/clip.ts`), but it runs _after_ the pipeline. If the user applies a clip region _before_ running the pipeline (i.e., filter `layer_01` to a spatial subset), Chile would be reduced to a manageable region. This is a UX/workflow change, not a code change.

### E — Chunked ST_Node (complex)

Break the boundary collection into geographic tiles, node each tile separately, then combine. Requires handling tile-boundary edge cases (segments that cross tile boundaries). Significant complexity; probably not worth it given other options.

## Key architectural facts

- `"Allocation failure"` = WASM `memory.grow()` failed. Physical heap exhausted. Distinct from DuckDB budget rejections.
- `"failed to allocate data of size X MiB (Y GiB/Y GiB used)"` = DuckDB budget manager. Budget trick helps.
- SPATIAL_JOIN (triggered by `ST_Intersects`/`ST_Within`/`ST_Contains` in a JOIN) pre-allocates ~1× RAM as virtual reservation. In WASM this maps real pages → OOM. Fix: CROSS JOIN against single-row tables, or `memory_limit='999GB'` trick when joined tables are small.
- `ST_Union_Agg` of a large complex polygon collection (e.g., all Chile polygons) OOMs. `lines.ts` already has a try/catch fallback for this.
- `ST_Node` is inherently global; cannot be chunked without complex tile-boundary handling.
- The retry loop in `index.ts` covers stages 3–4 (points + voronoi). Stage 5 (merge) has no retry/fallback — a merge OOM is currently unrecoverable.

**Why:** Documenting for next session so analysis doesn't need to be redone from scratch.
**How to apply:** Resume from "Remaining ideas" section. Option A is simplest to implement; option B is more correct but has the chunked-union complexity; options C/D require workflow changes.

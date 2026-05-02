import type { AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";

const SNAP_TOLERANCE = 1e-8;
const SNAP_DIST = 2 * SNAP_TOLERANCE;

export async function stageMerge(conn: AsyncDuckDBConnection): Promise<void> {
  // _05_tmp1: extension-only Voronoi lines. NOT EXISTS with explicit bbox
  // prefilter avoids SPATIAL_JOIN — planner uses HASH_JOIN + FILTER. Bbox
  // predicates are necessary conditions for ST_Within so semantics unchanged.
  await conn.query(`--sql
    CREATE OR REPLACE TABLE layer_05_tmp1 AS
    WITH
    voronoi_lines AS (SELECT ST_Boundary(geom) AS geom FROM layer_04),
    cut_lines AS (
      SELECT geom FROM (
        SELECT ST_CollectionExtract(ST_Difference(v.geom, c.geom), 2) AS geom
        FROM voronoi_lines v CROSS JOIN layer_03a c
      ) WHERE NOT ST_IsEmpty(geom)
    ),
    sections_pt AS (
      SELECT geom, ST_PointOnSurface(geom) AS pt
      FROM (SELECT UNNEST(ST_Dump(geom)).geom AS geom FROM cut_lines)
    )
    SELECT s.geom
    FROM sections_pt s
    WHERE NOT EXISTS (
      SELECT 1 FROM layer_01 p
      WHERE ST_X(s.pt) >= ST_XMin(p.geom)
        AND ST_X(s.pt) <= ST_XMax(p.geom)
        AND ST_Y(s.pt) >= ST_YMin(p.geom)
        AND ST_Y(s.pt) <= ST_YMax(p.geom)
        AND ST_Within(s.pt, p.geom)
    )
  `);

  // _05_tmp2: snap _05_tmp1 endpoints to discrete _02b corner set. GEOS
  // ST_Difference (in _05_tmp1) drifts ~1e-7° from original vertices. Snapping
  // to nearest segment via ST_ClosestPoint can land just past a corner,
  // leaving a sub-nanodegree gap that fuses neighbours in ST_Polygonize.
  // Snapping to a discrete corner set guarantees convergence at exact corners.
  await conn.query(`--sql
    CREATE OR REPLACE TABLE layer_05_tmp2 AS
    WITH
    corners AS (
      SELECT DISTINCT pt FROM (
        SELECT ST_StartPoint(geom) AS pt FROM layer_02b
        UNION ALL
        SELECT ST_EndPoint(geom) FROM layer_02b
      )
    ),
    ext AS (
      SELECT ROW_NUMBER() OVER () AS id, geom,
        ST_StartPoint(geom) AS start_pt,
        ST_EndPoint(geom) AS end_pt
      FROM layer_05_tmp1
    ),
    start_snap AS (
      SELECT e.id,
        MIN_BY(c.pt, ST_Distance(c.pt, e.start_pt)) AS snap_pt
      FROM ext e CROSS JOIN corners c
      WHERE ST_X(c.pt) BETWEEN ST_X(e.start_pt) - ${SNAP_DIST}
                           AND ST_X(e.start_pt) + ${SNAP_DIST}
        AND ST_Y(c.pt) BETWEEN ST_Y(e.start_pt) - ${SNAP_DIST}
                           AND ST_Y(e.start_pt) + ${SNAP_DIST}
        AND ST_Distance(c.pt, e.start_pt) < ${SNAP_DIST}
      GROUP BY e.id
    ),
    end_snap AS (
      SELECT e.id,
        MIN_BY(c.pt, ST_Distance(c.pt, e.end_pt)) AS snap_pt
      FROM ext e CROSS JOIN corners c
      WHERE ST_X(c.pt) BETWEEN ST_X(e.end_pt) - ${SNAP_DIST}
                           AND ST_X(e.end_pt) + ${SNAP_DIST}
        AND ST_Y(c.pt) BETWEEN ST_Y(e.end_pt) - ${SNAP_DIST}
                           AND ST_Y(e.end_pt) + ${SNAP_DIST}
        AND ST_Distance(c.pt, e.end_pt) < ${SNAP_DIST}
      GROUP BY e.id
    ),
    pts_as_list AS (
      SELECT
        COALESCE(ss.snap_pt, e.start_pt) AS close_s,
        COALESCE(es.snap_pt, e.end_pt) AS close_e,
        list_transform(
          generate_series(1, ST_NPoints(e.geom)),
          lambda i: ST_PointN(e.geom, i::INTEGER)
        ) AS pts
      FROM ext e
      LEFT JOIN start_snap ss ON e.id = ss.id
      LEFT JOIN end_snap es ON e.id = es.id
    )
    SELECT ST_MakeLine(list_concat(
      [close_s], list_slice(pts, 2, -2), [close_e]
    )) AS geom
    FROM pts_as_list
  `);

  // Release upstream tables before the noding stage. layer_04 is kept alive
  // because the orphan fallback in _05 routes by Voronoi-cell containment.
  await conn.query("DROP TABLE IF EXISTS layer_02a");
  await conn.query("DROP TABLE IF EXISTS layer_03a");
  await conn.query("DROP TABLE IF EXISTS layer_05_tmp1");

  // _05_tmp3: split noding+polygonize from the spatial join so DuckDB can
  // release noding working memory before the next stage's bbox-prefiltered
  // join begins.
  await conn.query(`--sql
    CREATE OR REPLACE TABLE layer_05_tmp3 AS
    WITH
    lines AS (
      SELECT geom FROM layer_02b
      UNION ALL
      SELECT geom FROM layer_05_tmp2
    ),
    noded AS (SELECT ST_Node(ST_Collect(list(geom))) AS geom FROM lines)
    SELECT UNNEST(ST_Dump(ST_Polygonize(list(geom)))).geom AS geom
    FROM noded
  `);

  await conn.query("DROP TABLE IF EXISTS layer_02b");
  await conn.query("DROP TABLE IF EXISTS layer_05_tmp2");

  // _05_tmp4: one interior point per polygon part. layer_01 in edge-matcher
  // is (fid, geom) only — attributes live in layer_attr — so * EXCLUDE
  // projects just fid.
  await conn.query(`--sql
    CREATE OR REPLACE TABLE layer_05_tmp4 AS
    WITH parts AS (
      SELECT * EXCLUDE (geom), UNNEST(ST_Dump(geom)).geom AS part_geom
      FROM layer_01
    )
    SELECT * EXCLUDE (part_geom), ST_PointOnSurface(part_geom) AS pt
    FROM parts
  `);

  // _05: assign each cell to its source polygon. Bbox-prefiltered ST_Within
  // plans as PIECEWISE_MERGE_JOIN with ST_Within as a residual FILTER — no
  // SPATIAL_JOIN, no memory_limit override needed. LEFT JOIN catches orphan
  // cells — sub-cells too small to contain any layer_01 interior point (e.g.
  // tiny slivers in stairstep boundary regions). Each orphan is routed to the
  // fid whose Voronoi cell (layer_04) contains the orphan's interior point —
  // that's the territory map the pipeline already trusts. Distance-to-nearest
  // layer_01 point would mis-route slivers in stairstep zones, ignoring topology.
  await conn.query(`--sql
    CREATE OR REPLACE TABLE layer_05 AS
    WITH
    cells AS (
      SELECT ROW_NUMBER() OVER () AS cid, geom AS vgeom FROM layer_05_tmp3
    ),
    all_joined AS (
      SELECT c.vgeom, p.* EXCLUDE (pt)
      FROM cells c
      LEFT JOIN layer_05_tmp4 AS p
        ON ST_X(p.pt) >= ST_XMin(c.vgeom)
       AND ST_X(p.pt) <= ST_XMax(c.vgeom)
       AND ST_Y(p.pt) >= ST_YMin(c.vgeom)
       AND ST_Y(p.pt) <= ST_YMax(c.vgeom)
       AND ST_Within(p.pt, c.vgeom)
      QUALIFY ROW_NUMBER() OVER (PARTITION BY c.cid ORDER BY p.fid NULLS LAST) = 1
    ),
    unmatched AS (
      SELECT ROW_NUMBER() OVER () AS uid, vgeom,
             ST_PointOnSurface(vgeom) AS upt
      FROM all_joined WHERE fid IS NULL
    ),
    fallback AS (
      SELECT u.vgeom, o.* EXCLUDE (geom)
      FROM unmatched u
      JOIN layer_04 v
        ON ST_X(u.upt) >= ST_XMin(v.geom)
       AND ST_X(u.upt) <= ST_XMax(v.geom)
       AND ST_Y(u.upt) >= ST_YMin(v.geom)
       AND ST_Y(u.upt) <= ST_YMax(v.geom)
       AND ST_Within(u.upt, v.geom)
      JOIN layer_01 o ON o.fid = v.fid
    )
    SELECT * EXCLUDE (vgeom), ST_Collect(list(vgeom)) AS geom
    FROM (
      SELECT * FROM all_joined WHERE fid IS NOT NULL
      UNION ALL
      SELECT * FROM fallback
    )
    GROUP BY ALL
  `);

  await conn.query("DROP TABLE IF EXISTS layer_04");
  await conn.query("DROP TABLE IF EXISTS layer_05_tmp3");
  await conn.query("DROP TABLE IF EXISTS layer_05_tmp4");
}

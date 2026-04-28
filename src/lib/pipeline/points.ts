import type { AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";

const SNAP_TOLERANCE = 1e-8;

export async function stagePoints(conn: AsyncDuckDBConnection, distance: number): Promise<void> {
  // Buffered union of exterior-line endpoints — marks the shared-boundary zone.
  // Subtracting this zone from interpolated points removes redundant Voronoi
  // generators at junction vertices.
  await conn.query(`--sql
    CREATE OR REPLACE TABLE layer_03a AS
    SELECT ST_Union_Agg(ST_Buffer(ST_Boundary(geom), ${SNAP_TOLERANCE})) AS geom
    FROM layer_02a
  `);

  // Interpolated points along each _02a line minus the junction zone,
  // UNION ALL with line endpoints minus the junction zone. CROSS JOIN against
  // single-row layer_03a is safe (nested loop, no SPATIAL_JOIN).
  await conn.query(`--sql
    CREATE OR REPLACE TABLE layer_03b AS
    SELECT fid, geom FROM (
      SELECT
        a.fid,
        UNNEST(ST_Dump(ST_Difference(
          ST_LineInterpolatePoints(
            a.geom,
            LEAST(${distance} / ST_Length(a.geom), 1.0),
            true
          ),
          b.geom
        ))).geom AS geom
      FROM layer_02a AS a
      CROSS JOIN layer_03a AS b
      UNION ALL
      SELECT
        a.fid,
        UNNEST(ST_Dump(ST_Boundary(
          ST_Difference(a.geom, b.geom)
        ))).geom AS geom
      FROM layer_02a AS a
      CROSS JOIN layer_03a AS b
    )
    WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom)
  `);
}

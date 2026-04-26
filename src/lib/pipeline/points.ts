import type { AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";

export async function stagePoints(conn: AsyncDuckDBConnection, distance: number): Promise<void> {
  // Small buffer around all shared line endpoints to exclude them from interpolation
  await conn.query(`--sql
    CREATE OR REPLACE TABLE layer_03_tmp AS
    SELECT ST_Union_Agg(ST_Buffer(ST_Boundary(geom), 0.00000001)) AS geom
    FROM layer_02
  `);

  await conn.query(`--sql
    CREATE OR REPLACE TABLE layer_03 AS
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
      FROM layer_02 AS a
      CROSS JOIN layer_03_tmp AS b
      UNION ALL
      SELECT
        a.fid,
        UNNEST(ST_Dump(ST_Boundary(
          ST_Difference(a.geom, b.geom)
        ))).geom AS geom
      FROM layer_02 AS a
      CROSS JOIN layer_03_tmp AS b
    )
    WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom)
  `);

  await conn.query("DROP TABLE IF EXISTS layer_03_tmp");
}

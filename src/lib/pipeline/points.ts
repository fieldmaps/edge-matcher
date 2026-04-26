import type { AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";

export async function stagePoints(conn: AsyncDuckDBConnection, distance: number): Promise<void> {
  // Shared interior edges appear in layer_02 for both adjacent polygons, generating
  // coincident seeds. Deduplicating by (x,y) eliminates them — lowest fid wins —
  // which matches the old lines.ts semantics where subtracted shared edges produced
  // no seeds on interior boundaries at all.
  await conn.query(`--sql
    CREATE OR REPLACE TABLE layer_03 AS
    SELECT fid, geom FROM (
      SELECT
        a.fid,
        UNNEST(ST_Dump(
          ST_LineInterpolatePoints(
            a.geom,
            LEAST(${distance} / ST_Length(a.geom), 1.0),
            true
          )
        )).geom AS geom
      FROM layer_02 AS a
    )
    WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ST_X(geom), ST_Y(geom) ORDER BY fid) = 1
  `);
}

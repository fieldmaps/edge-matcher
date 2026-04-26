import type { AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";

export async function stageLines(conn: AsyncDuckDBConnection): Promise<void> {
  await conn.query(`--sql
    CREATE OR REPLACE TABLE layer_02_tmp AS
    SELECT fid, ST_Boundary(geom) AS geom
    FROM layer_01
  `);

  await conn.query(`--sql
    CREATE OR REPLACE TABLE layer_02 AS
    SELECT
      a.fid,
      UNNEST(ST_Dump(ST_LineMerge(ST_CollectionExtract(
        CASE WHEN sub.neighbor_union IS NOT NULL
          THEN ST_Difference(a.geom, sub.neighbor_union)
          ELSE a.geom
        END, 2
      )))).geom AS geom
    FROM layer_02_tmp AS a
    LEFT JOIN LATERAL (
      SELECT ST_Union_Agg(b.geom) AS neighbor_union
      FROM layer_02_tmp AS b
      WHERE b.fid != a.fid AND ST_Intersects(a.geom, b.geom)
    ) AS sub ON true
  `);

  await conn.query("DROP TABLE IF EXISTS layer_02_tmp");
}

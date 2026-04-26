import type { AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";

export async function stageLines(conn: AsyncDuckDBConnection): Promise<void> {
  await conn.query(`--sql
    CREATE OR REPLACE TABLE layer_02 AS
    SELECT fid, UNNEST(ST_Dump(ST_Boundary(geom))).geom AS geom
    FROM layer_01
  `);
}

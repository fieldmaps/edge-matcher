import type { AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";

export async function stageLines(conn: AsyncDuckDBConnection): Promise<void> {
  try {
    // ST_Boundary(ST_Union_Agg) gives only exterior-facing edges — shared interior
    // edges cancel out inside the union. Intersecting each polygon's boundary ring
    // with that exterior boundary extracts its exterior-only edges.
    // CROSS JOIN against a single-row table avoids SPATIAL_JOIN.
    // ST_Union_Agg OOMs on large datasets → caught below.
    await conn.query(`--sql
      CREATE OR REPLACE TABLE layer_02_ext AS
      SELECT ST_Boundary(ST_Union_Agg(geom)) AS geom FROM layer_01
    `);
    await conn.query(`--sql
      CREATE OR REPLACE TABLE layer_02 AS
      SELECT
        a.fid,
        UNNEST(ST_Dump(ST_CollectionExtract(
          ST_Intersection(ST_Boundary(a.geom), b.geom), 2
        ))).geom AS geom
      FROM layer_01 AS a
      CROSS JOIN layer_02_ext AS b
    `);
    await conn.query("DROP TABLE IF EXISTS layer_02_ext");
  } catch {
    // Fallback: all boundary rings. Interior shared edges appear twice (once per
    // adjacent polygon), but stagePoints deduplicates coincident seeds via QUALIFY.
    await conn.query("DROP TABLE IF EXISTS layer_02_ext");
    await conn.query(`--sql
      CREATE OR REPLACE TABLE layer_02 AS
      SELECT fid, UNNEST(ST_Dump(ST_Boundary(geom))).geom AS geom
      FROM layer_01
    `);
  }
}

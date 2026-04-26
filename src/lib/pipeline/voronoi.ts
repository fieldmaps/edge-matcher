import type { AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";

export async function stageVoronoi(conn: AsyncDuckDBConnection): Promise<void> {
  // Voronoi diagram from all input points
  await conn.query(`--sql
    CREATE OR REPLACE TABLE layer_04_tmp1 AS
    SELECT UNNEST(ST_Dump(
      ST_CollectionExtract(
        ST_VoronoiDiagram(ST_Collect(list(geom))), 3
      )
    )).geom AS geom
    FROM layer_03
  `);

  // Assign source fid to each Voronoi cell via point-in-polygon.
  // ST_Intersects handles generators on cell boundaries that ST_Within would miss.
  await conn.query(`--sql
    CREATE OR REPLACE TABLE layer_04_tmp2 AS
    SELECT a.fid, b.geom
    FROM layer_03 AS a
    JOIN layer_04_tmp1 AS b ON ST_Intersects(a.geom, b.geom)
  `);

  // Validate that every source point was assigned to a Voronoi cell
  const [pts, assigned] = await Promise.all([
    conn.query("SELECT COUNT(DISTINCT fid) AS n FROM layer_03"),
    conn.query("SELECT COUNT(DISTINCT fid) AS n FROM layer_04_tmp2"),
  ]);
  const ptCount = Number(pts.toArray()[0].n);
  const assignedCount = Number(assigned.toArray()[0].n);
  if (assignedCount < ptCount) {
    await conn.query("DROP TABLE IF EXISTS layer_04_tmp1");
    await conn.query("DROP TABLE IF EXISTS layer_04_tmp2");
    throw new Error(`Voronoi assignment incomplete: ${assignedCount}/${ptCount} fids assigned`);
  }

  // Union Voronoi cells by fid
  await conn.query(`--sql
    CREATE OR REPLACE TABLE layer_04 AS
    SELECT fid, ST_Union_Agg(geom) AS geom
    FROM layer_04_tmp2
    GROUP BY fid
  `);

  await conn.query("DROP TABLE IF EXISTS layer_04_tmp1");
  await conn.query("DROP TABLE IF EXISTS layer_04_tmp2");
}

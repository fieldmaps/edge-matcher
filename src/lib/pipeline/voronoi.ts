import type { AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";

export async function stageVoronoi(conn: AsyncDuckDBConnection): Promise<void> {
  // Voronoi diagram from all generator points
  await conn.query(`--sql
    CREATE OR REPLACE TABLE layer_04_tmp1 AS
    SELECT UNNEST(ST_Dump(
      ST_CollectionExtract(
        ST_VoronoiDiagram(ST_Collect(list(geom))), 3
      )
    )).geom AS geom
    FROM layer_03b
  `);

  // Assign source fid to each Voronoi cell via point-in-polygon. ST_Intersects
  // (not ST_Within) handles generators that land exactly on a cell boundary.
  // ST_Intersects in JOIN ON triggers SPATIAL_JOIN's ~1× memory_limit virtual
  // reservation; in WASM that becomes real allocation, so we override to 999GB
  // for the join only. SPATIAL_JOIN builds its own internal index — see
  // docs/performance.md for why an explicit RTREE here was net-negative.
  const origLimit = (await conn.query("SELECT current_setting('memory_limit') AS v")).toArray()[0]
    .v as string;
  await conn.query("SET memory_limit = '999GB'");
  try {
    await conn.query(`--sql
      CREATE OR REPLACE TABLE layer_04_tmp2 AS
      SELECT a.fid, b.geom
      FROM layer_03b AS a
      JOIN layer_04_tmp1 AS b ON ST_Intersects(a.geom, b.geom)
    `);
  } finally {
    await conn.query(`SET memory_limit = '${origLimit}'`);
  }

  // Validate that every source point was assigned to a Voronoi cell. Failure
  // throws into the retry loop, which doubles spacing and tries again.
  const [pts, assigned] = await Promise.all([
    conn.query("SELECT COUNT(DISTINCT fid) AS n FROM layer_03b"),
    conn.query("SELECT COUNT(DISTINCT fid) AS n FROM layer_04_tmp2"),
  ]);
  const ptCount = Number(pts.toArray()[0].n);
  const assignedCount = Number(assigned.toArray()[0].n);
  if (assignedCount < ptCount) {
    throw new Error(`Voronoi assignment incomplete: ${assignedCount}/${ptCount} fids assigned`);
  }

  await conn.query("DROP TABLE IF EXISTS layer_03b");
  await conn.query("DROP TABLE IF EXISTS layer_04_tmp1");

  // Union Voronoi cells by fid. ST_MakeValid defends against invalid cells
  // produced by ST_VoronoiDiagram on degenerate point configurations — feeding
  // an invalid polygon to ST_Union_Agg segfaults GEOS.
  await conn.query(`--sql
    CREATE OR REPLACE TABLE layer_04 AS
    SELECT fid, ST_Union_Agg(ST_MakeValid(geom)) AS geom
    FROM layer_04_tmp2
    GROUP BY fid
  `);

  await conn.query("DROP TABLE IF EXISTS layer_04_tmp2");
}

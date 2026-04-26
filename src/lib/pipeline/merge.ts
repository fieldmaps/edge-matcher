import type { AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";

export async function stageMerge(conn: AsyncDuckDBConnection): Promise<void> {
  // Node ALL boundaries together so adjacent merged polygons share consistent
  // edge structure — no topology seams. Polygonize inline.
  await conn.query(`--sql
    CREATE OR REPLACE TABLE layer_05_pieces AS
    WITH orig_bd AS (
      SELECT ST_Union_Agg(ST_Boundary(geom)) AS geom FROM layer_01
    ),
    voro_bd AS (
      SELECT ST_Union_Agg(ST_Boundary(geom)) AS geom FROM layer_04
    ),
    noded AS (
      SELECT ST_Node(ST_Collect(list(geom))) AS geom FROM (
        SELECT geom FROM orig_bd
        UNION ALL
        SELECT geom FROM voro_bd
      )
    )
    SELECT row_number() OVER () AS pid, geom
    FROM (
      SELECT UNNEST(ST_Dump(ST_Polygonize(list(geom)))).geom AS geom
      FROM noded
    )
  `);

  // Point-on-surface kept separate to avoid loading piece geometry into spatial join
  await conn.query(`--sql
    CREATE OR REPLACE TABLE layer_05_pts AS
    SELECT pid, ST_PointOnSurface(geom) AS pt
    FROM layer_05_pieces
  `);

  // SPATIAL_JOIN pre-allocates ~1× RAM as virtual reservation; raising memory_limit past
  // that threshold lets the join proceed — no physical pages are mapped until real data
  // demands them. Restore original limit afterward.
  const origLimit = (await conn.query("SELECT current_setting('memory_limit') AS v")).toArray()[0].v as string;
  await conn.query("SET memory_limit = '999GB'");

  // Original polygon assignment (takes priority)
  await conn.query("CREATE INDEX layer_01_ridx ON layer_01 USING RTREE (geom)");
  await conn.query(`--sql
    CREATE OR REPLACE TABLE layer_05_orig AS
    SELECT p.pid, o.fid
    FROM layer_05_pts AS p
    JOIN layer_01 AS o ON ST_Within(p.pt, o.geom)
  `);
  await conn.query("DROP INDEX layer_01_ridx");

  // Voronoi assignment for pieces outside original polygons
  await conn.query("CREATE INDEX layer_04_ridx ON layer_04 USING RTREE (geom)");
  await conn.query(`--sql
    CREATE OR REPLACE TABLE layer_05_voro AS
    SELECT p.pid, v.fid
    FROM layer_05_pts AS p
    JOIN layer_04 AS v ON ST_Within(p.pt, v.geom)
    WHERE p.pid NOT IN (SELECT pid FROM layer_05_orig)
  `);
  await conn.query("DROP INDEX layer_04_ridx");

  await conn.query(`SET memory_limit = '${origLimit}'`);

  await conn.query("DROP TABLE IF EXISTS layer_05_pts");

  await conn.query(`--sql
    CREATE OR REPLACE TABLE layer_05 AS
    SELECT fid, ST_Multi(ST_Union_Agg(pieces.geom)) AS geom
    FROM (
      SELECT p.pid, p.geom, COALESCE(orig.fid, voro.fid) AS fid
      FROM layer_05_pieces AS p
      LEFT JOIN layer_05_orig AS orig ON p.pid = orig.pid
      LEFT JOIN layer_05_voro AS voro ON p.pid = voro.pid
    ) AS pieces
    WHERE fid IS NOT NULL
    GROUP BY fid
  `);

  await conn.query("DROP TABLE IF EXISTS layer_05_pieces");
  await conn.query("DROP TABLE IF EXISTS layer_05_orig");
  await conn.query("DROP TABLE IF EXISTS layer_05_voro");
}

import type { AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";

export interface ClipResult {
  geojson: string;
  bounds: [number, number, number, number] | null;
}

export async function runClip(conn: AsyncDuckDBConnection): Promise<ClipResult> {
  await conn.query("DROP TABLE IF EXISTS clip_selected");
  // ST_MaximumInscribedCircle gives the pole of inaccessibility — the most interior
  // point of the polygon, maximally distant from any edge. This is robust for irregular
  // shapes where a bbox centroid might land outside the geometry.
  // Fallback: if no clip polygon strictly contains the probe point (e.g. a border gap),
  // pick the nearest by centroid distance.
  await conn.query(`--sql
    CREATE TABLE clip_selected AS
    WITH probe AS (
      SELECT (ST_MaximumInscribedCircle(geom)).center AS geom FROM layer_01 LIMIT 1
    ),
    contained AS (
      SELECT c.geom
      FROM clip_layer c, probe p
      WHERE ST_Contains(c.geom, p.geom)
      LIMIT 1
    ),
    fallback AS (
      SELECT c.geom
      FROM clip_layer c, probe p
      WHERE NOT EXISTS (SELECT 1 FROM contained)
      ORDER BY ST_Distance(ST_Centroid(c.geom), p.geom)
      LIMIT 1
    )
    SELECT geom FROM contained
    UNION ALL
    SELECT geom FROM fallback
    LIMIT 1
  `);

  const cnt = (await conn.query("SELECT COUNT(*) AS n FROM clip_selected")).toArray()[0] as {
    n: bigint;
  };
  if (Number(cnt.n) === 0) throw new Error("No clip polygon overlaps the input data.");

  await conn.query("DROP TABLE IF EXISTS layer_clip");
  await conn.query(`--sql
    CREATE TABLE layer_clip AS
    SELECT fid, geom FROM (
      SELECT a.fid, ST_Intersection(a.geom, c.geom) AS geom
      FROM layer_05 a CROSS JOIN clip_selected c
      WHERE ST_Intersects(a.geom, c.geom)
    ) WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom)
  `);

  await conn.query("DROP TABLE IF EXISTS clip_selected");

  const attrDesc = await conn.query("DESCRIBE layer_attr");
  const attrSchema = attrDesc.toArray() as Array<{ column_name: string; column_type: string }>;
  const isIncompatible = (t: string) =>
    t === "BLOB" ||
    t === "HUGEINT" ||
    t === "UHUGEINT" ||
    t.startsWith("STRUCT") ||
    t.startsWith("MAP") ||
    t.includes("[]");
  const propKeys = attrSchema.filter((r) => r.column_name !== "fid").map((r) => r.column_name);
  const attrExprs = attrSchema
    .filter((r) => r.column_name !== "fid")
    .map((r) => {
      const col = JSON.stringify(r.column_name);
      return isIncompatible(r.column_type) ? `CAST(b.${col} AS VARCHAR) AS ${col}` : `b.${col}`;
    });
  const selectCols = attrExprs.length > 0 ? ", " + attrExprs.join(", ") : "";

  const rows = await conn.query(`--sql
    SELECT ST_AsGeoJSON(a.geom) AS _geom${selectCols}
    FROM layer_clip AS a
    LEFT JOIN layer_attr AS b ON a.fid = b.fid
    WHERE a.geom IS NOT NULL
  `);

  const features = rows.toArray().map((row: Record<string, unknown>) => {
    const props: Record<string, unknown> = {};
    for (const k of propKeys) props[k] = row[k];
    return { type: "Feature", geometry: JSON.parse(row._geom as string), properties: props };
  });
  const geojson = JSON.stringify({ type: "FeatureCollection", features }, (_, v) =>
    typeof v === "bigint" ? Number(v) : v,
  );

  let bounds: [number, number, number, number] | null = null;
  try {
    const bboxResult = await conn.query(`--sql
      SELECT MIN(ST_XMin(geom)) AS xmin, MIN(ST_YMin(geom)) AS ymin,
             MAX(ST_XMax(geom)) AS xmax, MAX(ST_YMax(geom)) AS ymax
      FROM layer_clip WHERE geom IS NOT NULL
    `);
    const row = bboxResult.toArray()[0] as Record<string, number>;
    const { xmin, ymin, xmax, ymax } = row;
    if (isFinite(xmin) && isFinite(ymin) && isFinite(xmax) && isFinite(ymax)) {
      bounds = [xmin, ymin, xmax, ymax];
    }
  } catch {
    // bounds stays null
  }

  return { geojson, bounds };
}

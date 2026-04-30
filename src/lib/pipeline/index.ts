import type { AsyncDuckDB, AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";
import { stageClean } from "./clean";
import { stageLines } from "./lines";
import { stageMerge } from "./merge";
import { stagePoints } from "./points";
import { stageVoronoi } from "./voronoi";

export type ProgressFn = (stage: number, label: string) => void;

export class PipelineError extends Error {
  constructor(message: string, public readonly failedStage: number) {
    super(message);
    this.name = "PipelineError";
  }
}

const MAX_ATTEMPTS = 10;
const MAX_POINTS = 10_000_000;

export interface PipelineResult {
  geojson: string;
  bounds: [number, number, number, number] | null;
}

export async function getOriginalGeojson(conn: AsyncDuckDBConnection): Promise<string> {
  const origRows = await conn.query(`--sql
    SELECT ST_AsGeoJSON(geom) AS _geom FROM layer_01 WHERE geom IS NOT NULL
  `);
  const features = origRows.toArray().map((row: Record<string, unknown>) => ({
    type: "Feature",
    geometry: JSON.parse(row._geom as string),
    properties: {},
  }));
  return JSON.stringify({ type: "FeatureCollection", features });
}

async function runValidation(
  conn: AsyncDuckDBConnection,
  finalTable: string,
  origTable: string,
): Promise<void> {
  // Each check is wrapped independently so a single failure (e.g. ST_Union_Agg
  // in the gap check OOMing on a huge result) doesn't abort the others.
  try {
    const r = await conn.query(`--sql
      SELECT ST_CoverageInvalidEdges_Agg(geom) IS NOT NULL AS bad
      FROM (SELECT UNNEST(ST_Dump(geom)).geom AS geom FROM ${finalTable})
    `);
    if (r.toArray()[0].bad) console.warn(`OVERLAPS in ${finalTable}`);
  } catch (e) {
    console.warn("overlap check failed:", e);
  }

  try {
    const r = await conn.query(`--sql
      WITH u AS (
        SELECT ST_Union_Agg(geom) AS g
        FROM (SELECT UNNEST(ST_Dump(geom)).geom AS geom FROM ${finalTable})
      )
      SELECT ST_NumInteriorRings(g) AS n FROM u
    `);
    const n = Number(r.toArray()[0].n ?? 0);
    if (n > 0) console.warn(`GAPS in ${finalTable}: ${n} interior rings`);
  } catch (e) {
    console.warn("gap check failed:", e);
  }

  try {
    const [a, b] = await Promise.all([
      conn.query(`SELECT COUNT(*) AS n FROM ${finalTable}`),
      conn.query(`SELECT COUNT(*) AS n FROM ${origTable}`),
    ]);
    const na = Number((a.toArray()[0] as { n: bigint | number }).n);
    const nb = Number((b.toArray()[0] as { n: bigint | number }).n);
    if (na !== nb) console.warn(`ROW MISMATCH: ${finalTable}=${na} vs ${origTable}=${nb}`);
  } catch (e) {
    console.warn("row count check failed:", e);
  }
}

export async function runPipeline(
  db: AsyncDuckDB,
  conn: AsyncDuckDBConnection,
  distance: number,
  onProgress: ProgressFn,
): Promise<PipelineResult> {
  // Optional pre-stage: detect overlaps/gaps and run mapshaper -clean in a
  // Worker if needed. Reported under stage 1 (Load) so clean inputs show
  // no extra step. See pipeline/clean.ts for the detection thresholds.
  await stageClean(db, conn, (label) => onProgress(1, label));

  // Stage 2: lines (single attempt; _02a/_02b are stable across retries)
  onProgress(2, "Extracting boundary lines");
  await stageLines(conn);

  // Stages 3+4: points → MAX_POINTS check → voronoi, retry with doubling
  let succeeded = false;
  let lastFailedStage = "";
  let lastDistance = distance;
  for (let i = 0; i < MAX_ATTEMPTS; i++) {
    const d = distance * Math.pow(2, i);
    lastDistance = d;
    let inVoronoi = false;
    try {
      onProgress(
        3,
        i === 0
          ? "Interpolating points"
          : `Interpolating points (retry ${i}, distance=${d.toFixed(6)}, ${lastFailedStage} failed)`,
      );
      await stagePoints(conn, d);

      const cnt = Number(
        (await conn.query("SELECT COUNT(*) AS n FROM layer_03b")).toArray()[0].n,
      );
      if (cnt > MAX_POINTS) throw new Error(`too many points: ${cnt.toLocaleString()}`);

      inVoronoi = true;
      onProgress(4, "Building Voronoi diagram");
      await stageVoronoi(conn);

      succeeded = true;
      break;
    } catch (e) {
      lastFailedStage = inVoronoi ? "voronoi" : "points";
      console.warn(`Attempt ${i + 1} failed at ${lastFailedStage} stage (distance=${d}):`, e);
      // Drop only points/voronoi tables; preserve _02a/_02b across retries.
      for (const t of [
        "layer_03a",
        "layer_03b",
        "layer_04_tmp1",
        "layer_04_tmp2",
        "layer_04",
      ]) {
        await conn.query(`DROP TABLE IF EXISTS ${t}`);
      }
    }
  }
  if (!succeeded) {
    const failedStageNum = lastFailedStage === "voronoi" ? 4 : 3;
    throw new PipelineError(
      `Failed to generate Voronoi polygons after ${MAX_ATTEMPTS} attempts (last distance=${lastDistance.toFixed(6)}). The dataset may be too large or have topology errors.`,
      failedStageNum,
    );
  }

  // Stage 5: merge
  onProgress(5, "Merging polygons");
  await stageMerge(conn);

  // Topology validation (warn-only)
  await runValidation(conn, "layer_05", "layer_01");

  // Bounds for map fit
  let bounds: [number, number, number, number] | null = null;
  try {
    const bboxResult = await conn.query(`--sql
      SELECT
        MIN(ST_XMin(geom)) AS xmin,
        MIN(ST_YMin(geom)) AS ymin,
        MAX(ST_XMax(geom)) AS xmax,
        MAX(ST_YMax(geom)) AS ymax
      FROM layer_05
      WHERE geom IS NOT NULL
    `);
    const row = bboxResult.toArray()[0] as Record<string, number>;
    const { xmin, ymin, xmax, ymax } = row;
    if (isFinite(xmin) && isFinite(ymin) && isFinite(xmax) && isFinite(ymax)) {
      bounds = [xmin, ymin, xmax, ymax];
    }
  } catch {
    // bounds stays null
  }

  // GeoJSON export with attribute join. ST_AsGeoJSON avoids GDAL driver
  // type/geometry incompatibilities.
  const attrDesc = await conn.query("DESCRIBE layer_attr");
  const attrSchema = attrDesc.toArray() as Array<{
    column_name: string;
    column_type: string;
  }>;
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
    FROM layer_05 AS a
    LEFT JOIN layer_attr AS b ON a.fid = b.fid
    WHERE a.geom IS NOT NULL
  `);

  const features = rows.toArray().map((row: Record<string, unknown>) => {
    const props: Record<string, unknown> = {};
    for (const k of propKeys) props[k] = row[k];
    return {
      type: "Feature",
      geometry: JSON.parse(row._geom as string),
      properties: props,
    };
  });
  const geojson = JSON.stringify({ type: "FeatureCollection", features }, (_, v) =>
    typeof v === "bigint" ? Number(v) : v,
  );

  return { geojson, bounds };
}

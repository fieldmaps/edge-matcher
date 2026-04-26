import type { AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";
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

export async function runPipeline(
  conn: AsyncDuckDBConnection,
  distance: number,
  onProgress: ProgressFn,
): Promise<PipelineResult> {
  // Stage 2: boundary lines
  onProgress(2, "Extracting boundary lines");
  await stageLines(conn);

  // Stage 3+4: points → Voronoi (with retry, doubling distance on failure)
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

      inVoronoi = true;
      onProgress(4, "Building Voronoi diagram");
      await stageVoronoi(conn);

      succeeded = true;
      break;
    } catch (e) {
      lastFailedStage = inVoronoi ? "voronoi" : "points";
      console.warn(`Attempt ${i + 1} failed at ${lastFailedStage} stage (distance=${d}):`, e);
      await conn.query("DROP TABLE IF EXISTS layer_03");
      await conn.query("DROP TABLE IF EXISTS layer_04");
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

  // Query bounds before extraction
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

  // Build GeoJSON via ST_AsGeoJSON — avoids GDAL driver type/geometry incompatibilities.
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

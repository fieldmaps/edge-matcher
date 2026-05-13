import type { AsyncDuckDB, AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";

export type CleanProgress = (label: string) => void;

interface DetectionResult {
  dirty: boolean;
  reason: string;
}

async function detectDirty(conn: AsyncDuckDBConnection): Promise<DetectionResult> {
  // ST_CoverageInvalidEdges_Agg flags both overlap edges and near-coincident
  // segments. We deliberately don't check for gaps separately — the only
  // cheap-ish gap query (ST_NumInteriorRings(ST_Union_Agg(...))) OOMs on
  // realistic admin coverages (col_admin3 took >1min and crashed). Mapshaper
  // handles gaps in the same pass as overlaps anyway, so a dedicated gap
  // detector wouldn't change behaviour.
  //
  // If the detection query itself throws (typical for OOM or TopologyException
  // on already-broken coverages), treat that as a positive signal of dirtiness
  // and run the cleaner — it's NOT a degraded fallback because we're being
  // more conservative, not less.
  try {
    const r = await conn.query(`--sql
      SELECT ST_CoverageInvalidEdges_Agg(geom) IS NOT NULL AS bad
      FROM (SELECT UNNEST(ST_Dump(geom)).geom AS geom FROM layer_01)
    `);
    const bad = (r.toArray()[0] as { bad: boolean | null }).bad === true;
    return bad
      ? { dirty: true, reason: "invalid coverage edges" }
      : { dirty: false, reason: "clean" };
  } catch (e) {
    console.warn("coverage validity check failed; treating as dirty:", e);
    return { dirty: true, reason: "validity check threw — likely dirty" };
  }
}

// Build the GeoJSON payload as a single Uint8Array, streaming each row
// directly into a TextEncoder. Avoids two intermediate full copies
// (Feature object array + JSON.stringify result string) compared to the
// JSON.stringify path. The returned ArrayBuffer is transferable — we hand
// ownership to the worker via postMessage's transfer list, eliminating the
// structural-clone copy. ST_AsGeoJSON already emits the geometry as a JSON
// string, so we splice it in raw rather than parsing+restringifying.
async function exportLayerToGeoJSONBytes(conn: AsyncDuckDBConnection): Promise<Uint8Array> {
  const rows = await conn.query(`--sql
    SELECT fid, ST_AsGeoJSON(geom) AS _geom
    FROM layer_01
    WHERE geom IS NOT NULL AND NOT ST_IsEmpty(geom)
  `);
  const enc = new TextEncoder();
  const chunks: Uint8Array[] = [];
  let total = 0;
  const push = (s: string) => {
    const bytes = enc.encode(s);
    chunks.push(bytes);
    total += bytes.length;
  };
  push('{"type":"FeatureCollection","features":[');
  let first = true;
  for (const row of rows.toArray() as Array<{ fid: bigint | number; _geom: string }>) {
    if (!first) push(",");
    first = false;
    push('{"type":"Feature","geometry":');
    push(row._geom);
    push(`,"properties":{"fid":${Number(row.fid)}}}`);
  }
  push("]}");
  const out = new Uint8Array(total);
  let offset = 0;
  for (const c of chunks) {
    out.set(c, offset);
    offset += c.length;
  }
  return out;
}

// The cleaning Worker is a real Vite-bundled module worker (see
// ./mapshaper.worker.ts) — Vite handles bundling mapshaper alongside the
// `flatbush` and `buffer` polyfills it needs to actually run `-clean`.
// Dynamic-imported here so the worker chunk only loads when cleaning fires.
//
// Both directions of the postMessage round-trip transfer the ArrayBuffer
// rather than copy it. On a 100k-polygon coverage that saves a full
// duplicate of the GeoJSON payload (potentially hundreds of MB) at each
// hop — main → worker on the way in, worker → main on the way back.
async function runWorkerClean(input: Uint8Array): Promise<Uint8Array> {
  const { default: MapshaperWorker } = await import("./mapshaper.worker?worker");
  return new Promise((resolve, reject) => {
    const worker = new MapshaperWorker();
    const cleanup = () => worker.terminate();
    worker.onmessage = (ev: MessageEvent<{ bytes?: ArrayBuffer; error?: string }>) => {
      cleanup();
      if (ev.data.error) reject(new Error(ev.data.error));
      else if (ev.data.bytes) resolve(new Uint8Array(ev.data.bytes));
      else reject(new Error("mapshaper worker returned no bytes"));
    };
    worker.onerror = (ev) => {
      cleanup();
      reject(new Error(ev.message || "mapshaper worker error"));
    };
    worker.postMessage({ bytes: input.buffer }, [input.buffer]);
  });
}

export async function stageClean(
  db: AsyncDuckDB,
  conn: AsyncDuckDBConnection,
  onLabel: CleanProgress,
): Promise<boolean> {
  onLabel("Checking coverage validity…");
  const { dirty, reason } = await detectDirty(conn);
  if (!dirty) return false;

  onLabel(`Cleaning coverage (${reason})…`);
  const inputBytes = await exportLayerToGeoJSONBytes(conn);
  const cleanedBytes = await runWorkerClean(inputBytes);

  const fileName = "__edge_matcher_clean.geojson";
  await db.registerFileBuffer(fileName, cleanedBytes);
  try {
    await conn.query(`--sql
      CREATE OR REPLACE TABLE layer_01 AS
      SELECT
        CAST(fid AS BIGINT) AS fid,
        ST_Force2D(ST_MakeValid(geom)) AS geom
      FROM ST_Read('${fileName}')
      WHERE fid IS NOT NULL
    `);
  } finally {
    try {
      await db.dropFile(fileName);
    } catch {
      // best-effort
    }
  }
  return true;
}

import type { AsyncDuckDB, AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";
import { duckdbState } from "./duckdb.svelte";

export type ExportSource = "extend" | "clip";

export type ExportKind = "geojson_cached" | "gdal" | "parquet";

export interface ExportFormat {
  id: string;
  label: string;
  ext: string;
  mime: string;
  kind: ExportKind;
  driver?: string;
  rank: number;
}

export interface ExportResult {
  blob: Blob;
  filename: string;
}

interface DriverMeta {
  label: string;
  ext: string;
  mime: string;
  rank: number;
  forceMulti?: boolean;
  layerOptions?: string[];
}

// Curated set of GDAL output drivers under DuckDB-WASM's spatial extension.
// Only streaming-text writers (KML, LIBKML, GML) and native Parquet are
// reliable. The binary GDAL writers we tried fail at the WASM filesystem
// boundary and are excluded:
//   - ESRI Shapefile → "Failed to write .shp header". Multi-file companion
//     output (.shp/.shx/.dbf/.prj/.cpg) needs atomic creation across five
//     paths; neither the in-memory BUFFER filesystem nor OPFS supports it.
//   - GeoPackage (GPKG) → SQLite-backed; needs real seek-write file
//     semantics. We tried routing through OPFS (db.open with manual
//     fileHandling, registerOPFSFileName for the path + -journal/-wal/-shm
//     siblings). That cleared the initial "file is not a database" error
//     and got far enough to create and populate the GPKG metadata tables,
//     but COMMIT then failed with "disk I/O error" — SQLite's default
//     journal_mode is DELETE, which calls unlink() on the journal file at
//     commit, and the registered duckdb-wasm OPFS handle blocks unlink.
//     Fixing it properly needs either an OPFS-backed DB session
//     (db.open({ path: "opfs://..." }) — only mode where auto file-handling
//     fires per shouldOPFSFileHandling() in the runtime), or a way to
//     inject `PRAGMA journal_mode = MEMORY/PERSIST` before the GPKG driver
//     starts its transactions. Neither is reachable from where we sit.
//   - FlatGeobuf (FGB) → "Unexpected I/O failure: writing feature" mid-
//     stream in BUFFER mode (`sz != written` from VSIFWriteL); the OPFS
//     route inherits the GPKG infrastructure problem and is unverified.
// Also excluded:
//   - GeoJSON: covered by the cached-string entry pinned at rank 0.
//   - GPX: GDAL only writes waypoint/track/route schemas, not polygons.
// KML and LIBKML both produce the same .kml output; we surface whichever
// the loaded build provides.
const KNOWN_DRIVERS: Record<string, DriverMeta> = {
  KML: {
    label: "KML (.kml)",
    ext: ".kml",
    mime: "application/vnd.google-earth.kml+xml",
    rank: 40,
  },
  LIBKML: {
    label: "KML (.kml)",
    ext: ".kml",
    mime: "application/vnd.google-earth.kml+xml",
    rank: 40,
  },
  GML: {
    label: "GML (.gml)",
    ext: ".gml",
    mime: "application/gml+xml",
    rank: 70,
    forceMulti: true,
  },
};

const SOURCES: Record<ExportSource, { table: string; suffix: string }> = {
  extend: { table: "layer_05", suffix: "_ee" },
  clip: { table: "layer_clip", suffix: "_em" },
};

let cachedFormats: Promise<ExportFormat[]> | null = null;

export function resetFormatsCache(): void {
  cachedFormats = null;
}

export async function listFormats(): Promise<ExportFormat[]> {
  if (!cachedFormats) cachedFormats = discoverFormats();
  return cachedFormats;
}

async function discoverFormats(): Promise<ExportFormat[]> {
  const conn = duckdbState.conn;
  if (!conn) throw new Error("DuckDB is not ready yet.");

  const formats: ExportFormat[] = [
    {
      id: "geojson_cached",
      label: "GeoJSON (.geojson)",
      ext: ".geojson",
      mime: "application/geo+json",
      kind: "geojson_cached",
      rank: 0,
    },
  ];

  const seenLabels = new Set<string>([formats[0].label]);
  const pushUnique = (f: ExportFormat) => {
    if (seenLabels.has(f.label)) return;
    seenLabels.add(f.label);
    formats.push(f);
  };

  // Intersect the curated KNOWN_DRIVERS set with what the loaded spatial
  // extension actually supports. A future build that drops a driver simply
  // hides the corresponding menu entry; nothing surfaces beyond the curated
  // list.
  const drivers = await conn.query("SELECT short_name FROM ST_Drivers() WHERE can_create");
  const available = new Set(
    (drivers.toArray() as Array<{ short_name: string }>).map((r) => r.short_name),
  );

  for (const [shortName, meta] of Object.entries(KNOWN_DRIVERS)) {
    if (!available.has(shortName)) continue;
    pushUnique({
      id: `gdal:${shortName}`,
      label: meta.label,
      ext: meta.ext,
      mime: meta.mime,
      kind: "gdal",
      driver: shortName,
      rank: meta.rank,
    });
  }

  pushUnique({
    id: "parquet",
    label: "GeoParquet (.parquet)",
    ext: ".parquet",
    mime: "application/vnd.apache.parquet",
    kind: "parquet",
    rank: 30,
  });

  formats.sort((a, b) => {
    if (a.rank !== b.rank) return a.rank - b.rank;
    return a.label.localeCompare(b.label);
  });
  return formats;
}

export async function runExport(
  source: ExportSource,
  format: ExportFormat,
  filenameStem: string,
  cachedGeoJSON?: string,
): Promise<ExportResult> {
  const { suffix } = SOURCES[source];
  const filename = `${filenameStem}${suffix}${format.ext}`;

  switch (format.kind) {
    case "geojson_cached": {
      if (!cachedGeoJSON) throw new Error("No cached GeoJSON to download.");
      return {
        blob: new Blob([cachedGeoJSON], { type: format.mime }),
        filename,
      };
    }
    case "gdal":
      return exportGdal(source, format, filenameStem);
    case "parquet":
      return exportParquet(source, format, filenameStem);
  }
}

function requireDb(): { db: AsyncDuckDB; conn: AsyncDuckDBConnection } {
  const db = duckdbState.db;
  const conn = duckdbState.conn;
  if (!db || !conn) throw new Error("DuckDB is not ready yet.");
  return { db, conn };
}

function vfsName(ext: string): string {
  const id =
    typeof crypto !== "undefined" && "randomUUID" in crypto
      ? crypto.randomUUID()
      : `${Date.now()}_${Math.random().toString(36).slice(2)}`;
  return `__edge_export_${id}${ext}`;
}

function quotePath(p: string): string {
  return "'" + p.replace(/'/g, "''") + "'";
}

async function dropFileSafe(db: AsyncDuckDB, name: string): Promise<void> {
  try {
    await db.dropFile(name);
  } catch {
    // best-effort cleanup; leaks are page-session-scoped
  }
}

function toBlob(bytes: Uint8Array, type: string): Blob {
  // Copy into a fresh ArrayBuffer so the Blob constructor's BlobPart type
  // resolves cleanly under SharedArrayBuffer-aware DOM lib typings.
  const ab = new ArrayBuffer(bytes.byteLength);
  new Uint8Array(ab).set(bytes);
  return new Blob([ab], { type });
}

async function buildJoinSelect(
  conn: AsyncDuckDBConnection,
  table: string,
  geomExpr: string,
): Promise<string> {
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
  const attrExprs = attrSchema
    .filter((r) => r.column_name !== "fid")
    .map((r) => {
      const col = JSON.stringify(r.column_name);
      return isIncompatible(r.column_type) ? `CAST(b.${col} AS VARCHAR) AS ${col}` : `b.${col}`;
    });
  const cols = attrExprs.length > 0 ? ", " + attrExprs.join(", ") : "";
  return `SELECT ${geomExpr}${cols} FROM ${table} AS a LEFT JOIN layer_attr AS b ON a.fid = b.fid WHERE a.geom IS NOT NULL`;
}

async function exportGdal(
  source: ExportSource,
  format: ExportFormat,
  stem: string,
): Promise<ExportResult> {
  const { db, conn } = requireDb();
  const { table, suffix } = SOURCES[source];
  const meta = format.driver ? KNOWN_DRIVERS[format.driver] : undefined;
  const geomExpr = meta?.forceMulti ? "ST_Multi(a.geom) AS geom" : "a.geom AS geom";
  const select = await buildJoinSelect(conn, table, geomExpr);

  const layerOptionsClause =
    meta?.layerOptions && meta.layerOptions.length > 0
      ? `, LAYER_CREATION_OPTIONS (${meta.layerOptions.map(quotePath).join(", ")})`
      : "";

  const path = vfsName(format.ext);
  try {
    await conn.query(
      `COPY (${select}) TO ${quotePath(path)} WITH (FORMAT GDAL, DRIVER ${quotePath(format.driver!)}${layerOptionsClause})`,
    );
    const bytes = await db.copyFileToBuffer(path);
    return {
      blob: toBlob(bytes, format.mime),
      filename: `${stem}${suffix}${format.ext}`,
    };
  } finally {
    await dropFileSafe(db, path);
  }
}

async function exportParquet(
  source: ExportSource,
  format: ExportFormat,
  stem: string,
): Promise<ExportResult> {
  const { db, conn } = requireDb();
  const { table, suffix } = SOURCES[source];
  const select = await buildJoinSelect(conn, table, "a.geom AS geometry");

  const path = vfsName(format.ext);
  try {
    await conn.query(`COPY (${select}) TO ${quotePath(path)} (FORMAT PARQUET, COMPRESSION ZSTD)`);
    const bytes = await db.copyFileToBuffer(path);
    return {
      blob: toBlob(bytes, format.mime),
      filename: `${stem}${suffix}${format.ext}`,
    };
  } finally {
    await dropFileSafe(db, path);
  }
}

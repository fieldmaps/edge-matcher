import type { AsyncDuckDB, AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";
import { DuckDBDataProtocol } from "@duckdb/duckdb-wasm";
import { zip as fflateZip } from "fflate";
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
  // Pass LAYER_NAME to the COPY query so the in-file layer matches the
  // download filename. Safe for any GDAL driver that exposes a layer/
  // document name (GPKG, KML, GML).
  setLayerName?: boolean;
  // Route the COPY destination through OPFS (opfs://...) instead of the
  // in-memory BUFFER filesystem. Required for drivers whose writers need
  // real seek-write file semantics. Only works because duckdb-wasm is
  // opened with an opfs:// DB path (see duckdb.svelte.ts) — that flips on
  // shouldOPFSFileHandling() which makes the runtime auto-manage the
  // lifecycle of any 'opfs://...' literal in a query, including SQLite's
  // journal-file unlink at commit.
  opfsBacked?: boolean;
}

// Curated set of GDAL output drivers under DuckDB-WASM's spatial extension.
// All GDAL drivers route their output through OPFS via a registered
// FileSystemFileHandle (see `opfsBacked` branch in exportGdal). The
// alternative — letting COPY TO write to a plain BUFFER path — appeared
// to "work" but actually produced 1-byte placeholder files because
// GDAL's VSI write layer doesn't compose with duckdb-wasm's BUFFER
// filesystem. Native Parquet COPY (`FORMAT PARQUET`) is the only writer
// that works through BUFFER; it has its own non-GDAL code path.
// ESRI Shapefile is delivered as a .zip: each companion (.shp/.shx/
// .dbf/.prj/.cpg) gets its own registered OPFS handle, then fflate
// bundles the results client-side. /vsizip/ would be cleaner but
// reports "Read-write random access not supported" — Shapefile's
// header back-patching needs random-write semantics that the VSI
// handler doesn't provide.
// Excluded:
//   - GeoJSON: covered by the primary download button which serves the
//     already-cached string. Listing it here would duplicate that entry.
//   - GPX: GDAL only writes waypoint/track/route schemas, not polygons.
// KML and LIBKML both produce the same .kml output; we surface whichever
// the loaded build provides.
const KNOWN_DRIVERS: Record<string, DriverMeta> = {
  "ESRI Shapefile": {
    label: "Shapefile (.shp.zip)",
    ext: ".shp.zip",
    mime: "application/zip",
    rank: 10,
    forceMulti: true,
    setLayerName: true,
    opfsBacked: true,
    // ENCODING=UTF-8 makes GDAL write the .cpg sidecar declaring UTF-8 and
    // ensures non-ASCII attribute values round-trip through the .dbf.
    layerOptions: ["ENCODING=UTF-8"],
  },
  GPKG: {
    label: "GeoPackage (.gpkg)",
    ext: ".gpkg",
    mime: "application/geopackage+sqlite3",
    rank: 20,
    forceMulti: true,
    setLayerName: true,
    opfsBacked: true,
  },
  // FGB writes a spatial index by default; the back-patched header needs
  // random-write semantics, same precedent as Shapefile.
  FlatGeobuf: {
    label: "FlatGeobuf (.fgb)",
    ext: ".fgb",
    mime: "application/vnd.flatgeobuf",
    rank: 35,
    forceMulti: true,
    setLayerName: true,
    opfsBacked: true,
  },
  // LIBKML is preferred over KML when the build provides it: the older KML
  // driver writes a minimal <Style> with only <LineStyle>, which QGIS honours
  // by drawing just polygon outlines. LIBKML writes a complete style block.
  // Iteration order matters because both share the same label and the
  // pushUnique de-dup keeps the first one seen.
  LIBKML: {
    label: "KML (.kml)",
    ext: ".kml",
    mime: "application/vnd.google-earth.kml+xml",
    rank: 40,
    forceMulti: true,
    setLayerName: true,
    opfsBacked: true,
  },
  KML: {
    label: "KML (.kml)",
    ext: ".kml",
    mime: "application/vnd.google-earth.kml+xml",
    rank: 40,
    forceMulti: true,
    setLayerName: true,
    opfsBacked: true,
  },
  GML: {
    label: "GML (.gml)",
    ext: ".gml",
    mime: "application/gml+xml",
    rank: 70,
    forceMulti: true,
    setLayerName: true,
    opfsBacked: true,
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

  // GeoJSON is intentionally omitted: the primary download button already
  // serves the cached GeoJSON, so listing it here would duplicate that entry.
  const formats: ExportFormat[] = [];

  const seenLabels = new Set<string>();
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

function exportId(): string {
  return typeof crypto !== "undefined" && "randomUUID" in crypto
    ? crypto.randomUUID()
    : `${Date.now()}_${Math.random().toString(36).slice(2)}`;
}

function vfsName(ext: string): string {
  return `__edge_export_${exportId()}${ext}`;
}

async function readOpfsFileBytes(handle: FileSystemFileHandle): Promise<Uint8Array> {
  const file = await handle.getFile();
  return new Uint8Array(await file.arrayBuffer());
}

async function removeOpfsEntries(name: string): Promise<void> {
  // Best-effort cleanup of the main file plus any SQLite siblings GDAL's
  // GPKG driver may have left behind.
  const root = await navigator.storage.getDirectory();
  for (const sfx of ["", "-journal", "-wal", "-shm"]) {
    try {
      await root.removeEntry(name + sfx);
    } catch {
      // not present — fine
    }
  }
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

  const layerName = `${stem}${suffix}`;
  const layerNameClause = meta?.setLayerName
    ? `, LAYER_NAME ${quotePath(layerName)}`
    : "";
  // The pipeline transforms input geometries to EPSG:4326 (loader.ts) and
  // the DB session sets geometry_always_xy=true, so all output GDAL drivers
  // should declare WGS84 explicitly. Without this, GDAL writes no CRS info
  // (no .prj for Shapefile, "Undefined geographic SRS" in GPKG, no
  // srsName in GML).
  const srsClause = `, SRS 'EPSG:4326'`;
  const filename = `${layerName}${format.ext}`;

  if (meta?.opfsBacked) {
    const root = await navigator.storage.getDirectory();
    const baseName = `__edge_export_${exportId()}`;

    if (format.driver === "ESRI Shapefile") {
      // Shapefile is multi-file. Pre-register OPFS handles for each
      // companion (.shp/.shx/.dbf/.prj/.cpg) so GDAL can write through
      // them, then bundle the results with fflate. /vsizip/ would be
      // cleaner but rejects the random-write pattern Shapefile uses.
      const exts = [".shp", ".shx", ".dbf", ".prj", ".cpg"];
      const companions: Array<{ ext: string; name: string; handle: FileSystemFileHandle }> = [];
      for (const ext of exts) {
        const fname = `${baseName}${ext}`;
        const handle = await root.getFileHandle(fname, { create: true });
        await db.registerFileHandle(
          fname,
          handle,
          DuckDBDataProtocol.BROWSER_FSACCESS,
          true,
        );
        companions.push({ ext, name: fname, handle });
      }
      try {
        await conn.query(
          `COPY (${select}) TO ${quotePath(`${baseName}.shp`)} WITH (FORMAT GDAL, DRIVER 'ESRI Shapefile'${srsClause}${layerNameClause}${layerOptionsClause})`,
        );
        const filesToZip: Record<string, Uint8Array> = {};
        for (const c of companions) {
          const file = await c.handle.getFile();
          const bytes = new Uint8Array(await file.arrayBuffer());
          if (bytes.length > 0) {
            filesToZip[`${layerName}${c.ext}`] = bytes;
          }
        }
        if (!filesToZip[`${layerName}.shp`]) {
          throw new Error("Shapefile export produced no .shp output");
        }
        const zipped = await new Promise<Uint8Array>((resolve, reject) => {
          fflateZip(filesToZip, (err, data) => (err ? reject(err) : resolve(data)));
        });
        return { blob: toBlob(zipped, format.mime), filename };
      } finally {
        for (const c of companions) {
          await dropFileSafe(db, c.name);
          try {
            await root.removeEntry(c.name);
          } catch {
            // not present — fine
          }
        }
      }
    }

    // Single-file OPFS branch (GPKG, KML, LIBKML, GML).
    // Bypass the runtime's auto-OPFS path scanning (which opens an exclusive
    // SyncAccessHandle on opfs:// paths and conflicts with SQLite's own open
    // call inside GDAL's GPKG driver, surfacing as "file is in use"). Instead
    // we acquire the OPFS FileSystemFileHandle ourselves and register it via
    // registerFileHandle with a plain name; the auto-OPFS regex only matches
    // single-quoted opfs:// literals so this name is invisible to it. After
    // COPY, we read the bytes back through the same FileSystemFileHandle.
    const name = `${baseName}${format.ext}`;
    const fileHandle = await root.getFileHandle(name, { create: true });
    await db.registerFileHandle(
      name,
      fileHandle,
      DuckDBDataProtocol.BROWSER_FSACCESS,
      true,
    );
    try {
      await conn.query(
        `COPY (${select}) TO ${quotePath(name)} WITH (FORMAT GDAL, DRIVER ${quotePath(format.driver!)}${srsClause}${layerNameClause}${layerOptionsClause})`,
      );
      const bytes = await readOpfsFileBytes(fileHandle);
      return { blob: toBlob(bytes, format.mime), filename };
    } finally {
      await dropFileSafe(db, name);
      await removeOpfsEntries(name);
    }
  }

  const path = vfsName(format.ext);
  try {
    await conn.query(
      `COPY (${select}) TO ${quotePath(path)} WITH (FORMAT GDAL, DRIVER ${quotePath(format.driver!)}${srsClause}${layerNameClause}${layerOptionsClause})`,
    );
    const bytes = await db.copyFileToBuffer(path);
    return { blob: toBlob(bytes, format.mime), filename };
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

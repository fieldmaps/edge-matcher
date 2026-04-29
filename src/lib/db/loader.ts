import type { AsyncDuckDB, AsyncDuckDBConnection } from "@duckdb/duckdb-wasm";
import { unzip } from "fflate";

// DuckDB's spatial extension hooks into read_parquet to parse the GeoParquet "geo"
// key-value metadata entry. Certain files have a field in that JSON that causes
// stoi() to throw "no conversion" (e.g. a null or non-numeric value where an int
// is expected). Renaming the key in the footer bytes prevents the hook from firing.
// The geometry column is then a plain BLOB (WKB), handled below with ST_GeomFromWKB.
//
// Thrift compact: KeyValue.key is field 1 (BINARY), encoded as byte 0x18 (delta=1,
// type=8) + varint length + bytes. We locate 0x18 0x03 "geo" in the footer region
// and overwrite "geo" with "___".
function removeGeoMetaKey(buffer: Uint8Array): Uint8Array {
  const len = buffer.length;
  if (len < 12) return buffer;
  const footerSize =
    buffer[len - 8] | (buffer[len - 7] << 8) | (buffer[len - 6] << 16) | (buffer[len - 5] << 24);
  const footerStart = len - 8 - footerSize;
  if (footerStart < 4 || footerSize <= 0) return buffer;
  const needle = new Uint8Array([0x18, 0x03, 0x67, 0x65, 0x6f]); // 0x18 + len(3) + "geo"
  const result = new Uint8Array(buffer);
  for (let i = footerStart; i <= footerStart + footerSize - needle.length; i++) {
    if (
      result[i] === needle[0] &&
      result[i + 1] === needle[1] &&
      result[i + 2] === needle[2] &&
      result[i + 3] === needle[3] &&
      result[i + 4] === needle[4]
    ) {
      result[i + 2] = 0x5f;
      result[i + 3] = 0x5f;
      result[i + 4] = 0x5f; // "___"
      break;
    }
  }
  return result;
}

const SINGLE_EXTS = [".parquet", ".geojson", ".geojsonl", ".gpkg", ".fgb", ".kml", ".gml", ".gpx"];
const SHP_EXTS = [".shp", ".dbf", ".shx", ".prj", ".cpg"];

// Normalized geometry expression: fid + MakeValid + Force2D + optional Transform.
// ST_Read tags geometry with source CRS; single-arg ST_Transform infers it.
// Parquet geometries are untagged EPSG:4326 — skip transform.
// BLOB means WKB from a GeoParquet file whose "geo" key was stripped; parse explicitly.
// MakeValid is applied to the source before Transform so invalid input never reaches the projection.
function loadGeomExpr(geomType: string, quotedCol: string): string {
  if (geomType === "BLOB") return `ST_Force2D(ST_MakeValid(ST_GeomFromWKB(${quotedCol})))`;
  if (geomType !== "GEOMETRY")
    return `ST_Force2D(ST_Transform(ST_MakeValid(${quotedCol}), 'EPSG:4326'))`;
  return `ST_Force2D(ST_MakeValid(${quotedCol}))`;
}

// Bare CRS-handled expression for filter predicates — no MakeValid/Force2D so DuckDB
// can skip them on non-matching rows.
function rawGeomExpr(geomType: string, quotedCol: string): string {
  if (geomType === "BLOB") return `ST_GeomFromWKB(${quotedCol})`;
  if (geomType !== "GEOMETRY") return `ST_Transform(${quotedCol}, 'EPSG:4326')`;
  return quotedCol;
}

function ext(name: string): string {
  const i = name.lastIndexOf(".");
  return i === -1 ? "" : name.slice(i).toLowerCase();
}

function isIncluded(file: File): boolean {
  const e = ext(file.name);
  return SINGLE_EXTS.includes(e) || SHP_EXTS.includes(e);
}

async function extractZip(file: File): Promise<File[]> {
  const data = new Uint8Array(await file.arrayBuffer());
  const entries = await new Promise<Record<string, Uint8Array>>((resolve, reject) => {
    unzip(data, (err, result) => (err ? reject(err) : resolve(result)));
  });
  const extracted: File[] = [];
  for (const [path, bytes] of Object.entries(entries)) {
    if (path.startsWith("__MACOSX/") || bytes.length === 0) continue;
    const name = path.split("/").pop()!;
    const inner = new File([bytes.slice()], name);
    Object.defineProperty(inner, "webkitRelativePath", {
      value: path,
      writable: false,
      configurable: true,
      enumerable: true,
    });
    extracted.push(inner);
  }
  return extracted;
}

async function expandZips(files: File[]): Promise<File[]> {
  const result: File[] = [];
  for (const file of files) {
    if (ext(file.name) === ".zip") {
      result.push(...(await extractZip(file)));
    } else {
      result.push(file);
    }
  }
  return result;
}

function groupByType(files: File[]): {
  parquet?: File;
  spatial?: File;
  shapefile?: File[];
} {
  const filtered = files.filter(isIncluded);
  const shpFiles = filtered.filter((f) => SHP_EXTS.includes(ext(f.name)));
  if (shpFiles.length > 0) return { shapefile: shpFiles };
  const parquet = filtered.find((f) => ext(f.name) === ".parquet");
  if (parquet) return { parquet };
  const spatial = filtered.find(
    (f) => SINGLE_EXTS.includes(ext(f.name)) && ext(f.name) !== ".parquet",
  );
  if (spatial) return { spatial };
  return {};
}

export async function loadFile(
  db: AsyncDuckDB,
  conn: AsyncDuckDBConnection,
  files: File[],
): Promise<void> {
  const expanded = await expandZips(files);
  const group = groupByType(expanded);

  // Drop any tables from a previous run
  await conn.query("DROP TABLE IF EXISTS raw_layer");
  await conn.query("DROP TABLE IF EXISTS layer_01");
  await conn.query("DROP TABLE IF EXISTS layer_attr");

  let filePath: string;
  let isParquet = false;

  if (group.parquet) {
    const file = group.parquet;
    const buffer = removeGeoMetaKey(new Uint8Array(await file.arrayBuffer()));
    await db.registerFileBuffer(file.name, buffer);
    filePath = file.name;
    isParquet = true;
  } else if (group.spatial) {
    const file = group.spatial;
    const buffer = new Uint8Array(await file.arrayBuffer());
    await db.registerFileBuffer(file.name, buffer);
    filePath = file.name;
  } else if (group.shapefile) {
    // Register all component files; use the .shp path for ST_Read
    const relPaths = group.shapefile.map(
      (f) => (f as File & { webkitRelativePath: string }).webkitRelativePath || f.name,
    );
    await Promise.all(
      group.shapefile.map(async (file, i) => {
        const buffer = new Uint8Array(await file.arrayBuffer());
        await db.registerFileBuffer(relPaths[i], buffer);
      }),
    );
    filePath = relPaths.find((p) => p.toLowerCase().endsWith(".shp")) ?? relPaths[0];
  } else {
    throw new Error(
      "No supported file found. Drop a GeoJSON, GeoParquet, GeoPackage, Shapefile, or ZIP.",
    );
  }

  // Single-quote the path for SQL string literals (double quotes = identifiers in SQL)
  const sqlPath = "'" + filePath.replace(/'/g, "''") + "'";

  // Load into raw_layer with stable fid
  const readFn = isParquet ? `read_parquet(${sqlPath})` : `ST_Read(${sqlPath})`;
  await conn.query(`
    CREATE OR REPLACE TABLE raw_layer AS
    SELECT *, row_number() OVER () AS fid FROM ${readFn}
  `);

  // Detect geometry column and _bbox columns to exclude
  const desc = await conn.query("DESCRIBE raw_layer");
  const schema = desc.toArray() as Array<{
    column_name: string;
    column_type: string;
  }>;

  // After removeGeoMetaKey, a GeoParquet geometry column may appear as BLOB (WKB).
  // Fall back to name-based detection for parquet when no tagged GEOMETRY column exists.
  const WKB_NAMES = ["geometry", "geom", "wkb_geometry", "the_geom"];
  const geomRow =
    schema.find((r) => r.column_type.startsWith("GEOMETRY")) ??
    (isParquet
      ? schema.find(
          (r) => r.column_type === "BLOB" && WKB_NAMES.includes(r.column_name.toLowerCase()),
        )
      : undefined);
  if (!geomRow) throw new Error("No geometry column found in the loaded file.");

  const { column_name: geomCol, column_type: geomType } = geomRow;
  const excludeCols = schema
    .filter(
      (r) =>
        r.column_type.startsWith("GEOMETRY") ||
        (r.column_type === "BLOB" && r.column_name === geomCol) ||
        (r.column_name.endsWith("_bbox") && r.column_type.startsWith("STRUCT")),
    )
    .map((r) => JSON.stringify(r.column_name));
  const excludeSQL = excludeCols.join(", ");

  // Attribute table: all non-geometry, non-bbox columns (keeps fid)
  await conn.query(`
    CREATE OR REPLACE TABLE layer_attr AS
    SELECT * EXCLUDE (${excludeSQL})
    FROM raw_layer
  `);

  const quotedGeomCol = JSON.stringify(geomCol);
  await conn.query(`
    CREATE OR REPLACE TABLE layer_01 AS
    SELECT fid, ${loadGeomExpr(geomType, quotedGeomCol)} AS geom
    FROM raw_layer
  `);

  await conn.query("DROP TABLE raw_layer");
}

export async function loadClipFile(
  db: AsyncDuckDB,
  conn: AsyncDuckDBConnection,
  files: File[],
): Promise<void> {
  const expanded = await expandZips(files);
  const group = groupByType(expanded);

  await conn.query("DROP TABLE IF EXISTS clip_raw");
  await conn.query("DROP TABLE IF EXISTS clip_layer");

  let filePath: string;
  let isParquet = false;

  if (group.parquet) {
    const file = group.parquet;
    const buffer = removeGeoMetaKey(new Uint8Array(await file.arrayBuffer()));
    await db.registerFileBuffer(file.name, buffer);
    filePath = file.name;
    isParquet = true;
  } else if (group.spatial) {
    const file = group.spatial;
    const buffer = new Uint8Array(await file.arrayBuffer());
    await db.registerFileBuffer(file.name, buffer);
    filePath = file.name;
  } else if (group.shapefile) {
    const relPaths = group.shapefile.map(
      (f) => (f as File & { webkitRelativePath: string }).webkitRelativePath || f.name,
    );
    await Promise.all(
      group.shapefile.map(async (file, i) => {
        const buffer = new Uint8Array(await file.arrayBuffer());
        await db.registerFileBuffer(relPaths[i], buffer);
      }),
    );
    filePath = relPaths.find((p) => p.toLowerCase().endsWith(".shp")) ?? relPaths[0];
  } else {
    throw new Error(
      "No supported file found. Drop a GeoJSON, GeoParquet, GeoPackage, Shapefile, or ZIP.",
    );
  }

  const sqlPath = "'" + filePath.replace(/'/g, "''") + "'";
  const readFn = isParquet ? `read_parquet(${sqlPath})` : `ST_Read(${sqlPath})`;

  // Use LIMIT 0 to detect schema without materializing the full (potentially large) file.
  await conn.query(`CREATE TABLE clip_raw AS SELECT * FROM ${readFn} LIMIT 0`);
  const desc = await conn.query("DESCRIBE clip_raw");
  await conn.query("DROP TABLE clip_raw");
  const schema = desc.toArray() as Array<{ column_name: string; column_type: string }>;

  const WKB_NAMES = ["geometry", "geom", "wkb_geometry", "the_geom"];
  const geomRow =
    schema.find((r) => r.column_type.startsWith("GEOMETRY")) ??
    (isParquet
      ? schema.find(
          (r) => r.column_type === "BLOB" && WKB_NAMES.includes(r.column_name.toLowerCase()),
        )
      : undefined);
  if (!geomRow) throw new Error("No geometry column found in the clip file.");

  const { column_name: geomCol, column_type: geomType } = geomRow;
  const quotedGeomCol = JSON.stringify(geomCol);

  // Pre-filter on the raw geometry column so DuckDB skips MakeValid/Force2D/Transform
  // for non-matching rows. For a 1GB world-countries file this means those ops only run
  // on the 1-2 features that actually overlap the input layer.
  await conn.query(`
    CREATE TABLE clip_layer AS
    WITH bbox AS (
      SELECT ST_MakeEnvelope(
        MIN(ST_XMin(geom)), MIN(ST_YMin(geom)),
        MAX(ST_XMax(geom)), MAX(ST_YMax(geom))
      ) AS env FROM layer_01
    )
    SELECT ${loadGeomExpr(geomType, quotedGeomCol)} AS geom
    FROM ${readFn}, bbox
    WHERE ST_Intersects(${rawGeomExpr(geomType, quotedGeomCol)}, bbox.env)
  `);
}

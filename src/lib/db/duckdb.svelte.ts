import * as duckdb from "@duckdb/duckdb-wasm";

class DuckDBState {
  db: duckdb.AsyncDuckDB | null = null;
  conn: duckdb.AsyncDuckDBConnection | null = null;
  ready = $state(false);
  initError = $state<string | null>(null);
  // OPFS-backed DB filenames for this tab session. The primary DB is opened
  // at opfs:// to enable shouldOPFSFileHandling() (needed by the GPKG export
  // path); it stays empty. The data DB is ATTACHed with STORAGE_VERSION
  // v1.5.0 so CRS-tagged GEOMETRY columns from ST_Read can be persisted.
  // Both names are tracked here so beforeunload can clean them up.
  sessionDbName: string | null = null;
  sessionDataDbName: string | null = null;
}

export const duckdbState = new DuckDBState();

function makeSessionDbName(): string {
  const id =
    typeof crypto !== "undefined" && "randomUUID" in crypto
      ? crypto.randomUUID()
      : `${Date.now()}_${Math.random().toString(36).slice(2)}`;
  return `__edge_matcher_${id}.duckdb`;
}

export async function initDuckDB(): Promise<void> {
  if (duckdbState.ready || duckdbState.initError) return;
  try {
    const bundles = duckdb.getJsDelivrBundles();
    const bundle = await duckdb.selectBundle(bundles);

    const workerUrl = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker}");`], {
        type: "text/javascript",
      }),
    );
    const worker = new Worker(workerUrl);
    const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
    const instance = new duckdb.AsyncDuckDB(logger, worker);
    await instance.instantiate(bundle.mainModule, bundle.pthreadWorker);
    URL.revokeObjectURL(workerUrl);

    // OPFS-backed DB session enables duckdb-wasm's auto OPFS file-handling
    // path (shouldOPFSFileHandling() in the runtime). Auto mode auto-registers
    // any 'opfs://...' literal that appears in a query and unregisters it
    // afterwards — required to make the GPKG export driver's SQLite work,
    // since SQLite needs the runtime to manage journal-file lifecycle for it.
    // The primary DB is opened on opfs:// solely to flip that switch on; we
    // never write tables to it, because instance.open() doesn't accept a
    // storage-version option, so its file is stuck at the default v1.0.0+,
    // which can't store CRS-tagged GEOMETRY columns produced by ST_Read.
    duckdbState.sessionDbName = makeSessionDbName();
    await instance.open({
      path: `opfs://${duckdbState.sessionDbName}`,
      accessMode: duckdb.DuckDBAccessMode.READ_WRITE,
      opfs: { fileHandling: "auto" },
    });

    duckdbState.db = instance;
    const conn = await instance.connect();
    duckdbState.conn = conn;

    // Data DB: ATTACH a second OPFS file with STORAGE_VERSION 'v1.5.0' so
    // CRS-tagged GEOMETRY columns persist correctly. We pre-register the
    // empty file under a plain name (no opfs:// prefix) and ATTACH that
    // name; the runtime's auto-OPFS regex only matches single-quoted
    // 'opfs://' literals, so the registered handle is invisible to its
    // post-query dropFiles() pass and stays alive for the session.
    duckdbState.sessionDataDbName = makeSessionDbName();
    const root = await navigator.storage.getDirectory();
    const dataHandle = await root.getFileHandle(duckdbState.sessionDataDbName, { create: true });
    await instance.registerFileHandle(
      duckdbState.sessionDataDbName,
      dataHandle,
      duckdb.DuckDBDataProtocol.BROWSER_FSACCESS,
      true,
    );
    await conn.query(
      `ATTACH '${duckdbState.sessionDataDbName.replace(/'/g, "''")}' AS edge (STORAGE_VERSION 'v1.5.0')`,
    );
    await conn.query("USE edge");

    await conn.query("SET threads = 1");
    await conn.query("SET preserve_insertion_order = false");

    try {
      try {
        await conn.query("LOAD spatial;");
      } catch {
        await conn.query("INSTALL spatial; LOAD spatial;");
      }
      await conn.query("SET geometry_always_xy = true");
    } catch {
      console.warn("DuckDB spatial extension unavailable — GDAL support disabled.");
    }

    if (typeof window !== "undefined") {
      window.addEventListener("beforeunload", () => {
        // Best-effort cleanup of both OPFS-backed DB files on clean tab close.
        // Fire-and-forget — beforeunload doesn't await async work.
        const names = [duckdbState.sessionDbName, duckdbState.sessionDataDbName].filter(
          (n): n is string => typeof n === "string",
        );
        if (names.length === 0) return;
        navigator.storage
          ?.getDirectory()
          .then((root) =>
            Promise.allSettled(names.map((name) => root.removeEntry(name))).then(() => undefined),
          )
          .catch(() => {});
      });
    }

    duckdbState.ready = true;
  } catch (e) {
    duckdbState.initError = e instanceof Error ? e.message : String(e);
  }
}

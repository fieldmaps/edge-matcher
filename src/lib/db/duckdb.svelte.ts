import * as duckdb from "@duckdb/duckdb-wasm";

class DuckDBState {
  db: duckdb.AsyncDuckDB | null = null;
  conn: duckdb.AsyncDuckDBConnection | null = null;
  ready = $state(false);
  initError = $state<string | null>(null);
  // OPFS-backed DB filename for this tab session. Set during init and used by
  // the beforeunload handler to remove the file on clean tab close.
  sessionDbName: string | null = null;
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
    duckdbState.sessionDbName = makeSessionDbName();
    await instance.open({
      path: `opfs://${duckdbState.sessionDbName}`,
      accessMode: duckdb.DuckDBAccessMode.READ_WRITE,
      opfs: { fileHandling: "auto" },
    });

    duckdbState.db = instance;
    const conn = await instance.connect();
    duckdbState.conn = conn;

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
        // Best-effort cleanup of the OPFS-backed DB file on clean tab close.
        // Fire-and-forget — beforeunload doesn't await async work.
        const name = duckdbState.sessionDbName;
        if (!name) return;
        navigator.storage
          ?.getDirectory()
          .then((root) => root.removeEntry(name))
          .catch(() => {});
      });
    }

    duckdbState.ready = true;
  } catch (e) {
    duckdbState.initError = e instanceof Error ? e.message : String(e);
  }
}

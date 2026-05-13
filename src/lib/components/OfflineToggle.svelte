<script lang="ts">
  import * as duckdb from "@duckdb/duckdb-wasm";
  import { DUCKDB_BUNDLES, duckdbState, initDuckDB } from "$lib/db/duckdb.svelte";
  import { onMount } from "svelte";

  const LAND_GEOJSON_URL = "/data/ne_50m_land.geojson";
  const LS_KEY = "edge-matcher:offline-ready";

  // "loading" is a pre-hydration sentinel: SSR/initial client render shows
  // nothing so the user never sees a wrong-state flash (e.g. "Enable offline
  // use" briefly visible before localStorage is checked and we switch to
  // "Offline ready"). onMount resolves it to the real initial status.
  type Status = "loading" | "idle" | "downloading" | "ready" | "removing" | "failed";
  let status = $state<Status>("loading");
  let progress = $state(0);
  let total = $state(0);
  let errorMsg = $state<string | null>(null);

  onMount(() => {
    if (typeof localStorage !== "undefined" && localStorage.getItem(LS_KEY) === "1") {
      status = "ready";
    } else {
      status = "idle";
    }
  });

  function variantToPlatform(name: string): string {
    if (name === "coi") return "wasm_threads";
    if (name === "eh") return "wasm_eh";
    return "wasm_mvp";
  }

  async function fetchWithProgress(url: string, onChunk: (bytes: number) => void): Promise<void> {
    const res = await fetch(url, { cache: "reload" });
    if (!res.ok) throw new Error(`${url}: HTTP ${res.status}`);
    const reader = res.body?.getReader();
    if (!reader) {
      // No streaming body (e.g. opaque response) — just drain.
      const blob = await res.blob();
      onChunk(blob.size);
      return;
    }
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      if (value) onChunk(value.byteLength);
    }
  }

  async function probeSize(url: string): Promise<number> {
    try {
      const head = await fetch(url, { method: "HEAD", cache: "no-store" });
      const len = head.headers.get("content-length");
      return len ? parseInt(len, 10) : 0;
    } catch {
      return 0;
    }
  }

  async function enable() {
    status = "downloading";
    progress = 0;
    total = 0;
    errorMsg = null;

    try {
      // Ensure DuckDB is initialized — we read the engine version from the
      // running instance to construct the extension URL, instead of pinning a
      // constant that would drift out of sync with the installed package.
      await initDuckDB();
      const conn = duckdbState.conn;
      if (!conn) throw new Error(duckdbState.initError ?? "DuckDB failed to initialize");

      const versionRow = (await conn.query("SELECT version() AS v")).toArray()[0] as { v: string };
      const engineVersion = versionRow.v.startsWith("v") ? versionRow.v : `v${versionRow.v}`;

      // Pick the bundle the current browser will actually load at runtime.
      const bundle = await duckdb.selectBundle(DUCKDB_BUNDLES);
      const variantName = (Object.keys(DUCKDB_BUNDLES) as Array<keyof typeof DUCKDB_BUNDLES>).find(
        (k) => DUCKDB_BUNDLES[k]!.mainModule === bundle.mainModule,
      );
      const platform = variantToPlatform(variantName ?? "mvp");
      const extensionUrl = `https://extensions.duckdb.org/${engineVersion}/${platform}/spatial.duckdb_extension.wasm`;

      const urls: string[] = [
        bundle.mainModule, // duckdb-{variant}.wasm
        bundle.mainWorker!, // duckdb-browser-{variant}.worker.js
        ...(bundle.pthreadWorker ? [bundle.pthreadWorker] : []),
        extensionUrl,
        LAND_GEOJSON_URL,
      ];

      // Probe sizes in parallel for a meaningful progress bar.
      const sizes = await Promise.all(urls.map(probeSize));
      total = sizes.reduce((a, b) => a + b, 0);

      // Download sequentially so byte counters track without races.
      for (const url of urls) {
        await fetchWithProgress(url, (bytes) => {
          progress += bytes;
        });
      }

      localStorage.setItem(LS_KEY, "1");
      status = "ready";
    } catch (e) {
      errorMsg = e instanceof Error ? e.message : String(e);
      status = "failed";
    }
  }

  async function disable() {
    const ok = window.confirm(
      "Remove offline cache?\n\nThis deletes the cached app shell, DuckDB, spatial extension, and basemap data, then unregisters the service worker. The next page load will fetch from the network again. Use this if you're troubleshooting after an update.",
    );
    if (!ok) return;

    status = "removing";
    errorMsg = null;

    try {
      // 1. Delete every cache on this origin (precache + runtime caches).
      if ("caches" in self) {
        const names = await caches.keys();
        await Promise.all(names.map((n) => caches.delete(n)));
      }

      // 2. Unregister all service workers for this origin.
      if (navigator.serviceWorker) {
        const regs = await navigator.serviceWorker.getRegistrations();
        await Promise.all(regs.map((r) => r.unregister()));
      }

      // 3. Clear the offline-ready flag.
      localStorage.removeItem(LS_KEY);

      status = "idle";
      progress = 0;
      total = 0;
    } catch (e) {
      errorMsg = e instanceof Error ? e.message : String(e);
      status = "failed";
    }
  }

  function fmt(bytes: number): string {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  }
</script>

<div class="offline">
  {#if status === "loading"}
    <!-- Reserved-space placeholder; prevents flash + layout shift before hydration. -->
  {:else if status === "idle"}
    <button type="button" onclick={enable}>Enable offline use</button>
    <p class="hint">Downloads ~50&nbsp;MB so the tool works without network.</p>
  {:else if status === "downloading"}
    <button type="button" disabled>Downloading…</button>
    <p class="hint">
      {fmt(progress)}{total > 0 ? ` of ${fmt(total)}` : ""}
    </p>
    {#if total > 0}
      <div class="bar"><div class="bar-fill" style="width: {Math.min(100, (progress / total) * 100)}%"></div></div>
    {/if}
  {:else if status === "ready"}
    <p class="ready">Offline ready</p>
    <button type="button" class="remove" onclick={disable}>Remove offline cache</button>
  {:else if status === "removing"}
    <button type="button" disabled>Removing…</button>
  {:else if status === "failed"}
    <button type="button" onclick={enable}>Retry offline download</button>
    {#if errorMsg}<p class="err">{errorMsg}</p>{/if}
  {/if}
</div>

<style>
  .offline {
    display: flex;
    flex-direction: column;
    gap: 0.35rem;
    min-height: 56px;
  }
  button {
    padding: 0.5rem 0.75rem;
    font-size: 0.85rem;
    border: 1px solid #d1d5db;
    background: #f9fafb;
    border-radius: 6px;
    cursor: pointer;
  }
  button:disabled {
    cursor: progress;
    opacity: 0.7;
  }
  button:hover:not(:disabled) {
    background: #f3f4f6;
  }
  .hint {
    font-size: 0.75rem;
    color: #6b7280;
    margin: 0;
  }
  .ready {
    font-size: 0.8rem;
    color: #047857;
    margin: 0;
  }
  .remove {
    align-self: flex-start;
    padding: 0.25rem 0;
    background: transparent;
    border: none;
    font-size: 0.75rem;
    color: #6b7280;
    text-decoration: underline;
    cursor: pointer;
  }
  .remove:hover:not(:disabled) {
    color: #b91c1c;
    background: transparent;
  }
  .err {
    font-size: 0.75rem;
    color: #b91c1c;
    margin: 0;
    word-break: break-word;
  }
  .bar {
    height: 4px;
    background: #e5e7eb;
    border-radius: 2px;
    overflow: hidden;
  }
  .bar-fill {
    height: 100%;
    background: #6b7280;
    transition: width 120ms linear;
  }
</style>

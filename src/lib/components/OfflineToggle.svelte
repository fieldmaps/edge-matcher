<script lang="ts">
  import * as duckdb from "@duckdb/duckdb-wasm";
  import { DUCKDB_BUNDLES } from "$lib/db/duckdb.svelte";
  import { onMount } from "svelte";

  // Keep in sync with scripts/sync-duckdb-extensions.mjs.
  const DUCKDB_ENGINE_VERSION = "v1.5.2";
  const LAND_GEOJSON_URL = "/data/ne_50m_land.geojson";
  const LS_KEY = "edge-matcher:offline-ready";

  type Status = "idle" | "downloading" | "ready" | "failed";
  let status = $state<Status>("idle");
  let progress = $state(0);
  let total = $state(0);
  let errorMsg = $state<string | null>(null);

  onMount(() => {
    if (typeof localStorage !== "undefined" && localStorage.getItem(LS_KEY) === "1") {
      status = "ready";
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
      // Pick the bundle the current browser will actually load at runtime.
      const bundle = await duckdb.selectBundle(DUCKDB_BUNDLES);
      const variantName = (Object.keys(DUCKDB_BUNDLES) as Array<keyof typeof DUCKDB_BUNDLES>).find(
        (k) => DUCKDB_BUNDLES[k]!.mainModule === bundle.mainModule,
      );
      const platform = variantToPlatform(variantName ?? "mvp");
      const extensionUrl = `/duckdb/extensions/${DUCKDB_ENGINE_VERSION}/${platform}/spatial.duckdb_extension.wasm`;

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

  function fmt(bytes: number): string {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(0)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  }
</script>

<div class="offline">
  {#if status === "idle"}
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

<script lang="ts">
  import { duckdbState, initDuckDB } from "$lib/db/duckdb.svelte";
  import { loadClipFile, loadFile } from "$lib/db/loader";
  import { runClip } from "$lib/pipeline/clip";
  import { getOriginalGeojson, PipelineError, runPipeline } from "$lib/pipeline/index";
  import { onMount, untrack } from "svelte";
  import DropZone from "./DropZone.svelte";
  import MapView from "./MapView.svelte";

  const STAGE_LABELS = [
    "Load file",
    "Extract boundary lines",
    "Interpolate points",
    "Build Voronoi diagram",
    "Merge polygons",
  ];

  let files = $state<File[]>([]);
  let distance = $state(0.0002);
  let running = $state(false);
  let currentStage = $state(0); // 0=idle, 1-5=active stage, 6=done
  let errorStage = $state(0);   // stage number that failed, 0=none
  let stageLabel = $state("");
  let resultGeoJSON = $state<string | null>(null);
  let originalGeoJSON = $state<string | null>(null);
  let resultBounds = $state<[number, number, number, number] | null>(null);
  let error = $state<string | null>(null);

  let clipFiles = $state<File[]>([]);
  let clipRunning = $state(false);
  let clipStageLabel = $state("");
  let clipGeoJSON = $state<string | null>(null);
  let clipError = $state<string | null>(null);

  let clearMap: (() => void) | undefined;
  let clearClip: (() => void) | undefined;

  onMount(() => {
    initDuckDB();
  });

  $effect(() => {
    const f = files;
    if (f.length > 0 && duckdbState.ready) {
      untrack(() => {
        if (!running) handleRun();
      });
    }
  });

  $effect(() => {
    const f = clipFiles;
    if (f.length > 0 && resultGeoJSON) {
      untrack(() => {
        if (!clipRunning) handleClip();
      });
    }
  });

  async function handleRun() {
    clearMap?.();
    error = null;
    running = true;
    resultGeoJSON = null;
    originalGeoJSON = null;
    resultBounds = null;
    currentStage = 0;
    errorStage = 0;
    stageLabel = "";
    clipFiles = [];
    clipGeoJSON = null;
    clipError = null;
    await duckdbState.conn?.query("DROP TABLE IF EXISTS clip_layer");
    await duckdbState.conn?.query("DROP TABLE IF EXISTS layer_clip");

    try {
      currentStage = 1;
      stageLabel = "Loading file…";
      await loadFile(duckdbState.db!, duckdbState.conn!, files);

      const origGeoJSON = await getOriginalGeojson(duckdbState.conn!);
      const bboxResult = await duckdbState.conn!.query(`
        SELECT MIN(ST_XMin(geom)) AS xmin, MIN(ST_YMin(geom)) AS ymin,
               MAX(ST_XMax(geom)) AS xmax, MAX(ST_YMax(geom)) AS ymax
        FROM layer_01 WHERE geom IS NOT NULL
      `);
      const bboxRow = bboxResult.toArray()[0] as Record<string, number>;
      const { xmin, ymin, xmax, ymax } = bboxRow;
      if (isFinite(xmin) && isFinite(ymin) && isFinite(xmax) && isFinite(ymax)) {
        resultBounds = [xmin, ymin, xmax, ymax];
      }
      originalGeoJSON = origGeoJSON;

      const result = await runPipeline(duckdbState.conn!, distance, (stage, label) => {
        currentStage = stage;
        stageLabel = label;
      });

      resultGeoJSON = result.geojson;
      resultBounds = result.bounds ?? resultBounds;
      currentStage = 6;
      stageLabel = "Done";
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
      errorStage = e instanceof PipelineError ? (e as PipelineError).failedStage : currentStage;
      currentStage = 0;
    } finally {
      running = false;
    }
  }

  function stageStatus(idx: number): "pending" | "active" | "done" | "error" {
    const stageNum = idx + 1;
    if (errorStage > 0) {
      if (stageNum < errorStage) return "done";
      if (stageNum === errorStage) return "error";
      return "pending";
    }
    if (currentStage === 0) return "pending";
    if (currentStage === 6) return "done";
    if (stageNum < currentStage) return "done";
    if (stageNum === currentStage) return "active";
    return "pending";
  }

  function fileStem(file: File): string {
    return file.name.replace(/\.[^.]+$/, "");
  }

  function download() {
    if (!resultGeoJSON) return;
    const blob = new Blob([resultGeoJSON], { type: "application/geo+json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${fileStem(files[0])}_ee.geojson`;
    a.click();
    URL.revokeObjectURL(url);
  }

  async function handleClip() {
    clearClip?.();
    clipError = null;
    clipRunning = true;
    clipGeoJSON = null;
    try {
      clipStageLabel = "Loading clip file…";
      await loadClipFile(duckdbState.db!, duckdbState.conn!, clipFiles);
      clipStageLabel = "Clipping…";
      const result = await runClip(duckdbState.conn!);
      clipGeoJSON = result.geojson;
      resultBounds = result.bounds ?? resultBounds;
    } catch (e) {
      clipError = e instanceof Error ? e.message : String(e);
    } finally {
      clipRunning = false;
      clipStageLabel = "";
    }
  }

  function downloadClip() {
    if (!clipGeoJSON) return;
    const blob = new Blob([clipGeoJSON], { type: "application/geo+json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${fileStem(clipFiles[0])}_em.geojson`;
    a.click();
    URL.revokeObjectURL(url);
  }
</script>

<div class="layout">
  <aside class="sidebar">
    <header>
      <h1>Edge Extender</h1>
      <p class="subtitle">
        Extend polygon boundaries using Voronoi diagrams — runs entirely in your browser.
      </p>
    </header>

    <DropZone bind:files disabled={running} />

    <div class="field">
      <label for="distance">Point spacing (degrees)</label>
      <input
        id="distance"
        type="number"
        bind:value={distance}
        min="0.00001"
        step="0.0001"
        disabled={running}
      />
      <p class="field-hint">Default 0.0002 ≈ 22 m. Larger values are faster but less precise.</p>
    </div>

    {#if duckdbState.initError}
      <div class="error-panel">
        <strong>Initialisation error:</strong>
        {duckdbState.initError}
      </div>
    {/if}

    {#if currentStage > 0 || errorStage > 0}
      <ol class="stages">
        {#each STAGE_LABELS as label, i}
          {@const status = stageStatus(i)}
          <li class={status}>
            {#if status === "error"}
              <span class="stage-x">✕</span>
            {:else}
              <span class="stage-dot"></span>
            {/if}
            <span class="stage-label"
              >{i + 1 === currentStage && stageLabel ? stageLabel : label}</span
            >
          </li>
        {/each}
      </ol>
    {/if}

    {#if error}
      <div class="error-panel">{error}</div>
    {/if}

    {#if resultGeoJSON}
      <button class="download-btn" onclick={download}>Download GeoJSON</button>
    {/if}

    {#if resultGeoJSON && !running}
      <div class="clip-section">
        <h2 class="clip-heading">Edge Matching</h2>
        <p class="subtitle">Drop a clipping boundary to trim the extended result.</p>
        <DropZone bind:files={clipFiles} disabled={clipRunning} />
        {#if clipRunning}
          <p class="clip-status">{clipStageLabel}</p>
        {/if}
        {#if clipError}
          <div class="error-panel">{clipError}</div>
        {/if}
        {#if clipGeoJSON}
          <button class="download-btn" onclick={downloadClip}>Download GeoJSON (matched)</button>
        {/if}
      </div>
    {/if}

    <p class="privacy">Your files never leave your device.</p>
  </aside>

  <div class="map-container">
    <MapView
      geojson={resultGeoJSON}
      originalGeojson={originalGeoJSON}
      clipGeojson={clipGeoJSON}
      bounds={resultBounds}
      registerClear={(fn: () => void) => { clearMap = fn; }}
      registerClearClip={(fn: () => void) => { clearClip = fn; }}
    />
  </div>
</div>

<style>
  .layout {
    display: grid;
    grid-template-columns: 320px 1fr;
    height: 100dvh;
    overflow: hidden;
  }

  .sidebar {
    display: flex;
    flex-direction: column;
    gap: 1rem;
    padding: 1.25rem;
    overflow-y: auto;
    border-right: 1px solid #e5e7eb;
    background: #fff;
  }

  header h1 {
    font-size: 1.25rem;
    font-weight: 700;
    color: #111;
    margin: 0 0 0.25rem;
  }

  .subtitle {
    font-size: 0.8rem;
    color: #6b7280;
    margin: 0;
    line-height: 1.4;
  }

  .field {
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
  }

  .field label {
    font-size: 0.85rem;
    font-weight: 500;
    color: #374151;
  }

  .field input {
    padding: 0.4rem 0.6rem;
    border: 1px solid #d1d5db;
    border-radius: 6px;
    font-size: 0.875rem;
    width: 100%;
    box-sizing: border-box;
  }

  .field input:disabled {
    background: #f3f4f6;
    color: #9ca3af;
  }

  .field-hint {
    font-size: 0.75rem;
    color: #9ca3af;
    margin: 0;
  }

  .stages {
    list-style: none;
    padding: 0;
    margin: 0;
    display: flex;
    flex-direction: column;
    gap: 0.5rem;
  }

  .stages li {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    font-size: 0.85rem;
    color: #9ca3af;
  }

  .stages li.done {
    color: #16a34a;
  }

  .stages li.active {
    color: #1d4ed8;
    font-weight: 500;
  }

  .stages li.error {
    color: #dc2626;
    font-weight: 500;
  }

  .stage-x {
    width: 8px;
    font-size: 0.75rem;
    line-height: 1;
    flex-shrink: 0;
    text-align: center;
  }

  .stage-dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    background: currentColor;
    flex-shrink: 0;
  }

  .stages li.active .stage-dot {
    animation: pulse 1s ease-in-out infinite;
  }

  @keyframes pulse {
    0%,
    100% {
      opacity: 1;
    }
    50% {
      opacity: 0.3;
    }
  }

  .error-panel {
    background: #fef2f2;
    border: 1px solid #fca5a5;
    border-radius: 6px;
    padding: 0.6rem 0.75rem;
    font-size: 0.825rem;
    color: #b91c1c;
    word-break: break-word;
  }

  .download-btn {
    background: #1d4ed8;
    color: #fff;
    border: none;
    border-radius: 6px;
    padding: 0.6rem 1rem;
    font-size: 0.875rem;
    font-weight: 500;
    cursor: pointer;
    width: 100%;
  }

  .download-btn:hover {
    background: #1e40af;
  }

  .privacy {
    font-size: 0.75rem;
    color: #9ca3af;
    margin: 0;
    margin-top: auto;
  }

  .map-container {
    height: 100%;
    overflow: hidden;
  }

  .clip-section {
    display: flex;
    flex-direction: column;
    gap: 0.75rem;
    padding-top: 0.75rem;
    border-top: 1px solid #e5e7eb;
  }

  .clip-heading {
    font-size: 1rem;
    font-weight: 600;
    color: #111;
    margin: 0;
  }

  .clip-status {
    font-size: 0.825rem;
    color: #6b7280;
    margin: 0;
  }
</style>

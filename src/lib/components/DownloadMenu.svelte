<script lang="ts">
  import {
    listFormats,
    runExport,
    type ExportFormat,
    type ExportResult,
    type ExportSource,
  } from "$lib/db/export";

  let {
    primaryLabel,
    filenameStem,
    cachedGeoJSON,
    exportSource,
    disabled = false,
  }: {
    primaryLabel: string;
    filenameStem: string;
    cachedGeoJSON: string;
    exportSource: ExportSource;
    disabled?: boolean;
  } = $props();

  let rootEl: HTMLDivElement | undefined = $state();
  let caretEl: HTMLButtonElement | undefined = $state();
  let menuEl: HTMLUListElement | undefined = $state();
  let open = $state(false);
  let busy = $state(false);
  let error = $state<string | null>(null);
  let formats = $state<ExportFormat[] | null>(null);
  let formatsError = $state<string | null>(null);
  let loadingFormats = $state(false);

  const cachedGeoJSONFormat: ExportFormat = {
    id: "geojson_cached",
    label: "GeoJSON (.geojson)",
    ext: ".geojson",
    mime: "application/geo+json",
    kind: "geojson_cached",
    rank: 0,
  };

  $effect(() => {
    if (!open) return;
    const onDocMouseDown = (e: MouseEvent) => {
      if (rootEl && !rootEl.contains(e.target as Node)) {
        open = false;
      }
    };
    document.addEventListener("mousedown", onDocMouseDown);
    return () => document.removeEventListener("mousedown", onDocMouseDown);
  });

  async function ensureFormats() {
    if (formats || loadingFormats) return;
    loadingFormats = true;
    formatsError = null;
    try {
      formats = await listFormats();
    } catch (e) {
      formatsError = e instanceof Error ? e.message : String(e);
    } finally {
      loadingFormats = false;
    }
  }

  function triggerDownload({ blob, filename }: ExportResult) {
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  }

  async function handlePrimary() {
    error = null;
    try {
      const r = await runExport(
        exportSource,
        cachedGeoJSONFormat,
        filenameStem,
        cachedGeoJSON,
      );
      triggerDownload(r);
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    }
  }

  async function handleToggle() {
    open = !open;
    if (open) {
      error = null;
      await ensureFormats();
    }
  }

  async function handlePick(fmt: ExportFormat) {
    open = false;
    busy = true;
    error = null;
    try {
      const r = await runExport(exportSource, fmt, filenameStem, cachedGeoJSON);
      triggerDownload(r);
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    } finally {
      busy = false;
    }
  }

  function handleMenuKeydown(e: KeyboardEvent) {
    if (e.key === "Escape") {
      open = false;
      caretEl?.focus();
    }
  }
</script>

<div class="dl-group" bind:this={rootEl}>
  <button
    type="button"
    class="dl-btn dl-primary"
    onclick={handlePrimary}
    disabled={disabled || busy}
  >
    {primaryLabel}
  </button>
  <button
    bind:this={caretEl}
    type="button"
    class="dl-btn dl-caret"
    aria-haspopup="menu"
    aria-expanded={open}
    aria-label="More download formats"
    onclick={handleToggle}
    disabled={disabled || busy}
  >
    <svg
      viewBox="0 0 12 12"
      width="12"
      height="12"
      aria-hidden="true"
      class="dl-caret-icon"
      class:dl-caret-open={open}
    >
      <path
        d="M2 4 L6 8 L10 4"
        stroke="currentColor"
        stroke-width="1.5"
        fill="none"
        stroke-linecap="round"
        stroke-linejoin="round"
      />
    </svg>
  </button>

  {#if open}
    <ul
      bind:this={menuEl}
      class="dl-menu"
      role="menu"
      onkeydown={handleMenuKeydown}
    >
      {#if loadingFormats || (formats === null && !formatsError)}
        <li class="dl-info" role="none">Loading formats…</li>
      {:else if formatsError}
        <li class="dl-error" role="none">{formatsError}</li>
      {:else if formats}
        {#each formats as f (f.id)}
          <li role="none">
            <button
              type="button"
              role="menuitem"
              class="dl-item"
              disabled={busy}
              onclick={() => handlePick(f)}
            >
              {f.label}
            </button>
          </li>
        {/each}
      {/if}
    </ul>
  {/if}

  {#if busy}
    <p class="dl-status">Exporting…</p>
  {/if}
  {#if error}
    <p class="dl-error-line">{error}</p>
  {/if}
</div>

<style>
  .dl-group {
    position: relative;
    display: flex;
    flex-wrap: wrap;
  }

  .dl-btn {
    background: #1d4ed8;
    color: #fff;
    border: none;
    padding: 0.6rem 1rem;
    font-size: 0.875rem;
    font-weight: 500;
    cursor: pointer;
  }

  .dl-btn:hover:not(:disabled) {
    background: #1e40af;
  }

  .dl-btn:disabled {
    opacity: 0.6;
    cursor: not-allowed;
  }

  .dl-primary {
    flex: 1;
    border-radius: 6px 0 0 6px;
  }

  .dl-caret {
    padding: 0.6rem 0.5rem;
    border-radius: 0 6px 6px 0;
    border-left: 1px solid rgba(255, 255, 255, 0.25);
    display: inline-flex;
    align-items: center;
    justify-content: center;
  }

  .dl-caret-icon {
    transition: transform 0.15s ease;
  }

  .dl-caret-open {
    transform: rotate(180deg);
  }

  .dl-menu {
    position: absolute;
    right: 0;
    top: calc(100% + 4px);
    background: #fff;
    border: 1px solid #e5e7eb;
    border-radius: 6px;
    padding: 0.25rem;
    min-width: 100%;
    z-index: 10;
    box-shadow: 0 6px 16px rgba(0, 0, 0, 0.08);
    list-style: none;
    margin: 0;
  }

  .dl-item {
    width: 100%;
    text-align: left;
    background: transparent;
    border: none;
    padding: 0.5rem 0.75rem;
    font-size: 0.875rem;
    color: #111;
    cursor: pointer;
    border-radius: 4px;
  }

  .dl-item:hover:not(:disabled) {
    background: #f3f4f6;
  }

  .dl-item:disabled {
    color: #9ca3af;
    cursor: not-allowed;
  }

  .dl-info {
    color: #6b7280;
    font-size: 0.8rem;
    padding: 0.5rem 0.75rem;
  }

  .dl-error {
    color: #b91c1c;
    font-size: 0.8rem;
    padding: 0.5rem 0.75rem;
  }

  .dl-status {
    width: 100%;
    margin: 0.4rem 0 0;
    font-size: 0.8rem;
    color: #6b7280;
  }

  .dl-error-line {
    width: 100%;
    margin: 0.4rem 0 0;
    font-size: 0.8rem;
    color: #b91c1c;
    word-break: break-word;
  }
</style>

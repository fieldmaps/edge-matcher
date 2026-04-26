<script lang="ts">
  import { unzip } from "fflate";

  let {
    files = $bindable<File[]>([]),
    disabled = false,
  }: {
    files?: File[];
    disabled?: boolean;
  } = $props();

  const SINGLE_EXTS = [
    ".parquet",
    ".geojson",
    ".geojsonl",
    ".gpkg",
    ".fgb",
    ".kml",
    ".gml",
    ".gpx",
  ];
  const SHP_EXTS = [".shp", ".dbf", ".shx", ".prj", ".cpg"];

  let dragging = $state(false);

  function extOf(name: string): string {
    const i = name.lastIndexOf(".");
    return i === -1 ? "" : name.slice(i).toLowerCase();
  }

  function isIncluded(file: File): boolean {
    const e = extOf(file.name);
    return SINGLE_EXTS.includes(e) || SHP_EXTS.includes(e);
  }

  function sortKey(file: File): string {
    return (
      (file as File & { webkitRelativePath: string }).webkitRelativePath ||
      file.name
    );
  }

  function filterAndSort(fileList: File[]): File[] {
    return fileList
      .filter(isIncluded)
      .sort((a, b) => sortKey(a).localeCompare(sortKey(b)));
  }

  function summarize(fileList: File[]): string {
    const shpStems = new Map<string, string>();
    const singles: string[] = [];
    for (const f of fileList) {
      const relPath =
        (f as File & { webkitRelativePath: string }).webkitRelativePath || "";
      const lname = f.name.toLowerCase();
      if (SHP_EXTS.some((e) => lname.endsWith(e))) {
        const fullPath = relPath || f.name;
        const stem = fullPath.slice(0, fullPath.lastIndexOf(".")).toLowerCase();
        if (lname.endsWith(".shp") || !shpStems.has(stem)) {
          shpStems.set(
            stem,
            lname.endsWith(".shp") ? f.name : stem.split("/").pop()! + ".shp",
          );
        }
        continue;
      }
      singles.push(f.name);
    }
    return [...singles, ...Array.from(shpStems.values()).sort()].join(", ");
  }

  async function extractZip(file: File): Promise<File[]> {
    const data = new Uint8Array(await file.arrayBuffer());
    const entries = await new Promise<Record<string, Uint8Array>>(
      (resolve, reject) => {
        unzip(data, (err, result) => (err ? reject(err) : resolve(result)));
      },
    );
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

  async function expandZips(fileList: File[]): Promise<File[]> {
    const result: File[] = [];
    for (const file of fileList) {
      if (extOf(file.name) === ".zip") {
        result.push(...(await extractZip(file)));
      } else {
        result.push(file);
      }
    }
    return result;
  }

  async function readEntry(
    entry: FileSystemEntry,
    basePath = "",
  ): Promise<File[]> {
    if (entry.isFile) {
      const f = await new Promise<File>((resolve, reject) => {
        (entry as FileSystemFileEntry).file(resolve, reject);
      });
      const path = basePath ? `${basePath}/${f.name}` : f.name;
      const data = new Uint8Array(await f.arrayBuffer());
      const located = new File([data], f.name, {
        type: f.type,
        lastModified: f.lastModified,
      });
      Object.defineProperty(located, "webkitRelativePath", {
        value: path,
        writable: false,
        configurable: true,
        enumerable: true,
      });
      return [located];
    } else if (entry.isDirectory) {
      const newBase = basePath ? `${basePath}/${entry.name}` : entry.name;
      const reader = (entry as FileSystemDirectoryEntry).createReader();
      const entries = await new Promise<FileSystemEntry[]>((resolve) => {
        const results: FileSystemEntry[] = [];
        function readBatch() {
          reader.readEntries((batch) => {
            if (batch.length === 0) resolve(results);
            else {
              results.push(...batch);
              readBatch();
            }
          });
        }
        readBatch();
      });
      const nested = await Promise.all(
        entries.map((e) => readEntry(e, newBase)),
      );
      return nested.flat();
    }
    return [];
  }

  function handleDragOver(event: DragEvent) {
    if (disabled) return;
    event.preventDefault();
    dragging = true;
  }

  function handleDragLeave(event: DragEvent) {
    const zone = event.currentTarget as HTMLElement;
    if (!zone.contains(event.relatedTarget as Node)) dragging = false;
  }

  async function handleDrop(event: DragEvent) {
    event.preventDefault();
    if (disabled) return;
    dragging = false;
    const items = Array.from(event.dataTransfer?.items ?? []);
    const entries = items
      .map((item) => item.webkitGetAsEntry())
      .filter(Boolean) as FileSystemEntry[];
    try {
      const allFiles = (
        await Promise.all(entries.map((e) => readEntry(e)))
      ).flat();
      files = filterAndSort(await expandZips(allFiles));
    } catch (e) {
      console.error("Failed to read dropped files:", e);
    }
  }

  async function handleBrowse(event: Event) {
    const input = event.target as HTMLInputElement;
    files = filterAndSort(await expandZips(Array.from(input.files ?? [])));
  }
</script>

<div
  class="drop-zone"
  class:dragging
  class:disabled
  ondragover={handleDragOver}
  ondragleave={handleDragLeave}
  ondrop={handleDrop}
  role="region"
  aria-label="File upload drop zone"
>
  <p class="drop-message">Drop a file here</p>
  <label class="browse-label">
    <input
      type="file"
      accept={[...SINGLE_EXTS, ".json", ...SHP_EXTS, ".zip"].join(",")}
      multiple
      onchange={handleBrowse}
      {disabled}
      class="file-input"
    />
    <span class="browse-link">or browse</span>
  </label>
  <p class="hint">GeoJSON · GeoParquet · GeoPackage · Shapefile (ZIP)</p>

  {#if files.length > 0}
    <p class="file-list">
      <span class="filenames">{summarize(files)}</span>
    </p>
  {/if}
</div>

<style>
  .drop-zone {
    border: 2px dashed #9ca3af;
    border-radius: 8px;
    padding: 1.5rem 1rem;
    text-align: center;
    transition:
      border-color 0.15s,
      background-color 0.15s;
    background: #f9fafb;
  }
  .drop-zone.dragging {
    border-color: #1d4ed8;
    background: #eff6ff;
  }
  .drop-zone.disabled {
    opacity: 0.5;
    pointer-events: none;
  }
  .drop-message {
    margin: 0 0 0.25rem;
    font-size: 0.95rem;
    color: #374151;
  }
  .hint {
    margin: 0.25rem 0 0;
    font-size: 0.75rem;
    color: #9ca3af;
  }
  .browse-label {
    display: inline-block;
    cursor: pointer;
  }
  .file-input {
    position: absolute;
    opacity: 0;
    width: 0.1px;
    height: 0.1px;
    overflow: hidden;
  }
  .browse-link {
    font-size: 0.875rem;
    color: #1d4ed8;
    text-decoration: underline;
    cursor: pointer;
  }
  .browse-label:focus-within .browse-link {
    outline: 2px solid #1d4ed8;
    outline-offset: 2px;
    border-radius: 2px;
  }
  .file-list {
    margin: 0.75rem 0 0;
    font-size: 0.85rem;
    color: #374151;
  }
  .filenames {
    font-family: monospace;
    color: #111;
    word-break: break-all;
  }
</style>

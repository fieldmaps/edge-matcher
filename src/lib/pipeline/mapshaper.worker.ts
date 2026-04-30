import { Buffer } from "buffer";
import Flatbush from "flatbush";
import mapshaperSource from "mapshaper/mapshaper.js?raw";
import mproj from "mproj";

// Mapshaper's UMD loader (require$1) checks `typeof window` first, then
// `window.modules` to resolve CommonJS deps. We're in a Worker — there is
// no window — so we shim self as window and pre-populate the module
// registry with the deps `-i` + `-clean` exercise:
//   - buffer: referenced at top-level for B$3.
//   - flatbush: required by getBoundsSearchFunction inside cleanLayers.
//   - mproj: required at top-level by projection setup; mproj.internal is
//     touched during `-i` import for CRS detection even on plain GeoJSON.
// Other require$1 calls (fs, iconv-lite, geopackage, tokml, togeojson, rw,
// adm-zip, zlib) belong to file-I/O paths -clean doesn't touch.
//
// Mapshaper's runningInBrowser() also tests `typeof window.document !==
// 'undefined'`. Without a document shim, runningInBrowser returns false
// and the CLI startup path runs printStartupMessages() → process.execArgv,
// which throws ReferenceError in a Worker. The {} stub is enough.
//
// `process` is also stubbed defensively. Mapshaper has unguarded
// `process.pid` references in geopackage temp-file helpers and a few
// child-process spawn paths; -clean shouldn't reach them, but the stub
// turns any accidental hit into a benign undefined-property read instead
// of a ReferenceError.
interface ShimGlobal {
  window: typeof self;
  document: object;
  process: { env: Record<string, string>; pid: number; execArgv: string[] };
  mapshaper?: MapshaperApi;
}
const g = self as unknown as ShimGlobal;
g.window = self;
g.document = {};
g.process = { env: {}, pid: 0, execArgv: [] };
(g.window as unknown as { modules: Record<string, unknown>; document: object }).modules = {
  buffer: { Buffer },
  flatbush: Flatbush,
  mproj: mproj,
};
(g.window as unknown as { document: object }).document = g.document;

// Execute mapshaper's UMD in worker scope. The IIFE's `window.mapshaper = api`
// branch fires (because window is shimmed), populating self.mapshaper.
new Function(mapshaperSource)();

interface MapshaperApi {
  applyCommands: (
    cmd: string,
    input: Record<string, Uint8Array | string>,
    cb: (err: Error | null, output: Record<string, string | Uint8Array>) => void,
  ) => void;
}

const mapshaper = g.mapshaper as MapshaperApi | undefined;

self.onmessage = (e: MessageEvent<{ bytes: ArrayBuffer }>) => {
  if (!mapshaper) {
    self.postMessage({ error: "mapshaper failed to load in worker" });
    return;
  }
  try {
    // Wrap the transferred ArrayBuffer as a Buffer (our polyfill — zero-copy
    // view over the same memory). Mapshaper's cli.readFile detects it via
    // B$3.isBuffer and decodes to utf8 internally on import; the cache
    // entry is then deleted, so the bytes can be GC'd before -clean runs.
    const inputBuf = Buffer.from(e.data.bytes);
    mapshaper.applyCommands(
      "-i in.geojson -clean -o format=geojson out.geojson",
      { "in.geojson": inputBuf as unknown as Uint8Array },
      (err, output) => {
        if (err) {
          self.postMessage({ error: err.message || String(err) });
          return;
        }
        const keys = Object.keys(output || {});
        if (keys.length === 0) {
          self.postMessage({ error: "mapshaper produced no output" });
          return;
        }
        const raw = output[keys[0]];
        // Coerce to a transferable ArrayBuffer; encode if mapshaper handed
        // us a string. Avoid slice() unless the bytes are a sub-view —
        // slice copies, which defeats the whole point.
        let bytes: Uint8Array;
        if (typeof raw === "string") bytes = new TextEncoder().encode(raw);
        else bytes = raw;
        const isFullView = bytes.byteOffset === 0 && bytes.byteLength === bytes.buffer.byteLength;
        const ab = (
          isFullView
            ? bytes.buffer
            : bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength)
        ) as ArrayBuffer;
        self.postMessage({ bytes: ab }, { transfer: [ab] });
      },
    );
  } catch (err) {
    self.postMessage({ error: err instanceof Error ? err.message : String(err) });
  }
};

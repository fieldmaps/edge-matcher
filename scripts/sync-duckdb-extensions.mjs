#!/usr/bin/env node
// Downloads DuckDB extension .wasm files for the engine version that the
// installed @duckdb/duckdb-wasm package targets. Runs from `prebuild`.
//
// By default skips any file that already exists on disk, so repeated builds
// are fast. Pass `--force` to re-download all files (useful after bumping
// ENGINE_VERSION below).
//
// DuckDB-WASM constructs extension URLs as
//   {custom_extension_repository}/v{ENGINE_VERSION}/wasm_{PLATFORM}/{name}.duckdb_extension.wasm
// so the files must live at the matching path under public/.

import { access, mkdir, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(__dirname, "..");
const FORCE = process.argv.includes("--force");

async function exists(p) {
  try {
    await access(p);
    return true;
  } catch {
    return false;
  }
}

// Bump this together with @duckdb/duckdb-wasm in package.json.
// Find the engine version from upstream release notes or by querying
//   SELECT version() at runtime.
const ENGINE_VERSION = "v1.5.2";
const EXTENSIONS = ["spatial"];
// wasm_threads (coi) is omitted because DUCKDB_BUNDLES in
// src/lib/db/duckdb.svelte.ts deliberately excludes the coi variant —
// its OPFS data-DB path breaks structured-clone of FileSystemSyncAccessHandle.
const PLATFORMS = ["wasm_mvp", "wasm_eh"];
const UPSTREAM = "https://extensions.duckdb.org";
const TARGET = resolve(ROOT, "public/duckdb/extensions");

for (const platform of PLATFORMS) {
  for (const name of EXTENSIONS) {
    const url = `${UPSTREAM}/${ENGINE_VERSION}/${platform}/${name}.duckdb_extension.wasm`;
    const dest = resolve(TARGET, ENGINE_VERSION, platform, `${name}.duckdb_extension.wasm`);
    if (!FORCE && (await exists(dest))) {
      console.log(`cached ${ENGINE_VERSION}/${platform}/${name}`);
      continue;
    }
    process.stdout.write(`fetching ${url} ... `);
    const res = await fetch(url);
    if (!res.ok) {
      console.error(`FAILED (${res.status})`);
      process.exit(1);
    }
    const buf = new Uint8Array(await res.arrayBuffer());
    await mkdir(dirname(dest), { recursive: true });
    await writeFile(dest, buf);
    console.log(`${(buf.length / 1024).toFixed(0)} KB`);
  }
}

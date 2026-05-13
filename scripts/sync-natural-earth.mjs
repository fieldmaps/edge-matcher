#!/usr/bin/env node
// Downloads Natural Earth 1:50m land polygons into public/data/. Runs from
// `prebuild`. Skips if already present; pass `--force` to re-download.
//
// Source: martynafford/natural-earth-geojson (public domain Natural Earth
// data, pre-converted to GeoJSON). ~2.7 MB uncompressed, ~500 KB gzipped.

import { access, mkdir, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(__dirname, "..");
const FORCE = process.argv.includes("--force");

const FILES = [
  {
    url: "https://raw.githubusercontent.com/martynafford/natural-earth-geojson/master/50m/physical/ne_50m_land.json",
    dest: resolve(ROOT, "public/data/ne_50m_land.geojson"),
  },
];

async function exists(p) {
  try {
    await access(p);
    return true;
  } catch {
    return false;
  }
}

for (const { url, dest } of FILES) {
  const rel = dest.replace(ROOT + "/", "");
  if (!FORCE && (await exists(dest))) {
    console.log(`cached ${rel}`);
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
  console.log(`${(buf.length / 1024).toFixed(0)} KB → ${rel}`);
}

#!/usr/bin/env node
// Generates minimal PWA icons under public/icons/ from an inline SVG. Re-run
// after editing the SVG below. Sharp is already a transitive dep so no extra
// install is needed.

import { mkdir, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const sharp = (await import("sharp")).default;

const __dirname = dirname(fileURLToPath(import.meta.url));
const OUT = resolve(__dirname, "..", "public", "icons");

const svg = (safeZone = 0) => {
  const pad = safeZone;
  return `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512">
  <rect width="512" height="512" rx="${safeZone ? 0 : 96}" fill="#aad4e0"/>
  <g transform="translate(${pad},${pad}) scale(${(512 - 2 * pad) / 512})" fill="none" stroke="#222" stroke-width="14" stroke-linejoin="round">
    <polygon points="120,180 256,120 392,180 392,332 256,392 120,332"/>
    <polygon points="200,220 256,196 312,220 312,292 256,316 200,292" fill="#f5f5f3"/>
  </g>
</svg>`;
};

await mkdir(OUT, { recursive: true });
/** @type {Array<[number, string, number]>} */
const targets = [
  [192, "icon-192.png", 0],
  [512, "icon-512.png", 0],
  [512, "icon-maskable-512.png", 64], // ~12.5% safe-zone padding
];
for (const [size, name, safeZone] of targets) {
  const buf = await sharp(Buffer.from(svg(safeZone))).resize(size, size).png().toBuffer();
  await writeFile(resolve(OUT, name), buf);
  console.log(`wrote ${name} (${(buf.length / 1024).toFixed(1)} KB)`);
}

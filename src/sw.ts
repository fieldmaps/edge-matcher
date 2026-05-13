/// <reference lib="webworker" />
import { cleanupOutdatedCaches, precacheAndRoute } from "workbox-precaching";

declare const self: ServiceWorkerGlobalScope;

self.skipWaiting();
self.addEventListener("activate", (event) => {
  event.waitUntil(self.clients.claim());
});

// App-shell precache (injected at build time).
precacheAndRoute(self.__WB_MANIFEST);
cleanupOutdatedCaches();

// Hand-rolled runtime caching. Workbox's registerRoute() did not fire in our
// setup (likely a generateSW/RegExpRoute interaction); writing the fetch
// handler directly keeps the behaviour transparent and predictable.
//
// Both DuckDB wasm bundles are loaded from upstream: engine from jsDelivr
// (Cloudflare 25 MiB cap on static assets), spatial extension from the
// canonical extensions.duckdb.org (which DuckDB itself version-pins via the
// engine's compile-time version). Each route owns its origin policy.
const JSDELIVR = "https://cdn.jsdelivr.net";
const DUCKDB_EXT = "https://extensions.duckdb.org";
const RUNTIME_CACHES: { name: string; match: (u: URL) => boolean }[] = [
  {
    name: "duckdb-wasm",
    match: (u) =>
      u.origin === JSDELIVR &&
      /^\/npm\/@duckdb\/duckdb-wasm@[^/]+\/dist\/duckdb-(mvp|eh)\.wasm$/.test(u.pathname),
  },
  {
    name: "duckdb-extensions",
    match: (u) =>
      u.origin === DUCKDB_EXT &&
      /^\/v[\d.]+\/wasm_(mvp|eh|threads)\/.+\.duckdb_extension\.wasm$/.test(u.pathname),
  },
  {
    name: "basemap",
    match: (u) => u.origin === self.location.origin && /\/data\/.*\.geojson$/.test(u.pathname),
  },
];

self.addEventListener("fetch", (event) => {
  const { request } = event;
  if (request.method !== "GET") return;
  const url = new URL(request.url);

  const route = RUNTIME_CACHES.find((r) => r.match(url));
  if (!route) return;

  event.respondWith(
    (async () => {
      const cache = await caches.open(route.name);
      const hit = await cache.match(request);
      if (hit) return hit;
      const res = await fetch(request);
      if (res.ok || res.status === 0) {
        // Don't await — return the response immediately, cache fills in bg.
        cache.put(request, res.clone()).catch(() => {});
      }
      return res;
    })(),
  );
});

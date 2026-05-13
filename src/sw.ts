/// <reference lib="webworker" />
import { cleanupOutdatedCaches, precacheAndRoute } from "workbox-precaching";

declare const self: ServiceWorkerGlobalScope;

self.skipWaiting();
self.addEventListener("activate", (event) => {
  event.waitUntil(
    (async () => {
      // basemap URL is stable but its contents update on every build (Natural
      // Earth re-fetched from upstream master). A new SW activation means a
      // new build was deployed, so any cached body is potentially stale —
      // wipe it and let the next request re-populate. URL-versioned caches
      // (duckdb-wasm, duckdb-extensions) handle their own freshness via
      // evictOtherVersions() below.
      await caches.delete("basemap");
      await self.clients.claim();
    })(),
  );
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
//
// `versionKey` returns a stable identifier for the URL's version. After a
// cache write, entries with a different versionKey are deleted from the same
// cache — so stale URLs from prior package bumps don't accumulate. Routes
// without `versionKey` (basemap) are wiped on SW activation instead.
const JSDELIVR = "https://cdn.jsdelivr.net";
const DUCKDB_EXT = "https://extensions.duckdb.org";

interface Route {
  name: string;
  match: (u: URL) => boolean;
  versionKey?: (u: URL) => string | null;
}

const RUNTIME_CACHES: Route[] = [
  {
    name: "duckdb-wasm",
    match: (u) =>
      u.origin === JSDELIVR &&
      /^\/npm\/@duckdb\/duckdb-wasm@[^/]+\/dist\/duckdb-(mvp|eh)\.wasm$/.test(u.pathname),
    versionKey: (u) => u.pathname.match(/^\/npm\/@duckdb\/duckdb-wasm@([^/]+)\//)?.[1] ?? null,
  },
  {
    name: "duckdb-extensions",
    match: (u) =>
      u.origin === DUCKDB_EXT &&
      /^\/v[\d.]+\/wasm_(mvp|eh|threads)\/.+\.duckdb_extension\.wasm$/.test(u.pathname),
    versionKey: (u) => u.pathname.match(/^\/(v[\d.]+)\//)?.[1] ?? null,
  },
  {
    name: "basemap",
    match: (u) => u.origin === self.location.origin && /\/data\/.*\.geojson$/.test(u.pathname),
  },
];

async function evictOtherVersions(cache: Cache, route: Route, current: Request): Promise<void> {
  if (!route.versionKey) return;
  const currentVersion = route.versionKey(new URL(current.url));
  if (!currentVersion) return;
  const keys = await cache.keys();
  await Promise.all(
    keys.map(async (k) => {
      if (k.url === current.url) return;
      const v = route.versionKey!(new URL(k.url));
      if (v && v !== currentVersion) await cache.delete(k);
    }),
  );
}

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
        // Don't await — return the response immediately; cache fill + eviction
        // run in the background.
        cache
          .put(request, res.clone())
          .then(() => evictOtherVersions(cache, route, request))
          .catch(() => {});
      }
      return res;
    })(),
  );
});

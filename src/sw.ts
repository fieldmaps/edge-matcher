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
const RUNTIME_CACHES = [
  { name: "duckdb-wasm", match: /\/_astro\/duckdb-[a-z]+\.[^/]+\.wasm$/ },
  { name: "duckdb-extensions", match: /\/duckdb\/extensions\/.*\.wasm$/ },
  { name: "basemap", match: /\/data\/.*\.geojson$/ },
];

self.addEventListener("fetch", (event) => {
  const { request } = event;
  if (request.method !== "GET") return;
  const url = new URL(request.url);
  if (url.origin !== self.location.origin) return;

  const route = RUNTIME_CACHES.find((r) => r.match.test(url.pathname));
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

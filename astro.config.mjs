import svelte from "@astrojs/svelte";
import AstroPWA from "@vite-pwa/astro";
import { defineConfig } from "astro/config";
import { fileURLToPath } from "node:url";

export default defineConfig({
  integrations: [
    svelte(),
    AstroPWA({
      strategies: "injectManifest",
      srcDir: "src",
      filename: "sw.ts",
      registerType: "autoUpdate",
      injectRegister: "auto",
      // App-shell precache only — large WASM/GeoJSON are runtime-cached by
      // the hand-rolled fetch handler in src/sw.ts when the user clicks
      // "Enable offline".
      injectManifest: {
        globPatterns: ["**/*.{html,css,js,ico,svg,png,webmanifest,woff,woff2}"],
        globIgnores: ["**/duckdb/**", "**/data/**"],
        maximumFileSizeToCacheInBytes: 6 * 1024 * 1024,
      },
      manifest: {
        name: "Edge Matcher",
        short_name: "Edge Matcher",
        description: "Extend polygon boundaries with Voronoi diagrams, entirely in the browser.",
        theme_color: "#dde6ed",
        background_color: "#ffffff",
        display: "standalone",
        start_url: "/",
        icons: [
          { src: "/icons/icon-192.png", sizes: "192x192", type: "image/png" },
          { src: "/icons/icon-512.png", sizes: "512x512", type: "image/png" },
          {
            src: "/icons/icon-maskable-512.png",
            sizes: "512x512",
            type: "image/png",
            purpose: "maskable",
          },
        ],
      },
    }),
  ],
  vite: {
    resolve: {
      alias: {
        $lib: fileURLToPath(new URL("./src/lib", import.meta.url)),
      },
    },
    server: {
      headers: {
        "Cross-Origin-Opener-Policy": "same-origin",
        "Cross-Origin-Embedder-Policy": "require-corp",
      },
    },
    optimizeDeps: {
      exclude: ["@duckdb/duckdb-wasm"],
    },
    build: {
      // Astro's static SSR prunes scoped CSS for components that don't render
      // at SSR time (e.g. {#if} branches). Disabling code-split keeps every
      // component's styles in the client bundle.
      cssCodeSplit: false,
    },
  },
});

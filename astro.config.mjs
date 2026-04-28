import svelte from "@astrojs/svelte";
import { defineConfig } from "astro/config";
import { fileURLToPath } from "node:url";

import cloudflare from "@astrojs/cloudflare";

export default defineConfig({
  integrations: [svelte()],

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
  },

  adapter: cloudflare(),
});
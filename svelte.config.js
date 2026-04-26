import { relative, sep } from "node:path";

/** @type {import('@sveltejs/vite-plugin-svelte').SvelteConfig} */
const config = {
  compilerOptions: {
    runes: ({ filename }) => {
      const relativePath = relative(import.meta.dirname, filename);
      const pathSegments = relativePath.toLowerCase().split(sep);
      return pathSegments.includes("node_modules") ? undefined : true;
    },
  },
};

export default config;

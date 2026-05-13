export interface Tool {
  slug: string;
  name: string;
  tagline: string;
  iconHref: string;
}

export const tools: Tool[] = [
  {
    slug: "extend",
    name: "Edge Extender",
    tagline:
      "Fill gaps between adjacent polygons by extending boundaries outward with Voronoi diagrams.",
    iconHref: "/icons/tools/edge-extender.svg",
  },
];

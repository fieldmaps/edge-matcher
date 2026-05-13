<script lang="ts">
  import type {
    FilterSpecification,
    GeoJSONSource,
    LayerSpecification,
    Map as MaplibreMap,
    StyleSpecification,
  } from "maplibre-gl";
  import "maplibre-gl/dist/maplibre-gl.css";
  import { onDestroy, onMount } from "svelte";

  let {
    geojson = null,
    originalGeojson = null,
    clipGeojson = null,
    bounds = null,
    registerClear = undefined,
    registerClearClip = undefined,
  }: {
    geojson?: string | null;
    originalGeojson?: string | null;
    clipGeojson?: string | null;
    bounds?: [number, number, number, number] | null;
    registerClear?: (fn: () => void) => void;
    registerClearClip?: (fn: () => void) => void;
  } = $props();

  let container: HTMLDivElement | undefined;
  let map: MaplibreMap | undefined;
  let blobUrl: string | undefined;
  let origBlobUrl: string | undefined;
  let clipBlobUrl: string | undefined;
  let spinning = false;
  let animFrame: number | undefined;
  let lastTime: number | undefined;
  const SPIN_SPEED = 6;

  function spinStep(timestamp: number) {
    if (!spinning || !map) return;
    if (lastTime !== undefined) {
      const delta = (timestamp - lastTime) / 1000;
      const center = map.getCenter();
      center.lng -= SPIN_SPEED * delta;
      map.setCenter(center);
    }
    lastTime = timestamp;
    animFrame = requestAnimationFrame(spinStep);
  }

  function startSpin() {
    spinning = true;
    lastTime = undefined;
    animFrame = requestAnimationFrame(spinStep);
  }

  function stopSpin() {
    if (!spinning) return;
    spinning = false;
    if (animFrame !== undefined) cancelAnimationFrame(animFrame);
  }
  const polyFilter: FilterSpecification = [
    "match",
    ["geometry-type"],
    ["Polygon", "MultiPolygon"],
    true,
    false,
  ];

  const lineWidth = ["interpolate", ["linear"], ["zoom"], 4, 0.2, 10, 0.6, 14, 1] as unknown as number;

  // Dedicated effect for bounds — fires whenever bounds changes, independent of data effects.
  $effect(() => {
    const b = bounds;
    if (!b) return;
    if (!map) return;
    function apply() {
      if (!map || !b) return;
      const [minLng, minLat, maxLng, maxLat] = b;
      stopSpin();
      map.fitBounds([[minLng, minLat], [maxLng, maxLat]], { padding: 40, animate: true });
    }
    if (map.isStyleLoaded()) apply();
    else map.once("load", apply);
  });

  $effect(() => {
    const orig = originalGeojson;
    if (!orig || !map) return;

    if (origBlobUrl) URL.revokeObjectURL(origBlobUrl);
    origBlobUrl = URL.createObjectURL(new Blob([orig], { type: "application/json" }));
    const oUrl = origBlobUrl;

    function apply() {
      if (!map) return;
      if (map.getSource("original")) {
        (map.getSource("original") as GeoJSONSource).setData(oUrl);
      } else {
        map.addSource("original", { type: "geojson", data: oUrl });
        map.addLayer({ id: "original-fill", type: "fill", source: "original", filter: polyFilter, paint: { "fill-color": "#8dc65a", "fill-opacity": 1 } });
        map.addLayer({ id: "original-line", type: "line", source: "original", paint: { "line-color": "#222222", "line-width": lineWidth } });
      }
    }

    if (map.isStyleLoaded()) apply();
    else map.once("load", apply);
  });

  $effect(() => {
    const result = geojson;
    if (!result || !map) return;

    if (blobUrl) URL.revokeObjectURL(blobUrl);
    blobUrl = URL.createObjectURL(new Blob([result], { type: "application/json" }));
    const rUrl = blobUrl;

    function apply() {
      if (!map) return;
      // Insert result layers below original if original is already shown
      const before = map.getLayer("original-fill") ? "original-fill" : undefined;
      if (map.getSource("result")) {
        (map.getSource("result") as GeoJSONSource).setData(rUrl);
      } else {
        map.addSource("result", { type: "geojson", data: rUrl });
        map.addLayer({ id: "result-fill", type: "fill", source: "result", filter: polyFilter, paint: { "fill-color": "#aad4e0", "fill-opacity": 1 } }, before);
        map.addLayer({ id: "result-line", type: "line", source: "result", paint: { "line-color": "#222222", "line-width": lineWidth } }, before);
      }
    }

    if (map.isStyleLoaded()) apply();
    else map.once("load", () => apply());
  });

  // Local fallback: solid water + land polygons over a Natural Earth GeoJSON.
  // Used both when offline (style fetch fails) and as the bedrock layer
  // underneath OpenFreeMap when online — so if any tile request fails, the
  // user still sees land vs water rather than blank background.
  const LAND_SOURCE_ID = "ne-land";
  const LAND_LAYER_ID = "ne-land-fill";
  const LAND_URL = "/data/ne_50m_land.geojson";
  const WATER_COLOR = "#dde6ed";
  const LAND_COLOR = "#f5f5f3";

  const fallbackStyle: StyleSpecification = {
    version: 8,
    projection: { type: "globe" },
    sources: {
      [LAND_SOURCE_ID]: { type: "geojson", data: LAND_URL },
    },
    layers: [
      { id: "background", type: "background", paint: { "background-color": WATER_COLOR } },
      {
        id: LAND_LAYER_ID,
        type: "fill",
        source: LAND_SOURCE_ID,
        paint: { "fill-color": LAND_COLOR },
      },
    ],
  };

  async function loadStyle(): Promise<StyleSpecification> {
    try {
      const remote = (await fetch("https://tiles.openfreemap.org/styles/positron").then((r) =>
        r.json(),
      )) as StyleSpecification & { sources: Record<string, unknown> };
      remote.projection = { type: "globe" };
      remote.sources[LAND_SOURCE_ID] = { type: "geojson", data: LAND_URL };
      const landLayer: LayerSpecification = {
        id: LAND_LAYER_ID,
        type: "fill",
        source: LAND_SOURCE_ID,
        paint: { "fill-color": LAND_COLOR },
      };
      // Insert just after the first background layer so OpenFreeMap's own
      // land/water/road layers paint on top of our bedrock when online.
      const bgIdx = remote.layers.findIndex((l) => l.type === "background");
      const insertAt = bgIdx >= 0 ? bgIdx + 1 : 0;
      remote.layers.splice(insertAt, 0, landLayer);
      return remote;
    } catch {
      return fallbackStyle;
    }
  }

  onMount(async () => {
    if (!container) return;
    const maplibregl = await import("maplibre-gl");
    const style = await loadStyle();
    const size = Math.min(container.clientWidth, container.clientHeight);
    map = new maplibregl.Map({
      container,
      style,
      center: [20, 5],
      zoom: Math.log2((size * Math.PI) / 512),
      attributionControl: { compact: true },
    });
    map.once("load", () => {
      startSpin();
      map.on("mousedown", stopSpin);
      map.on("touchstart", stopSpin);
      map.on("wheel", stopSpin);
      registerClear?.(() => {
        if (!map) return;
        const layers = ["original-fill", "original-line", "result-fill", "result-line", "clip-fill", "clip-line"];
        const sources = ["original", "result", "clip"];
        for (const layer of layers) {
          if (map.getLayer(layer)) map.removeLayer(layer);
        }
        for (const source of sources) {
          if (map.getSource(source)) map.removeSource(source);
        }
      });
      registerClearClip?.(() => {
        if (!map) return;
        if (map.getLayer("clip-fill")) map.removeLayer("clip-fill");
        if (map.getLayer("clip-line")) map.removeLayer("clip-line");
        if (map.getSource("clip")) map.removeSource("clip");
      });
    });
  });

  $effect(() => {
    const clip = clipGeojson;
    if (!clip || !map) return;

    if (clipBlobUrl) URL.revokeObjectURL(clipBlobUrl);
    clipBlobUrl = URL.createObjectURL(new Blob([clip], { type: "application/json" }));
    const cUrl = clipBlobUrl;

    function apply() {
      if (!map) return;
      const before = map.getLayer("original-fill") ? "original-fill" : undefined;
      if (map.getSource("clip")) {
        (map.getSource("clip") as GeoJSONSource).setData(cUrl);
      } else {
        map.addSource("clip", { type: "geojson", data: cUrl });
        map.addLayer({ id: "clip-fill", type: "fill", source: "clip", filter: polyFilter, paint: { "fill-color": "#FB9A99", "fill-opacity": 1 } }, before);
        map.addLayer({ id: "clip-line", type: "line", source: "clip", paint: { "line-color": "#222222", "line-width": lineWidth } }, before);
      }
    }

    if (map.isStyleLoaded()) apply();
    else map.once("load", () => apply());
  });

  onDestroy(() => {
    stopSpin();
    map?.remove();
    if (blobUrl) URL.revokeObjectURL(blobUrl);
    if (origBlobUrl) URL.revokeObjectURL(origBlobUrl);
    if (clipBlobUrl) URL.revokeObjectURL(clipBlobUrl);
  });
</script>

<div bind:this={container} class="map"></div>

<style>
  .map {
    width: 100%;
    height: 100%;
  }
</style>

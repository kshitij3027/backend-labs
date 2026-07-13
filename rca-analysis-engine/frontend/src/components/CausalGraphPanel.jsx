import { useEffect, useMemo, useRef, useState } from "react";
import {
  levelClass,
  severityColor,
  severitySize,
  formatOffset,
  truncate,
} from "../util.js";

// Interactive Plotly causal-graph network plot (C12).
//
// A thin wrapper over the CDN-loaded `window.Plotly` global (see index.html) — no Plotly
// in the npm bundle. Given the selected incident's serialized `causal_graph`
// ({nodes:[{id,service,level,message,timestamp}], edges:[{source,target,strength}]}) it:
//
//   * lays nodes out DETERMINISTICALLY — x = seconds from the incident start (parsed from
//     the node timestamps; falls back to chronological order when timestamps collapse),
//     y = a per-service lane with a small sawtooth jitter so simultaneous same-service
//     events don't stack. Causal edges always run forward in time, so the graph reads
//     left-to-right with no layout library;
//   * draws edges as line segments whose width scales with causal `strength` (bucketed
//     into weak / medium / strong tiers);
//   * draws nodes with marker SIZE and COLOUR keyed to severity (CRITICAL largest/reddest
//     -> INFO smallest/bluest), root-cause nodes ringed, hover = full event detail;
//   * on click, BFS-walks the edges from the clicked node to its downstream reachable set
//     and highlights that blast radius (dimming the rest), surfacing the count. The
//     selection is lifted to the parent (`focusNodeId` / `onFocusNode`) so clicking a
//     root cause in the RootCausesPanel highlights the same cone here, and vice-versa.
//
// Props:
//   incident    — the selected IncidentReport (or null)
//   focusNodeId — the currently highlighted node id (shared with RootCausesPanel), or null
//   onFocusNode — (event_id) => void; parent toggles the highlight (same id again clears)

// Edge width tiers by causal strength (clamped [0.1, 1.0] upstream). A few discrete
// widths read more clearly than a continuous scale and let us batch edges into a handful
// of traces instead of one-per-edge.
const EDGE_TIERS = [
  { key: "strong", width: 3.4, test: (s) => s >= 0.75 },
  { key: "medium", width: 2.2, test: (s) => s >= 0.45 && s < 0.75 },
  { key: "weak", width: 1.2, test: (s) => s < 0.45 },
];

const EDGE_COLOR_NORMAL = "rgba(122, 138, 165, 0.34)";
const EDGE_COLOR_DIM = "rgba(122, 138, 165, 0.08)";
const EDGE_COLOR_HILITE = "rgba(107, 163, 255, 0.85)";

const PLOTLY_CONFIG = { responsive: true, displayModeBar: false };

/** The severity buckets shown in the legend, most severe first. */
const LEGEND_LEVELS = ["CRITICAL", "ERROR", "WARNING", "INFO"];

/** Best-effort parse of a node timestamp (ISO string or epoch s/ms) to epoch ms; NaN if unparseable. */
function parseMillis(value) {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value < 1e12 ? value * 1000 : value;
  }
  const t = Date.parse(value);
  return Number.isFinite(t) ? t : NaN;
}

/**
 * Deterministic 2D layout for the causal graph. Returns per-node positions plus the
 * ordered service lanes. Same incident -> same positions (no randomness, no wall clock),
 * so the plot is stable across re-renders and live WebSocket refreshes.
 */
function computeLayout(graph) {
  const nodes = Array.isArray(graph?.nodes) ? graph.nodes : [];
  const edges = Array.isArray(graph?.edges) ? graph.edges : [];

  // Service lanes in first-appearance order (nodes arrive chronological from the backend),
  // giving each distinct service a stable integer row.
  const laneOf = new Map();
  const services = [];
  for (const n of nodes) {
    if (!laneOf.has(n.service)) {
      laneOf.set(n.service, services.length);
      services.push(n.service);
    }
  }

  // Parse timestamps once; use a real seconds axis only when there's a positive spread,
  // otherwise fall back to chronological index so nodes never all collapse onto x=0.
  const millis = nodes.map((n) => parseMillis(n.timestamp));
  const finite = millis.filter(Number.isFinite);
  const tMin = finite.length ? Math.min(...finite) : 0;
  const tMax = finite.length ? Math.max(...finite) : 0;
  const usableTime = tMax - tMin > 0;

  const laneFill = new Map(); // lane -> running count, for the jitter sawtooth
  const pos = new Map(); // id -> { x, y, sec }
  nodes.forEach((n, i) => {
    const lane = laneOf.get(n.service) ?? 0;
    const k = laneFill.get(lane) ?? 0;
    laneFill.set(lane, k + 1);

    const hasTime = usableTime && Number.isFinite(millis[i]);
    const sec = hasTime ? (millis[i] - tMin) / 1000 : NaN;
    const x = hasTime ? sec : i;
    // Sawtooth jitter in {-0.22,-0.11,0,+0.11,+0.22} keeps same-lane events from stacking
    // while staying well inside the 1.0 gap between lanes.
    const jitter = ((k % 5) - 2) * 0.11;
    pos.set(n.id, { x, y: lane + jitter, sec });
  });

  return { nodes, edges, services, pos, usableTime };
}

/** source -> [targets] adjacency for the downstream BFS. */
function buildAdjacency(edges) {
  const adj = new Map();
  for (const e of edges) {
    if (!adj.has(e.source)) adj.set(e.source, []);
    adj.get(e.source).push(e.target);
  }
  return adj;
}

/** Downstream reachable set from `startId` (inclusive) via BFS over `adj`; empty for null. */
function reachableFrom(startId, adj) {
  const seen = new Set();
  if (startId == null) return seen;
  seen.add(startId);
  const stack = [startId];
  while (stack.length) {
    const cur = stack.pop();
    for (const next of adj.get(cur) ?? []) {
      if (!seen.has(next)) {
        seen.add(next);
        stack.push(next);
      }
    }
  }
  return seen;
}

/** Build the Plotly `data` traces (edges first, nodes on top) for the current highlight. */
function buildTraces({ nodes, edges, pos }, rootIds, highlight, focusId) {
  const active = highlight.size > 0;

  // One edge trace per (width tier x visual state): null-separated segments share a trace.
  const buckets = new Map(); // `${tier}:${state}` -> { x:[], y:[], width, color }
  const bucketFor = (tier, state, color) => {
    const key = `${tier.key}:${state}`;
    let b = buckets.get(key);
    if (!b) {
      b = { x: [], y: [], width: tier.width, color };
      buckets.set(key, b);
    }
    return b;
  };

  for (const e of edges) {
    const a = pos.get(e.source);
    const b = pos.get(e.target);
    if (!a || !b) continue;
    const strength = Number(e.strength);
    const tier = EDGE_TIERS.find((t) => t.test(strength)) ?? EDGE_TIERS[2];

    let state = "normal";
    let color = EDGE_COLOR_NORMAL;
    if (active) {
      const on = highlight.has(e.source) && highlight.has(e.target);
      state = on ? "hi" : "dim";
      color = on ? EDGE_COLOR_HILITE : EDGE_COLOR_DIM;
    }
    const bucket = bucketFor(tier, state, color);
    bucket.x.push(a.x, b.x, null);
    bucket.y.push(a.y, b.y, null);
  }

  const edgeTraces = [];
  for (const b of buckets.values()) {
    edgeTraces.push({
      type: "scatter",
      mode: "lines",
      x: b.x,
      y: b.y,
      line: { width: b.width, color: b.color, shape: "spline" },
      hoverinfo: "skip",
      showlegend: false,
    });
  }

  // Single node trace with per-point arrays so a re-render only re-colours/re-sizes.
  const x = [];
  const y = [];
  const sizes = [];
  const colors = [];
  const opacities = [];
  const lineWidths = [];
  const lineColors = [];
  const texts = [];
  const hovertexts = [];
  const customdata = [];

  for (const n of nodes) {
    const p = pos.get(n.id);
    if (!p) continue;
    const isRoot = rootIds.has(n.id);
    const isFocus = n.id === focusId;
    const inHi = highlight.has(n.id);

    x.push(p.x);
    y.push(p.y);
    sizes.push(severitySize(n.level));
    colors.push(severityColor(n.level));
    opacities.push(active ? (inHi ? 1 : 0.16) : 1);

    // Focus node gets the brightest ring; root causes a gold ring; others a faint edge.
    if (isFocus) {
      lineWidths.push(3.5);
      lineColors.push("#e6edf3");
    } else if (isRoot) {
      lineWidths.push(2.5);
      lineColors.push("#ffd166");
    } else {
      lineWidths.push(1);
      lineColors.push("rgba(12, 17, 22, 0.85)");
    }

    // Declutter: when a blast radius is highlighted, only label the highlighted nodes.
    texts.push(!active || inHi ? truncate(String(n.service ?? ""), 14) : "");
    const offset = Number.isFinite(p.sec) ? formatOffset(p.sec) : String(n.timestamp ?? "");
    hovertexts.push(
      `${n.service ?? "—"} · ${String(n.level ?? "").toUpperCase()} · ${offset}` +
        `<br>${truncate(String(n.message ?? ""), 90)}`
    );
    customdata.push(n.id);
  }

  const nodeTrace = {
    type: "scatter",
    mode: "markers+text",
    x,
    y,
    text: texts,
    textposition: "top center",
    textfont: { size: 9, color: "#9aa7b4" },
    customdata,
    hovertext: hovertexts,
    hoverinfo: "text",
    cliponaxis: false,
    marker: {
      size: sizes,
      color: colors,
      opacity: opacities,
      line: { width: lineWidths, color: lineColors },
    },
    showlegend: false,
  };

  return [...edgeTraces, nodeTrace];
}

/** Plotly layout: dark, chromeless, service lanes as (unlabelled) rows, time on x. */
function buildLayout({ services, usableTime }) {
  const laneMax = Math.max(0, services.length - 1);
  return {
    autosize: true,
    margin: { l: 20, r: 16, t: 8, b: 34 },
    paper_bgcolor: "rgba(0,0,0,0)",
    plot_bgcolor: "rgba(0,0,0,0)",
    hovermode: "closest",
    showlegend: false,
    dragmode: "pan",
    font: { family: "system-ui, sans-serif", color: "#9aa7b4" },
    xaxis: {
      title: {
        text: usableTime ? "seconds since incident start" : "event order",
        font: { size: 10, color: "#6b7785" },
      },
      color: "#9aa7b4",
      gridcolor: "rgba(42, 51, 64, 0.55)",
      zeroline: false,
      showline: false,
      ticks: "",
      tickfont: { size: 10, color: "#6b7785" },
    },
    yaxis: {
      // Reversed so the first-seen service lane sits at the top; padded for the jitter.
      range: [laneMax + 0.6, -0.6],
      showgrid: false,
      zeroline: false,
      showline: false,
      showticklabels: false,
      ticks: "",
    },
    hoverlabel: {
      bgcolor: "#0c1116",
      bordercolor: "#2a3340",
      font: { color: "#e6edf3", size: 12 },
      align: "left",
    },
  };
}

export default function CausalGraphPanel({ incident, focusNodeId, onFocusNode }) {
  const plotRef = useRef(null);

  // Guard the CDN global. It's a synchronous <script> in <head>, so it's normally ready
  // before React mounts; the retry only matters if the CDN was slow / blocked.
  const [plotlyReady, setPlotlyReady] = useState(
    () => typeof window !== "undefined" && !!window.Plotly
  );
  useEffect(() => {
    if (plotlyReady) return undefined;
    const t = setTimeout(() => setPlotlyReady(!!window.Plotly), 500);
    return () => clearTimeout(t);
  }, [plotlyReady]);

  const graph = incident?.causal_graph;
  const layoutData = useMemo(() => computeLayout(graph), [graph]);
  const adjacency = useMemo(() => buildAdjacency(layoutData.edges), [layoutData]);
  const rootIds = useMemo(
    () => new Set((incident?.root_causes ?? []).map((r) => r.event_id)),
    [incident]
  );

  const hasNodes = layoutData.nodes.length > 0;
  const hasEdges = layoutData.edges.length > 0;

  // Only treat a focus id as active if it actually names a node in THIS incident (a stale
  // selection after a live update simply clears).
  const effectiveFocus =
    focusNodeId != null && layoutData.pos.has(focusNodeId) ? focusNodeId : null;
  const highlight = useMemo(
    () => reachableFrom(effectiveFocus, adjacency),
    [effectiveFocus, adjacency]
  );

  // (Re)draw whenever the incident, the highlight, or Plotly-readiness changes.
  useEffect(() => {
    const el = plotRef.current;
    const Plotly = typeof window !== "undefined" ? window.Plotly : null;
    if (!el || !Plotly || !hasNodes) return undefined;

    const data = buildTraces(layoutData, rootIds, highlight, effectiveFocus);
    const layout = buildLayout(layoutData);
    Plotly.react(el, data, layout, PLOTLY_CONFIG);

    const handleClick = (ev) => {
      const point = ev?.points?.[0];
      const id = point?.customdata;
      if (id != null && typeof onFocusNode === "function") onFocusNode(id);
    };
    // Plotly augments the div with an EventEmitter after the first plot; rebind cleanly.
    if (typeof el.removeAllListeners === "function") el.removeAllListeners("plotly_click");
    if (typeof el.on === "function") el.on("plotly_click", handleClick);

    return () => {
      if (typeof el.removeAllListeners === "function") {
        el.removeAllListeners("plotly_click");
      }
    };
  }, [plotlyReady, hasNodes, layoutData, rootIds, highlight, effectiveFocus, onFocusNode]);

  // Tear the plot down on unmount so Plotly's resize listener doesn't leak.
  useEffect(() => {
    const el = plotRef.current;
    return () => {
      if (el && typeof window !== "undefined" && window.Plotly) window.Plotly.purge(el);
    };
  }, []);

  const nodeCount = layoutData.nodes.length;
  const edgeCount = layoutData.edges.length;
  const focusService = effectiveFocus ? findService(layoutData.nodes, effectiveFocus) : null;
  const blastCount = effectiveFocus ? Math.max(0, highlight.size - 1) : 0;

  return (
    <section className="panel graphpanel" data-testid="causal-graph-panel">
      <div className="panel__head">
        <h2 className="panel__title">Causal Graph</h2>
        <span className="panel__count">
          {nodeCount} node{nodeCount === 1 ? "" : "s"} · {edgeCount} edge
          {edgeCount === 1 ? "" : "s"}
        </span>
      </div>

      {!plotlyReady ? (
        <p className="placeholder__note">
          The Plotly library could not be loaded from the CDN, so the interactive causal
          graph is unavailable. Root causes and impact are still shown alongside.
        </p>
      ) : !hasNodes ? (
        <p className="placeholder__note">
          No causal graph for this incident — no events formed causal nodes.
        </p>
      ) : (
        <>
          <div className="graphpanel__bar">
            {effectiveFocus ? (
              <>
                <span className="graphpanel__blast">
                  <span className="graphpanel__blast-svc">{focusService}</span> ·{" "}
                  <strong>{blastCount}</strong> downstream event
                  {blastCount === 1 ? "" : "s"} in blast radius
                </span>
                <button
                  type="button"
                  className="btn btn--ghost"
                  onClick={() => onFocusNode?.(effectiveFocus)}
                >
                  Reset
                </button>
              </>
            ) : (
              <span className="graphpanel__hint">
                Click a node to trace its downstream blast radius
              </span>
            )}
          </div>

          <div ref={plotRef} className="graphpanel__plot" data-testid="causal-graph-plot" />

          {!hasEdges && (
            <p className="graphpanel__note">
              No causal edges — these events did not form a propagation chain.
            </p>
          )}

          <ul className="graphlegend" aria-label="Node size and colour = severity">
            {LEGEND_LEVELS.map((lvl) => (
              <li key={lvl} className="graphlegend__item">
                <span
                  className={`graphlegend__swatch dot--${levelClass(lvl)}`}
                  style={{
                    width: `${severitySize(lvl) / 2 + 3}px`,
                    height: `${severitySize(lvl) / 2 + 3}px`,
                    background: severityColor(lvl),
                  }}
                  aria-hidden="true"
                />
                {lvl}
              </li>
            ))}
            <li className="graphlegend__item graphlegend__item--root">
              <span className="graphlegend__ring" aria-hidden="true" /> root cause
            </li>
          </ul>
        </>
      )}
    </section>
  );
}

/** Service name for a node id (small linear scan; node counts are incident-sized). */
function findService(nodes, id) {
  const n = nodes.find((node) => node.id === id);
  return n ? n.service : id;
}

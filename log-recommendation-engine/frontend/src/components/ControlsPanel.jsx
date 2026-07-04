import { useEffect, useMemo, useState } from "react";

// The runtime knobs this panel exposes as sliders. Each entry: [key, label, hint].
// All five live in the closed unit interval [0, 1] with a 0.05 step (weights are
// technically uncapped server-side, but 0–1 is the useful tuning band and matches
// the other four ranges, so the UI keeps them consistent). Ordering follows the
// scoring blend: the three signal weights first, then the exploration / diversity
// dials that reshape the final ordering.
const SLIDERS = [
  ["weight_semantic", "Semantic weight", "embedding similarity"],
  ["weight_contextual", "Contextual weight", "service / severity / tags / recency"],
  ["weight_feedback", "Feedback weight", "learned helpful / unhelpful signal"],
  ["epsilon_explore", "Explore ε", "chance of surfacing a wildcard match"],
  ["diversity_threshold", "Diversity", "how aggressively to de-duplicate near-dupes"],
];

const MIN = 0;
const MAX = 1;
const STEP = 0.05;

// Snap a raw slider value into the [0,1] grid so the compare below is exact and we
// never PUT a value like 0.30000000000000004.
function snap(v) {
  const x = Number(v);
  if (!Number.isFinite(x)) return 0;
  const clamped = Math.min(MAX, Math.max(MIN, x));
  return Math.round(clamped / STEP) * STEP;
}

function toFixed2(v) {
  const x = Number(v);
  return Number.isFinite(x) ? x.toFixed(2) : "—";
}

// Feature Area B controls (C17): retune the ranking knobs at runtime (PUT /config,
// no restart). Sliders are LOCAL until Apply — we don't PUT on every drag; one Apply
// pushes only the fields that actually changed. On success the parent re-runs the
// last query so the new weighting is visible, and re-seeds `config` (bumping the
// version). A 422 (out-of-range / unknown key) surfaces its `detail` inline.
//
// Props:
//   config   — the effective ConfigResponse.config map (source of truth), or null
//   version  — the config version (shown; changes on every successful Apply)
//   onApply(updates) -> Promise: PUTs the diff, re-runs the query; may throw
export default function ControlsPanel({ config, version, onApply }) {
  // Local slider state, keyed by tunable name. Seeded from `config`.
  const [vals, setVals] = useState({});
  const [applying, setApplying] = useState(false);
  const [msg, setMsg] = useState(null);

  // The server-side baseline we diff against (snapped so equality is exact).
  const baseline = useMemo(() => {
    const b = {};
    for (const [key] of SLIDERS) {
      const raw = config ? config[key] : undefined;
      if (Number.isFinite(Number(raw))) b[key] = snap(raw);
    }
    return b;
  }, [config]);

  // Re-seed the sliders whenever the upstream config changes (mount, or after an
  // Apply bumps the version). Only overwrites keys we know — leaves nothing stale.
  useEffect(() => {
    setVals((prev) => ({ ...prev, ...baseline }));
  }, [baseline]);

  // Which keys were moved off their server baseline — the minimal PUT payload.
  const changed = useMemo(() => {
    const out = {};
    for (const [key] of SLIDERS) {
      const now = vals[key];
      const was = baseline[key];
      if (Number.isFinite(Number(now)) && snap(now) !== was) {
        out[key] = snap(now);
      }
    }
    return out;
  }, [vals, baseline]);

  const dirty = Object.keys(changed).length > 0;

  function reset() {
    setVals((prev) => ({ ...prev, ...baseline }));
    setMsg(null);
  }

  async function handleApply() {
    if (!dirty || applying) return;
    setApplying(true);
    setMsg(null);
    try {
      await onApply(changed); // parent PUTs + re-runs the last query
      setMsg({ ok: true, text: "Applied — ranking retuned. Re-ran your query." });
    } catch (e) {
      // e.message already carries the backend 422 `detail` (via postJSON).
      setMsg({ ok: false, text: e?.message || "Failed to apply config." });
    } finally {
      setApplying(false);
    }
  }

  return (
    <section className="card controls">
      <div className="card__head">
        <h2 className="card__title">Ranking controls</h2>
        <span className="card__hint">
          {version != null ? `config v${version}` : "runtime · no restart"}
        </span>
      </div>

      {!config ? (
        <div className="empty">Config unavailable — controls disabled.</div>
      ) : (
        <>
          <div className="controls__list">
            {SLIDERS.map(([key, label, hint]) => {
              const v = Number.isFinite(Number(vals[key])) ? Number(vals[key]) : 0;
              const isChanged = key in changed;
              return (
                <div className="control" key={key}>
                  <div className="control__top">
                    <label className="control__label" htmlFor={`ctl-${key}`}>
                      {label}
                    </label>
                    <span
                      className={`control__val ${isChanged ? "control__val--dirty" : ""}`}
                    >
                      {toFixed2(v)}
                    </span>
                  </div>
                  <input
                    id={`ctl-${key}`}
                    className="control__range"
                    type="range"
                    min={MIN}
                    max={MAX}
                    step={STEP}
                    value={v}
                    onChange={(e) =>
                      setVals((s) => ({ ...s, [key]: Number(e.target.value) }))
                    }
                  />
                  <span className="control__hint">{hint}</span>
                </div>
              );
            })}
          </div>

          <div className="row controls__actions">
            <button onClick={handleApply} disabled={!dirty || applying}>
              {applying ? "Applying…" : "Apply"}
            </button>
            <button
              type="button"
              className="ghost"
              onClick={reset}
              disabled={!dirty || applying}
            >
              Reset
            </button>
            {dirty && !applying && (
              <span className="controls__pending">
                {Object.keys(changed).length} unsaved
              </span>
            )}
          </div>

          {msg && (
            <div className={`save-msg ${msg.ok ? "save-msg--ok" : "save-msg--err"}`}>
              {msg.text}
            </div>
          )}
        </>
      )}
    </section>
  );
}

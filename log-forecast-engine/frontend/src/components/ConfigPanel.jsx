import { useEffect, useState } from "react";
import { putConfig, postRetrain } from "../api.js";

// Feature Area B controls: adjust confidence thresholds and per-model weights at
// runtime (PUT /config, no restart) and trigger a retrain (POST /retrain) for
// the selected metric. Reflects the current /config values and gives inline
// validation feedback (high > medium; both in [0,1]; weights non-negative).
export default function ConfigPanel({ config, metric, onSaved }) {
  const [high, setHigh] = useState(0.85);
  const [medium, setMedium] = useState(0.65);
  const [weights, setWeights] = useState({});
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState(null);
  const [retraining, setRetraining] = useState(false);

  // Re-seed local form state whenever the upstream config changes.
  useEffect(() => {
    if (!config) return;
    if (Number.isFinite(Number(config.high_confidence_threshold))) {
      setHigh(Number(config.high_confidence_threshold));
    }
    if (Number.isFinite(Number(config.medium_confidence_threshold))) {
      setMedium(Number(config.medium_confidence_threshold));
    }
    setWeights({ ...(config.model_weights || {}) });
  }, [config]);

  function clientValidate() {
    if (high < 0 || high > 1 || medium < 0 || medium > 1) {
      return "thresholds must be between 0 and 1";
    }
    if (high <= medium) {
      return "high threshold must be greater than medium";
    }
    for (const [, w] of Object.entries(weights)) {
      if (Number(w) < 0) return "weights must be non-negative";
    }
    return null;
  }

  async function handleSave() {
    const err = clientValidate();
    if (err) {
      setMsg({ ok: false, text: err });
      return;
    }
    setSaving(true);
    setMsg(null);
    try {
      const numericWeights = {};
      for (const [k, v] of Object.entries(weights)) {
        numericWeights[k] = Number(v);
      }
      const updated = await putConfig({
        high_confidence_threshold: Number(high),
        medium_confidence_threshold: Number(medium),
        model_weights: numericWeights,
      });
      setMsg({ ok: true, text: "Config saved (applied without restart)." });
      if (onSaved) onSaved(updated);
    } catch (e) {
      setMsg({ ok: false, text: e.message || "Save failed." });
    } finally {
      setSaving(false);
    }
  }

  async function handleRetrain() {
    setRetraining(true);
    setMsg(null);
    try {
      const res = await postRetrain(metric);
      setMsg({
        ok: true,
        text: `Retrain ${res.status || "scheduled"} (${res.mode || "?"}) for ${metric}.`,
      });
    } catch (e) {
      setMsg({ ok: false, text: e.message || "Retrain failed." });
    } finally {
      setRetraining(false);
    }
  }

  const weightNames = Object.keys(weights);

  return (
    <section className="card">
      <div className="card__head">
        <h2 className="card__title">Runtime Config</h2>
        <span className="card__hint">no restart required</span>
      </div>

      {!config ? (
        <div className="empty">Config unavailable.</div>
      ) : (
        <>
          <div className="field">
            <label htmlFor="high-thr">High confidence ≥</label>
            <input
              id="high-thr"
              type="range"
              min="0"
              max="1"
              step="0.01"
              value={high}
              onChange={(e) => setHigh(Number(e.target.value))}
            />
            <span className="field__val">{high.toFixed(2)}</span>
          </div>

          <div className="field">
            <label htmlFor="med-thr">Medium confidence ≥</label>
            <input
              id="med-thr"
              type="range"
              min="0"
              max="1"
              step="0.01"
              value={medium}
              onChange={(e) => setMedium(Number(e.target.value))}
            />
            <span className="field__val">{medium.toFixed(2)}</span>
          </div>

          {weightNames.length > 0 && (
            <>
              <div className="card__hint" style={{ margin: "12px 0 6px" }}>
                model weights
              </div>
              {weightNames.map((name) => (
                <div className="field" key={name}>
                  <label htmlFor={`w-${name}`}>{name}</label>
                  <input
                    id={`w-${name}`}
                    type="number"
                    min="0"
                    step="0.05"
                    value={weights[name]}
                    onChange={(e) =>
                      setWeights((w) => ({ ...w, [name]: e.target.value }))
                    }
                    style={{ width: 90 }}
                  />
                </div>
              ))}
            </>
          )}

          <div className="row" style={{ marginTop: 8 }}>
            <button onClick={handleSave} disabled={saving}>
              {saving ? "Saving…" : "Save config"}
            </button>
            <button className="ghost" onClick={handleRetrain} disabled={retraining}>
              {retraining ? "Scheduling…" : `Retrain ${metric}`}
            </button>
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

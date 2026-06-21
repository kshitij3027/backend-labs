import { useCallback, useEffect, useRef, useState } from "react";
import {
  getModels,
  getAdaptiveStatus,
  getServices,
  promote,
  train,
} from "../api.js";

// Model / serving / adaptive control panel.
//
// Polls GET /api/models, /api/adaptive/status and /api/services every ~5s and
// renders: the champion + A/B (group A/B versions + split) with per-version
// serving metrics; the adaptive drift signal (recent accuracy / threshold /
// retrains / is_training); and the multi-service service list. Includes a
// "Train new version" action (POST /train) and a per-version "Promote"
// (POST /models/promote). Actions trigger an immediate refresh so the panel
// reflects the new state without waiting for the next poll tick.

const POLL_MS = 5000;

function pct(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "—";
  }
  return `${(Number(value) * 100).toFixed(1)}%`;
}

function fmtMetric(m) {
  // serving_metrics is a free-form dict per version; show the most useful bits
  // compactly (requests + a representative accuracy if present), else a dash.
  if (!m || typeof m !== "object") return "—";
  const reqs = m.requests ?? m.count ?? m.served ?? null;
  const acc =
    m.accuracy ?? m.severity_accuracy ?? m.avg_confidence ?? null;
  const parts = [];
  if (reqs !== null && reqs !== undefined) parts.push(`${reqs} req`);
  if (acc !== null && acc !== undefined) parts.push(pct(acc));
  return parts.length ? parts.join(" · ") : "—";
}

export default function ModelPanel() {
  const [models, setModels] = useState(null);
  const [adaptive, setAdaptive] = useState(null);
  const [services, setServices] = useState(null);
  const [error, setError] = useState(null);
  const [busy, setBusy] = useState(false); // an action (train/promote) in flight
  const mountedRef = useRef(true);

  const refresh = useCallback(async () => {
    try {
      const [m, a, s] = await Promise.all([
        getModels(),
        getAdaptiveStatus(),
        getServices(),
      ]);
      if (!mountedRef.current) return;
      setModels(m);
      setAdaptive(a);
      setServices(s);
      setError(null);
    } catch (e) {
      if (!mountedRef.current) return;
      setError(e.message || "Failed to load model panel");
    }
  }, []);

  useEffect(() => {
    mountedRef.current = true;
    refresh();
    const id = setInterval(refresh, POLL_MS);
    return () => {
      mountedRef.current = false;
      clearInterval(id);
    };
  }, [refresh]);

  const onTrain = useCallback(async () => {
    if (busy) return;
    setBusy(true);
    setError(null);
    try {
      await train();
      await refresh();
    } catch (e) {
      if (mountedRef.current) setError(e.message || "Train failed");
    } finally {
      if (mountedRef.current) setBusy(false);
    }
  }, [busy, refresh]);

  const onPromote = useCallback(
    async (version) => {
      if (busy || !version) return;
      setBusy(true);
      setError(null);
      try {
        await promote(version);
        await refresh();
      } catch (e) {
        if (mountedRef.current) setError(e.message || "Promote failed");
      } finally {
        if (mountedRef.current) setBusy(false);
      }
    },
    [busy, refresh],
  );

  const versions = (models && Array.isArray(models.models) && models.models) || [];
  const isTraining = adaptive ? adaptive.is_training : false;

  return (
    <div className="card panel-card">
      <div className="panel-card__head">
        <h3 className="card__title">Models &amp; Serving</h3>
        <button
          type="button"
          className="btn btn--primary"
          onClick={onTrain}
          disabled={busy || isTraining}
          title="Train a new model version in the background"
        >
          {isTraining ? "Training…" : "Train new version"}
        </button>
      </div>

      {error ? <div className="form-card__error">{error}</div> : null}

      {/* A/B configuration summary */}
      <div className="panel-meta">
        <span className="panel-meta__item">
          Champion: <strong>{(models && models.champion) || "—"}</strong>
        </span>
        <span className="panel-meta__item">
          A: <strong>{(models && models.a_version) || "—"}</strong>
        </span>
        <span className="panel-meta__item">
          B: <strong>{(models && models.b_version) || "—"}</strong>
        </span>
        <span className="panel-meta__item">
          Split B:{" "}
          <strong>
            {models && models.split_b !== undefined && models.split_b !== null
              ? `${Math.round(Number(models.split_b) * 100)}%`
              : "—"}
          </strong>
        </span>
      </div>

      {/* Per-version table */}
      {versions.length === 0 ? (
        <div className="empty-state">No model versions yet</div>
      ) : (
        <div className="table-wrap">
          <table className="model-table">
            <thead>
              <tr>
                <th>Version</th>
                <th>Role</th>
                <th>Serving</th>
                <th className="model-table__action">Action</th>
              </tr>
            </thead>
            <tbody>
              {versions.map((v) => {
                const id = v.version ?? v.id ?? "—";
                const role = [];
                if (v.is_champion) role.push("champion");
                if (v.ab_group) role.push(`group ${v.ab_group}`);
                return (
                  <tr key={id}>
                    <td>
                      <code>{id}</code>
                    </td>
                    <td>
                      {role.length ? (
                        <span className="tag">{role.join(" · ")}</span>
                      ) : (
                        "—"
                      )}
                    </td>
                    <td>{fmtMetric(v.serving_metrics)}</td>
                    <td className="model-table__action">
                      <button
                        type="button"
                        className="btn btn--ghost btn--sm"
                        onClick={() => onPromote(id)}
                        disabled={busy || v.is_champion}
                      >
                        {v.is_champion ? "Champion" : "Promote"}
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {/* Adaptive drift signal */}
      <div className="panel-section">
        <h4 className="panel-section__title">Adaptive Learning</h4>
        <div className="panel-stats">
          <div className="panel-stat">
            <span className="panel-stat__label">Recent accuracy</span>
            <span className="panel-stat__value">
              {adaptive ? pct(adaptive.recent_accuracy) : "—"}
            </span>
          </div>
          <div className="panel-stat">
            <span className="panel-stat__label">Threshold</span>
            <span className="panel-stat__value">
              {adaptive ? pct(adaptive.threshold) : "—"}
            </span>
          </div>
          <div className="panel-stat">
            <span className="panel-stat__label">Retrains</span>
            <span className="panel-stat__value">
              {adaptive ? (adaptive.retrains_triggered ?? 0) : "—"}
            </span>
          </div>
          <div className="panel-stat">
            <span className="panel-stat__label">Training</span>
            <span
              className="panel-stat__value"
              style={{ color: isTraining ? "#f59e0b" : "#22c55e" }}
            >
              {adaptive ? (isTraining ? "yes" : "no") : "—"}
            </span>
          </div>
        </div>
      </div>

      {/* Known services (multi-service model) */}
      <div className="panel-section">
        <h4 className="panel-section__title">
          Services
          {services && services.status ? (
            <span className="card__title-sub"> ({services.status})</span>
          ) : null}
        </h4>
        {services && Array.isArray(services.services) && services.services.length > 0 ? (
          <div className="chip-row">
            {services.services.map((svc) => (
              <span key={svc} className="chip">
                {svc}
              </span>
            ))}
          </div>
        ) : (
          <div className="empty-state empty-state--inline">No services</div>
        )}
      </div>
    </div>
  );
}

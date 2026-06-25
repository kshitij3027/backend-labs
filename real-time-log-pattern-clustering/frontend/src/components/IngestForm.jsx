import { useState } from "react";
import { postCluster } from "../api.js";

// Severity levels the backend understands, in ascending order. Order = <select>
// option order.
const LEVELS = ["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL"];

// Algorithms the backend runs each log through, in display order. Used to keep
// the three result rows in a stable order regardless of response ordering.
const ALGO_ORDER = ["kmeans", "dbscan", "hdbscan"];

// Sentinel cluster id for noise / new pattern.
const NOISE_ID = -1;

/** Human label for a cluster id (noise sentinel gets a friendly name). */
function labelForCluster(clusterId) {
  return clusterId === NOISE_ID ? "noise/new" : `Cluster ${clusterId}`;
}

/** Format a confidence (0..1) as a whole-number percentage; null-safe. */
function fmtConfidence(v) {
  if (!Number.isFinite(v)) return "—";
  return `${(v * 100).toFixed(0)}%`;
}

// Quick-fill presets: representative logs for each major pattern family so the
// reviewer can demo the clustering without hand-typing realistic payloads.
const PRESETS = {
  Security: {
    service: "auth",
    level: "ERROR",
    message: "Multiple failed login attempts from 10.0.0.5",
    source_ip: "10.0.0.5",
  },
  Performance: {
    service: "api-gateway",
    level: "WARN",
    message: "Request to /v1/orders took 4200ms (threshold 1000ms)",
    source_ip: "10.0.0.42",
  },
  Normal: {
    service: "web",
    level: "INFO",
    message: "User 8821 fetched dashboard in 87ms",
    source_ip: "10.0.0.17",
  },
};

/** Order the three algorithm results into a stable, known sequence. */
function orderResults(results) {
  if (!Array.isArray(results)) return [];
  const byAlgo = new Map();
  for (const r of results) {
    if (r && typeof r.algorithm === "string") byAlgo.set(r.algorithm, r);
  }
  const ordered = [];
  // Known algorithms first, in canonical order …
  for (const algo of ALGO_ORDER) {
    if (byAlgo.has(algo)) {
      ordered.push(byAlgo.get(algo));
      byAlgo.delete(algo);
    }
  }
  // … then anything unexpected, preserving its arrival order.
  for (const r of results) {
    if (r && byAlgo.has(r.algorithm)) {
      ordered.push(r);
      byAlgo.delete(r.algorithm);
    }
  }
  return ordered;
}

/**
 * Cluster a Log — manual ingest form (C17).
 *
 * Compose (or quick-fill) a single log entry, POST it to `/cluster`, and render
 * the returned `ClusterAssignment` inline: the masked message, the discovered
 * pattern type plus new-pattern / anomaly badges, and one card per clustering
 * algorithm (cluster id + confidence + an anomaly flag). The button shows a
 * busy state while posting, errors render inline, and the previous result stays
 * visible until a fresh one arrives. Fully self-contained — no props.
 */
export default function IngestForm() {
  const [service, setService] = useState("auth");
  const [level, setLevel] = useState("ERROR");
  const [message, setMessage] = useState(
    "Multiple failed login attempts from 10.0.0.5",
  );
  const [sourceIp, setSourceIp] = useState("10.0.0.5");

  const [result, setResult] = useState(null);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState(null);

  function applyPreset(name) {
    const p = PRESETS[name];
    if (!p) return;
    setService(p.service);
    setLevel(p.level);
    setMessage(p.message);
    setSourceIp(p.source_ip);
  }

  async function handleSubmit(e) {
    e.preventDefault();
    if (submitting) return;

    // Build the LogEntry body per the backend `/cluster` contract. Optional
    // fields are only included when present so we never send empty strings.
    const body = {
      timestamp: new Date().toISOString(),
      service: service.trim() || "unknown",
      level,
      message,
    };
    const ip = sourceIp.trim();
    if (ip) body.source_ip = ip;

    setSubmitting(true);
    setError(null);
    try {
      const res = await postCluster(body);
      // Keep prior result visible until the new one is in hand (above), then swap.
      setResult(res && typeof res === "object" ? res : null);
    } catch (err) {
      setError(err?.message || "Failed to cluster log");
    } finally {
      setSubmitting(false);
    }
  }

  const results = orderResults(result?.results);

  return (
    <section className="panel">
      <div className="panel__head">
        <h3 className="section__title panel__title">Cluster a Log</h3>
        <div className="preset-bar" aria-label="Quick fill presets">
          {Object.keys(PRESETS).map((name) => (
            <button
              key={name}
              type="button"
              className="btn btn--ghost btn--sm"
              onClick={() => applyPreset(name)}
            >
              {name}
            </button>
          ))}
        </div>
      </div>

      <form className="ingest-form" onSubmit={handleSubmit}>
        <div className="form-grid">
          <div className="form-row">
            <label htmlFor="ingest-service">Service</label>
            <input
              id="ingest-service"
              type="text"
              value={service}
              onChange={(e) => setService(e.target.value)}
              placeholder="auth"
            />
          </div>
          <div className="form-row">
            <label htmlFor="ingest-level">Level</label>
            <select
              id="ingest-level"
              value={level}
              onChange={(e) => setLevel(e.target.value)}
            >
              {LEVELS.map((lv) => (
                <option key={lv} value={lv}>
                  {lv}
                </option>
              ))}
            </select>
          </div>
        </div>

        <div className="form-row">
          <label htmlFor="ingest-message">Message</label>
          <textarea
            id="ingest-message"
            rows={3}
            value={message}
            onChange={(e) => setMessage(e.target.value)}
            placeholder="Multiple failed login attempts from 10.0.0.5"
          />
        </div>

        <div className="form-row">
          <label htmlFor="ingest-ip">
            Source IP <span className="form-row__opt">(optional)</span>
          </label>
          <input
            id="ingest-ip"
            type="text"
            value={sourceIp}
            onChange={(e) => setSourceIp(e.target.value)}
            placeholder="10.0.0.5"
          />
        </div>

        <button
          type="submit"
          className="btn btn--primary"
          disabled={submitting}
          aria-busy={submitting}
        >
          {submitting ? "Clustering…" : "Cluster this log"}
        </button>
      </form>

      {error ? <div className="panel__error">{error}</div> : null}

      {result ? (
        <div className="ingest-result" aria-live="polite">
          <div className="ingest-result__masked">
            <span className="cluster-detail__rep-label">Masked message</span>
            <code className="cluster-detail__rep-line">
              {result.masked_message || "—"}
            </code>
          </div>

          <div className="ingest-result__badges">
            {result.pattern_type ? (
              <span className="chip">{result.pattern_type}</span>
            ) : null}
            {result.is_new_pattern ? (
              <span className="badge badge--new">new pattern</span>
            ) : null}
            {result.is_anomaly ? (
              <span className="badge badge--anomaly">anomaly</span>
            ) : null}
          </div>

          <div className="result-rows">
            {results.length ? (
              results.map((r) => (
                <div
                  key={r.algorithm}
                  className={`result-row ${
                    r.is_anomaly ? "result-row--anomaly" : ""
                  }`}
                >
                  <span className="result-row__algo">{r.algorithm}</span>
                  <span className="result-row__cluster">
                    {labelForCluster(r.cluster_id)}
                  </span>
                  <span className="result-row__conf">
                    {fmtConfidence(r.confidence)}
                  </span>
                  {r.is_anomaly ? (
                    <span className="badge badge--anomaly">anomaly</span>
                  ) : (
                    <span className="result-row__ok">ok</span>
                  )}
                </div>
              ))
            ) : (
              <div className="muted">No per-algorithm results returned.</div>
            )}
          </div>
        </div>
      ) : null}
    </section>
  );
}

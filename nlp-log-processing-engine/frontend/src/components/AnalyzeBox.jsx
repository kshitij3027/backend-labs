import { useState } from "react";
import { postAnalyze } from "../api.js";
import { truncate } from "../util.js";

// Free-text analyze box: a textarea + "Analyze" button that POSTs one log line to
// /api/analyze and hands the result to the parent via `onResult`. Ctrl/Cmd+Enter submits.
// A few example log lines prefill the box on click so the dashboard is usable with zero
// typing. Loading and error states are surfaced inline; the button is disabled while a
// request is in flight or the box is empty.
//
// Props:
//   onResult — (AnalysisResponse) => void, called with each successful analysis.

// Example lines span several intents / severities and exercise the log-entity NER (SERVICE,
// USER_ID, IP, HOST, ERROR_CODE, PATH, URL, PORT) so one click shows a rich result.
const EXAMPLES = [
  "auth-svc: authentication failed for user 4821 from 10.52.44.216 (invalid password E401)",
  "FATAL: payments-api segfault — out of memory, worker killed on web-01",
  "deployment of billing-svc v2.3.1 completed successfully via gateway",
  "WARNING: high memory usage 92% on cache-svc, cpu throttling detected",
  "GET /api/orders returned 503 from order-svc on port 8443, upstream timed out",
];

export default function AnalyzeBox({ onResult }) {
  const [text, setText] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const submit = async () => {
    const message = text.trim();
    if (!message || loading) return;
    setLoading(true);
    setError(null);
    try {
      const result = await postAnalyze(message);
      if (typeof onResult === "function") onResult(result);
    } catch (e) {
      setError(e?.message || "Analysis failed");
    } finally {
      setLoading(false);
    }
  };

  const onKeyDown = (e) => {
    // Ctrl/Cmd+Enter submits (a plain Enter stays a newline — these are log lines).
    if ((e.metaKey || e.ctrlKey) && e.key === "Enter") {
      e.preventDefault();
      submit();
    }
  };

  const canSubmit = !loading && text.trim().length > 0;

  return (
    <section className="panel analyzebox">
      <div className="panel__head">
        <h2 className="panel__title">Analyze a log line</h2>
        <span className="analyzebox__hint" aria-hidden="true">
          ⌘/Ctrl + Enter
        </span>
      </div>

      <textarea
        className="analyzebox__input"
        value={text}
        onChange={(e) => setText(e.target.value)}
        onKeyDown={onKeyDown}
        placeholder="Paste a log line, e.g. 'auth-svc: login failed for user 4821 from 10.0.0.5'"
        rows={3}
        spellCheck={false}
        aria-label="Log line to analyze"
      />

      <div className="analyzebox__examples">
        <span className="analyzebox__examples-label">Try</span>
        {EXAMPLES.map((ex) => (
          <button
            key={ex}
            type="button"
            className="chipbtn"
            title={ex}
            onClick={() => {
              setText(ex);
              setError(null);
            }}
          >
            {truncate(ex, 40)}
          </button>
        ))}
      </div>

      <div className="analyzebox__actions">
        <button
          type="button"
          className="btn btn--primary"
          onClick={submit}
          disabled={!canSubmit}
        >
          {loading ? "Analyzing…" : "Analyze"}
        </button>
        {error ? (
          <span className="analyzebox__error" role="alert">
            {error}
          </span>
        ) : null}
      </div>
    </section>
  );
}

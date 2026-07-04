import { useState } from "react";
import { SEVERITIES, parseTags } from "../util.js";

// Incident submission form (C16). A controlled form describing the incident the
// operator is facing; on submit it assembles the POST /recommend request body and
// hands it to the parent via `onSubmit(body)` — App owns the fetch + result state.
//
// Fields: title + description (both required), optional service / severity / tags
// (comma-separated -> array) facets that sharpen contextual ranking, and top_k
// (how many suggestions to return). Two "restrict" toggles ask the backend to
// hard-filter matches to the same service / severity rather than merely favouring
// them. Submit is disabled while a request is in flight so a slow query can't be
// double-fired.
export default function RecommendForm({ onSubmit, submitting }) {
  const [title, setTitle] = useState("");
  const [description, setDescription] = useState("");
  const [service, setService] = useState("");
  const [severity, setSeverity] = useState("");
  const [tags, setTags] = useState("");
  const [topK, setTopK] = useState(5);
  const [restrictService, setRestrictService] = useState(false);
  const [restrictSeverity, setRestrictSeverity] = useState(false);
  const [localError, setLocalError] = useState(null);

  function handleSubmit(e) {
    e.preventDefault();
    // Client-side gate: title + description are the semantic query and are required.
    if (!title.trim() || !description.trim()) {
      setLocalError("Title and description are both required.");
      return;
    }
    setLocalError(null);

    // Assemble the request body. Optional facets are only sent when non-empty so
    // the backend sees a clean, minimal query (an empty service ≠ "match empty").
    const body = {
      title: title.trim(),
      description: description.trim(),
      top_k: Number(topK) || 5,
    };
    const svc = service.trim();
    if (svc) body.service = svc;
    if (severity) body.severity = severity;
    const parsedTags = parseTags(tags);
    if (parsedTags.length) body.tags = parsedTags;
    if (restrictService) body.restrict_service = true;
    if (restrictSeverity) body.restrict_severity = true;

    onSubmit(body);
  }

  return (
    <section className="card form-card">
      <div className="card__head">
        <h2 className="card__title">Describe the incident</h2>
        <span className="card__hint">find similar past incidents + their fixes</span>
      </div>

      <form className="recform" onSubmit={handleSubmit}>
        <div className="field">
          <label htmlFor="rf-title">
            Title <span className="req">*</span>
          </label>
          <input
            id="rf-title"
            type="text"
            value={title}
            placeholder="e.g. Checkout API returning 500s under load"
            onChange={(e) => setTitle(e.target.value)}
          />
        </div>

        <div className="field">
          <label htmlFor="rf-desc">
            Description <span className="req">*</span>
          </label>
          <textarea
            id="rf-desc"
            rows={4}
            value={description}
            placeholder="Symptoms, error messages, what changed, blast radius…"
            onChange={(e) => setDescription(e.target.value)}
          />
        </div>

        <div className="recform__grid">
          <div className="field">
            <label htmlFor="rf-service">Service</label>
            <input
              id="rf-service"
              type="text"
              value={service}
              placeholder="checkout-api"
              onChange={(e) => setService(e.target.value)}
            />
          </div>

          <div className="field">
            <label htmlFor="rf-severity">Severity</label>
            <select
              id="rf-severity"
              value={severity}
              onChange={(e) => setSeverity(e.target.value)}
            >
              <option value="">(any)</option>
              {SEVERITIES.map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </select>
          </div>

          <div className="field">
            <label htmlFor="rf-topk">Results</label>
            <input
              id="rf-topk"
              type="number"
              min="1"
              max="50"
              value={topK}
              onChange={(e) => setTopK(e.target.value)}
            />
          </div>
        </div>

        <div className="field">
          <label htmlFor="rf-tags">Tags</label>
          <input
            id="rf-tags"
            type="text"
            value={tags}
            placeholder="comma,separated,e.g. timeout, database, oom"
            onChange={(e) => setTags(e.target.value)}
          />
        </div>

        <div className="recform__toggles">
          <label className="check">
            <input
              type="checkbox"
              checked={restrictService}
              onChange={(e) => setRestrictService(e.target.checked)}
            />
            Only this service
          </label>
          <label className="check">
            <input
              type="checkbox"
              checked={restrictSeverity}
              onChange={(e) => setRestrictSeverity(e.target.checked)}
            />
            Only this severity
          </label>
        </div>

        {localError && (
          <div className="save-msg save-msg--err" role="alert">
            {localError}
          </div>
        )}

        <div className="row">
          <button type="submit" disabled={submitting}>
            {submitting ? "Finding matches…" : "Recommend fixes"}
          </button>
        </div>
      </form>
    </section>
  );
}

import SuggestionCard from "./SuggestionCard.jsx";

// Results panel (C16). Renders one of four states for the POST /recommend cycle:
//
//   * error      — the request failed (shows the backend detail, if any).
//   * loading    — a request is in flight and there is no prior result yet.
//   * idle       — nothing submitted yet (first paint): a gentle prompt.
//   * result     — a meta strip (count / cached / recommendation_id) followed by
//                  a ranked list of SuggestionCards, OR an empty state when the
//                  query matched nothing.
//
// `result` is the raw RecommendResponse. `recommendation_id` is surfaced here (and
// kept in App state) because C17's feedback votes reference it. `onVote` is the C17
// per-card vote handler (App records the vote against `recommendation_id`, then
// re-runs the query to show the live re-rank); it is passed straight to each card.
export default function SuggestionList({ result, submitting, error, onVote }) {
  if (error) {
    return (
      <section className="card results">
        <div className="banner banner--err" role="alert">
          {error}
        </div>
      </section>
    );
  }

  if (submitting && !result) {
    return (
      <section className="card results">
        <div className="loading">
          <span className="spinner" aria-hidden="true" />
          <span>Searching for similar incidents…</span>
        </div>
      </section>
    );
  }

  if (!result) {
    return (
      <section className="card results">
        <div className="empty">
          Describe an incident on the left, then hit <b>Recommend fixes</b> to see
          the most similar past incidents and how they were resolved.
        </div>
      </section>
    );
  }

  const suggestions = Array.isArray(result.suggestions) ? result.suggestions : [];
  const count = Number.isFinite(Number(result.count))
    ? Number(result.count)
    : suggestions.length;

  return (
    <section className="card results">
      <div className="results__head">
        <h2 className="card__title">
          {count} {count === 1 ? "suggestion" : "suggestions"}
        </h2>
        <div className="results__meta">
          {result.cached ? (
            <span className="tagline tagline--cached" title="served from cache">
              cached
            </span>
          ) : (
            <span className="tagline tagline--fresh" title="freshly computed">
              fresh
            </span>
          )}
          {result.recommendation_id != null && (
            <span className="tagline" title="recommendation id (used for feedback)">
              rec #{result.recommendation_id}
            </span>
          )}
          {submitting && <span className="tagline">updating…</span>}
        </div>
      </div>

      {suggestions.length === 0 ? (
        <div className="empty">
          No matches — try a different description or add more incidents to the
          corpus.
        </div>
      ) : (
        <div className="suggestion-list">
          {suggestions.map((s, i) => (
            <SuggestionCard
              key={`${s.incident_id}-${i}`}
              suggestion={s}
              rank={i + 1}
              onVote={onVote}
            />
          ))}
        </div>
      )}
    </section>
  );
}

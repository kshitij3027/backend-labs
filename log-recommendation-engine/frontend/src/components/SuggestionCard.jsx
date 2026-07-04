import { useState } from "react";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  Cell,
  ResponsiveContainer,
} from "recharts";
import { fmt, pct, severityTier } from "../util.js";

// Colours for the three ranking signals (kept in sync with the CSS legend swatches).
const SIGNAL_COLOR = {
  semantic: "var(--sem)",
  contextual: "var(--ctx)",
  feedback: "var(--fb)",
};

// The three contextual sub-signals we surface as a compact detail row.
const CTX_SIGNALS = [
  ["service", "service"],
  ["severity", "severity"],
  ["tags", "tags"],
  ["recency", "recency"],
];

// One ranked suggestion (C16). The RESOLUTION is the product payload — the fix
// that resolved the matched historical incident — so it is rendered prominently.
// Alongside it we explain *why* this incident ranked where it did: the final
// blended `score` plus a horizontal bar chart of the WEIGHTED contribution of each
// signal (semantic / contextual / feedback = weight × raw value), which is exactly
// how the backend composes the score. Raw per-signal values and the contextual
// sub-signals ride along underneath for the curious.
//
// C17 adds a feedback footer: 👍 / 👎 buttons that call `onVote(incident_id, helpful)`.
// The parent (App) records the vote (POST /feedback with the current recommendation_id)
// and then re-runs the query so the re-rank is visible. While a vote is in flight the
// buttons disable; when it resolves we show a small "recorded" ack (with the returned
// helpful/unhelpful tallies when the parent hands them back).
export default function SuggestionCard({ suggestion, rank, onVote }) {
  const {
    incident_id,
    title,
    service,
    severity,
    tags = [],
    resolution,
    score,
    semantic,
    contextual,
    feedback,
    breakdown = {},
  } = suggestion || {};

  const weights = breakdown.weights || {};
  const explored = Boolean(breakdown.explored);
  const ctxDetail = breakdown.contextual_detail || {};

  // Local vote lifecycle for THIS card. `voting` disables both buttons while the
  // POST + re-run round-trips; `ack` holds the outcome to render underneath.
  const [voting, setVoting] = useState(false);
  const [ack, setAck] = useState(null); // {helpful, counts?: {helpful_count, unhelpful_count}, error?}

  async function vote(helpful) {
    if (voting || typeof onVote !== "function" || incident_id == null) return;
    setVoting(true);
    setAck(null);
    try {
      // Parent returns the FeedbackResponse (or undefined) — surface its tallies.
      const res = await onVote(incident_id, helpful);
      setAck({
        helpful,
        counts:
          res && (res.helpful_count != null || res.unhelpful_count != null)
            ? {
                helpful_count: res.helpful_count,
                unhelpful_count: res.unhelpful_count,
              }
            : null,
      });
    } catch (e) {
      setAck({ helpful, error: e?.message || "Vote failed." });
    } finally {
      setVoting(false);
    }
  }

  // Weighted contribution of each signal to the blended score: weight × raw value.
  // `feedback` is bounded [-1, 1] so its contribution can be negative — the chart
  // domain and a diverging colour handle that. Missing weights fall back to the
  // raw value (still informative) rather than collapsing the bar to zero.
  const contrib = (key, raw) => {
    const w = Number(weights[key]);
    const v = Number(raw);
    if (!Number.isFinite(v)) return 0;
    return Number.isFinite(w) ? w * v : v;
  };

  const barData = [
    { key: "semantic", label: "semantic", raw: semantic, value: contrib("semantic", semantic) },
    { key: "contextual", label: "contextual", raw: contextual, value: contrib("contextual", contextual) },
    { key: "feedback", label: "feedback", raw: feedback, value: contrib("feedback", feedback) },
  ];

  const hasNegative = barData.some((d) => d.value < 0);
  const maxAbs = Math.max(0.001, ...barData.map((d) => Math.abs(d.value)));
  // Symmetric domain when any contribution is negative; otherwise anchor at 0.
  const domain = hasNegative ? [-maxAbs, maxAbs] : [0, maxAbs];

  const sevTier = severityTier(severity);

  return (
    <article className={`suggestion ${explored ? "suggestion--explored" : ""}`}>
      <div className="suggestion__head">
        <span className="suggestion__rank">#{rank}</span>
        <h3 className="suggestion__title">{title || "(untitled incident)"}</h3>
        <span className="suggestion__score" title="blended relevance score">
          <span className="suggestion__score-val">{pct(score)}</span>
          <span className="suggestion__score-lab">match</span>
        </span>
      </div>

      <div className="chips">
        {service && <span className="chip chip--service">{service}</span>}
        {severity && (
          <span className={`chip chip--sev chip--sev-${sevTier}`}>{severity}</span>
        )}
        {explored && (
          <span className="chip chip--explored" title="surfaced via exploration">
            explored
          </span>
        )}
        {tags.map((t) => (
          <span className="chip chip--tag" key={t}>
            {t}
          </span>
        ))}
      </div>

      <div className="resolution">
        <span className="resolution__label">Resolution</span>
        <p className="resolution__text">{resolution || "No resolution recorded."}</p>
      </div>

      <div className="breakdown">
        <div className="breakdown__head">
          <span className="breakdown__title">Why this ranked here</span>
          <span className="breakdown__legend">
            <span className="legend legend--sem">semantic</span>
            <span className="legend legend--ctx">contextual</span>
            <span className="legend legend--fb">feedback</span>
          </span>
        </div>

        <ResponsiveContainer width="100%" height={112}>
          <BarChart
            data={barData}
            layout="vertical"
            margin={{ top: 4, right: 16, bottom: 4, left: 8 }}
          >
            <XAxis
              type="number"
              domain={domain}
              stroke="var(--text-faint)"
              fontSize={10}
              tickFormatter={(v) => fmt(v, 2)}
            />
            <YAxis
              type="category"
              dataKey="label"
              width={72}
              stroke="var(--text-faint)"
              fontSize={11}
            />
            <Tooltip
              cursor={{ fill: "rgba(255,255,255,0.04)" }}
              formatter={(val, _n, item) => {
                const raw = item?.payload?.raw;
                return [
                  `${fmt(val, 3)} contribution (raw ${fmt(raw, 3)})`,
                  item?.payload?.label,
                ];
              }}
            />
            <Bar dataKey="value" radius={[0, 4, 4, 0]} isAnimationActive={false}>
              {barData.map((d) => (
                <Cell key={d.key} fill={SIGNAL_COLOR[d.key]} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>

        <div className="breakdown__raw">
          <span className="raw">
            semantic <b>{fmt(semantic)}</b>
          </span>
          <span className="raw">
            contextual <b>{fmt(contextual)}</b>
          </span>
          <span className="raw">
            feedback <b>{fmt(feedback)}</b>
          </span>
          {Number.isFinite(Number(breakdown.base)) && (
            <span className="raw">
              base <b>{fmt(breakdown.base)}</b>
            </span>
          )}
        </div>

        {Object.keys(ctxDetail).length > 0 && (
          <div className="ctxdetail">
            {CTX_SIGNALS.filter(([k]) => Number.isFinite(Number(ctxDetail[k]))).map(
              ([k, label]) => (
                <span className="ctxdetail__item" key={k}>
                  {label} <b>{fmt(ctxDetail[k])}</b>
                </span>
              ),
            )}
          </div>
        )}
      </div>

      {/* Feedback footer (C17): vote on whether this suggestion was useful. The
          vote is recorded against the current recommendation_id and then the query
          is re-run so the re-rank shows immediately. Hidden if no handler wired. */}
      {typeof onVote === "function" && (
        <div className="vote">
          <span className="vote__prompt">Was this helpful?</span>
          <div className="vote__buttons">
            <button
              type="button"
              className="vote__btn vote__btn--up"
              onClick={() => vote(true)}
              disabled={voting}
              aria-label="Mark this suggestion helpful"
            >
              👍 Helpful
            </button>
            <button
              type="button"
              className="vote__btn vote__btn--down"
              onClick={() => vote(false)}
              disabled={voting}
              aria-label="Mark this suggestion not helpful"
            >
              👎 Not helpful
            </button>
          </div>

          {voting && <span className="vote__status">Recording…</span>}

          {ack && !ack.error && (
            <span className="vote__ack" role="status">
              ✓ Recorded {ack.helpful ? "helpful" : "not helpful"}
              {ack.counts
                ? ` · ${fmt(ack.counts.helpful_count, 0)}👍 / ${fmt(
                    ack.counts.unhelpful_count,
                    0,
                  )}👎`
                : ""}
            </span>
          )}

          {ack && ack.error && (
            <span className="vote__ack vote__ack--err" role="alert">
              {ack.error}
            </span>
          )}
        </div>
      )}
    </article>
  );
}

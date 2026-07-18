import { useCallback, useEffect, useState } from "react";
import { getStats } from "../api.js";

// Freshest-stats hook backing the C12 charts row.
//
// Two sources feed the SAME aggregate snapshot; both go through one monotonic merge:
//   * REST  — GET /api/stats once on mount, then a ~5s fallback poll (paused while the tab
//             is hidden). On a fetch error we keep the last-good snapshot and raise `stale`;
//             a later success clears it.
//   * WS    — the `{"type":"stats"}` frame the backend pushes on every analyze, handed in as
//             `lastStats` (see hooks/useWebSocket.js). This is the low-latency path, so the
//             charts move the instant a line is analysed rather than on the next poll tick.
//
// `lastStats` is `null` until the very first WS frame arrives (on the first in-session
// analyze). That initial null — and any other contentless value — is IGNORED: it must not be
// applied and, crucially, must not suppress the REST data. (An earlier design tracked a WS
// "sequence" and discarded any REST poll that resolved after a WS bump; because the initial
// mount had no live frame yet, that scheme could drop the mount-fetch and every poll, leaving
// the charts empty until a real WS frame landed. We no longer track a sequence at all.)
//
// The backend aggregator is monotonic (`total_analyzed` only ever grows), and each snapshot is
// a full picture at read time, so the merge simply keeps the snapshot with the higher (or
// equal) `total_analyzed`. That makes the two sources order-independent: a real WS frame lands
// immediately (analyzing bumps the total, so it always wins), while a slow in-flight poll that
// resolves *after* a newer WS frame is a no-op instead of flickering the live number backwards.
//
// Every snapshot is normalised to the full shape with empty defaults, so chart components
// never have to null-check a distribution.

const POLL_INTERVAL_MS = 5000;

/** The /api/stats shape with every container empty — the dashboard's zero-data state. */
const EMPTY_STATS = {
  total_analyzed: 0,
  intent_distribution: {},
  sentiment_distribution: {},
  entity_type_distribution: {},
  trending_keywords: [],
  recent: [],
  throughput_per_sec: 0,
};

/** Coerce any partial / malformed snapshot into the full shape with safe defaults. */
function normalize(raw) {
  const s = raw && typeof raw === "object" ? raw : {};
  const asObj = (v) => (v && typeof v === "object" && !Array.isArray(v) ? v : {});
  const asArr = (v) => (Array.isArray(v) ? v : []);
  const asNum = (v) => (Number.isFinite(Number(v)) ? Number(v) : 0);
  return {
    total_analyzed: asNum(s.total_analyzed),
    intent_distribution: asObj(s.intent_distribution),
    sentiment_distribution: asObj(s.sentiment_distribution),
    entity_type_distribution: asObj(s.entity_type_distribution),
    trending_keywords: asArr(s.trending_keywords),
    recent: asArr(s.recent),
    throughput_per_sec: asNum(s.throughput_per_sec),
  };
}

/** A usable WS/REST snapshot: a non-null, non-array object that actually carries fields. */
function hasContent(v) {
  return v != null && typeof v === "object" && !Array.isArray(v) && Object.keys(v).length > 0;
}

/**
 * Return the freshest stats snapshot, merging the REST poll with the live WS frame.
 * @param {object|null} lastStats the latest `{"type":"stats"}` payload from useWebSocket().
 * @returns {{stats: object, stale: boolean}} normalised snapshot + a stale-data flag.
 */
export function useStats(lastStats) {
  const [stats, setStats] = useState(EMPTY_STATS);
  const [stale, setStale] = useState(false);

  // Merge a snapshot in monotonically: the higher/equal `total_analyzed` wins, so neither a
  // late REST poll nor an out-of-order WS frame can move the live counts backwards. `>=` (not
  // `>`) lets same-total snapshots through so a rolling field like throughput_per_sec still
  // refreshes while the count is static. Stable identity (empty deps) so the effects below
  // mount their poll/listeners exactly once.
  const applySnapshot = useCallback((raw) => {
    const next = normalize(raw);
    setStats((prev) => (next.total_analyzed >= prev.total_analyzed ? next : prev));
  }, []);

  // Apply each live WS `stats` frame immediately — the low-latency path. A contentless value
  // (the initial `null` before the first frame, or an empty object) is ignored, so it can
  // neither blank the charts nor block the REST data below.
  useEffect(() => {
    if (!hasContent(lastStats)) return;
    applySnapshot(lastStats);
    setStale(false);
  }, [lastStats, applySnapshot]);

  // REST bootstrap on mount + 5s fallback poll (skipped while the tab is hidden).
  useEffect(() => {
    let alive = true;
    let timer = null;

    const poll = async () => {
      if (!alive || document.hidden) return; // paused when backgrounded
      try {
        const fresh = await getStats();
        if (!alive) return;
        applySnapshot(fresh);
        setStale(false);
      } catch {
        if (alive) setStale(true); // keep the last-good snapshot, just mark it stale
      }
    };

    poll(); // immediate bootstrap so charts aren't empty until the first tick
    timer = setInterval(poll, POLL_INTERVAL_MS);

    // Catch up right away when the tab returns to the foreground (covers paused ticks).
    const onVisibility = () => {
      if (!document.hidden) poll();
    };
    document.addEventListener("visibilitychange", onVisibility);

    return () => {
      alive = false;
      if (timer) clearInterval(timer);
      document.removeEventListener("visibilitychange", onVisibility);
    };
  }, [applySnapshot]);

  return { stats, stale };
}

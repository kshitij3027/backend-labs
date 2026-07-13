import { useCallback, useEffect, useRef, useState } from "react";
import { getIncidents } from "../api.js";
import { useWebSocket } from "./useWebSocket.js";

// Incident store for the dashboard: loads the existing history on mount, then merges
// live `incident_update` pushes from the WebSocket. Newest-first throughout, deduped by
// `incident_id`, and capped so a long-lived tab can't grow unbounded.

// Cap the retained list (the backend keeps far more; the dashboard only needs recent).
const MAX_INCIDENTS = 100;

/**
 * Merge two newest-first incident lists, `prev` ahead of `next`, deduping by
 * `incident_id` (first occurrence wins) and capping the length. Used to fold a fresh
 * history fetch into whatever live pushes have already arrived without dropping either.
 */
function mergeHistory(prev, next) {
  const seen = new Set();
  const out = [];
  for (const r of [...prev, ...(next || [])]) {
    if (!r || !r.incident_id || seen.has(r.incident_id)) continue;
    seen.add(r.incident_id);
    out.push(r);
  }
  return out.slice(0, MAX_INCIDENTS);
}

/**
 * @returns {{incidents: object[], status: string, error: string|null,
 *            selectedId: string|null, setSelectedId: (id: string|null) => void}}
 */
export function useIncidents() {
  const [incidents, setIncidents] = useState([]);
  const [error, setError] = useState(null);
  const [selectedId, setSelectedId] = useState(null);

  const aliveRef = useRef(true);

  // Fetch the history and fold it into the current list. Called on mount and again
  // whenever the socket (re)connects, so a reconnect catches up incidents created while
  // we were offline and a failed initial load self-heals once the backend is reachable.
  const loadHistory = useCallback(async () => {
    try {
      const history = await getIncidents();
      if (!aliveRef.current) return;
      setIncidents((prev) => mergeHistory(prev, history));
      setError(null);
      // Default-select the newest incident, but never override a user's choice.
      setSelectedId((cur) => cur ?? history[0]?.incident_id ?? null);
    } catch (e) {
      if (!aliveRef.current) return;
      setError(e?.message || "Failed to load incidents");
    }
  }, []);

  // Live push handler: prepend the new report, dedupe by id, cap the list.
  const onIncident = useCallback((report) => {
    if (!report || !report.incident_id) return;
    setIncidents((prev) => mergeHistory([report], prev));
    // Auto-select the first incident to arrive so the detail pane is never empty;
    // never steal a selection the user already made.
    setSelectedId((cur) => cur ?? report.incident_id);
  }, []);

  const status = useWebSocket(onIncident);

  // Initial load on mount.
  useEffect(() => {
    aliveRef.current = true;
    loadHistory();
    return () => {
      aliveRef.current = false;
    };
  }, [loadHistory]);

  // Re-sync history on every (re)connect.
  useEffect(() => {
    if (status === "live") loadHistory();
  }, [status, loadHistory]);

  return { incidents, status, error, selectedId, setSelectedId };
}

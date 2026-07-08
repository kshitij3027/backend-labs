import { useCallback, useEffect, useRef, useState } from "react";
import { getDashboard } from "../api.js";

/**
 * Poll GET /api/v1/dashboard on a fixed interval and expose the latest payload.
 *
 * Graceful degradation is the whole point of this hook:
 *   * On a successful poll we set `data` + `lastUpdated` and clear `error`.
 *   * On a failed poll (non-2xx, network error, or the api.js 4s abort) we set
 *     `error` but KEEP the previous `data` — the dashboard keeps showing the last
 *     good snapshot instead of blanking out. `stale` (error && data) lets the UI
 *     flag that the numbers are frozen.
 *
 * The loop pauses while the browser tab is hidden (visibilitychange) so a
 * backgrounded tab doesn't hammer the backend, and does an immediate catch-up
 * fetch the moment it becomes visible again.
 *
 * @param {number} intervalMs poll cadence in ms (default 5000)
 * @returns {{data: object|null, error: string|null, lastUpdated: Date|null,
 *            loading: boolean, stale: boolean}}
 */
export function useDashboard(intervalMs = 5000) {
  const [data, setData] = useState(null);
  const [error, setError] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);
  const [loading, setLoading] = useState(true);

  // Guards against state updates after unmount (StrictMode double-mount / fast nav).
  const aliveRef = useRef(true);
  const intervalRef = useRef(null);

  const tick = useCallback(async () => {
    try {
      const payload = await getDashboard();
      if (!aliveRef.current) return;
      setData(payload);
      setLastUpdated(new Date());
      setError(null);
    } catch (e) {
      if (!aliveRef.current) return;
      // Graceful degradation: KEEP prior `data`, only flag the error.
      setError(e?.message || "Backend unreachable");
    } finally {
      if (aliveRef.current) setLoading(false);
    }
  }, []);

  // Stable ref to the latest tick so the interval + visibility listener never
  // capture a stale closure.
  const tickRef = useRef(tick);
  tickRef.current = tick;

  useEffect(() => {
    aliveRef.current = true;

    const start = () => {
      if (intervalRef.current != null) return; // already running
      intervalRef.current = setInterval(() => tickRef.current(), intervalMs);
    };
    const stop = () => {
      if (intervalRef.current != null) {
        clearInterval(intervalRef.current);
        intervalRef.current = null;
      }
    };

    // Pause polling while the tab is hidden; resume with an immediate refresh.
    const onVisibility = () => {
      if (document.hidden) {
        stop();
      } else {
        tickRef.current(); // immediate catch-up on return
        start();
      }
    };

    // Fire immediately on mount, then run on the interval (unless mounted hidden).
    tickRef.current();
    if (!document.hidden) start();
    document.addEventListener("visibilitychange", onVisibility);

    return () => {
      aliveRef.current = false;
      stop();
      document.removeEventListener("visibilitychange", onVisibility);
    };
  }, [intervalMs]);

  return {
    data,
    error,
    lastUpdated,
    loading,
    stale: Boolean(error && data),
  };
}

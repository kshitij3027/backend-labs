import { useEffect, useRef, useState } from "react";
import { WS_URL, getStats } from "../api.js";

// Reconnect backoff bounds (ms). Each failed/closed socket waits a bit longer,
// capped, so a backend that is briefly down does not get hammered but the
// dashboard still recovers quickly once it returns.
const RECONNECT_MIN_MS = 1000;
const RECONNECT_MAX_MS = 15000;

/**
 * Subscribe to the live `/ws/stream` snapshot feed.
 *
 * Behaviour:
 *   * On mount it fires a one-shot `getStats()` fetch and wraps the bare stats
 *     object as `{ stats }` so the cards have data to render immediately, even
 *     before the first WebSocket frame arrives.
 *   * Opens a WebSocket to {@link WS_URL}, parsing each JSON snapshot into state.
 *     Each frame is shaped `{ type, stats, quality, patterns, anomalies }`.
 *   * Auto-reconnects with exponential backoff on close or error.
 *   * Cleans up the socket and any pending reconnect timer on unmount.
 *
 * @returns {{ snapshot: (object|null), connected: boolean }}
 *   `snapshot` is the latest full WS frame, or `{ stats }` from the initial
 *   REST fetch, or `null` before anything has arrived.
 */
export function useClusterStream() {
  const [snapshot, setSnapshot] = useState(null);
  const [connected, setConnected] = useState(false);

  // Refs survive re-renders without re-triggering the effect.
  const socketRef = useRef(null);
  const reconnectTimerRef = useRef(null);
  const backoffRef = useRef(RECONNECT_MIN_MS);
  // Guards async callbacks from touching state after the component unmounts.
  const mountedRef = useRef(true);

  useEffect(() => {
    mountedRef.current = true;

    // Paint-on-mount: one REST stats snapshot so the cards aren't blank pre-WS.
    // The REST endpoint returns the bare `StatsSnapshot`, so wrap it to match
    // the WS frame shape the components read (`snapshot.stats`). A failure here
    // is non-fatal — the socket will deliver a full frame shortly.
    getStats()
      .then((stats) => {
        if (mountedRef.current && stats) {
          // Only adopt the REST snapshot if a richer WS frame hasn't landed yet.
          setSnapshot((prev) => prev || { stats });
        }
      })
      .catch(() => {
        /* ignore: the WS feed is the primary source */
      });

    const clearReconnectTimer = () => {
      if (reconnectTimerRef.current) {
        clearTimeout(reconnectTimerRef.current);
        reconnectTimerRef.current = null;
      }
    };

    const scheduleReconnect = () => {
      if (!mountedRef.current || reconnectTimerRef.current) {
        return;
      }
      const delay = backoffRef.current;
      reconnectTimerRef.current = setTimeout(() => {
        reconnectTimerRef.current = null;
        connect();
      }, delay);
      // Exponentially grow the next delay, capped.
      backoffRef.current = Math.min(backoffRef.current * 2, RECONNECT_MAX_MS);
    };

    const connect = () => {
      if (!mountedRef.current) {
        return;
      }
      let ws;
      try {
        ws = new WebSocket(WS_URL);
      } catch {
        // Construction itself can throw (e.g. bad URL); retry on backoff.
        scheduleReconnect();
        return;
      }
      socketRef.current = ws;

      ws.onopen = () => {
        if (!mountedRef.current) {
          return;
        }
        setConnected(true);
        // Healthy connection — reset the backoff for the next drop.
        backoffRef.current = RECONNECT_MIN_MS;
      };

      ws.onmessage = (event) => {
        if (!mountedRef.current) {
          return;
        }
        try {
          const data = JSON.parse(event.data);
          setSnapshot(data);
        } catch {
          /* ignore malformed frames; keep the last good snapshot */
        }
      };

      ws.onclose = () => {
        if (!mountedRef.current) {
          return;
        }
        setConnected(false);
        scheduleReconnect();
      };

      ws.onerror = () => {
        // An error is generally followed by onclose; close defensively so the
        // reconnect path runs exactly once.
        try {
          ws.close();
        } catch {
          /* already closing */
        }
      };
    };

    connect();

    return () => {
      mountedRef.current = false;
      clearReconnectTimer();
      const ws = socketRef.current;
      if (ws) {
        // Detach handlers so a close fired during teardown can't setState or
        // schedule another reconnect.
        ws.onopen = null;
        ws.onmessage = null;
        ws.onclose = null;
        ws.onerror = null;
        try {
          ws.close();
        } catch {
          /* ignore */
        }
        socketRef.current = null;
      }
    };
  }, []);

  return { snapshot, connected };
}

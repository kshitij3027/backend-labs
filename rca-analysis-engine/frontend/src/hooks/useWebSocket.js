import { useEffect, useRef, useState } from "react";

// Live incident feed over the backend's `/ws` WebSocket.
//
// The URL is RELATIVE to the current origin (`ws(s)://<host>/ws`), so the same build
// connects through Vite's dev-server ws proxy in dev and through nginx's `/ws` upgrade
// block in prod — no hardcoded host. The hook:
//   * parses each frame and forwards `{"type":"incident_update","data":<report>}` to
//     the caller's `onIncident` callback;
//   * sends a periodic `"ping"` keepalive (the backend replies `"pong"`, which we
//     ignore) so idle proxy timeouts don't silently drop the socket;
//   * reconnects with exponential backoff (+ jitter) on close/error;
//   * exposes a coarse `status`: "connecting" | "live" | "offline".

// Reconnect backoff bounds (ms) and keepalive cadence (ms).
const RECONNECT_BASE_MS = 1000;
const RECONNECT_MAX_MS = 15000;
const PING_INTERVAL_MS = 25000;

/** Build the same-origin `/ws` URL, upgrading to wss on an https page. */
function wsUrl() {
  const proto = window.location.protocol === "https:" ? "wss" : "ws";
  return `${proto}://${window.location.host}/ws`;
}

/**
 * Subscribe to the live incident feed.
 * @param {(report: object) => void} onIncident called with each incident_update payload
 * @returns {"connecting"|"live"|"offline"} current connection status
 */
export function useWebSocket(onIncident) {
  const [status, setStatus] = useState("connecting");

  // Keep the latest callback in a ref so re-renders that pass a new function identity
  // don't tear down and rebuild the socket (the effect runs once, on mount).
  const onIncidentRef = useRef(onIncident);
  onIncidentRef.current = onIncident;

  useEffect(() => {
    let alive = true; // guards against state updates / reconnects after unmount
    let ws = null;
    let reconnectTimer = null;
    let pingTimer = null;
    let attempt = 0; // consecutive failed connects — drives the backoff delay

    const stopPing = () => {
      if (pingTimer) {
        clearInterval(pingTimer);
        pingTimer = null;
      }
    };

    const scheduleReconnect = () => {
      if (!alive || reconnectTimer) return; // one pending reconnect at a time
      const base = Math.min(RECONNECT_MAX_MS, RECONNECT_BASE_MS * 2 ** attempt);
      const delay = base * (0.8 + Math.random() * 0.4); // ±20% jitter
      attempt += 1;
      reconnectTimer = setTimeout(() => {
        reconnectTimer = null;
        connect();
      }, delay);
    };

    const connect = () => {
      if (!alive) return;
      // Show "connecting" while we (re)establish, but don't downgrade a live socket.
      setStatus((s) => (s === "live" ? s : "connecting"));

      let socket;
      try {
        socket = new WebSocket(wsUrl());
      } catch {
        setStatus("offline");
        scheduleReconnect();
        return;
      }
      ws = socket;

      socket.onopen = () => {
        if (!alive) return;
        attempt = 0; // reset backoff on a good connection
        setStatus("live");
        pingTimer = setInterval(() => {
          if (socket.readyState === WebSocket.OPEN) {
            try {
              socket.send("ping");
            } catch {
              /* a failed send surfaces via onclose/onerror -> reconnect */
            }
          }
        }, PING_INTERVAL_MS);
      };

      socket.onmessage = (event) => {
        if (!alive) return;
        // Ignore the "pong" keepalive reply and any non-string/non-JSON frame.
        if (typeof event.data !== "string" || event.data === "pong") return;
        let msg;
        try {
          msg = JSON.parse(event.data);
        } catch {
          return;
        }
        if (msg && msg.type === "incident_update" && msg.data) {
          const cb = onIncidentRef.current;
          if (typeof cb === "function") cb(msg.data);
        }
      };

      // close and error both mean "the socket is gone" — tear down the keepalive,
      // flag offline, and schedule a backoff reconnect (once, guarded above).
      const onDown = () => {
        stopPing();
        if (!alive) return;
        setStatus("offline");
        scheduleReconnect();
      };
      socket.onclose = onDown;
      socket.onerror = onDown;
    };

    connect();

    return () => {
      alive = false;
      stopPing();
      if (reconnectTimer) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
      if (ws) {
        // Detach handlers so the unmount-triggered close can't schedule a reconnect.
        ws.onopen = ws.onmessage = ws.onclose = ws.onerror = null;
        try {
          ws.close();
        } catch {
          /* ignore */
        }
      }
    };
  }, []);

  return status;
}

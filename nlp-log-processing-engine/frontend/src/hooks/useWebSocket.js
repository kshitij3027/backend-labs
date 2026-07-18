import { useEffect, useRef, useState } from "react";

// Live feed over the backend's `/ws` WebSocket.
//
// The URL is RELATIVE to the current origin (`ws(s)://<host>/ws`), so the same build
// connects through Vite's dev-server ws proxy in dev and through nginx's `/ws` upgrade
// block in prod — no hardcoded host. The backend broadcasts two frame types:
//   * {"type":"analysis","data":<AnalysisResponse>}  — one per analyzed line
//   * {"type":"stats","data":<snapshot>}             — a fresh /api/stats snapshot
//
// The hook:
//   * parses each frame, keeping the latest analysis (`lastAnalysis`) and stats snapshot
//     (`lastStats`), and prepending each analysis to a capped, newest-first `feed`;
//   * sends a periodic `"ping"` keepalive (the backend replies `"pong"`, which we ignore)
//     so idle proxy timeouts don't silently drop the socket;
//   * reconnects with exponential backoff (+ jitter) on close/error;
//   * exposes a coarse `status`: "connecting" | "live" | "offline";
//   * is robust to malformed frames (a bad frame is ignored, never throws).

// Reconnect backoff bounds (ms), keepalive cadence (ms), and how many feed items to retain.
const RECONNECT_BASE_MS = 1000;
const RECONNECT_MAX_MS = 15000;
const PING_INTERVAL_MS = 25000;
const FEED_CAP = 50;

/** Build the same-origin `/ws` URL, upgrading to wss on an https page. */
function wsUrl() {
  const proto = window.location.protocol === "https:" ? "wss" : "ws";
  return `${proto}://${window.location.host}/ws`;
}

/**
 * Subscribe to the live feed.
 * @returns {{status: "connecting"|"live"|"offline",
 *            lastAnalysis: object|null,
 *            lastStats: object|null,
 *            feed: object[]}}
 */
export function useWebSocket() {
  const [status, setStatus] = useState("connecting");
  const [lastAnalysis, setLastAnalysis] = useState(null);
  const [lastStats, setLastStats] = useState(null);
  const [feed, setFeed] = useState([]);

  // Monotonic client-side id for stable React keys — analysis frames carry no id of their
  // own. A ref so it survives re-renders without being reset.
  const keyRef = useRef(0);

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

    const handleFrame = (raw) => {
      // Ignore the "pong" keepalive reply and any non-string / non-JSON frame.
      if (typeof raw !== "string" || raw === "pong") return;
      let msg;
      try {
        msg = JSON.parse(raw);
      } catch {
        return;
      }
      if (!msg || typeof msg !== "object") return;

      if (msg.type === "analysis" && msg.data && typeof msg.data === "object") {
        // Stamp a stable key + client receive-time (the payload has no timestamp) so the
        // feed can render a clock and React can key rows without index churn.
        const item = { ...msg.data, _key: (keyRef.current += 1), _ts: Date.now() };
        setLastAnalysis(msg.data);
        setFeed((prev) => [item, ...prev].slice(0, FEED_CAP));
      } else if (msg.type === "stats" && msg.data && typeof msg.data === "object") {
        setLastStats(msg.data);
      }
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
        handleFrame(event.data);
      };

      // close and error both mean "the socket is gone" — tear down the keepalive, flag
      // offline, and schedule a backoff reconnect (once, guarded above).
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

  return { status, lastAnalysis, lastStats, feed };
}

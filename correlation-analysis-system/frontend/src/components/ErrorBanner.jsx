import { useEffect, useState } from "react";
import { relativeTime } from "../util.js";

// Top-of-page outage banner. Renders nothing while healthy; when the polling hook
// reports an `error` it shows a red strip explaining that the backend is
// unreachable and (if we ever had a good poll) how stale the shown snapshot is.
//
// While an error persists the polling hook keeps setting the SAME error string, so
// React wouldn't re-render and the "N ago" would freeze. A tiny self-tick (only
// mounted while `error` is truthy) forces a refresh every 5s so the relative time
// keeps counting up during an outage.
//
// Props:
//   error       — error message string, or null when healthy
//   lastUpdated — Date of the last SUCCESSFUL poll, or null if none yet
export default function ErrorBanner({ error, lastUpdated }) {
  const [, setNowTick] = useState(0);

  useEffect(() => {
    if (!error) return undefined;
    const id = setInterval(() => setNowTick((n) => n + 1), 5000);
    return () => clearInterval(id);
  }, [error]);

  if (!error) return null;

  const detail = lastUpdated
    ? `showing last update from ${relativeTime(lastUpdated)}`
    : "no data yet";

  return (
    <div className="errorbanner" role="alert">
      <span className="errorbanner__icon" aria-hidden="true">
        ⚠
      </span>
      <span className="errorbanner__text">
        Backend unreachable — {detail}
      </span>
    </div>
  );
}

// Top-of-page outage banner. Renders nothing while healthy; when the WebSocket is
// offline or the initial history fetch failed, it shows a red strip explaining the
// degraded state. The dashboard keeps showing the last-good incidents underneath, so
// this only annotates that live updates are paused / data may be stale.
//
// Props:
//   message — the message to show, or null/empty when healthy (banner hidden).
export default function ErrorBanner({ message }) {
  if (!message) return null;

  return (
    <div className="errorbanner" role="alert">
      <span className="errorbanner__icon" aria-hidden="true">
        ⚠
      </span>
      <span className="errorbanner__text">{message}</span>
    </div>
  );
}

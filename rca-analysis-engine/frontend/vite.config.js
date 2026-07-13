import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Vite config for the RCA Analysis Engine dashboard.
//
// Production builds emit a static SPA into `dist/`, which the multi-stage
// Dockerfile copies into nginx; nginx then reverse-proxies `/api` (REST) and `/ws`
// (WebSocket) to the `backend` service. The same RELATIVE URLs the app uses
// (`/api/...`, `/ws`) work both in prod (behind nginx) and in local `npm run dev`,
// where the dev-server proxy below stands in for nginx.
//
// No prefix strip: this backend already namespaces every REST route under `/api/*`
// (`/api/health`, `/api/incidents`, ...), so the proxy forwards the path VERBATIM —
// the app calls `/api/incidents` and the proxy passes `http://localhost:8000/api/incidents`.
// This matches nginx.conf. The `/ws` entry sets `ws: true` so the dev server upgrades
// the WebSocket connection to the backend's `/ws` endpoint (mirrors nginx's upgrade block).
export default defineConfig({
  plugins: [react()],
  base: "/",
  build: {
    outDir: "dist",
  },
  server: {
    proxy: {
      // REST: forward the full path unchanged so `/api/incidents` ->
      // `http://localhost:8000/api/incidents`. NO `rewrite` — our endpoints
      // already live under `/api`.
      "/api": {
        target: "http://localhost:8000",
        changeOrigin: true,
      },
      // WebSocket: proxy `/ws` to the backend over a real ws:// upgrade so the live
      // incident feed works in local dev exactly as it does behind nginx in prod.
      "/ws": {
        target: "ws://localhost:8000",
        ws: true,
        changeOrigin: true,
      },
    },
  },
});

import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Vite config for the NLP Log Processing Engine dashboard.
//
// A production build emits a static SPA into `dist/`, which the multi-stage Dockerfile
// copies into nginx; nginx then reverse-proxies `/api` (REST) and `/ws` (WebSocket) to
// the `backend` service. The app only ever uses RELATIVE URLs (`/api/...`, `/ws`), so the
// exact same bundle works in prod (behind nginx) and in local `npm run dev`, where the
// dev-server proxy below stands in for nginx.
//
// No prefix strip: this backend already namespaces every REST route under `/api/*`
// (`/api/health`, `/api/analyze`, `/api/stats`, ...), so the proxy forwards the path
// VERBATIM — the app calls `/api/analyze` and the proxy passes it to
// `http://localhost:8000/api/analyze`. This mirrors nginx.conf. The `/ws` entry sets
// `ws: true` so the dev server upgrades the WebSocket connection to the backend's `/ws`
// endpoint (mirrors nginx's upgrade block), which streams the live `analysis` / `stats`
// frames the dashboard renders.
export default defineConfig({
  plugins: [react()],
  base: "/",
  build: {
    outDir: "dist",
  },
  server: {
    proxy: {
      // REST: forward the full path unchanged so `/api/analyze` ->
      // `http://localhost:8000/api/analyze`. NO `rewrite` — our endpoints already live
      // under `/api`.
      "/api": {
        target: "http://localhost:8000",
        changeOrigin: true,
      },
      // WebSocket: proxy `/ws` to the backend over a real ws:// upgrade so the live feed
      // works in local dev exactly as it does behind nginx in prod.
      "/ws": {
        target: "ws://localhost:8000",
        ws: true,
        changeOrigin: true,
      },
    },
  },
});

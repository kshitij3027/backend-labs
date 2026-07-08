import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Vite config for the correlation-analysis dashboard.
//
// Production builds emit a static SPA into `dist/`, which the multi-stage
// Dockerfile copies into nginx; nginx then reverse-proxies `/api` to the backend
// `backend` service. The same RELATIVE `/api/...` URLs used by the app code work
// both in prod (behind nginx) and in local `npm run dev`, where the dev-server
// proxy below stands in for nginx.
//
// IMPORTANT — no prefix strip. Unlike the sibling log-recommendation-engine (whose
// backend routes live at the root, so its proxy rewrites `/api/health` -> `/health`),
// THIS backend already namespaces every route under `/api/v1/*` (only `/health` is
// bare, and the dashboard never calls it). So we forward the path VERBATIM: the app
// calls `/api/v1/dashboard` and the proxy passes `http://localhost:8000/api/v1/dashboard`.
// This matches nginx.conf, which likewise proxies `/api/` straight through with no rewrite.
export default defineConfig({
  plugins: [react()],
  base: "/",
  build: {
    outDir: "dist",
  },
  server: {
    proxy: {
      // REST: forward the full path unchanged so `/api/v1/dashboard` ->
      // `http://localhost:8000/api/v1/dashboard`. NO `rewrite` — our endpoints
      // already live under `/api`.
      "/api": {
        target: "http://localhost:8000",
        changeOrigin: true,
      },
    },
  },
});

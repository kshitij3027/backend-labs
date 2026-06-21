import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Vite config for the dashboard.
//
// Production builds emit a static SPA into `dist/`, which the multi-stage
// Dockerfile copies into nginx; nginx then reverse-proxies `/api` and `/ws`
// to the backend `app` service. The same RELATIVE URLs used by the app code
// therefore work both in prod (behind nginx) and in local `npm run dev`,
// where the dev server's proxy below stands in for nginx.
export default defineConfig({
  plugins: [react()],
  build: {
    outDir: "dist",
  },
  server: {
    proxy: {
      // REST: drop the `/api` prefix so `/api/stats` -> `http://localhost:8000/stats`.
      "/api": {
        target: "http://localhost:8000",
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ""),
      },
      // WebSocket: `/ws/metrics` proxies straight through to the backend WS.
      "/ws": {
        target: "ws://localhost:8000",
        ws: true,
        changeOrigin: true,
      },
    },
  },
});

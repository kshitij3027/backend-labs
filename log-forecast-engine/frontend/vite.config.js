import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Vite config for the forecast dashboard.
//
// Production builds emit a static SPA into `dist/`, which the multi-stage
// Dockerfile copies into nginx; nginx then reverse-proxies `/api` to the
// backend `api` service. The same RELATIVE `/api/...` URLs used by the app
// code therefore work both in prod (behind nginx) and in local `npm run dev`,
// where the dev-server proxy below stands in for nginx.
export default defineConfig({
  plugins: [react()],
  base: "/",
  build: {
    outDir: "dist",
  },
  server: {
    proxy: {
      // REST: drop the `/api` prefix so `/api/predictions` -> `http://localhost:8000/predictions`.
      "/api": {
        target: "http://localhost:8000",
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ""),
      },
    },
  },
});

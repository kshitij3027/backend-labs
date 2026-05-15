import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// Dev server runs on port 3000 with proxy → backend:8000 for /api/* and /health.
// Production is served by nginx (see Dockerfile + nginx.conf); the same /api/* prefix
// is proxied at that layer too, so the frontend code never needs to know the backend URL.
export default defineConfig({
  plugins: [react()],
  server: {
    host: "0.0.0.0",
    port: 3000,
    proxy: {
      "/api": {
        target: "http://localhost:8000",
        changeOrigin: true,
      },
      "/health": {
        target: "http://localhost:8000",
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: "dist",
    sourcemap: false,
  },
});

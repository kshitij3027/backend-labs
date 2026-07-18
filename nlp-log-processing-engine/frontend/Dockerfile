# syntax=docker/dockerfile:1.6
#
# Multi-stage build for the NLP Log Processing Engine dashboard.
#
# Stage 1 (node) installs deps and produces the static SPA in /app/dist.
# Stage 2 (nginx) serves those assets and reverse-proxies /api (REST) + /ws (WebSocket)
# to the `backend` service (see nginx.conf).
#
# IMPORTANT: the docker-compose `frontend` service builds with `context: .` (the PROJECT
# ROOT) and `dockerfile: frontend/Dockerfile`, so all COPY source paths below are written
# relative to the project root and therefore carry the `frontend/` prefix.

# ---- Stage 1: build the SPA -------------------------------------------------
FROM node:20-alpine AS build
WORKDIR /app

# Install dependencies first so this layer is cached unless the manifest changes.
# package-lock.json is copied when present (the `*` keeps the COPY from failing if no
# lockfile is committed yet). We use `npm install` rather than `npm ci` because a lockfile
# may not exist; `npm install` works with or without one.
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm install

# Copy the rest of the frontend source and build the production bundle.
COPY frontend/ ./
RUN npm run build

# ---- Stage 2: serve via nginx ----------------------------------------------
FROM nginx:1.27-alpine

# Static assets from the build stage.
COPY --from=build /app/dist /usr/share/nginx/html

# Our server config (SPA fallback + /api reverse proxy + /ws upgrade to `backend:8000`).
COPY frontend/nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

# nginx:alpine's default CMD already runs nginx in the foreground.

# Build stage: install deps + produce static assets
FROM node:20-alpine AS builder
WORKDIR /app
COPY frontend/package.json ./
# `npm install` instead of `npm ci` because we ship package.json without a lockfile.
# Add --no-audit --no-fund for a cleaner build log.
RUN npm install --no-audit --no-fund
COPY frontend/ ./
RUN npm run build

# Serve stage: nginx serves /usr/share/nginx/html
FROM nginx:1.27-alpine
COPY frontend/nginx.conf /etc/nginx/nginx.conf
COPY --from=builder /app/dist /usr/share/nginx/html
EXPOSE 3000
HEALTHCHECK --interval=10s --timeout=5s --retries=3 --start-period=10s \
  CMD wget -qO- http://127.0.0.1:3000/ > /dev/null || exit 1

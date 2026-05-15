import axios from "axios";

// Persisted token key — kept consistent across the app.
export const TOKEN_KEY = "rbac_access_token";

// Single axios instance. baseURL '/api' aligns with the nginx + vite proxies.
const api = axios.create({
  baseURL: "/api",
  headers: { "Content-Type": "application/json" },
});

// Request interceptor — auto-attach the bearer token if one is stored.
api.interceptors.request.use((config) => {
  const token = localStorage.getItem(TOKEN_KEY);
  if (token) {
    config.headers = config.headers || {};
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

// Response interceptor — on 401 from a protected endpoint, scrub the token so the UI
// drops back to the login screen instead of looping with a stale token. We intentionally
// do NOT auto-redirect here — the AuthContext will detect missing user state and render Login.
api.interceptors.response.use(
  (r) => r,
  (err) => {
    if (err.response && err.response.status === 401) {
      localStorage.removeItem(TOKEN_KEY);
    }
    return Promise.reject(err);
  },
);

export default api;

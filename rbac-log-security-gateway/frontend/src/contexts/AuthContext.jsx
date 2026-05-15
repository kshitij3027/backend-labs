import { createContext, useEffect, useState, useCallback } from "react";
import api, { TOKEN_KEY } from "../api/client";

export const AuthContext = createContext({
  user: null,
  loading: true,
  loginError: null,
  login: async () => {},
  logout: () => {},
});

export function AuthProvider({ children }) {
  const [user, setUser] = useState(null);
  const [loading, setLoading] = useState(true);
  const [loginError, setLoginError] = useState(null);

  // Hydrate: if a token exists in localStorage, fetch /auth/profile to validate it.
  useEffect(() => {
    const token = localStorage.getItem(TOKEN_KEY);
    if (!token) {
      setLoading(false);
      return;
    }
    api
      .get("/auth/profile")
      .then((r) => setUser(r.data))
      .catch(() => {
        // Token invalid/expired — interceptor already cleared it.
        setUser(null);
      })
      .finally(() => setLoading(false));
  }, []);

  const login = useCallback(async (username, password) => {
    setLoginError(null);
    try {
      const r = await api.post("/auth/login", { username, password });
      localStorage.setItem(TOKEN_KEY, r.data.access_token);
      setUser(r.data.user_info);
      return r.data.user_info;
    } catch (err) {
      const detail =
        err.response && err.response.data && err.response.data.detail
          ? typeof err.response.data.detail === "string"
            ? err.response.data.detail
            : "invalid credentials"
          : "network error";
      setLoginError(detail);
      throw err;
    }
  }, []);

  const logout = useCallback(() => {
    localStorage.removeItem(TOKEN_KEY);
    setUser(null);
    setLoginError(null);
  }, []);

  return (
    <AuthContext.Provider value={{ user, loading, loginError, login, logout }}>
      {children}
    </AuthContext.Provider>
  );
}

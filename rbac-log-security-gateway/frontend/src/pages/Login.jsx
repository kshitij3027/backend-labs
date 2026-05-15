import { useState } from "react";
import useAuth from "../hooks/useAuth";

export default function Login() {
  const { login, loginError } = useAuth();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [submitting, setSubmitting] = useState(false);

  async function onSubmit(e) {
    e.preventDefault();
    setSubmitting(true);
    try {
      await login(username, password);
    } catch (_err) {
      // error surfaced by context.loginError
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <main className="container">
      <h1>RBAC Log Security Gateway</h1>
      <h2>Sign in</h2>
      <form onSubmit={onSubmit} className="login-form" aria-label="login">
        <label>
          Username
          <input
            type="text"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            autoComplete="username"
            required
            data-testid="login-username"
          />
        </label>
        <label>
          Password
          <input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            autoComplete="current-password"
            required
            data-testid="login-password"
          />
        </label>
        <button type="submit" disabled={submitting} data-testid="login-submit">
          {submitting ? "Signing in…" : "Sign in"}
        </button>
        {loginError && (
          <p className="error" role="alert" data-testid="login-error">
            {loginError}
          </p>
        )}
      </form>
      <section>
        <h3>Demo users</h3>
        <ul>
          <li><code>alice</code> / <code>admin123</code> — administrator</li>
          <li><code>bob</code> / <code>dev123</code> — developer</li>
          <li><code>carol</code> / <code>analyst123</code> — analyst</li>
          <li><code>dave</code> / <code>support123</code> — support</li>
        </ul>
      </section>
    </main>
  );
}

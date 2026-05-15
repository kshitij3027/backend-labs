import { AuthProvider } from "./contexts/AuthContext";
import Login from "./pages/Login";
import useAuth from "./hooks/useAuth";

function LandingShell() {
  const { user, loading, logout } = useAuth();

  if (loading) {
    return (
      <main className="container">
        <p>Loading…</p>
      </main>
    );
  }

  if (!user) {
    return <Login />;
  }

  return (
    <main className="container">
      <header className="row" data-testid="logged-in-header">
        <h1>RBAC Log Security Gateway</h1>
        <button onClick={logout} data-testid="logout-button">
          Log out
        </button>
      </header>
      <section>
        <h2>Welcome, {user.display_name || user.username}!</h2>
        <p data-testid="logged-in-username">Signed in as <strong>{user.username}</strong></p>
        <p>
          Roles:{" "}
          {user.roles.map((r) => (
            <span key={r} className="role-chip" data-testid={`role-${r}`}>
              {r}
            </span>
          ))}
        </p>
        <p className="note">
          Protected dashboard + log search + admin pages land in subsequent commits.
        </p>
      </section>
    </main>
  );
}

export default function App() {
  return (
    <AuthProvider>
      <LandingShell />
    </AuthProvider>
  );
}

import { Link } from "react-router-dom";
import useAuth from "../hooks/useAuth";

const ROLE_DEFAULT_SCOPE = {
  administrator: "*",
  developer: "application",
  analyst: "business",
  support: "application.auth",
};

export default function Dashboard() {
  const { user } = useAuth();
  const isAdmin = user.roles.includes("administrator");
  const scope = user.roles.map((r) => ROLE_DEFAULT_SCOPE[r] || "?").join(", ");

  return (
    <main className="container">
      <h1>Dashboard</h1>
      <p>
        Welcome, <strong data-testid="dashboard-username">{user.display_name || user.username}</strong>!
        You're signed in as <strong>{user.username}</strong>.
      </p>
      <section className="tile-row" data-testid="role-summary">
        <div className="tile">
          <h3>Roles</h3>
          <p>
            {user.roles.map((r) => (
              <span key={r} className="role-chip" data-testid={`role-${r}`}>{r}</span>
            ))}
          </p>
        </div>
        <div className="tile">
          <h3>Default scope</h3>
          <p data-testid="default-scope"><code>{scope}</code></p>
        </div>
        <div className="tile">
          <h3>Log search</h3>
          <p>Query logs constrained by your role policies. Visit <Link to="/logs">Logs</Link>.</p>
        </div>
        {isAdmin && (
          <div className="tile admin-tile" data-testid="admin-tile-audit">
            <h3>Audit dashboard</h3>
            <p>Admin-only: review every request the gateway has handled.</p>
            <Link to="/admin" data-testid="admin-tile-audit-link">View admin →</Link>
          </div>
        )}
        {isAdmin && (
          <div className="tile admin-tile" data-testid="admin-tile-policies">
            <h3>RBAC policies</h3>
            <p>Admin-only: inspect role → permission mappings.</p>
          </div>
        )}
      </section>
    </main>
  );
}

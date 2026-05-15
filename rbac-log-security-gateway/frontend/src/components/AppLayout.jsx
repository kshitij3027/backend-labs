import { Link, Outlet } from "react-router-dom";
import useAuth from "../hooks/useAuth";

export default function AppLayout() {
  const { user, logout } = useAuth();

  return (
    <>
      <header className="app-nav">
        <div className="row">
          <Link to="/dashboard" className="brand">RBAC Gateway</Link>
          <nav data-testid="primary-nav">
            <Link to="/dashboard" data-testid="nav-dashboard">Dashboard</Link>
            {user && user.roles.includes("administrator") && (
              <Link to="/admin" data-testid="nav-admin">Admin</Link>
            )}
          </nav>
          <div className="user-block">
            {user && (
              <>
                <span data-testid="header-username">{user.username}</span>
                <button onClick={logout} data-testid="logout-button">Log out</button>
              </>
            )}
          </div>
        </div>
      </header>
      <Outlet />
    </>
  );
}

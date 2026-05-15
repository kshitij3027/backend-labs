import { Navigate, useLocation } from "react-router-dom";
import useAuth from "../hooks/useAuth";

/**
 * Guards a route. If auth is still loading, render a small placeholder.
 * If no user is signed in, redirect to /login (preserving the attempted path).
 */
export default function ProtectedRoute({ children, requiredRole }) {
  const { user, loading } = useAuth();
  const location = useLocation();

  if (loading) {
    return (
      <main className="container">
        <p>Loading…</p>
      </main>
    );
  }

  if (!user) {
    return <Navigate to="/login" state={{ from: location.pathname }} replace />;
  }

  if (requiredRole && !user.roles.includes(requiredRole)) {
    return (
      <main className="container">
        <h2>Forbidden</h2>
        <p data-testid="forbidden-message">Your role ({user.roles.join(", ")}) does not have access to this page.</p>
      </main>
    );
  }

  return children;
}

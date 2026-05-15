import { createBrowserRouter, Navigate } from "react-router-dom";
import AppLayout from "./components/AppLayout";
import ProtectedRoute from "./components/ProtectedRoute";
import Dashboard from "./pages/Dashboard";
import Login from "./pages/Login";

// Admin and LogSearch placeholders live here so the routes are wired even before C13
// fully implements them. C13 replaces these with real pages.
function AdminPlaceholder() {
  return (
    <main className="container">
      <h1>Admin</h1>
      <p>Admin page lands in C13.</p>
    </main>
  );
}

function LogSearchPlaceholder() {
  return (
    <main className="container">
      <h1>Log search</h1>
      <p>Log search page lands in C13.</p>
    </main>
  );
}

export const router = createBrowserRouter([
  {
    path: "/login",
    element: <Login />,
  },
  {
    element: (
      <ProtectedRoute>
        <AppLayout />
      </ProtectedRoute>
    ),
    children: [
      { path: "/", element: <Navigate to="/dashboard" replace /> },
      { path: "/dashboard", element: <Dashboard /> },
      { path: "/logs", element: <LogSearchPlaceholder /> },
      {
        path: "/admin",
        element: (
          <ProtectedRoute requiredRole="administrator">
            <AdminPlaceholder />
          </ProtectedRoute>
        ),
      },
    ],
  },
  { path: "*", element: <Navigate to="/dashboard" replace /> },
]);

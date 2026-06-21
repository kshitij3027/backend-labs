// Central Chart.js registration.
//
// react-chartjs-2 v5 does NOT auto-register Chart.js controllers/elements/scales
// (that was removed so bundles can tree-shake). Each chart component imports this
// module for its side effect, so the elements every chart in this dashboard uses
// are registered exactly once, app-wide — this is what prevents the classic
// "category" is not a registered scale / "arc"/"bar" is not a registered element
// runtime errors.
import {
  Chart as ChartJS,
  ArcElement,
  BarElement,
  CategoryScale,
  LinearScale,
  Tooltip,
  Legend,
} from "chart.js";

ChartJS.register(
  ArcElement, // doughnut/pie segments
  BarElement, // bar (vertical + horizontal) rectangles
  CategoryScale, // discrete-label axis (x for bars, y for horizontal bars)
  LinearScale, // numeric value axis
  Tooltip,
  Legend,
);

// Shared dark-theme defaults so every chart reads legibly on the slate background
// without each component re-specifying colors. Components can still override.
ChartJS.defaults.color = "#94a3b8"; // --text-muted
ChartJS.defaults.borderColor = "rgba(51, 65, 85, 0.6)"; // --border, softened
ChartJS.defaults.font.family =
  '"Inter", system-ui, -apple-system, "Segoe UI", Roboto, sans-serif';

export { ChartJS };

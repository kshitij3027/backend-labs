import { useMemo, useState } from "react";
import {
  LOG_LEVELS,
  SOURCE_TYPES,
  hhmmss,
  levelClass,
  levelRank,
  shortId,
  sourceLabel,
  truncate,
} from "../util.js";

// Sortable column header (keyboard operable). `col` is the sort key it drives.
function SortHead({ label, col, sort, onSort }) {
  const active = sort.key === col;
  const arrow = active ? (sort.dir === "asc" ? "▲" : "▼") : "";
  return (
    <th
      className={"datatable__sort" + (active ? " datatable__sort--active" : "")}
      role="button"
      tabIndex={0}
      aria-sort={active ? (sort.dir === "asc" ? "ascending" : "descending") : "none"}
      onClick={() => onSort(col)}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          onSort(col);
        }
      }}
    >
      <span className="datatable__sortlabel">
        {label}
        <span className="datatable__arrow" aria-hidden="true">
          {arrow}
        </span>
      </span>
    </th>
  );
}

// Sort value for a log row on a given key. Time is numeric epoch seconds, Level is
// ranked by severity (so desc surfaces ERROR first), Source/Service are strings.
function sortValue(ev, key) {
  if (key === "timestamp") {
    const x = Number(ev?.timestamp);
    return Number.isFinite(x) ? x : -Infinity;
  }
  if (key === "level") return levelRank(ev?.level);
  return String(ev?.[key] ?? "");
}

// Recent raw log events with client-side sorting + source/level filtering. Level is
// colour-coded (ERROR red / WARN amber / INFO grey) and error rows get a subtle
// tint. The raw rows are never mutated — sorting/filtering work on a copy.
//
// Props:
//   logs — dashboard.recent_logs, or [] while loading / degraded
export default function LogsTable({ logs = [] }) {
  const rows = Array.isArray(logs) ? logs : [];
  const [sort, setSort] = useState({ key: "timestamp", dir: "desc" });
  const [filterSource, setFilterSource] = useState(null); // null = All
  const [filterLevel, setFilterLevel] = useState(null); // null = All

  const onSort = (key) =>
    setSort((prev) =>
      prev.key === key
        ? { key, dir: prev.dir === "asc" ? "desc" : "asc" }
        : { key, dir: "desc" },
    );

  const visible = useMemo(() => {
    const base = rows.filter(
      (ev) =>
        (!filterSource || ev?.source === filterSource) &&
        (!filterLevel || String(ev?.level ?? "").toUpperCase() === filterLevel),
    );
    const mul = sort.dir === "asc" ? 1 : -1;
    return base.sort((a, b) => {
      const va = sortValue(a, sort.key);
      const vb = sortValue(b, sort.key);
      if (typeof va === "string" || typeof vb === "string") {
        return mul * String(va).localeCompare(String(vb));
      }
      return mul * (va - vb);
    });
  }, [rows, filterSource, filterLevel, sort]);

  return (
    <section className="panel">
      <div className="panel__head">
        <h2 className="panel__title">Recent logs</h2>
        <span className="panel__count">{rows.length}</span>
      </div>

      <div className="filters">
        <div className="filters__group" aria-label="Filter by source">
          <span className="filters__label">Source</span>
          <button
            type="button"
            className={`chip${filterSource === null ? " chip--on" : ""}`}
            onClick={() => setFilterSource(null)}
            aria-pressed={filterSource === null}
          >
            All
          </button>
          {SOURCE_TYPES.map((s) => (
            <button
              key={s}
              type="button"
              className={`chip${filterSource === s ? " chip--on" : ""}`}
              onClick={() => setFilterSource((cur) => (cur === s ? null : s))}
              aria-pressed={filterSource === s}
            >
              {sourceLabel(s)}
            </button>
          ))}
        </div>

        <div className="filters__group" aria-label="Filter by level">
          <span className="filters__label">Level</span>
          <button
            type="button"
            className={`chip${filterLevel === null ? " chip--on" : ""}`}
            onClick={() => setFilterLevel(null)}
            aria-pressed={filterLevel === null}
          >
            All
          </button>
          {LOG_LEVELS.map((l) => (
            <button
              key={l}
              type="button"
              className={`chip chip--lvl-${levelClass(l)}${
                filterLevel === l ? " chip--on" : ""
              }`}
              onClick={() => setFilterLevel((cur) => (cur === l ? null : l))}
              aria-pressed={filterLevel === l}
            >
              <span className="chip__dot" aria-hidden="true" />
              {l}
            </button>
          ))}
        </div>

        <span className="filters__count">{visible.length} shown</span>
      </div>

      <div className="tablewrap">
        <table className="datatable">
          <thead>
            <tr>
              <SortHead label="Time" col="timestamp" sort={sort} onSort={onSort} />
              <SortHead label="Source" col="source" sort={sort} onSort={onSort} />
              <SortHead label="Service" col="service" sort={sort} onSort={onSort} />
              <SortHead label="Level" col="level" sort={sort} onSort={onSort} />
              <th>Message</th>
              <th>Corr ID</th>
            </tr>
          </thead>
          <tbody>
            {visible.length === 0 ? (
              <tr>
                <td className="datatable__empty" colSpan={6}>
                  {rows.length === 0 ? "No logs yet" : "No logs match these filters"}
                </td>
              </tr>
            ) : (
              visible.map((ev, i) => {
                const lvl = levelClass(ev?.level);
                return (
                  <tr
                    key={ev?.id ?? i}
                    className={lvl === "error" ? "datatable__row--error" : undefined}
                  >
                    <td className="datatable__time">{hhmmss(ev?.timestamp)}</td>
                    <td>{ev?.source ?? "—"}</td>
                    <td>{ev?.service ?? "—"}</td>
                    <td>
                      <span className={`level level--${lvl}`}>
                        {String(ev?.level ?? "—").toUpperCase()}
                      </span>
                    </td>
                    <td className="datatable__msg" title={ev?.message ?? ""}>
                      {truncate(ev?.message, 90)}
                    </td>
                    <td className="datatable__corr" title={ev?.correlation_id ?? ""}>
                      {shortId(ev?.correlation_id)}
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}

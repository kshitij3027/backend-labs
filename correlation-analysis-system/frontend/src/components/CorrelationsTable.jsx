import { useMemo, useState } from "react";
import { CORRELATION_TYPES, fmt, hhmmss, typeClass, typeLabel } from "../util.js";

// A strength cell: a small proportional bar plus the 2dp value. Width is clamped
// to [0, 1] so an out-of-range strength can't overflow the cell.
function StrengthBar({ value }) {
  const x = Number(value);
  const pct = Number.isFinite(x) ? Math.max(0, Math.min(1, x)) * 100 : 0;
  return (
    <div className="strengthbar" title={fmt(value, 3)}>
      <div className="strengthbar__track">
        <div className="strengthbar__fill" style={{ width: `${pct}%` }} />
      </div>
      <span className="strengthbar__val">{fmt(value, 2)}</span>
    </div>
  );
}

// One event endpoint (source over service) for the A / B columns.
function Endpoint({ event }) {
  const ev = event ?? {};
  return (
    <div className="endpoint">
      <span className="endpoint__source">{ev.source ?? "—"}</span>
      {ev.service ? <span className="endpoint__service">{ev.service}</span> : null}
    </div>
  );
}

// Sortable column header. Renders an active-direction arrow and is keyboard
// operable (Enter/Space). `col` is the sort key it drives.
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

// Sort value for a correlation on a given key. Numeric keys return a number
// (non-finite → -Infinity so junk sinks to the bottom in asc order); the type key
// returns a string compared lexically.
function sortValue(c, key) {
  if (key === "correlation_type") return String(c?.correlation_type ?? "");
  const x = Number(c?.[key]);
  return Number.isFinite(x) ? x : -Infinity;
}

// Recent detected correlations with client-side sorting + type filtering. The raw
// rows are never mutated — every derived view works on a copy. Every field is read
// defensively so a partial row never crashes the render.
//
// Props:
//   correlations — dashboard.recent_correlations, or [] while loading / degraded
export default function CorrelationsTable({ correlations = [] }) {
  const rows = Array.isArray(correlations) ? correlations : [];
  const [sort, setSort] = useState({ key: "detected_at", dir: "desc" });
  const [filterType, setFilterType] = useState(null); // null = All

  // Toggle direction when re-clicking the active column; a new column starts desc.
  const onSort = (key) =>
    setSort((prev) =>
      prev.key === key
        ? { key, dir: prev.dir === "asc" ? "desc" : "asc" }
        : { key, dir: "desc" },
    );

  const visible = useMemo(() => {
    const base = filterType
      ? rows.filter((c) => c?.correlation_type === filterType)
      : rows.slice();
    const mul = sort.dir === "asc" ? 1 : -1;
    return base.sort((a, b) => {
      const va = sortValue(a, sort.key);
      const vb = sortValue(b, sort.key);
      if (typeof va === "string" || typeof vb === "string") {
        return mul * String(va).localeCompare(String(vb));
      }
      return mul * (va - vb);
    });
  }, [rows, filterType, sort]);

  return (
    <section className="panel">
      <div className="panel__head">
        <h2 className="panel__title">Recent correlations</h2>
        <span className="panel__count">{rows.length}</span>
      </div>

      <div className="filters">
        <div className="filters__group">
          <button
            type="button"
            className={`chip${filterType === null ? " chip--on" : ""}`}
            onClick={() => setFilterType(null)}
            aria-pressed={filterType === null}
          >
            All
          </button>
          {CORRELATION_TYPES.map((t) => (
            <button
              key={t}
              type="button"
              className={`chip type--${typeClass(t)}${filterType === t ? " chip--on" : ""}`}
              onClick={() => setFilterType((cur) => (cur === t ? null : t))}
              aria-pressed={filterType === t}
            >
              <span className="chip__dot" aria-hidden="true" />
              {typeLabel(t)}
            </button>
          ))}
        </div>
        <span className="filters__count">{visible.length} shown</span>
      </div>

      <div className="tablewrap">
        <table className="datatable">
          <thead>
            <tr>
              <SortHead label="Time" col="detected_at" sort={sort} onSort={onSort} />
              <SortHead label="Type" col="correlation_type" sort={sort} onSort={onSort} />
              <th>A</th>
              <th>B</th>
              <SortHead label="Strength" col="strength" sort={sort} onSort={onSort} />
              <SortHead label="Conf." col="confidence" sort={sort} onSort={onSort} />
            </tr>
          </thead>
          <tbody>
            {visible.length === 0 ? (
              <tr>
                <td className="datatable__empty" colSpan={6}>
                  {rows.length === 0
                    ? "No correlations detected yet"
                    : "No correlations match this filter"}
                </td>
              </tr>
            ) : (
              visible.map((c, i) => (
                <tr key={c?.id ?? i}>
                  <td className="datatable__time">{hhmmss(c?.detected_at)}</td>
                  <td>
                    <span className={`typechip type--${typeClass(c?.correlation_type)}`}>
                      <span className="typechip__dot" aria-hidden="true" />
                      <span className="typechip__label">
                        {typeLabel(c?.correlation_type)}
                      </span>
                    </span>
                  </td>
                  <td>
                    <Endpoint event={c?.event_a} />
                  </td>
                  <td>
                    <Endpoint event={c?.event_b} />
                  </td>
                  <td>
                    <StrengthBar value={c?.strength} />
                  </td>
                  <td className="datatable__num">{fmt(c?.confidence, 2)}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </section>
  );
}

import { Fragment } from "react";
import { clamp01, fmt, sourceLabel } from "../util.js";

// Source×source correlation-strength heatmap — hand-rolled CSS grid (no charting
// lib). Each cell's background interpolates card→accent by its strength via a
// per-cell `--v` custom property (see .heatmap__cell in styles.css). The matrix is
// symmetric with a 0 diagonal (a source never correlates with itself).
//
// Props:
//   matrix — dashboard.matrix ({ sources: string[], cells: number[][] }),
//            or the empty shape { sources: [], cells: [] } while degraded.
export default function MatrixHeatmap({ matrix }) {
  const sources = Array.isArray(matrix?.sources) ? matrix.sources : [];
  const cells = Array.isArray(matrix?.cells) ? matrix.cells : [];
  const n = sources.length;
  // Only render once the payload is coherent (square cells matching the sources).
  const ready = n > 0 && cells.length === n;

  return (
    <section className="panel">
      <div className="panel__head">
        <h2 className="panel__title">Source correlation matrix</h2>
        <span className="panel__count">{n ? `${n}×${n}` : "0"}</span>
      </div>

      {!ready ? (
        <div className="chart-empty" style={{ minHeight: 180 }}>
          Waiting for data…
        </div>
      ) : (
        <>
          <div className="heatmap-wrap">
            <div
              className="heatmap"
              style={{ gridTemplateColumns: `auto repeat(${n}, minmax(0, 1fr))` }}
            >
              <div className="heatmap__corner" aria-hidden="true" />
              {sources.map((s) => (
                <div key={`col-${s}`} className="heatmap__colhead" title={s}>
                  {sourceLabel(s)}
                </div>
              ))}

              {sources.map((rowSrc, i) => (
                <Fragment key={`row-${rowSrc}`}>
                  <div className="heatmap__rowhead" title={rowSrc}>
                    {sourceLabel(rowSrc)}
                  </div>
                  {sources.map((colSrc, j) => {
                    const v = clamp01(cells[i]?.[j]);
                    const diag = i === j;
                    const cls =
                      "heatmap__cell" +
                      (diag ? " heatmap__cell--diag" : "") +
                      (!diag && v >= 0.55 ? " heatmap__cell--strong" : "");
                    return (
                      <div
                        key={`${rowSrc}-${colSrc}`}
                        className={cls}
                        style={{ "--v": v }}
                        title={`${rowSrc} ↔ ${colSrc}: ${fmt(v, 2)}`}
                        aria-label={`${rowSrc} to ${colSrc}: ${fmt(v, 2)}`}
                      >
                        {!diag && v > 0 ? fmt(v, 2) : ""}
                      </div>
                    );
                  })}
                </Fragment>
              ))}
            </div>
          </div>

          <div className="heatmap__scale" aria-hidden="true">
            <span className="heatmap__scalelabel">0</span>
            <span className="heatmap__scalebar" />
            <span className="heatmap__scalelabel">1</span>
          </div>
        </>
      )}
    </section>
  );
}

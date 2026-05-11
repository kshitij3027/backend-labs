from collections import deque
from typing import Any, Dict

import dash
import httpx
import plotly.graph_objects as go
from dash import Input, Output, State, dcc, html


_HISTORY_LEN = 60
_STATE_CLASS = {
    "normal": "state-normal",
    "pressure": "state-pressure",
    "overload": "state-overload",
    "recovery": "state-recovery",
}
_STATE_COLOR = {
    "normal": "#28a745",
    "pressure": "#ffc107",
    "overload": "#dc3545",
    "recovery": "#17a2b8",
}


def build_app(base_url: str = "http://localhost:8000") -> dash.Dash:
    app = dash.Dash(__name__, title="Adaptive Backpressure Manager")
    history: Dict[str, deque] = {
        "ts": deque(maxlen=_HISTORY_LEN),
        "score": deque(maxlen=_HISTORY_LEN),
        "throttle": deque(maxlen=_HISTORY_LEN),
        "critical": deque(maxlen=_HISTORY_LEN),
        "high": deque(maxlen=_HISTORY_LEN),
        "normal": deque(maxlen=_HISTORY_LEN),
        "low": deque(maxlen=_HISTORY_LEN),
        "cpu": deque(maxlen=_HISTORY_LEN),
        "mem": deque(maxlen=_HISTORY_LEN),
    }

    app.layout = html.Div(
        [
            html.H1("Adaptive Backpressure Manager", style={"textAlign": "center"}),
            html.Div(
                id="state-banner",
                className="state-normal",
                children="STATE: normal",
                style={
                    "padding": "16px",
                    "fontSize": "20px",
                    "fontWeight": "bold",
                    "textAlign": "center",
                    "backgroundColor": _STATE_COLOR["normal"],
                    "color": "white",
                    "marginBottom": "16px",
                    "borderRadius": "4px",
                },
            ),
            html.Div(
                [
                    dcc.Graph(id="pressure-chart"),
                    dcc.Graph(id="queue-chart"),
                    dcc.Graph(id="throttle-chart"),
                    dcc.Graph(id="resource-gauges"),
                ],
                style={"display": "grid", "gridTemplateColumns": "1fr 1fr", "gap": "16px"},
            ),
            html.Hr(),
            html.Div(
                [
                    html.H3("Manual Ingest"),
                    dcc.Input(id="manual-msg", type="text", placeholder="message body", style={"width": "300px"}),
                    dcc.Dropdown(
                        id="manual-priority",
                        options=[{"label": p, "value": p} for p in ("critical", "high", "normal", "low")],
                        value="normal",
                        style={"width": "180px", "display": "inline-block", "marginLeft": "8px"},
                    ),
                    html.Button("Submit", id="manual-submit", n_clicks=0, style={"marginLeft": "8px"}),
                    html.Span(id="manual-result", style={"marginLeft": "12px"}),
                ],
                style={"marginBottom": "16px"},
            ),
            html.Div(
                [
                    html.H3("Load Tester"),
                    html.Label("RPS: "),
                    dcc.Input(id="lt-rps", type="number", value=200, min=1, max=10000, step=50),
                    html.Label("Duration (s): ", style={"marginLeft": "8px"}),
                    dcc.Input(id="lt-duration", type="number", value=30, min=1, max=600, step=5),
                    html.Label("Spike x: ", style={"marginLeft": "8px"}),
                    dcc.Input(id="lt-spike", type="number", value=10.0, min=1.0, max=50.0, step=1.0),
                    html.Button("Start", id="lt-start", n_clicks=0, style={"marginLeft": "8px"}),
                    html.Button("Stop", id="lt-stop", n_clicks=0, style={"marginLeft": "8px"}),
                    html.Button(
                        "10x SPIKE",
                        id="lt-spike-now",
                        n_clicks=0,
                        style={"marginLeft": "16px", "backgroundColor": "#dc3545", "color": "white"},
                    ),
                    html.Span(id="lt-result", style={"marginLeft": "12px"}),
                ]
            ),
            dcc.Interval(id="poll", interval=1000, n_intervals=0),
            dcc.Store(id="history-store", data={}),
        ]
    )

    def _fetch() -> tuple[Dict[str, Any], Dict[str, Any]]:
        try:
            with httpx.Client(base_url=base_url, timeout=2.0) as c:
                s = c.get("/api/v1/system/status").json()
                m = c.get("/api/v1/metrics/json").json()
                return s, m
        except Exception:
            return {}, {}

    @app.callback(
        Output("state-banner", "children"),
        Output("state-banner", "style"),
        Output("pressure-chart", "figure"),
        Output("queue-chart", "figure"),
        Output("throttle-chart", "figure"),
        Output("resource-gauges", "figure"),
        Input("poll", "n_intervals"),
    )
    def refresh(_: int):
        status, metrics = _fetch()
        level = status.get("backpressure", {}).get("pressure_level", "normal")
        score = status.get("backpressure", {}).get("pressure_score", 0.0)
        throttle = status.get("backpressure", {}).get("throttle_rate", 1.0)
        qsizes = metrics.get("queue_sizes", {"critical": 0, "high": 0, "normal": 0, "low": 0})
        cpu = 0.0
        mem = 0.0

        history["ts"].append(len(history["ts"]))
        history["score"].append(score)
        history["throttle"].append(throttle)
        for k in ("critical", "high", "normal", "low"):
            history[k].append(qsizes.get(k, 0))
        history["cpu"].append(cpu)
        history["mem"].append(mem)

        banner_style = {
            "padding": "16px",
            "fontSize": "20px",
            "fontWeight": "bold",
            "textAlign": "center",
            "backgroundColor": _STATE_COLOR.get(level, "#888"),
            "color": "white",
            "marginBottom": "16px",
            "borderRadius": "4px",
        }
        banner_text = f"STATE: {level.upper()}  (score {score:.2f})"

        ts = list(history["ts"])
        pressure_fig = go.Figure(
            data=[go.Scatter(x=ts, y=list(history["score"]), mode="lines", name="pressure_score")],
            layout=go.Layout(title="Pressure Score", yaxis=dict(range=[0, 1])),
        )
        queue_fig = go.Figure(
            data=[
                go.Scatter(x=ts, y=list(history["critical"]), name="critical", stackgroup="q", line=dict(color="#dc3545")),
                go.Scatter(x=ts, y=list(history["high"]), name="high", stackgroup="q", line=dict(color="#fd7e14")),
                go.Scatter(x=ts, y=list(history["normal"]), name="normal", stackgroup="q", line=dict(color="#0d6efd")),
                go.Scatter(x=ts, y=list(history["low"]), name="low", stackgroup="q", line=dict(color="#6c757d")),
            ],
            layout=go.Layout(title="Queue Depth by Priority"),
        )
        throttle_fig = go.Figure(
            data=[go.Scatter(x=ts, y=list(history["throttle"]), mode="lines", name="throttle_rate")],
            layout=go.Layout(title="Throttle Rate (AIMD)", yaxis=dict(range=[0, 1.05])),
        )
        gauges = go.Figure(
            data=[
                go.Indicator(mode="gauge+number", value=cpu * 100, title={"text": "CPU %"}, domain={"row": 0, "column": 0}),
                go.Indicator(mode="gauge+number", value=mem * 100, title={"text": "Memory %"}, domain={"row": 0, "column": 1}),
            ],
            layout=go.Layout(grid={"rows": 1, "columns": 2}, title="Resources"),
        )
        return banner_text, banner_style, pressure_fig, queue_fig, throttle_fig, gauges

    @app.callback(
        Output("manual-result", "children"),
        Input("manual-submit", "n_clicks"),
        State("manual-msg", "value"),
        State("manual-priority", "value"),
        prevent_initial_call=True,
    )
    def manual_submit(n, msg, prio):
        if not msg:
            return "(empty message)"
        try:
            with httpx.Client(base_url=base_url, timeout=2.0) as c:
                r = c.post("/api/v1/ingest", json={"message": msg, "priority": prio})
                return f"HTTP {r.status_code}: {r.text[:120]}"
        except Exception as e:
            return f"error: {e}"

    @app.callback(
        Output("lt-result", "children"),
        Input("lt-start", "n_clicks"),
        Input("lt-stop", "n_clicks"),
        Input("lt-spike-now", "n_clicks"),
        State("lt-rps", "value"),
        State("lt-duration", "value"),
        State("lt-spike", "value"),
        prevent_initial_call=True,
    )
    def loadtest_action(n_start, n_stop, n_spike, rps, duration, spike_mult):
        ctx = dash.callback_context
        if not ctx.triggered:
            return ""
        trig = ctx.triggered[0]["prop_id"].split(".")[0]
        try:
            with httpx.Client(base_url=base_url, timeout=4.0) as c:
                if trig == "lt-start":
                    r = c.post(
                        "/api/v1/loadtest/start",
                        json={"profile": "full", "rps": int(rps or 200), "duration_seconds": int(duration or 60), "spike_multiplier": float(spike_mult or 10.0)},
                    )
                elif trig == "lt-stop":
                    r = c.post("/api/v1/loadtest/stop")
                else:
                    r = c.post(
                        "/api/v1/loadtest/start",
                        json={"profile": "spike", "rps": 200, "duration_seconds": 30, "spike_multiplier": 10.0},
                    )
                return f"HTTP {r.status_code}"
        except Exception as e:
            return f"error: {e}"

    return app

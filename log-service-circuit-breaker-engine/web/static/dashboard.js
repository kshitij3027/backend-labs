(function () {
    'use strict';

    const wsUrl = window.__CB_WS_URL__;
    const cardsEl = document.getElementById('cards');
    const dotEl = document.getElementById('connection-dot');
    const dotText = document.getElementById('connection-text');
    const statusLine = document.getElementById('status-line');
    const uptimeEl = document.getElementById('uptime');
    const pTotal = document.getElementById('p-total');
    const pSuccess = document.getElementById('p-success');
    const pFailed = document.getElementById('p-failed');
    const pFallback = document.getElementById('p-fallback');

    const MAX_POINTS = 150;
    const seriesByName = {}; // name -> {x: [], y: []}

    function setStatus(text, isError) {
        statusLine.textContent = text;
        statusLine.style.color = isError ? '#f85149' : '#8b949e';
    }

    function renderCards(circuits) {
        const names = Object.keys(circuits).sort();
        // Update or create one card per breaker
        const existing = new Set(Array.from(cardsEl.children).map(c => c.dataset.name));
        const seen = new Set();
        for (const name of names) {
            const c = circuits[name];
            seen.add(name);
            let card = cardsEl.querySelector(`[data-name="${name}"]`);
            if (!card) {
                card = document.createElement('div');
                card.className = 'card';
                card.dataset.name = name;
                card.innerHTML = `
                    <h3>${name}</h3>
                    <span class="state CLOSED">CLOSED</span>
                    <div class="stats">
                        <div class="row"><span>success rate</span><b class="rate">100%</b></div>
                        <div class="row"><span>total calls</span><b class="total">0</b></div>
                        <div class="row"><span>failures</span><b class="failed">0</b></div>
                        <div class="row"><span>timeouts</span><b class="timeouts">0</b></div>
                        <div class="row"><span>state changes</span><b class="changes">0</b></div>
                    </div>
                `;
                cardsEl.appendChild(card);
            }
            const state = c.state || c.current_state || 'CLOSED';
            const stateEl = card.querySelector('.state');
            stateEl.textContent = state;
            stateEl.className = `state ${state}`;
            card.classList.toggle('OPEN', state === 'OPEN');
            card.querySelector('.rate').textContent = ((c.success_rate ?? 1) * 100).toFixed(1) + '%';
            card.querySelector('.total').textContent = c.total_calls ?? 0;
            card.querySelector('.failed').textContent = c.failed_calls ?? 0;
            card.querySelector('.timeouts').textContent = c.timeout_calls ?? 0;
            card.querySelector('.changes').textContent = c.state_changes ?? 0;
        }
        // Remove cards for breakers that disappeared
        for (const card of Array.from(cardsEl.children)) {
            if (!seen.has(card.dataset.name)) card.remove();
        }
    }

    function updateChart(circuits, t) {
        const names = Object.keys(circuits).sort();
        const traces = [];
        for (const name of names) {
            if (!seriesByName[name]) seriesByName[name] = { x: [], y: [] };
            const series = seriesByName[name];
            series.x.push(new Date(t * 1000));
            series.y.push(((circuits[name].success_rate ?? 1) * 100));
            if (series.x.length > MAX_POINTS) {
                series.x.shift();
                series.y.shift();
            }
            traces.push({ x: series.x, y: series.y, name: name, mode: 'lines+markers', line: { width: 2 } });
        }
        Plotly.react('chart', traces, {
            paper_bgcolor: '#161b22',
            plot_bgcolor: '#0f1419',
            font: { color: '#c9d1d9' },
            margin: { l: 50, r: 20, t: 20, b: 40 },
            yaxis: { range: [0, 105], title: 'success %' },
            xaxis: { type: 'date' },
            legend: { orientation: 'h', y: -0.2 },
            showlegend: true,
        }, { displayModeBar: false });
    }

    function applySnapshot(snap) {
        renderCards(snap.circuits || {});
        updateChart(snap.circuits || {}, snap.generated_at || (Date.now() / 1000));
        const p = snap.processing || {};
        pTotal.textContent = p.total_processed ?? 0;
        pSuccess.textContent = p.successful_processed ?? 0;
        pFailed.textContent = p.failed_processed ?? 0;
        pFallback.textContent = p.fallback_responses ?? 0;
        if (typeof p.uptime_seconds === 'number') {
            uptimeEl.textContent = `uptime: ${p.uptime_seconds.toFixed(0)}s`;
        }
    }

    let ws = null;
    let reconnectTimer = null;

    function connect() {
        ws = new WebSocket(wsUrl);
        ws.addEventListener('open', () => {
            dotEl.classList.remove('disconnected');
            dotEl.classList.add('connected');
            dotText.textContent = 'connected';
        });
        ws.addEventListener('message', (ev) => {
            try {
                const snap = JSON.parse(ev.data);
                applySnapshot(snap);
            } catch (e) {
                console.error('bad WS frame', e);
            }
        });
        ws.addEventListener('close', () => {
            dotEl.classList.remove('connected');
            dotEl.classList.add('disconnected');
            dotText.textContent = 'reconnecting…';
            clearTimeout(reconnectTimer);
            reconnectTimer = setTimeout(connect, 1000);
        });
        ws.addEventListener('error', () => ws.close());
    }
    connect();

    async function postJson(path, body) {
        try {
            const r = await fetch(path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: body ? JSON.stringify(body) : undefined });
            if (!r.ok) {
                const text = await r.text();
                setStatus(`${path} failed: ${r.status} ${text}`, true);
                return null;
            }
            return await r.json();
        } catch (e) {
            setStatus(`${path} error: ${e.message}`, true);
            return null;
        }
    }

    document.getElementById('btn-process').addEventListener('click', async () => {
        setStatus('Processing 100 logs…');
        const r = await postJson('/api/process/logs', { count: 100 });
        if (r) setStatus(`Processed ${r.processed} logs (${r.successful} successful, ${r.fallback_responses} fallbacks, ${r.duration_ms.toFixed(1)}ms)`);
    });

    document.getElementById('btn-simulate').addEventListener('click', async () => {
        setStatus('Simulating DB failure for 30s…');
        const r = await postJson('/api/simulate/failures', { target: 'database_primary', duration: 30, failure_rate: 0.85 });
        if (r) setStatus(`Simulating failure on ${r.simulating} for ${r.duration}s @ ${(r.failure_rate * 100).toFixed(0)}%`);
    });

    document.getElementById('btn-reset').addEventListener('click', async () => {
        setStatus('Resetting breakers…');
        const r = await postJson('/api/reset', null);
        if (r) setStatus(`Reset ${r.circuits.length} breakers`);
    });
})();

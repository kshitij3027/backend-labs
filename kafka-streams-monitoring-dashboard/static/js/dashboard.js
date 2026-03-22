/* Kafka Streams Monitoring Dashboard — Socket.IO + Chart.js client */

// ---------------------------------------------------------------------------
// Socket.IO connection
// ---------------------------------------------------------------------------

const socket = io();

socket.on("connect", () => {
    document.getElementById("status-dot").classList.add("connected");
    document.getElementById("status-text").textContent = "Connected";
});

socket.on("disconnect", () => {
    document.getElementById("status-dot").classList.remove("connected");
    document.getElementById("status-text").textContent = "Disconnected";
});

// ---------------------------------------------------------------------------
// Shared Chart.js theme options
// ---------------------------------------------------------------------------

const GRID_COLOR = "rgba(45, 48, 64, 0.6)";
const TICK_COLOR = "#8b8fa3";

function lineChartOptions(yLabel) {
    return {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { intersect: false, mode: "index" },
        plugins: {
            legend: { labels: { color: TICK_COLOR, font: { size: 11 } } },
        },
        scales: {
            x: {
                ticks: { color: TICK_COLOR, maxTicksLimit: 8, font: { size: 10 } },
                grid: { color: GRID_COLOR },
            },
            y: {
                beginAtZero: true,
                ticks: { color: TICK_COLOR, font: { size: 10 } },
                grid: { color: GRID_COLOR },
                title: yLabel
                    ? { display: true, text: yLabel, color: TICK_COLOR, font: { size: 11 } }
                    : undefined,
            },
        },
    };
}

// ---------------------------------------------------------------------------
// Chart instances
// ---------------------------------------------------------------------------

const throughputChart = new Chart(document.getElementById("throughput-chart"), {
    type: "line",
    data: {
        labels: [],
        datasets: [{
            label: "Events",
            data: [],
            borderColor: "#00d4ff",
            backgroundColor: "rgba(0, 212, 255, 0.08)",
            fill: true,
            tension: 0.35,
            pointRadius: 2,
        }],
    },
    options: lineChartOptions("Events"),
});

const errorRateChart = new Chart(document.getElementById("error-rate-chart"), {
    type: "line",
    data: {
        labels: [],
        datasets: [{
            label: "Error Rate %",
            data: [],
            borderColor: "#ff5252",
            backgroundColor: "rgba(255, 82, 82, 0.08)",
            fill: true,
            tension: 0.35,
            pointRadius: 2,
        }],
    },
    options: lineChartOptions("Error %"),
});

const topicChart = new Chart(document.getElementById("topic-chart"), {
    type: "doughnut",
    data: {
        labels: ["log-events", "error-events", "user-events"],
        datasets: [{
            data: [0, 0, 0],
            backgroundColor: ["#00d4ff", "#ff5252", "#00e676"],
            borderColor: "#21242f",
            borderWidth: 2,
        }],
    },
    options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: {
                position: "bottom",
                labels: { color: TICK_COLOR, font: { size: 11 }, padding: 16 },
            },
        },
    },
});

const responseTimeChart = new Chart(document.getElementById("response-time-chart"), {
    type: "line",
    data: {
        labels: [],
        datasets: [{
            label: "Avg Response Time (ms)",
            data: [],
            borderColor: "#ffc107",
            backgroundColor: "rgba(255, 193, 7, 0.08)",
            fill: true,
            tension: 0.35,
            pointRadius: 2,
        }],
    },
    options: lineChartOptions("ms"),
});

// ---------------------------------------------------------------------------
// Update functions
// ---------------------------------------------------------------------------

function updateStatCards(metrics) {
    document.getElementById("total-events").textContent =
        (metrics.total_events || 0).toLocaleString();
    document.getElementById("error-rate").textContent =
        (metrics.error_rate || 0) + "%";
    document.getElementById("avg-response-time").textContent =
        Math.round(metrics.avg_response_time || 0) + "ms";
    document.getElementById("events-per-sec").textContent =
        (metrics.events_per_second || 0).toFixed(1);
}

function updateTimeSeriesCharts(historical) {
    if (!historical || !historical.labels) return;

    const labels = historical.labels.map(
        (t) => new Date(t * 1000).toLocaleTimeString()
    );

    throughputChart.data.labels = labels;
    throughputChart.data.datasets[0].data = historical.events;
    throughputChart.update("none");

    errorRateChart.data.labels = labels;
    errorRateChart.data.datasets[0].data = historical.error_rate;
    errorRateChart.update("none");

    responseTimeChart.data.labels = labels;
    responseTimeChart.data.datasets[0].data = historical.response_times;
    responseTimeChart.update("none");
}

function updateTopicChart(perTopic) {
    if (!perTopic) return;
    topicChart.data.datasets[0].data = [
        perTopic["log-events"] || 0,
        perTopic["error-events"] || 0,
        perTopic["user-events"] || 0,
    ];
    topicChart.update("none");
}

// ---------------------------------------------------------------------------
// Socket.IO event handler
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Alert handling
// ---------------------------------------------------------------------------

socket.on('alert_update', (data) => {
    if (data.alerts && data.alerts.length > 0) {
        data.alerts.forEach(alert => addAlertToList(alert));
        updateAlertCount();
    }
});

function addAlertToList(alert) {
    const alertsList = document.getElementById('alerts-list');
    const noAlerts = alertsList.querySelector('.no-alerts');
    if (noAlerts) noAlerts.remove();

    const alertEl = document.createElement('div');
    alertEl.className = `alert-item alert-${alert.severity}`;
    alertEl.innerHTML = `
        <div class="alert-header">
            <span class="alert-severity">${alert.severity.toUpperCase()}</span>
            <span class="alert-time">${new Date(alert.timestamp * 1000).toLocaleTimeString()}</span>
        </div>
        <div class="alert-message">${alert.message}</div>
        <div class="alert-action">${alert.action_required}</div>
    `;

    // Prepend (newest first)
    alertsList.insertBefore(alertEl, alertsList.firstChild);

    // Keep max 20 alerts visible
    while (alertsList.children.length > 20) {
        alertsList.removeChild(alertsList.lastChild);
    }

    // Flash animation
    alertEl.classList.add('alert-flash');
    setTimeout(() => alertEl.classList.remove('alert-flash'), 1000);
}

function updateAlertCount() {
    const count = document.getElementById('alerts-list').querySelectorAll('.alert-item').length;
    document.getElementById('alert-count').textContent = count;
}

// ---------------------------------------------------------------------------
// Metrics event handler
// ---------------------------------------------------------------------------

socket.on("metrics_update", (data) => {
    if (data.metrics) {
        updateStatCards(data.metrics);
        updateTopicChart(data.metrics.per_topic_counts);
    }
    if (data.historical) {
        updateTimeSeriesCharts(data.historical);
    }
    document.getElementById("last-update").textContent =
        "Updated: " + new Date().toLocaleTimeString();
});

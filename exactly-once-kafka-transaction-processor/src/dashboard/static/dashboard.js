/* Dashboard client-side JavaScript for the exactly-once transaction processor. */

// ---------------------------------------------------------------------------
// Chart setup
// ---------------------------------------------------------------------------

const ctx = document.getElementById("throughput-chart").getContext("2d");
const throughputChart = new Chart(ctx, {
    type: "line",
    data: {
        labels: [],
        datasets: [
            {
                label: "Total",
                data: [],
                borderColor: "#4f8ff7",
                backgroundColor: "rgba(79,143,247,0.08)",
                fill: true,
                tension: 0.35,
                pointRadius: 2,
            },
            {
                label: "Completed",
                data: [],
                borderColor: "#2dd4a8",
                backgroundColor: "transparent",
                fill: false,
                tension: 0.35,
                pointRadius: 2,
            },
            {
                label: "Failed",
                data: [],
                borderColor: "#f25757",
                backgroundColor: "transparent",
                fill: false,
                tension: 0.35,
                pointRadius: 2,
            },
        ],
    },
    options: {
        responsive: true,
        maintainAspectRatio: false,
        interaction: { intersect: false, mode: "index" },
        plugins: {
            legend: {
                labels: { color: "#8b8fa3", font: { size: 11 } },
            },
        },
        scales: {
            x: {
                ticks: { color: "#8b8fa3", maxTicksLimit: 8, font: { size: 10 } },
                grid: { color: "rgba(45,48,64,0.5)" },
            },
            y: {
                beginAtZero: true,
                ticks: { color: "#8b8fa3", font: { size: 10 } },
                grid: { color: "rgba(45,48,64,0.5)" },
            },
        },
    },
});

const MAX_CHART_POINTS = 30;

// ---------------------------------------------------------------------------
// DOM update helpers
// ---------------------------------------------------------------------------

function updateCounters(stats) {
    document.getElementById("total-count").textContent = stats.total_transactions;
    document.getElementById("completed-count").textContent = stats.completed_count;
    document.getElementById("failed-count").textContent = stats.failed_count;
    document.getElementById("success-rate").textContent = stats.success_rate.toFixed(1) + "%";
}

function updateEosStatus(stats) {
    const banner = document.getElementById("eos-banner");
    const text = document.getElementById("eos-status-text");
    const status = stats.guarantee_status || "CHECKING";

    text.textContent = status;
    if (status === "MAINTAINED") {
        banner.className = "eos-banner maintained";
    } else {
        banner.className = "eos-banner violated";
    }
}

function updateAccountsTable(stats) {
    const tbody = document.getElementById("accounts-table");
    if (!stats.accounts || stats.accounts.length === 0) {
        tbody.innerHTML = '<tr><td colspan="2" style="color:var(--text-secondary)">No accounts</td></tr>';
        return;
    }

    tbody.innerHTML = stats.accounts
        .map(
            (a) =>
                `<tr>
                    <td>${a.account_number}</td>
                    <td style="text-align:right">
                        <span class="balance-value balance-positive">$${a.balance.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}</span>
                    </td>
                </tr>`
        )
        .join("");
}

function updateRecentTransactions(stats) {
    const tbody = document.getElementById("recent-table");
    if (!stats.recent_transactions || stats.recent_transactions.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" style="color:var(--text-secondary);padding:16px">No transactions yet</td></tr>';
        return;
    }

    tbody.innerHTML = stats.recent_transactions
        .map((t) => {
            const badgeClass =
                t.status === "completed" ? "badge-completed" :
                t.status === "failed" ? "badge-failed" : "badge-pending";

            const from = t.from_account || "-";
            const to = t.to_account || "-";
            const accounts = from !== "-" && to !== "-"
                ? `${from} <span class="account-arrow">&rarr;</span> ${to}`
                : from !== "-" ? from : to;

            const time = t.created_at
                ? new Date(t.created_at).toLocaleTimeString()
                : "-";

            return `<tr>
                <td class="txn-id">${t.transaction_id.substring(0, 16)}...</td>
                <td class="txn-type">${t.type}</td>
                <td style="text-align:right">$${t.amount.toLocaleString(undefined, {minimumFractionDigits: 2, maximumFractionDigits: 2})}</td>
                <td>${accounts}</td>
                <td><span class="badge ${badgeClass}">${t.status}</span></td>
                <td style="color:var(--text-secondary);font-size:0.8rem">${time}</td>
            </tr>`;
        })
        .join("");
}

function updateChart(stats) {
    const now = new Date().toLocaleTimeString();
    const labels = throughputChart.data.labels;
    const totalData = throughputChart.data.datasets[0].data;
    const completedData = throughputChart.data.datasets[1].data;
    const failedData = throughputChart.data.datasets[2].data;

    labels.push(now);
    totalData.push(stats.total_transactions);
    completedData.push(stats.completed_count);
    failedData.push(stats.failed_count);

    // Keep only the last MAX_CHART_POINTS
    if (labels.length > MAX_CHART_POINTS) {
        labels.shift();
        totalData.shift();
        completedData.shift();
        failedData.shift();
    }

    throughputChart.update("none"); // skip animation for smooth updates
}

// ---------------------------------------------------------------------------
// Data fetching
// ---------------------------------------------------------------------------

function fetchStats() {
    fetch("/api/stats")
        .then((res) => res.json())
        .then((stats) => {
            updateCounters(stats);
            updateEosStatus(stats);
            updateAccountsTable(stats);
            updateRecentTransactions(stats);
            updateChart(stats);
            document.getElementById("last-updated").textContent =
                "Updated " + new Date().toLocaleTimeString();
        })
        .catch((err) => {
            console.error("Failed to fetch stats:", err);
        });
}

function verifyEos() {
    fetch("/api/verify-eos")
        .then((res) => res.json())
        .then((data) => {
            const banner = document.getElementById("eos-banner");
            const text = document.getElementById("eos-status-text");
            text.textContent = data.guarantee_status;
            banner.className = data.guarantee_status === "MAINTAINED"
                ? "eos-banner maintained"
                : "eos-banner violated";

            const details = data.checks.map(
                (c) => `${c.passed ? "PASS" : "FAIL"} ${c.name}: ${c.details}`
            ).join("\n");
            alert("EOS Verification:\n\n" + details + "\n\nOverall: " + data.guarantee_status);
        })
        .catch((err) => {
            console.error("Verify EOS failed:", err);
            alert("Failed to verify EOS. See console for details.");
        });
}

function injectFailure() {
    if (!confirm("This will crash the consumer process and restart it.\nThe exactly-once guarantee should be maintained.\n\nContinue?")) {
        return;
    }

    const resultEl = document.getElementById("inject-result");
    resultEl.textContent = "Crashing...";

    fetch("/api/inject-failure/consumer-crash", { method: "POST" })
        .then((res) => res.json())
        .then((data) => {
            if (data.status === "success") {
                resultEl.textContent = `Crashed PID ${data.old_pid}, restarted as PID ${data.new_pid}`;
            } else {
                resultEl.textContent = `Error: ${data.message}`;
            }
            setTimeout(() => { resultEl.textContent = ""; }, 8000);
        })
        .catch((err) => {
            resultEl.textContent = "Failed to inject failure";
            console.error("Inject failure error:", err);
        });
}

// ---------------------------------------------------------------------------
// Auto-refresh
// ---------------------------------------------------------------------------

fetchStats();
setInterval(fetchStats, 2000);

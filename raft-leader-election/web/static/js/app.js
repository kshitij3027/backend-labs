// Connect to SocketIO
const socket = io();

// State
let lastStatus = {};

// DOM elements
const nodesGrid = document.getElementById('nodes-grid');
const clusterTerm = document.getElementById('cluster-term');
const clusterLeader = document.getElementById('cluster-leader');
const clusterStatus = document.getElementById('cluster-status');
const killLeaderBtn = document.getElementById('kill-leader-btn');
const logContainer = document.getElementById('election-log-container');

// Render node cards
function renderNodes(statuses) {
    nodesGrid.innerHTML = '';

    let leader = null;
    let maxTerm = 0;

    Object.entries(statuses).sort().forEach(([nodeId, status]) => {
        const state = status.is_alive === false && status.state !== 'unreachable'
            ? 'stopped'
            : status.state;

        if (state === 'leader') leader = nodeId;
        if (status.term > maxTerm) maxTerm = status.term;

        const card = document.createElement('div');
        card.className = `node-card ${state}`;

        const stateIcon = {
            'leader': '&#x1f451;',
            'follower': '&#x1f465;',
            'candidate': '&#x2728;',
            'stopped': '&#x26d4;',
            'unreachable': '&#x274c;'
        }[state] || '?';

        card.innerHTML = `
            <div class="node-circle">${stateIcon}</div>
            <div class="node-name">${nodeId}</div>
            <div class="node-state">${state}</div>
            <div class="node-term">Term ${status.term}</div>
        `;

        nodesGrid.appendChild(card);
    });

    clusterTerm.textContent = `Term: ${maxTerm}`;
    clusterLeader.textContent = `Leader: ${leader || 'None'}`;
    clusterStatus.textContent = 'Status: Connected';
}

// Handle cluster updates from SocketIO
socket.on('cluster_update', (statuses) => {
    lastStatus = statuses;
    renderNodes(statuses);
});

socket.on('connect', () => {
    clusterStatus.textContent = 'Status: Connected';
});

socket.on('disconnect', () => {
    clusterStatus.textContent = 'Status: Disconnected';
});

// Kill leader button
killLeaderBtn.addEventListener('click', async () => {
    killLeaderBtn.disabled = true;
    killLeaderBtn.textContent = 'Killing...';

    try {
        const response = await fetch('/api/kill-leader', { method: 'POST' });
        const data = await response.json();

        if (data.success) {
            killLeaderBtn.textContent = `Killed ${data.killed}`;
        } else {
            killLeaderBtn.textContent = data.error || 'Failed';
        }
    } catch (err) {
        killLeaderBtn.textContent = 'Error';
    }

    setTimeout(() => {
        killLeaderBtn.disabled = false;
        killLeaderBtn.textContent = 'Kill Leader';
    }, 2000);
});

// Fetch and render election log
async function fetchElectionLog() {
    try {
        const response = await fetch('/api/election-log?limit=50');
        const events = await response.json();

        if (events.length === 0) {
            logContainer.innerHTML = '<p class="log-empty">No events yet</p>';
            return;
        }

        logContainer.innerHTML = '';
        events.forEach(event => {
            const entry = document.createElement('div');
            entry.className = 'log-entry';

            const time = new Date(event.timestamp * 1000).toLocaleTimeString();

            entry.innerHTML = `
                <span class="log-time">${time}</span>
                <span class="log-node">${event.node_id}</span>
                <span class="log-event ${event.event_type}">
                    [T${event.term}] ${event.event_type}${event.details ? ': ' + event.details : ''}
                </span>
            `;

            logContainer.appendChild(entry);
        });

        // Auto-scroll to bottom
        logContainer.scrollTop = logContainer.scrollHeight;
    } catch (err) {
        console.error('Failed to fetch election log:', err);
    }
}

// Poll election log every 1 second
setInterval(fetchElectionLog, 1000);
fetchElectionLog();

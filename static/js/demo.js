let running = false;
let paused = false;
let currentRecord = 0;
let currentBatch = 0;
const totalBatches = 12;
const recordsPerBatch = 100;

function initGrid() {
    const grid = document.getElementById('batchGrid');
    for (let i = 0; i < totalBatches; i++) {
        const cell = document.createElement('div');
        cell.className = 'batch-cell';
        cell.dataset.index = i;
        cell.addEventListener('click', () => showBatchDetails(i));
        grid.appendChild(cell);
    }
}

function playBeep() {
    try {
        const ctx = new (window.AudioContext || window.webkitAudioContext)();
        const osc = ctx.createOscillator();
        osc.type = 'sine';
        osc.frequency.value = 880;
        osc.connect(ctx.destination);
        osc.start();
        osc.stop(ctx.currentTime + 0.1);
    } catch (e) {
        console.warn('Audio not supported', e);
    }
}

function startDemo() {
    if (running) return;
    running = true;
    showToast('Demo started');
    playBeep();
    interval = setInterval(processRecord, 100);
}

function processRecord() {
    if (paused) return;
    currentRecord++;
    if (currentRecord % recordsPerBatch === 0) {
        updateBatch();
    }
    updateProgress();
    log(`Indexed record ${currentRecord}`);
}

function updateBatch() {
    const cell = document.querySelector(`.batch-cell[data-index="${currentBatch}"]`);
    if (cell) {
        cell.classList.add('completed');
    }
    currentBatch++;
    document.getElementById('batchesCounter').textContent = currentBatch;
    if (currentBatch >= totalBatches) {
        finishDemo();
    }
}

function updateProgress() {
    const totalRecords = totalBatches * recordsPerBatch;
    const recordPercent = (currentRecord / totalRecords) * 100;
    const batchPercent = (currentBatch / totalBatches) * 100;
    document.getElementById('recordsProgress').style.width = `${recordPercent}%`;
    document.getElementById('batchesProgress').style.width = `${batchPercent}%`;
    document.getElementById('recordsCounter').textContent = currentRecord;
    document.getElementById('speedCounter').textContent = (Math.random() * 50 + 50).toFixed(0);
    document.getElementById('speedDisplay').textContent = document.getElementById('speedCounter').textContent;
    const remaining = totalRecords - currentRecord;
    document.getElementById('etaDisplay').textContent = `${Math.max((remaining / 60).toFixed(1), 0)}s`;
}

function log(message) {
    const consoleEl = document.getElementById('logConsole');
    const entry = document.createElement('div');
    entry.textContent = `${new Date().toLocaleTimeString()} - ${message}`;
    consoleEl.appendChild(entry);
    consoleEl.scrollTop = consoleEl.scrollHeight;
}

function finishDemo() {
    clearInterval(interval);
    running = false;
    showToast('Migration complete');
    playBeep();
}

function showBatchDetails(index) {
    const body = document.getElementById('batchModalBody');
    const status = index < currentBatch ? 'Completed' : 'Pending';
    body.textContent = `Batch ${index + 1}: ${status}`;
    const modal = new bootstrap.Modal(document.getElementById('batchModal'));
    modal.show();
}

function showToast(message) {
    document.getElementById('toastBody').textContent = message;
    const toastEl = document.getElementById('demoToast');
    const toast = new bootstrap.Toast(toastEl);
    toast.show();
}

document.getElementById('startDemoBtn').addEventListener('click', startDemo);

setTimeout(startDemo, 3000);

let interval;

initGrid();

document.addEventListener('keydown', (e) => {
    if (e.code === 'Space') {
        paused = !paused;
        showToast(paused ? 'Demo paused' : 'Demo resumed');
    }
});

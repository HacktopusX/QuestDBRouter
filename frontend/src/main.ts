import "./styles.css";
import { LiveChart } from "./chart";
import { StreamClient } from "./ws-client";
import type { ChartMode } from "./types";

const wsUrl =
  import.meta.env.VITE_STREAM_WS ??
  (import.meta.env.DEV
    ? `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws`
    : `${location.protocol === "https:" ? "wss" : "ws"}://${location.hostname}:8080/ws`);

const topicInput = document.getElementById("topic-input") as HTMLInputElement;
const modeSelect = document.getElementById("mode-select") as HTMLSelectElement;
const connectBtn = document.getElementById("connect-btn") as HTMLButtonElement;
const statusDot = document.getElementById("status-dot") as HTMLSpanElement;
const statusText = document.getElementById("status-text") as HTMLSpanElement;
const tickCountEl = document.getElementById("tick-count") as HTMLSpanElement;
const lastPriceEl = document.getElementById("last-price") as HTMLSpanElement;
const changePctEl = document.getElementById("change-pct") as HTMLSpanElement;
const chartContainer = document.getElementById("chart") as HTMLDivElement;
const legendContainer = document.getElementById("chart-legend-host") as HTMLDivElement;
const volumeToggle = document.getElementById("volume-toggle") as HTMLInputElement;
const smaToggle = document.getElementById("sma-toggle") as HTMLInputElement;
const followToggle = document.getElementById("follow-toggle") as HTMLInputElement;
const fitBtn = document.getElementById("fit-btn") as HTMLButtonElement;

const chart = new LiveChart(chartContainer, legendContainer);
let tickCount = 0;
let connected = false;

chart.setFeatures({
  volume: volumeToggle.checked,
  sma: smaToggle.checked,
  autoFollow: followToggle.checked,
});

chart.setOnPriceChange((change) => {
  if (!change) {
    changePctEl.textContent = "—";
    changePctEl.className = "change-pct";
    return;
  }
  const sign = change.absolute >= 0 ? "+" : "";
  changePctEl.textContent = `${sign}${change.percent.toFixed(2)}%`;
  changePctEl.className = `change-pct ${change.absolute >= 0 ? "up" : "down"}`;
});

const client = new StreamClient({
  url: wsUrl,
  onRow: (row) => {
    const price = chart.applyRow(row);
    if (price != null) {
      tickCount += 1;
      tickCountEl.textContent = `${tickCount} ticks`;
      lastPriceEl.textContent = price.toLocaleString(undefined, {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2,
      });
    }
  },
  onReplay: (payload) => {
    chart.applyReplay(payload.rows);
    tickCount = payload.rows.length;
    tickCountEl.textContent = `${tickCount} ticks (replay)`;
    const last = payload.rows[payload.rows.length - 1];
    if (last) {
      const fields = new Map(last.fields.map(([k, v]) => [k.toLowerCase(), v]));
      const close = fields.get("close") ?? fields.get("price");
      if (typeof close === "number") {
        lastPriceEl.textContent = close.toLocaleString(undefined, {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2,
        });
      }
    }
  },
  onStatus: (isConnected, detail) => {
    connected = isConnected;
    statusDot.className = `status-dot ${isConnected ? "connected" : "disconnected"}`;
    statusText.textContent = detail ?? (isConnected ? "Live" : "Disconnected");
    connectBtn.textContent = isConnected ? "Disconnect" : "Connect";
    connectBtn.classList.toggle("connected", isConnected);
  },
});

function currentMode(): ChartMode {
  return modeSelect.value === "line" ? "line" : "ohlcv";
}

function syncFeatures(): void {
  chart.setFeatures({
    volume: volumeToggle.checked,
    sma: smaToggle.checked,
    autoFollow: followToggle.checked,
  });
}

connectBtn.addEventListener("click", () => {
  if (connected) {
    client.disconnect();
    return;
  }
  const topic = topicInput.value.trim();
  if (!topic) return;
  tickCount = 0;
  tickCountEl.textContent = "0 ticks";
  lastPriceEl.textContent = "—";
  changePctEl.textContent = "—";
  chart.setMode(currentMode());
  syncFeatures();
  chart.reset();
  client.connect(topic);
});

modeSelect.addEventListener("change", () => {
  chart.setMode(currentMode());
  volumeToggle.disabled = currentMode() === "line";
  syncFeatures();
  chart.reset();
  tickCount = 0;
  tickCountEl.textContent = "0 ticks";
  if (connected) {
    client.connect(topicInput.value.trim());
  }
});

volumeToggle.addEventListener("change", () => syncFeatures());
smaToggle.addEventListener("change", () => syncFeatures());
followToggle.addEventListener("change", () => syncFeatures());
fitBtn.addEventListener("click", () => chart.fitContent());

topicInput.addEventListener("keydown", (ev) => {
  if (ev.key === "Enter" && !connected) connectBtn.click();
});

chart.setMode(currentMode());
client.connect(topicInput.value.trim());

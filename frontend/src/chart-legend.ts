import type { ChartMode } from "./types";

export interface LegendValues {
  time: string;
  open?: number;
  high?: number;
  low?: number;
  close?: number;
  volume?: number;
  price?: number;
  sma?: number;
}

export class ChartLegend {
  private readonly root: HTMLElement;

  constructor(container: HTMLElement) {
    this.root = document.createElement("div");
    this.root.className = "chart-legend hidden";
    container.appendChild(this.root);
  }

  hide(): void {
    this.root.classList.add("hidden");
  }

  show(mode: ChartMode, values: LegendValues): void {
    this.root.classList.remove("hidden");
    const rows: string[] = [`<span class="legend-time">${values.time}</span>`];

    if (mode === "ohlcv") {
      if (values.open != null) rows.push(row("O", values.open));
      if (values.high != null) rows.push(row("H", values.high));
      if (values.low != null) rows.push(row("L", values.low));
      if (values.close != null) rows.push(row("C", values.close));
      if (values.volume != null) rows.push(row("V", values.volume, 0));
    } else if (values.price != null) {
      rows.push(row("Price", values.price));
    }

    if (values.sma != null) rows.push(row("SMA", values.sma));

    this.root.innerHTML = rows.join("");
  }

  destroy(): void {
    this.root.remove();
  }
}

function row(label: string, value: number, decimals = 2): string {
  const formatted = value.toLocaleString(undefined, {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
  return `<span class="legend-row"><span class="legend-label">${label}</span><span class="legend-value">${formatted}</span></span>`;
}

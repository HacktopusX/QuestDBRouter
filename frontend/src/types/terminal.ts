import type { ChartMode } from "@/lib/ilp/types";

export interface ChartFeatures {
  volume: boolean;
  sma: boolean;
  autoFollow: boolean;
}

export interface PriceChange {
  absolute: number;
  percent: number;
}

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

export type LayoutMode = "4" | "6" | "9";

export type TerminalView = "overview" | "charts" | "stream";

export interface StreamLogEntry {
  id: string;
  topic: string;
  time: number;
  price: number | null;
  at: number;
}

export interface PanelState {
  id: string;
  topic: string;
  focused: boolean;
  price: number | null;
  change: PriceChange | null;
  tickCount: number;
  live: boolean;
}

export interface TerminalSettings {
  mode: ChartMode;
  features: ChartFeatures;
  layout: LayoutMode;
  view: TerminalView;
}

export const DEFAULT_SYMBOLS = [
  "btc-usdt",
  "eth-usdt",
  "sol-usdt",
  "doge-usdt",
  "bnb-usdt",
  "xrp-usdt",
  "ada-usdt",
  "avax-usdt",
  "link-usdt",
] as const;

export const DEFAULT_SETTINGS: TerminalSettings = {
  mode: "ohlcv",
  features: { volume: true, sma: false, autoFollow: true },
  layout: "6",
  view: "charts",
};

export function layoutCapacity(layout: LayoutMode): number {
  switch (layout) {
    case "4":
      return 4;
    case "6":
      return 6;
    case "9":
      return 9;
  }
}

export function createPanelId(): string {
  return `panel-${crypto.randomUUID().slice(0, 8)}`;
}

export function createDefaultPanels(layout: LayoutMode): PanelState[] {
  const count = layoutCapacity(layout);
  return Array.from({ length: count }, (_, i) => ({
    id: createPanelId(),
    topic: DEFAULT_SYMBOLS[i] ?? `sym-${i + 1}`,
    focused: i === 0,
    price: null,
    change: null,
    tickCount: 0,
    live: false,
  }));
}

export function aggregateStats(panels: PanelState[]) {
  const live = panels.filter((p) => p.live).length;
  const ticks = panels.reduce((s, p) => s + p.tickCount, 0);
  const withChange = panels.filter((p) => p.change != null);
  const avgChange =
    withChange.length > 0
      ? withChange.reduce((s, p) => s + (p.change?.percent ?? 0), 0) / withChange.length
      : null;
  const topGainer = [...withChange].sort(
    (a, b) => (b.change?.percent ?? 0) - (a.change?.percent ?? 0),
  )[0];
  return { live, ticks, avgChange, panelCount: panels.length, topGainer };
}

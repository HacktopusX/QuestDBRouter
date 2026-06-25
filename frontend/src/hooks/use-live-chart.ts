import { useEffect, useRef, useState } from "react";
import { LiveChart } from "@/lib/chart";
import type { ChartMode } from "@/lib/ilp/types";
import type { ChartFeatures, LegendValues, PriceChange } from "@/types/terminal";

export function useLiveChart(mode: ChartMode, features: ChartFeatures) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<LiveChart | null>(null);
  const [legend, setLegend] = useState<LegendValues | null>(null);
  const [priceChange, setPriceChange] = useState<PriceChange | null>(null);

  useEffect(() => {
    if (!containerRef.current) return;
    const chart = new LiveChart(containerRef.current, setLegend);
    chart.setOnPriceChange(setPriceChange);
    chartRef.current = chart;
    return () => {
      chart.destroy();
      chartRef.current = null;
    };
  }, []);

  useEffect(() => {
    chartRef.current?.setMode(mode);
  }, [mode]);

  useEffect(() => {
    chartRef.current?.setFeatures(features);
  }, [features]);

  useEffect(() => {
    const id = requestAnimationFrame(() => chartRef.current?.resize());
    return () => cancelAnimationFrame(id);
  }, []);

  return {
    containerRef,
    chartRef,
    legend,
    priceChange,
    fitContent: () => chartRef.current?.fitContent(),
    resize: () => chartRef.current?.resize(),
    zoomIn: () => chartRef.current?.zoomIn(),
    zoomOut: () => chartRef.current?.zoomOut(),
    resetZoom: () => chartRef.current?.resetZoom(),
    applyRow: (row: import("@/lib/ilp/types").IlpRow) => chartRef.current?.applyRow(row) ?? null,
    applyReplay: (rows: import("@/lib/ilp/types").IlpRow[]) =>
      chartRef.current?.applyReplay(rows),
    reset: () => chartRef.current?.reset(),
  };
}

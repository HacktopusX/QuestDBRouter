import {
  CandlestickSeries,
  ColorType,
  createChart,
  CrosshairMode,
  HistogramSeries,
  LineSeries,
  type CandlestickData,
  type HistogramData,
  type IChartApi,
  type ISeriesApi,
  type LineData,
  type MouseEventParams,
  type UTCTimestamp,
} from "lightweight-charts";
import { ChartLegend, type LegendValues } from "./chart-legend";
import { smaAt, upsertClose, type ClosePoint } from "./indicators";
import type { ChartMode, IlpRow } from "./types";
import { fieldNumber, fieldsToMap, toChartTime } from "./types";

const THEME = {
  background: "#0b0e11",
  surface: "#131722",
  grid: "#1e222d",
  text: "#d1d4dc",
  muted: "#787b86",
  up: "#26a69a",
  down: "#ef5350",
  line: "#2962ff",
  sma: "#f7931a",
  crosshair: "#758696",
};

const SMA_PERIOD = 20;
const ROW_BUFFER_MAX = 500;

export interface ChartFeatures {
  volume: boolean;
  sma: boolean;
  autoFollow: boolean;
}

export interface PriceChange {
  absolute: number;
  percent: number;
}

export class LiveChart {
  private chart: IChartApi;
  private candleSeries: ISeriesApi<"Candlestick"> | null = null;
  private lineSeries: ISeriesApi<"Line"> | null = null;
  private volumeSeries: ISeriesApi<"Histogram"> | null = null;
  private smaSeries: ISeriesApi<"Line"> | null = null;
  private mode: ChartMode = "ohlcv";
  private features: ChartFeatures = { volume: true, sma: false, autoFollow: true };
  private closes: ClosePoint[] = [];
  private rowBuffer: IlpRow[] = [];
  private sessionOpen: number | null = null;
  private legend: ChartLegend;
  private onChange: ((change: PriceChange | null) => void) | null = null;
  private crosshairHandler: (param: MouseEventParams) => void;

  constructor(container: HTMLElement, legendContainer: HTMLElement) {
    this.legend = new ChartLegend(legendContainer);
    this.chart = createChart(container, {
      layout: {
        background: { type: ColorType.Solid, color: THEME.surface },
        textColor: THEME.text,
        fontFamily: "'IBM Plex Sans', system-ui, sans-serif",
        fontSize: 12,
        panes: {
          separatorColor: THEME.grid,
          separatorHoverColor: "#434651",
        },
      },
      grid: {
        vertLines: { color: THEME.grid },
        horzLines: { color: THEME.grid },
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: { color: THEME.crosshair, labelBackgroundColor: "#2a2e39" },
        horzLine: { color: THEME.crosshair, labelBackgroundColor: "#2a2e39" },
      },
      rightPriceScale: {
        borderColor: THEME.grid,
      },
      timeScale: {
        borderColor: THEME.grid,
        timeVisible: true,
        secondsVisible: true,
      },
      handleScroll: true,
      handleScale: true,
    });

    this.crosshairHandler = (param) => this.onCrosshairMove(param);
    this.chart.subscribeCrosshairMove(this.crosshairHandler);

    const ro = new ResizeObserver(() => {
      const { width, height } = container.getBoundingClientRect();
      this.chart.applyOptions({ width, height });
    });
    ro.observe(container);
  }

  setFeatures(features: Partial<ChartFeatures>): void {
    const prev = { ...this.features };
    this.features = { ...this.features, ...features };
    const needsRebuild =
      prev.volume !== this.features.volume || prev.sma !== this.features.sma;
    if (needsRebuild) {
      this.replayBuffer();
    }
  }

  setOnPriceChange(handler: (change: PriceChange | null) => void): void {
    this.onChange = handler;
  }

  setMode(mode: ChartMode): void {
    if (this.mode === mode) return;
    this.mode = mode;
    this.reset();
  }

  reset(): void {
    this.closes = [];
    this.rowBuffer = [];
    this.sessionOpen = null;
    this.onChange?.(null);
    this.resetSeries();
    this.legend.hide();
  }

  fitContent(): void {
    this.chart.timeScale().fitContent();
  }

  applyReplay(rows: IlpRow[]): void {
    this.reset();
    const sorted = [...rows].sort((a, b) => (a.timestamp_ns ?? 0) - (b.timestamp_ns ?? 0));
    for (const row of sorted) {
      this.applyRow(row, false);
    }
    this.chart.timeScale().fitContent();
  }

  applyRow(row: IlpRow, scroll = true): number | null {
    this.bufferRow(row);
    const time = toChartTime(row.timestamp_ns);
    if (time == null) return null;

    const fields = fieldsToMap(row.fields);

    if (this.mode === "ohlcv") {
      const open = fieldNumber(fields, "open");
      const high = fieldNumber(fields, "high");
      const low = fieldNumber(fields, "low");
      const close = fieldNumber(fields, "close");
      if (open == null || high == null || low == null || close == null) return null;

      if (!this.candleSeries) this.resetSeries();
      if (!this.candleSeries) return null;

      const bar: CandlestickData = {
        time: time as UTCTimestamp,
        open,
        high,
        low,
        close,
      };
      this.candleSeries.update(bar);
      this.updateVolume(time as UTCTimestamp, fieldNumber(fields, "volume"), open, close);
      this.updateSma(time as UTCTimestamp, close, scroll);
      this.trackPrice(close, scroll);
      return close;
    }

    const price = fieldNumber(fields, "price") ?? fieldNumber(fields, "close");
    if (price == null) return null;
    if (!this.lineSeries) this.resetSeries();
    if (!this.lineSeries) return null;
    this.pushLine(time as UTCTimestamp, price, scroll);
    this.updateSma(time as UTCTimestamp, price, scroll);
    this.trackPrice(price, scroll);
    return price;
  }

  destroy(): void {
    this.chart.unsubscribeCrosshairMove(this.crosshairHandler);
    this.legend.destroy();
    this.chart.remove();
  }

  private bufferRow(row: IlpRow): void {
    this.rowBuffer.push(row);
    if (this.rowBuffer.length > ROW_BUFFER_MAX) {
      this.rowBuffer.shift();
    }
  }

  private replayBuffer(): void {
    const rows = this.rowBuffer;
    this.closes = [];
    this.sessionOpen = null;
    this.rowBuffer = [];
    this.resetSeries();
    for (const row of rows) {
      this.applyRow(row, false);
    }
  }

  private resetSeries(): void {
    if (this.candleSeries) {
      this.chart.removeSeries(this.candleSeries);
      this.candleSeries = null;
    }
    if (this.lineSeries) {
      this.chart.removeSeries(this.lineSeries);
      this.lineSeries = null;
    }
    if (this.volumeSeries) {
      this.chart.removeSeries(this.volumeSeries);
      this.volumeSeries = null;
    }
    if (this.smaSeries) {
      this.chart.removeSeries(this.smaSeries);
      this.smaSeries = null;
    }

    if (this.mode === "ohlcv") {
      this.candleSeries = this.chart.addSeries(CandlestickSeries, {
        upColor: THEME.up,
        downColor: THEME.down,
        borderUpColor: THEME.up,
        borderDownColor: THEME.down,
        wickUpColor: THEME.up,
        wickDownColor: THEME.down,
      });

      if (this.features.volume) {
        this.volumeSeries = this.chart.addSeries(
          HistogramSeries,
          {
            priceFormat: { type: "volume" },
            priceScaleId: "",
          },
          1,
        );
        const panes = this.chart.panes();
        if (panes.length > 1) {
          panes[0]?.setStretchFactor(3);
          panes[1]?.setStretchFactor(1);
        }
      }

      if (this.features.sma) {
        this.smaSeries = this.chart.addSeries(LineSeries, {
          color: THEME.sma,
          lineWidth: 1,
          priceLineVisible: false,
          lastValueVisible: false,
          crosshairMarkerVisible: false,
        });
      }
    } else {
      this.lineSeries = this.chart.addSeries(LineSeries, {
        color: THEME.line,
        lineWidth: 2,
        crosshairMarkerRadius: 4,
      });

      if (this.features.sma) {
        this.smaSeries = this.chart.addSeries(LineSeries, {
          color: THEME.sma,
          lineWidth: 1,
          priceLineVisible: false,
          lastValueVisible: false,
          crosshairMarkerVisible: false,
        });
      }
    }
  }

  private pushLine(time: UTCTimestamp, price: number, scroll: boolean): void {
    const point: LineData = { time, value: price };
    this.lineSeries!.update(point);
    if (scroll && this.features.autoFollow) {
      this.chart.timeScale().scrollToRealTime();
    }
  }

  private updateVolume(
    time: UTCTimestamp,
    volume: number | null,
    open: number,
    close: number,
  ): void {
    if (!this.features.volume || !this.volumeSeries || volume == null) return;
    const bar: HistogramData = {
      time,
      value: volume,
      color: close >= open ? THEME.up : THEME.down,
    };
    this.volumeSeries.update(bar);
  }

  private updateSma(time: UTCTimestamp, close: number, scroll: boolean): void {
    if (!this.features.sma || !this.smaSeries) return;
    upsertClose(this.closes, time, close);
    const sma = smaAt(this.closes, SMA_PERIOD);
    if (sma == null) return;
    this.smaSeries.update({ time, value: sma });
    if (scroll && this.features.autoFollow) {
      this.chart.timeScale().scrollToRealTime();
    }
  }

  private trackPrice(price: number, scroll: boolean): void {
    if (this.sessionOpen == null) this.sessionOpen = price;
    if (this.sessionOpen != null) {
      const absolute = price - this.sessionOpen;
      const percent = (absolute / this.sessionOpen) * 100;
      this.onChange?.({ absolute, percent });
    }
    if (scroll && this.features.autoFollow) {
      this.chart.timeScale().scrollToRealTime();
    }
  }

  private onCrosshairMove(param: MouseEventParams): void {
    if (!param.time || !param.point) {
      this.legend.hide();
      return;
    }

    const values: LegendValues = {
      time: formatLegendTime(param.time as number),
    };

    if (this.candleSeries) {
      const data = param.seriesData.get(this.candleSeries);
      if (data && "open" in data) {
        values.open = data.open;
        values.high = data.high;
        values.low = data.low;
        values.close = data.close;
      }
    }

    if (this.lineSeries) {
      const data = param.seriesData.get(this.lineSeries);
      if (data && "value" in data) {
        values.price = data.value;
      }
    }

    if (this.volumeSeries) {
      const data = param.seriesData.get(this.volumeSeries);
      if (data && "value" in data) {
        values.volume = data.value;
      }
    }

    if (this.smaSeries) {
      const data = param.seriesData.get(this.smaSeries);
      if (data && "value" in data) {
        values.sma = data.value;
      }
    }

    this.legend.show(this.mode, values);
  }
}

function formatLegendTime(epochSec: number): string {
  const d = new Date(epochSec * 1000);
  return d.toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

import {
  CandlestickSeries,
  ColorType,
  createChart,
  CrosshairMode,
  HistogramSeries,
  LineSeries,
  type HistogramData,
  type IChartApi,
  type ISeriesApi,
  type LineData,
  type MouseEventParams,
  type UTCTimestamp,
} from "lightweight-charts";
import { SMA_PERIOD, smaAt, upsertClose, type ClosePoint } from "@/lib/chart/indicators";
import { CHART_THEME } from "@/lib/chart/theme";
import type { ChartMode, IlpRow } from "@/lib/ilp/types";
import { fieldNumber, fieldsToMap, toChartTime } from "@/lib/ilp/types";
import { formatLegendTime } from "@/lib/utils";
import type { ChartFeatures, LegendValues, PriceChange } from "@/types/terminal";

const ROW_BUFFER_MAX = 500;

export type { ChartFeatures, PriceChange, LegendValues };

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
  private onChange: ((change: PriceChange | null) => void) | null = null;
  private onLegend: ((values: LegendValues | null) => void) | null = null;
  private crosshairHandler: (param: MouseEventParams) => void;
  private resizeObserver: ResizeObserver;
  private container: HTMLElement;

  constructor(
    container: HTMLElement,
    onLegend?: (values: LegendValues | null) => void,
  ) {
    this.container = container;
    this.onLegend = onLegend ?? null;
    this.chart = createChart(container, {
      layout: {
        background: { type: ColorType.Solid, color: CHART_THEME.panel },
        textColor: CHART_THEME.text,
        fontFamily: CHART_THEME.sans,
        fontSize: 12,
        attributionLogo: false,
        panes: {
          separatorColor: CHART_THEME.grid,
          separatorHoverColor: "#434651",
        },
      },
      grid: {
        vertLines: { color: CHART_THEME.grid },
        horzLines: { color: CHART_THEME.grid },
      },
      crosshair: {
        mode: CrosshairMode.Normal,
        vertLine: { color: CHART_THEME.crosshair, labelBackgroundColor: "#1a1a1a" },
        horzLine: { color: CHART_THEME.crosshair, labelBackgroundColor: "#1a1a1a" },
      },
      rightPriceScale: { borderColor: CHART_THEME.grid },
      timeScale: {
        borderColor: CHART_THEME.grid,
        timeVisible: true,
        secondsVisible: true,
      },
      handleScroll: true,
      handleScale: true,
    });

    this.crosshairHandler = (param) => this.onCrosshairMove(param);
    this.chart.subscribeCrosshairMove(this.crosshairHandler);

    this.resizeObserver = new ResizeObserver(() => {
      const { width, height } = container.getBoundingClientRect();
      this.chart.applyOptions({ width, height });
    });
    this.resizeObserver.observe(container);
  }

  setFeatures(features: Partial<ChartFeatures>): void {
    const prev = { ...this.features };
    this.features = { ...this.features, ...features };
    if (prev.volume !== this.features.volume || prev.sma !== this.features.sma) {
      this.replayBuffer();
    }
  }

  setOnPriceChange(handler: (change: PriceChange | null) => void): void {
    this.onChange = handler;
  }

  setOnLegend(handler: (values: LegendValues | null) => void): void {
    this.onLegend = handler;
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
    this.onLegend?.(null);
    this.resetSeries();
  }

  fitContent(): void {
    this.chart.timeScale().fitContent();
  }

  resize(): void {
    const { width, height } = this.container.getBoundingClientRect();
    if (width > 0 && height > 0) {
      this.chart.applyOptions({ width, height });
    }
  }

  zoomIn(): void {
    this.zoomLogical(0.72);
  }

  zoomOut(): void {
    this.zoomLogical(1.38);
  }

  resetZoom(): void {
    this.chart.timeScale().resetTimeScale();
  }

  private zoomLogical(factor: number): void {
    const range = this.chart.timeScale().getVisibleLogicalRange();
    if (!range) return;
    const span = range.to - range.from;
    const center = (range.from + range.to) / 2;
    const newSpan = Math.max(span * factor, 2);
    this.chart.timeScale().setVisibleLogicalRange({
      from: center - newSpan / 2,
      to: center + newSpan / 2,
    });
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

      this.candleSeries.update({
        time: time as UTCTimestamp,
        open,
        high,
        low,
        close,
      });
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
    this.resizeObserver.disconnect();
    this.chart.unsubscribeCrosshairMove(this.crosshairHandler);
    this.chart.remove();
  }

  private bufferRow(row: IlpRow): void {
    this.rowBuffer.push(row);
    if (this.rowBuffer.length > ROW_BUFFER_MAX) this.rowBuffer.shift();
  }

  private replayBuffer(): void {
    const rows = this.rowBuffer;
    this.closes = [];
    this.sessionOpen = null;
    this.rowBuffer = [];
    this.resetSeries();
    for (const row of rows) this.applyRow(row, false);
  }

  private resetSeries(): void {
    for (const s of [this.candleSeries, this.lineSeries, this.volumeSeries, this.smaSeries]) {
      if (s) this.chart.removeSeries(s);
    }
    this.candleSeries = null;
    this.lineSeries = null;
    this.volumeSeries = null;
    this.smaSeries = null;

    if (this.mode === "ohlcv") {
      this.candleSeries = this.chart.addSeries(CandlestickSeries, {
        upColor: CHART_THEME.up,
        downColor: CHART_THEME.down,
        borderUpColor: CHART_THEME.up,
        borderDownColor: CHART_THEME.down,
        wickUpColor: CHART_THEME.up,
        wickDownColor: CHART_THEME.down,
        lastValueVisible: false,
        priceLineVisible: false,
      });

      if (this.features.volume) {
        this.volumeSeries = this.chart.addSeries(
          HistogramSeries,
          { priceFormat: { type: "volume" }, priceScaleId: "" },
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
          color: CHART_THEME.sma,
          lineWidth: 1,
          priceLineVisible: false,
          lastValueVisible: false,
          crosshairMarkerVisible: false,
        });
      }
    } else {
      this.lineSeries = this.chart.addSeries(LineSeries, {
        color: CHART_THEME.line,
        lineWidth: 2,
        crosshairMarkerRadius: 3,
        lastValueVisible: false,
        priceLineVisible: false,
      });
      if (this.features.sma) {
        this.smaSeries = this.chart.addSeries(LineSeries, {
          color: CHART_THEME.sma,
          lineWidth: 1,
          priceLineVisible: false,
          lastValueVisible: false,
          crosshairMarkerVisible: false,
        });
      }
    }
  }

  private pushLine(time: UTCTimestamp, price: number, scroll: boolean): void {
    this.lineSeries!.update({ time, value: price } as LineData);
    if (scroll && this.features.autoFollow) this.chart.timeScale().scrollToRealTime();
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
      color: close >= open ? CHART_THEME.up : CHART_THEME.down,
    };
    this.volumeSeries.update(bar);
  }

  private updateSma(time: UTCTimestamp, close: number, scroll: boolean): void {
    if (!this.features.sma || !this.smaSeries) return;
    upsertClose(this.closes, time, close);
    const sma = smaAt(this.closes, SMA_PERIOD);
    if (sma == null) return;
    this.smaSeries.update({ time, value: sma });
    if (scroll && this.features.autoFollow) this.chart.timeScale().scrollToRealTime();
  }

  private trackPrice(price: number, scroll: boolean): void {
    if (this.sessionOpen == null) this.sessionOpen = price;
    const absolute = price - this.sessionOpen;
    const percent = (absolute / this.sessionOpen) * 100;
    this.onChange?.({ absolute, percent });
    if (scroll && this.features.autoFollow) this.chart.timeScale().scrollToRealTime();
  }

  private onCrosshairMove(param: MouseEventParams): void {
    if (!param.time || !param.point) {
      this.onLegend?.(null);
      return;
    }

    const values: LegendValues = { time: formatLegendTime(param.time as number) };

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
      if (data && "value" in data) values.price = data.value;
    }
    if (this.volumeSeries) {
      const data = param.seriesData.get(this.volumeSeries);
      if (data && "value" in data) values.volume = data.value;
    }
    if (this.smaSeries) {
      const data = param.seriesData.get(this.smaSeries);
      if (data && "value" in data) values.sma = data.value;
    }

    this.onLegend?.(values);
  }
}

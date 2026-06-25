import type { UTCTimestamp } from "lightweight-charts";

export interface ClosePoint {
  time: UTCTimestamp;
  close: number;
}

export const SMA_PERIOD = 20;

export function upsertClose(points: ClosePoint[], time: UTCTimestamp, close: number): void {
  const last = points[points.length - 1];
  if (last && last.time === time) {
    last.close = close;
    return;
  }
  points.push({ time, close });
}

export function smaAt(points: ClosePoint[], period: number): number | null {
  if (points.length < period) return null;
  const slice = points.slice(-period);
  const sum = slice.reduce((acc, p) => acc + p.close, 0);
  return sum / period;
}

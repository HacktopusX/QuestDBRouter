export type IlpField = number | boolean | string;

export interface IlpRow {
  measurement: string;
  tags: [string, string][];
  fields: [string, IlpField][];
  timestamp_ns: number | null;
}

export interface ReplayOut {
  op: "replay";
  topic: string;
  rows: IlpRow[];
}

export type ChartMode = "ohlcv" | "line";

export function tagsToMap(tags: [string, string][]): Map<string, string> {
  return new Map(tags.map(([k, v]) => [k.toLowerCase(), v]));
}

export function fieldsToMap(fields: [string, IlpField][]): Map<string, IlpField> {
  return new Map(fields.map(([k, v]) => [k.toLowerCase(), v]));
}

export function toChartTime(timestampNs: number | null): number | null {
  if (timestampNs == null) return null;
  // QuestDB ILP timestamps are typically nanoseconds; fall back if already seconds/ms.
  if (timestampNs > 1e18) return Math.floor(timestampNs / 1_000_000_000);
  if (timestampNs > 1e15) return Math.floor(timestampNs / 1_000_000);
  if (timestampNs > 1e12) return Math.floor(timestampNs / 1_000);
  return timestampNs;
}

export function fieldNumber(fields: Map<string, IlpField>, name: string): number | null {
  const v = fields.get(name.toLowerCase());
  if (typeof v === "number") return v;
  if (typeof v === "string") {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

import type { IlpRow } from "@/lib/ilp/types";

type RowHandler = (row: IlpRow) => void;
type ReplayHandler = (rows: IlpRow[]) => void;

const rowHandlers = new Map<string, Set<RowHandler>>();
const replayHandlers = new Map<string, Set<ReplayHandler>>();

export function subscribePanelRows(panelId: string, handler: RowHandler): () => void {
  let set = rowHandlers.get(panelId);
  if (!set) {
    set = new Set();
    rowHandlers.set(panelId, set);
  }
  set.add(handler);
  return () => {
    set!.delete(handler);
    if (set!.size === 0) rowHandlers.delete(panelId);
  };
}

export function subscribePanelReplay(panelId: string, handler: ReplayHandler): () => void {
  let set = replayHandlers.get(panelId);
  if (!set) {
    set = new Set();
    replayHandlers.set(panelId, set);
  }
  set.add(handler);
  return () => {
    set!.delete(handler);
    if (set!.size === 0) replayHandlers.delete(panelId);
  };
}

export function emitPanelRow(panelId: string, row: IlpRow): void {
  rowHandlers.get(panelId)?.forEach((h) => h(row));
}

export function emitPanelReplay(panelId: string, rows: IlpRow[]): void {
  replayHandlers.get(panelId)?.forEach((h) => h(rows));
}

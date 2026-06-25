import { useCallback } from "react";
import { emitPanelReplay, emitPanelRow } from "@/lib/chart/chart-bus";
import { lastPriceFromRow } from "@/lib/ilp/types";
import { useTopicSubscription } from "@/hooks/use-topic-subscription";
import { useTerminal } from "@/providers/TerminalProvider";
import type { PanelState } from "@/types/terminal";

/** Keeps WebSocket subscriptions alive regardless of active view. */
export function PanelStreamBridge({ panel }: { panel: PanelState }) {
  const { bumpPanelTick, setPanelReplay } = useTerminal();

  const onRow = useCallback(
    (row: import("@/lib/ilp/types").IlpRow) => {
      const price = lastPriceFromRow(row);
      if (price != null) bumpPanelTick(panel.id, price);
      emitPanelRow(panel.id, row);
    },
    [bumpPanelTick, panel.id],
  );

  const onReplay = useCallback(
    (payload: import("@/lib/ilp/types").ReplayOut) => {
      const last = payload.rows[payload.rows.length - 1];
      const price = last ? lastPriceFromRow(last) : null;
      setPanelReplay(panel.id, payload.rows.length, price);
      emitPanelReplay(panel.id, payload.rows);
    },
    [panel.id, setPanelReplay],
  );

  useTopicSubscription(panel.topic, onRow, onReplay);
  return null;
}

export function PanelStreamBridges() {
  const { panels } = useTerminal();
  return (
    <>
      {panels.map((panel) => (
        <PanelStreamBridge key={panel.id} panel={panel} />
      ))}
    </>
  );
}

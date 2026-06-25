import { useMemo } from "react";
import { Trash2 } from "lucide-react";
import { ChartPanel } from "@/components/terminal/chart-panel";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { cn, formatPrice } from "@/lib/utils";
import { useStream } from "@/providers/StreamProvider";
import { useTerminal } from "@/providers/TerminalProvider";

function formatTickTime(ns: number): string {
  const ms = ns > 1e15 ? ns / 1_000_000 : ns > 1e12 ? ns / 1_000 : ns;
  return new Date(ms).toLocaleTimeString(undefined, {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    fractionalSecondDigits: 3,
  });
}

export function LivestreamView() {
  const { connected, statusDetail, tickLog, clearTickLog } = useStream();
  const { panels, focusPanel } = useTerminal();

  const focused = useMemo(
    () => panels.find((p) => p.focused) ?? panels[0],
    [panels],
  );

  if (!focused) {
    return (
      <div className="flex flex-1 items-center justify-center text-muted-foreground">
        No symbols configured
      </div>
    );
  }

  return (
    <div className="flex min-h-0 flex-1">
      <aside className="flex w-48 shrink-0 flex-col border-r border-border bg-card/20">
        <div className="border-b border-border px-3 py-2.5">
          <h2 className="text-[0.65rem] font-medium uppercase tracking-widest text-muted-foreground">
            Subscriptions
          </h2>
        </div>
        <ul className="flex-1 overflow-y-auto">
          {panels.map((panel) => (
            <li key={panel.id}>
              <button
                type="button"
                onClick={() => focusPanel(panel.id)}
                className={cn(
                  "flex w-full flex-col gap-0.5 border-b border-border/30 px-3 py-2.5 text-left transition-colors hover:bg-muted/20",
                  panel.id === focused.id && "border-l-2 border-l-primary bg-accent/30",
                )}
              >
                <span className="font-mono text-xs font-semibold uppercase text-primary">
                  {panel.topic}
                </span>
                <span className="font-mono text-[0.65rem] tabular-nums text-muted-foreground">
                  {panel.price != null ? formatPrice(panel.price) : "—"} · {panel.tickCount} ticks
                </span>
              </button>
            </li>
          ))}
        </ul>
      </aside>

      <div className="flex min-w-0 flex-1 flex-col">
        <div className="flex items-center justify-between border-b border-border bg-card/30 px-4 py-2">
          <div className="flex items-center gap-2">
            <Badge variant={connected ? "up" : "down"}>{connected ? "LIVE" : "OFFLINE"}</Badge>
            <span className="font-mono text-sm font-semibold uppercase text-primary">
              {focused.topic}
            </span>
            <span className="text-xs text-muted-foreground">{statusDetail}</span>
          </div>
        </div>
        <div className="min-h-0 flex-1 p-px">
          <ChartPanel key={focused.id} panel={focused} embedded />
        </div>
      </div>

      <aside className="flex w-72 shrink-0 flex-col border-l border-border bg-card/20">
        <div className="flex items-center justify-between border-b border-border px-3 py-2.5">
          <h2 className="text-[0.65rem] font-medium uppercase tracking-widest text-muted-foreground">
            Tick tape
          </h2>
          <Button
            variant="ghost"
            size="icon"
            className="h-6 w-6 text-muted-foreground"
            onClick={clearTickLog}
            title="Clear tape"
          >
            <Trash2 className="h-3 w-3" />
          </Button>
        </div>
        <Card className="m-2 rounded-lg border-border/50 bg-background/60 shadow-none">
          <CardHeader className="p-2 pb-0">
            <CardTitle className="text-[0.55rem]">Last {tickLog.length} events</CardTitle>
          </CardHeader>
          <CardContent className="max-h-[calc(100vh-12rem)] overflow-y-auto p-2 font-mono text-[0.62rem] leading-relaxed">
            {tickLog.length === 0 ? (
              <p className="text-muted-foreground">Waiting for ticks…</p>
            ) : (
              tickLog.map((entry) => (
                <div
                  key={entry.id}
                  className="mb-1.5 border-b border-border/20 pb-1.5 last:border-0"
                >
                  <div className="flex justify-between text-muted-foreground/70">
                    <span className="uppercase text-primary/80">{entry.topic}</span>
                    <span>{formatTickTime(entry.time)}</span>
                  </div>
                  <div className="text-terminal-up">
                    {entry.price != null ? formatPrice(entry.price) : "—"}
                  </div>
                </div>
              ))
            )}
          </CardContent>
        </Card>
      </aside>
    </div>
  );
}

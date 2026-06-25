import { ArrowRight, Radio } from "lucide-react";
import { StatsCards } from "@/components/terminal/stats-cards";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { cn, formatPercent, formatPrice } from "@/lib/utils";
import { useStream } from "@/providers/StreamProvider";
import { useTerminal } from "@/providers/TerminalProvider";
import { aggregateStats } from "@/types/terminal";

export function OverviewView() {
  const { connected } = useStream();
  const { panels, focusPanel, setView } = useTerminal();
  const stats = aggregateStats(panels);

  const sorted = [...panels].sort(
    (a, b) => (b.change?.percent ?? -Infinity) - (a.change?.percent ?? -Infinity),
  );

  return (
    <div className="flex min-h-0 flex-1 flex-col overflow-y-auto">
      <div className="border-b border-border/60 bg-card/20 px-6 py-5">
        <div className="flex flex-wrap items-end justify-between gap-4">
          <div>
            <p className="text-[0.65rem] uppercase tracking-widest text-muted-foreground">
              Overview
            </p>
            <h1 className="mt-1 font-mono text-3xl font-semibold tabular-nums text-foreground">
              {stats.live} / {stats.panelCount}
              <span className="ml-2 text-lg text-muted-foreground">live feeds</span>
            </h1>
            <p className="mt-1 text-sm text-muted-foreground">
              {connected ? "Router stream active" : "Waiting for WebSocket connection"}
            </p>
          </div>
          <div className="flex gap-2">
            <Button
              size="sm"
              className="gap-1.5"
              onClick={() => setView("charts")}
            >
              Open charts
              <ArrowRight className="h-3.5 w-3.5" />
            </Button>
            <Button
              variant="outline"
              size="sm"
              className="gap-1.5 border-primary/30"
              onClick={() => setView("stream")}
            >
              <Radio className="h-3.5 w-3.5" />
              Live stream
            </Button>
          </div>
        </div>
      </div>

      <StatsCards />

      <div className="grid min-h-0 flex-1 gap-px bg-border p-px lg:grid-cols-[1fr_320px]">
        <Card className="rounded-none border-0 bg-card/40 shadow-none">
          <CardHeader className="border-b border-border/40 px-4 py-3">
            <CardTitle>Markets</CardTitle>
          </CardHeader>
          <CardContent className="p-0">
            <table className="w-full text-left text-sm">
              <thead>
                <tr className="border-b border-border/40 text-[0.65rem] uppercase tracking-widest text-muted-foreground">
                  <th className="px-4 py-2 font-medium">Symbol</th>
                  <th className="px-4 py-2 font-medium">Price</th>
                  <th className="px-4 py-2 font-medium">Change</th>
                  <th className="px-4 py-2 font-medium">Ticks</th>
                  <th className="px-4 py-2 font-medium">Status</th>
                </tr>
              </thead>
              <tbody>
                {sorted.map((panel) => (
                  <tr
                    key={panel.id}
                    className="cursor-pointer border-b border-border/20 transition-colors hover:bg-muted/20"
                    onClick={() => {
                      focusPanel(panel.id);
                      setView("charts");
                    }}
                  >
                    <td className="px-4 py-2.5 font-mono text-xs font-semibold uppercase text-primary">
                      {panel.topic}
                    </td>
                    <td className="px-4 py-2.5 font-mono tabular-nums">
                      {panel.price != null ? formatPrice(panel.price) : "—"}
                    </td>
                    <td className="px-4 py-2.5">
                      {panel.change ? (
                        <span
                          className={cn(
                            "font-mono text-xs tabular-nums",
                            panel.change.absolute >= 0 ? "text-terminal-up" : "text-terminal-down",
                          )}
                        >
                          {formatPercent(panel.change.percent)}
                        </span>
                      ) : (
                        "—"
                      )}
                    </td>
                    <td className="px-4 py-2.5 font-mono text-xs tabular-nums text-muted-foreground">
                      {panel.tickCount}
                    </td>
                    <td className="px-4 py-2.5">
                      <Badge variant={panel.live ? "up" : "muted"} className="text-[0.6rem]">
                        {panel.live ? "Live" : "Idle"}
                      </Badge>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </CardContent>
        </Card>

        <Card className="rounded-none border-0 bg-card/40 shadow-none">
          <CardHeader className="border-b border-border/40 px-4 py-3">
            <CardTitle>Session</CardTitle>
          </CardHeader>
          <CardContent className="space-y-4 p-4">
            <StatRow label="Total ticks" value={stats.ticks.toLocaleString()} />
            <StatRow
              label="Avg change"
              value={stats.avgChange != null ? formatPercent(stats.avgChange) : "—"}
              positive={stats.avgChange != null ? stats.avgChange >= 0 : undefined}
            />
            <StatRow
              label="Top mover"
              value={stats.topGainer?.topic.toUpperCase() ?? "—"}
            />
            <StatRow
              label="Top change"
              value={
                stats.topGainer?.change
                  ? formatPercent(stats.topGainer.change.percent)
                  : "—"
              }
              positive={
                stats.topGainer?.change
                  ? stats.topGainer.change.absolute >= 0
                  : undefined
              }
            />
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

function StatRow({
  label,
  value,
  positive,
}: {
  label: string;
  value: string;
  positive?: boolean;
}) {
  return (
    <div className="flex items-center justify-between text-sm">
      <span className="text-muted-foreground">{label}</span>
      <span
        className={cn(
          "font-mono tabular-nums",
          positive === true && "text-terminal-up",
          positive === false && "text-terminal-down",
        )}
      >
        {value}
      </span>
    </div>
  );
}

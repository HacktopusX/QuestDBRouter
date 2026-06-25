import { BarChart2, Layers, Radio, Zap } from "lucide-react";
import { Card, CardContent } from "@/components/ui/card";
import { cn, formatPercent } from "@/lib/utils";
import { useStream } from "@/providers/StreamProvider";
import { useTerminal } from "@/providers/TerminalProvider";
import { aggregateStats } from "@/types/terminal";

export function StatsCards() {
  const { connected } = useStream();
  const { panels } = useTerminal();
  const stats = aggregateStats(panels);

  const cards = [
    {
      icon: BarChart2,
      label: "Active Symbols",
      value: String(stats.panelCount),
      sub: `${stats.live} live`,
      accent: "text-primary",
    },
    {
      icon: Zap,
      label: "Total Ticks",
      value: stats.ticks.toLocaleString(),
      sub: connected ? "streaming" : "idle",
      accent: "text-foreground",
    },
    {
      icon: Layers,
      label: "Avg Session",
      value: stats.avgChange != null ? formatPercent(stats.avgChange) : "—",
      sub: "per symbol",
      accent:
        stats.avgChange == null
          ? "text-muted-foreground"
          : stats.avgChange >= 0
            ? "text-terminal-up"
            : "text-terminal-down",
    },
    {
      icon: Radio,
      label: "Top Mover",
      value: stats.topGainer?.topic.toUpperCase() ?? "—",
      sub: stats.topGainer?.change
        ? formatPercent(stats.topGainer.change.percent)
        : "—",
      accent: "text-primary",
    },
  ];

  return (
    <div className="grid shrink-0 grid-cols-2 gap-px border-t border-border bg-border lg:grid-cols-4">
      {cards.map(({ icon: Icon, label, value, sub, accent }) => (
        <Card
          key={label}
          className="rounded-none border-0 border-border/40 bg-card/60 shadow-none"
        >
          <CardContent className="flex items-start gap-3 p-4">
            <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-primary/10">
              <Icon className="h-4 w-4 text-primary" />
            </div>
            <div className="min-w-0">
              <p className="text-[0.6rem] uppercase tracking-widest text-muted-foreground">
                {label}
              </p>
              <p className={cn("truncate font-mono text-lg font-semibold tabular-nums", accent)}>
                {value}
              </p>
              <p className="text-[0.65rem] text-muted-foreground">{sub}</p>
            </div>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}

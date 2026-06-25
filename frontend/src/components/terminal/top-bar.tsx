import { Bell, Search } from "lucide-react";
import { ConnectionStatus } from "@/components/terminal/connection-status";
import { GlobalControls } from "@/components/terminal/global-controls";
import { LayoutPicker } from "@/components/terminal/layout-picker";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { cn, formatPercent, formatPrice } from "@/lib/utils";
import { useStream } from "@/providers/StreamProvider";
import { useTerminal } from "@/providers/TerminalProvider";
import { aggregateStats } from "@/types/terminal";

export function TopBar() {
  const { connected } = useStream();
  const { panels, settings } = useTerminal();
  const stats = aggregateStats(panels);

  const viewLabel =
    settings.view === "overview"
      ? "overview"
      : settings.view === "stream"
        ? "live-stream"
        : "charts";

  const now = new Date().toLocaleString(undefined, {
    month: "long",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });

  const primaryPanel = panels.find((p) => p.focused) ?? panels[0];
  const navLabel = primaryPanel?.price != null ? formatPrice(primaryPanel.price) : "—";

  return (
    <header className="flex shrink-0 flex-col gap-2 border-b border-border bg-card/40 px-4 py-3 backdrop-blur-md">
      <div className="flex flex-wrap items-center gap-3">
        <div className="flex items-center gap-2 text-sm">
          <span className="text-muted-foreground">Engine</span>
          <span className="text-muted-foreground">/</span>
          <span className="font-medium text-foreground">{viewLabel}</span>
        </div>

        <Badge
          variant="outline"
          className={cn(
            "gap-1.5 border-border/60 bg-secondary/50 text-[0.65rem] font-normal",
            connected && "border-terminal-up/30 text-terminal-up",
          )}
        >
          <span
            className={cn(
              "h-1.5 w-1.5 rounded-full",
              connected ? "bg-terminal-up shadow-[0_0_6px] hsl(var(--terminal-up)/0.8)" : "bg-destructive",
            )}
          />
          {connected ? "Stream active" : "Disconnected"}
        </Badge>

        <span className="hidden text-xs text-muted-foreground lg:inline">{now}</span>

        <div className="ml-auto flex flex-wrap items-center gap-4">
          <Metric label="Focus" value={navLabel} />
          <Metric
            label="Avg"
            value={stats.avgChange != null ? formatPercent(stats.avgChange) : "—"}
            positive={stats.avgChange != null ? stats.avgChange >= 0 : undefined}
          />
          <Metric label="Feeds" value={String(stats.live)} />
          <Metric label="Ticks" value={stats.ticks.toLocaleString()} />

          <div className="flex items-center gap-1">
            <Button variant="ghost" size="icon" className="h-8 w-8 text-muted-foreground">
              <Search className="h-4 w-4" />
            </Button>
            <Button variant="ghost" size="icon" className="h-8 w-8 text-muted-foreground">
              <Bell className="h-4 w-4" />
            </Button>
          </div>

          <ConnectionStatus />
        </div>
      </div>

      <div className="flex flex-wrap items-center gap-3 border-t border-border/40 pt-2">
        {settings.view === "charts" && <LayoutPicker />}
        <GlobalControls />
      </div>
    </header>
  );
}

function Metric({
  label,
  value,
  positive,
}: {
  label: string;
  value: string;
  positive?: boolean;
}) {
  return (
    <div className="text-right">
      <p className="text-[0.6rem] uppercase tracking-widest text-muted-foreground">{label}</p>
      <p
        className={cn(
          "font-mono text-sm font-semibold tabular-nums",
          positive === true && "text-terminal-up",
          positive === false && "text-terminal-down",
          positive === undefined && "text-foreground",
        )}
      >
        {value}
      </p>
    </div>
  );
}

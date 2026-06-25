import {
  Activity,
  BarChart3,
  LayoutGrid,
  Radio,
  Settings,
  TrendingUp,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { cn, formatPercent } from "@/lib/utils";
import { useStream } from "@/providers/StreamProvider";
import { useTerminal } from "@/providers/TerminalProvider";
import { aggregateStats, type TerminalView } from "@/types/terminal";

const NAV: { id: TerminalView; label: string; icon: typeof LayoutGrid }[] = [
  { id: "overview", label: "Overview", icon: LayoutGrid },
  { id: "charts", label: "Charts", icon: BarChart3 },
  { id: "stream", label: "Live Stream", icon: Radio },
];

export function Sidebar() {
  const { connected } = useStream();
  const { panels, settings, setView } = useTerminal();
  const stats = aggregateStats(panels);
  const activeView = settings.view;

  return (
    <aside className="flex w-[220px] shrink-0 flex-col border-r border-sidebar-border bg-sidebar">
      <div className="flex items-center gap-2.5 border-b border-sidebar-border px-4 py-4">
        <div className="flex h-8 w-8 items-center justify-center rounded-full border border-primary/40 bg-primary/10">
          <span className="font-mono text-[0.6rem] font-bold text-primary">QR</span>
        </div>
        <div>
          <p className="text-sm font-semibold tracking-wide text-foreground">Quest Router</p>
          <p className="text-[0.65rem] text-muted-foreground">Trading Terminal</p>
        </div>
      </div>

      <nav className="flex flex-col gap-0.5 p-3">
        {NAV.map(({ id, label, icon: Icon }) => {
          const active = activeView === id;
          return (
            <button
              key={id}
              type="button"
              onClick={() => setView(id)}
              className={cn(
                "flex items-center gap-2.5 rounded-lg px-3 py-2 text-left text-sm transition-colors",
                active
                  ? "bg-sidebar-accent text-primary"
                  : "text-sidebar-foreground hover:bg-muted/50 hover:text-foreground",
              )}
            >
              <Icon className={cn("h-4 w-4", active && "text-primary")} />
              {label}
            </button>
          );
        })}
        <button
          type="button"
          disabled
          className="flex cursor-not-allowed items-center gap-2.5 rounded-lg px-3 py-2 text-left text-sm text-muted-foreground/40"
        >
          <TrendingUp className="h-4 w-4" />
          Signals
          <span className="ml-auto text-[0.55rem]">soon</span>
        </button>
      </nav>

      <div className="mt-2 px-3">
        <Card className="border-sidebar-border bg-card/50">
          <CardHeader className="p-3 pb-1">
            <CardTitle className="text-[0.6rem]">Stream Status</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2 p-3 pt-1">
            <div className="flex items-center justify-between text-xs">
              <span className="text-muted-foreground">Connection</span>
              <Badge variant={connected ? "up" : "down"} className="text-[0.6rem]">
                {connected ? "Active" : "Offline"}
              </Badge>
            </div>
            <div className="flex items-center justify-between text-xs">
              <span className="text-muted-foreground">Symbols</span>
              <span className="font-mono tabular-nums text-foreground">{stats.panelCount}</span>
            </div>
            <div className="flex items-center justify-between text-xs">
              <span className="text-muted-foreground">Live feeds</span>
              <span className="font-mono tabular-nums text-terminal-up">{stats.live}</span>
            </div>
            <div className="flex items-center justify-between text-xs">
              <span className="text-muted-foreground">Avg change</span>
              <span
                className={cn(
                  "font-mono tabular-nums",
                  stats.avgChange == null
                    ? "text-muted-foreground"
                    : stats.avgChange >= 0
                      ? "text-terminal-up"
                      : "text-terminal-down",
                )}
              >
                {stats.avgChange != null ? formatPercent(stats.avgChange) : "—"}
              </span>
            </div>
          </CardContent>
        </Card>
      </div>

      <div className="mt-auto border-t border-sidebar-border p-3">
        <button
          type="button"
          className="flex w-full items-center gap-2 rounded-lg px-3 py-2 text-sm text-muted-foreground transition-colors hover:bg-muted/50 hover:text-foreground"
        >
          <Settings className="h-4 w-4" />
          Settings
        </button>
        <div className="mt-2 flex flex-col gap-1 px-3 text-[0.6rem] text-muted-foreground/60">
          <div className="flex items-center gap-2">
            <Activity className="h-3 w-3 text-primary" />
            <span>{stats.ticks.toLocaleString()} ticks received</span>
          </div>
          <a
            href="https://www.tradingview.com/"
            target="_blank"
            rel="noopener noreferrer"
            className="pl-5 hover:text-muted-foreground"
          >
            Charts by TradingView
          </a>
        </div>
      </div>
    </aside>
  );
}

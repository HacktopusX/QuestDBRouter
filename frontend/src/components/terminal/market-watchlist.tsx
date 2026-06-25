import { cn, formatPercent, formatPrice } from "@/lib/utils";
import { useTerminal } from "@/providers/TerminalProvider";

export function MarketWatchlist() {
  const { panels, focusPanel } = useTerminal();

  return (
    <section className="flex w-52 shrink-0 flex-col border-r border-border bg-card/30">
      <div className="border-b border-border px-3 py-2.5">
        <h2 className="text-[0.65rem] font-medium uppercase tracking-widest text-muted-foreground">
          Markets
        </h2>
      </div>
      <ul className="flex-1 overflow-y-auto">
        {panels.map((panel) => (
          <li key={panel.id}>
            <button
              type="button"
              onClick={() => focusPanel(panel.id)}
              className={cn(
                "flex w-full items-center gap-2 border-b border-border/40 px-3 py-2.5 text-left transition-colors hover:bg-muted/30",
                panel.focused && "border-l-2 border-l-primary bg-accent/40",
              )}
            >
              <span
                className={cn(
                  "h-1.5 w-1.5 shrink-0 rounded-full",
                  panel.live ? "bg-terminal-up" : "bg-muted-foreground/30",
                )}
              />
              <div className="min-w-0 flex-1">
                <p className="truncate font-mono text-xs font-semibold uppercase text-foreground">
                  {panel.topic}
                </p>
                <p className="font-mono text-[0.65rem] tabular-nums text-muted-foreground">
                  {panel.price != null ? formatPrice(panel.price) : "—"}
                </p>
              </div>
              {panel.change && (
                <span
                  className={cn(
                    "font-mono text-[0.65rem] tabular-nums",
                    panel.change.absolute >= 0 ? "text-terminal-up" : "text-terminal-down",
                  )}
                >
                  {formatPercent(panel.change.percent)}
                </span>
              )}
            </button>
          </li>
        ))}
      </ul>
    </section>
  );
}

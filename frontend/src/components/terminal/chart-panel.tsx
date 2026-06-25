import { useEffect, useState } from "react";
import { Maximize2, Minimize2, StretchHorizontal, X } from "lucide-react";
import { ChartLegend } from "@/components/terminal/chart-legend";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useLiveChart } from "@/hooks/use-live-chart";
import { subscribePanelReplay, subscribePanelRows } from "@/lib/chart/chart-bus";
import { cn, formatPercent, formatPrice } from "@/lib/utils";
import { useTerminal } from "@/providers/TerminalProvider";
import type { PanelState } from "@/types/terminal";

interface ChartPanelProps {
  panel: PanelState;
  /** Large single-chart layout (live stream view). */
  embedded?: boolean;
  /** Grid pane expanded to fill the terminal. */
  fullscreen?: boolean;
}

export function ChartPanel({ panel, embedded = false, fullscreen = false }: ChartPanelProps) {
  const {
    settings,
    panels,
    focusPanel,
    expandPanel,
    collapsePanel,
    updatePanelTopic,
    updatePanelStats,
    removePanel,
  } = useTerminal();

  const [topicDraft, setTopicDraft] = useState(panel.topic);
  const {
    containerRef,
    legend,
    priceChange,
    fitContent,
    resize,
    applyRow,
    applyReplay,
    reset,
  } = useLiveChart(settings.mode, settings.features);

  useEffect(() => setTopicDraft(panel.topic), [panel.topic]);
  useEffect(() => reset(), [panel.topic, settings.mode, reset]);

  useEffect(() => {
    if (priceChange) updatePanelStats(panel.id, { change: priceChange });
  }, [priceChange, panel.id, updatePanelStats]);

  useEffect(() => {
    return subscribePanelRows(panel.id, (row) => {
      applyRow(row);
    });
  }, [panel.id, applyRow]);

  useEffect(() => {
    return subscribePanelReplay(panel.id, (rows) => {
      applyReplay(rows);
    });
  }, [panel.id, applyReplay]);

  useEffect(() => {
    const onFit = () => fitContent();
    const onResize = () => resize();
    window.addEventListener("terminal:fit-all", onFit);
    window.addEventListener("terminal:resize-charts", onResize);
    return () => {
      window.removeEventListener("terminal:fit-all", onFit);
      window.removeEventListener("terminal:resize-charts", onResize);
    };
  }, [fitContent, resize]);

  useEffect(() => {
    if (!fullscreen) return;
    const id = requestAnimationFrame(() => resize());
    return () => cancelAnimationFrame(id);
  }, [fullscreen, resize]);

  const commitTopic = () => {
    const next = topicDraft.trim();
    if (next && next !== panel.topic) updatePanelTopic(panel.id, next);
    else setTopicDraft(panel.topic);
  };

  const canClose = panels.length > 1 && !embedded && !fullscreen;
  const showFullscreenToggle = !embedded;

  return (
    <article
      className={cn(
        "flex h-full min-h-0 min-w-0 flex-col bg-terminal-panel",
        panel.focused && !embedded && !fullscreen && "gold-glow ring-1 ring-primary/30 ring-inset",
      )}
      onClick={() => !embedded && !fullscreen && focusPanel(panel.id)}
    >
      <header className="flex shrink-0 items-center gap-2 border-b border-border/50 bg-card/40 px-2.5 py-1.5">
        <span
          className={cn(
            "h-1.5 w-1.5 shrink-0 rounded-full",
            panel.live
              ? "bg-terminal-up shadow-[0_0_5px] hsl(var(--terminal-up)/0.7)"
              : "bg-muted-foreground/25",
          )}
        />
        <Input
          value={topicDraft}
          onChange={(e) => setTopicDraft(e.target.value)}
          onBlur={commitTopic}
          onKeyDown={(e) => {
            if (e.key === "Enter") {
              e.preventDefault();
              (e.target as HTMLInputElement).blur();
            }
          }}
          onClick={(e) => e.stopPropagation()}
          className="h-6 w-24 border-transparent bg-transparent px-1 font-mono text-[0.7rem] font-semibold uppercase tracking-wider text-primary focus-visible:border-border focus-visible:bg-background/50"
          spellCheck={false}
          autoComplete="off"
        />

        <div className="ml-auto flex items-center gap-2">
          <span className="font-mono text-xs font-semibold tabular-nums text-foreground">
            {panel.price != null ? formatPrice(panel.price) : "—"}
          </span>
          {panel.change ? (
            <Badge variant={panel.change.absolute >= 0 ? "up" : "down"} className="text-[0.6rem]">
              {formatPercent(panel.change.percent)}
            </Badge>
          ) : null}
        </div>

        <div className="flex gap-0.5 opacity-70 hover:opacity-100">
          {showFullscreenToggle && (
            <Button
              variant="ghost"
              size="icon"
              className="h-6 w-6 text-muted-foreground hover:text-primary"
              onClick={(e) => {
                e.stopPropagation();
                if (fullscreen) collapsePanel();
                else expandPanel(panel.id);
              }}
              title={fullscreen ? "Exit fullscreen (Esc)" : "Fullscreen"}
            >
              {fullscreen ? <Minimize2 className="h-3 w-3" /> : <Maximize2 className="h-3 w-3" />}
            </Button>
          )}
          <Button
            variant="ghost"
            size="icon"
            className="h-6 w-6 text-muted-foreground hover:text-primary"
            onClick={(e) => {
              e.stopPropagation();
              fitContent();
            }}
            title="Fit all data"
          >
            <StretchHorizontal className="h-3 w-3" />
          </Button>
          {canClose && (
            <Button
              variant="ghost"
              size="icon"
              className="h-6 w-6 text-muted-foreground hover:text-destructive"
              onClick={(e) => {
                e.stopPropagation();
                removePanel(panel.id);
              }}
              title="Remove"
            >
              <X className="h-3 w-3" />
            </Button>
          )}
        </div>
      </header>

      <div className="relative min-h-0 flex-1">
        <ChartLegend mode={settings.mode} values={legend} />
        <div ref={containerRef} className="h-full w-full" />
      </div>
    </article>
  );
}

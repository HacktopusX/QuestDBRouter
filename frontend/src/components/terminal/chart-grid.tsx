import { useCallback, useEffect, useLayoutEffect, useRef, useState, type CSSProperties } from "react";
import { ChartPanel } from "@/components/terminal/chart-panel";
import { LAYOUT_GRID, PANEL_SLOTS } from "@/lib/chart/grid-slots";
import { cn } from "@/lib/utils";
import { useTerminal } from "@/providers/TerminalProvider";

function measureGridBounds(el: HTMLElement): CSSProperties {
  const rect = el.getBoundingClientRect();
  return {
    position: "fixed",
    top: rect.top,
    left: rect.left,
    width: rect.width,
    height: rect.height,
    zIndex: 40,
  };
}

export function ChartGrid() {
  const { settings, panels, fullscreenPanelId, collapsePanel } = useTerminal();
  const gridRef = useRef<HTMLDivElement>(null);
  const [fsStyle, setFsStyle] = useState<CSSProperties | null>(null);

  const syncFullscreenBounds = useCallback(() => {
    const grid = gridRef.current;
    if (!grid || !fullscreenPanelId) {
      setFsStyle(null);
      return;
    }
    setFsStyle(measureGridBounds(grid));
  }, [fullscreenPanelId]);

  useLayoutEffect(() => {
    syncFullscreenBounds();
  }, [syncFullscreenBounds]);

  useEffect(() => {
    if (!fullscreenPanelId) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") collapsePanel();
    };
    window.addEventListener("keydown", onKey);
    window.addEventListener("resize", syncFullscreenBounds);
    return () => {
      window.removeEventListener("keydown", onKey);
      window.removeEventListener("resize", syncFullscreenBounds);
    };
  }, [fullscreenPanelId, collapsePanel, syncFullscreenBounds]);

  useEffect(() => {
    const grid = gridRef.current;
    if (!grid || !fullscreenPanelId) return;
    const ro = new ResizeObserver(() => syncFullscreenBounds());
    ro.observe(grid);
    return () => ro.disconnect();
  }, [fullscreenPanelId, syncFullscreenBounds]);

  useEffect(() => {
    const id = requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        window.dispatchEvent(new CustomEvent("terminal:resize-charts"));
      });
    });
    return () => cancelAnimationFrame(id);
  }, [fullscreenPanelId, settings.layout]);

  useEffect(() => {
    const id = requestAnimationFrame(() => {
      window.dispatchEvent(new CustomEvent("terminal:resize-charts"));
    });
    return () => cancelAnimationFrame(id);
  }, []);

  return (
    <div className="panel-grid-bg flex min-h-0 flex-1 flex-col">
      <div
        ref={gridRef}
        className={cn(
          "grid h-full min-h-0 flex-1 gap-px bg-border p-px",
          LAYOUT_GRID[settings.layout],
        )}
      >
        {panels.map((panel, index) => {
          const isFs = panel.id === fullscreenPanelId;
          const slot = PANEL_SLOTS[settings.layout][index] ?? "";

          return (
            <div key={panel.id} className={cn("relative min-h-0 h-full min-w-0", slot)}>
              {isFs ? <div className="h-full w-full bg-terminal-panel/40" aria-hidden /> : null}
              <div
                className={cn("h-full w-full min-h-0", isFs && "bg-background shadow-2xl")}
                style={isFs ? (fsStyle ?? undefined) : undefined}
              >
                <ChartPanel panel={panel} fullscreen={isFs} />
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

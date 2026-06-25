import { Plus } from "lucide-react";
import { GlobalControls } from "@/components/terminal/global-controls";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";
import { useStream } from "@/providers/StreamProvider";
import { useTerminal } from "@/providers/TerminalProvider";
import { layoutCapacity } from "@/types/terminal";

export function StreamPanel() {
  const { connected, statusDetail } = useStream();
  const { panels, addPanel, settings } = useTerminal();
  const atCapacity = panels.length >= layoutCapacity(settings.layout);

  const logs = [
    connected ? "[stream] WebSocket connected" : "[stream] Waiting for connection…",
    `[stream] ${panels.length} symbol subscriptions`,
    `[stream] Mode: ${settings.mode} · Vol: ${settings.features.volume ? "on" : "off"}`,
    statusDetail ? `[stream] ${statusDetail}` : null,
  ].filter(Boolean) as string[];

  return (
    <aside className="flex w-56 shrink-0 flex-col border-l border-border bg-card/30">
      <div className="border-b border-border px-3 py-2.5">
        <h2 className="text-[0.65rem] font-medium uppercase tracking-widest text-muted-foreground">
          Strategy
        </h2>
      </div>

      <Card className="m-3 rounded-lg border-border/60 bg-card/50 shadow-none">
        <CardHeader className="p-3 pb-1">
          <CardTitle>Routing</CardTitle>
        </CardHeader>
        <CardContent className="space-y-2 p-3 pt-0 text-xs">
          <Row label="scan" value={`${panels.length} topics`} dot="bg-primary" />
          <Row label="decide" value={settings.mode} dot="bg-terminal-up" />
          <Row
            label="stream"
            value={connected ? "live" : "idle"}
            dot={connected ? "bg-terminal-up" : "bg-muted-foreground"}
          />
        </CardContent>
      </Card>

      <Separator className="bg-border/60" />

      <div className="flex-1 overflow-hidden p-3">
        <p className="mb-2 text-[0.6rem] uppercase tracking-widest text-muted-foreground">
          Engine log
        </p>
        <div className="h-full overflow-y-auto rounded-lg border border-border/60 bg-background/80 p-2 font-mono text-[0.62rem] leading-relaxed text-muted-foreground">
          {logs.map((line, i) => (
            <p key={i} className="mb-1 text-terminal-up/80">
              {line}
            </p>
          ))}
        </div>
      </div>

      <div className="space-y-2 border-t border-border p-3">
        <Button
          variant="outline"
          size="sm"
          className="h-8 w-full gap-1 border-primary/30 text-xs text-primary hover:bg-primary/10"
          onClick={addPanel}
          disabled={atCapacity}
        >
          <Plus className="h-3.5 w-3.5" />
          Add chart
        </Button>
        <GlobalControls compact />
      </div>
    </aside>
  );
}

function Row({ label, value, dot }: { label: string; value: string; dot: string }) {
  return (
    <div className="flex items-center justify-between">
      <div className="flex items-center gap-2">
        <span className={`h-1.5 w-1.5 rounded-full ${dot}`} />
        <span className="text-muted-foreground">{label}</span>
      </div>
      <span className="font-mono text-foreground">{value}</span>
    </div>
  );
}

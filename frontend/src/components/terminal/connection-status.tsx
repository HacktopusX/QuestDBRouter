import { useStream } from "@/providers/StreamProvider";
import { useTerminal } from "@/providers/TerminalProvider";
import { cn } from "@/lib/utils";

export function ConnectionStatus() {
  const { connected, statusDetail } = useStream();
  const { panels } = useTerminal();

  return (
    <div className="flex items-center gap-2 rounded-full border border-border/60 bg-secondary/40 px-3 py-1">
      <span
        className={cn(
          "h-1.5 w-1.5 rounded-full",
          connected
            ? "bg-terminal-up shadow-[0_0_6px] hsl(var(--terminal-up)/0.8)"
            : "bg-destructive",
        )}
      />
      <span className="font-mono text-[0.65rem] text-muted-foreground">
        {connected ? `${panels.length} feeds` : statusDetail}
      </span>
    </div>
  );
}

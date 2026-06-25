import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { Button } from "@/components/ui/button";
import { useTerminal } from "@/providers/TerminalProvider";
import type { ChartMode } from "@/lib/ilp/types";
import { cn } from "@/lib/utils";

interface GlobalControlsProps {
  compact?: boolean;
}

export function GlobalControls({ compact }: GlobalControlsProps) {
  const { settings, setMode, setFeatures } = useTerminal();

  const fitAll = () => window.dispatchEvent(new CustomEvent("terminal:fit-all"));

  if (compact) {
    return (
      <div className="space-y-2">
        <label className="flex items-center justify-between text-xs text-muted-foreground">
          <span>Volume</span>
          <Switch
            checked={settings.features.volume}
            disabled={settings.mode === "line"}
            onCheckedChange={(v) => setFeatures({ volume: v })}
          />
        </label>
        <label className="flex items-center justify-between text-xs text-muted-foreground">
          <span>SMA</span>
          <Switch
            checked={settings.features.sma}
            onCheckedChange={(v) => setFeatures({ sma: v })}
          />
        </label>
        <label className="flex items-center justify-between text-xs text-muted-foreground">
          <span>Follow</span>
          <Switch
            checked={settings.features.autoFollow}
            onCheckedChange={(v) => setFeatures({ autoFollow: v })}
          />
        </label>
        <Button
          variant="outline"
          size="sm"
          className="h-7 w-full border-border/60 text-xs"
          onClick={fitAll}
        >
          Fit all
        </Button>
      </div>
    );
  }

  return (
    <div className="flex flex-wrap items-center gap-3">
      <div className="flex items-center gap-2">
        <span className="text-[0.6rem] uppercase tracking-widest text-muted-foreground">Mode</span>
        <Select value={settings.mode} onValueChange={(v) => setMode(v as ChartMode)}>
          <SelectTrigger className="h-7 w-[6.5rem] border-border/60 bg-card/50 text-xs">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="ohlcv">Candles</SelectItem>
            <SelectItem value="line">Line</SelectItem>
          </SelectContent>
        </Select>
      </div>

      <ControlSwitch
        label="Vol"
        checked={settings.features.volume}
        disabled={settings.mode === "line"}
        onCheckedChange={(v) => setFeatures({ volume: v })}
      />
      <ControlSwitch
        label="SMA"
        checked={settings.features.sma}
        onCheckedChange={(v) => setFeatures({ sma: v })}
      />
      <ControlSwitch
        label="Follow"
        checked={settings.features.autoFollow}
        onCheckedChange={(v) => setFeatures({ autoFollow: v })}
      />

      <Button
        variant="outline"
        size="sm"
        className="h-7 border-border/60 text-xs hover:border-primary/40 hover:bg-primary/5"
        onClick={fitAll}
      >
        Fit all
      </Button>
    </div>
  );
}

function ControlSwitch({
  label,
  checked,
  disabled,
  onCheckedChange,
}: {
  label: string;
  checked: boolean;
  disabled?: boolean;
  onCheckedChange: (v: boolean) => void;
}) {
  return (
    <label
      className={cn(
        "flex cursor-pointer items-center gap-1.5 text-xs text-muted-foreground",
        disabled && "opacity-50",
      )}
    >
      <Switch checked={checked} disabled={disabled} onCheckedChange={onCheckedChange} />
      {label}
    </label>
  );
}

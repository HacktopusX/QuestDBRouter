import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import { useTerminal } from "@/providers/TerminalProvider";
import type { LayoutMode } from "@/types/terminal";

const LAYOUTS: { value: LayoutMode; label: string }[] = [
  { value: "4", label: "2×2" },
  { value: "6", label: "2×3" },
  { value: "9", label: "3×3" },
];

export function LayoutPicker() {
  const { settings, setLayout } = useTerminal();

  return (
    <div className="flex items-center gap-2">
      <span className="text-[0.6rem] uppercase tracking-widest text-muted-foreground">Grid</span>
      <ToggleGroup
        type="single"
        value={settings.layout}
        onValueChange={(v) => v && setLayout(v as LayoutMode)}
        size="sm"
      >
        {LAYOUTS.map(({ value, label }) => (
          <ToggleGroupItem
            key={value}
            value={value}
            className="min-w-10 px-2 font-mono text-[0.65rem] data-[state=on]:bg-primary/20 data-[state=on]:text-primary"
          >
            {label}
          </ToggleGroupItem>
        ))}
      </ToggleGroup>
    </div>
  );
}

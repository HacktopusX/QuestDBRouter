import type { ChartMode } from "@/lib/ilp/types";
import { cn, formatPrice } from "@/lib/utils";
import type { LegendValues } from "@/types/terminal";

interface ChartLegendProps {
  mode: ChartMode;
  values: LegendValues | null;
}

function LegendRow({
  label,
  value,
  decimals = 2,
}: {
  label: string;
  value: number;
  decimals?: number;
}) {
  return (
    <div className="flex gap-2 font-mono text-[0.6rem] tabular-nums">
      <span className="min-w-[0.75rem] text-muted-foreground/70">{label}</span>
      <span className="text-foreground/90">{formatPrice(value, decimals)}</span>
    </div>
  );
}

/** Minimal crosshair readout — no notification/tooltip chrome */
export function ChartLegend({ mode, values }: ChartLegendProps) {
  if (!values) return null;

  return (
    <div
      className={cn(
        "pointer-events-none absolute left-2 top-2 z-10 flex flex-col gap-px",
        "rounded border border-border/30 bg-background/70 px-2 py-1 backdrop-blur-[2px]",
      )}
    >
      <span className="mb-0.5 font-mono text-[0.55rem] text-muted-foreground/60">{values.time}</span>
      {mode === "ohlcv" ? (
        <>
          {values.open != null && <LegendRow label="O" value={values.open} />}
          {values.high != null && <LegendRow label="H" value={values.high} />}
          {values.low != null && <LegendRow label="L" value={values.low} />}
          {values.close != null && <LegendRow label="C" value={values.close} />}
          {values.volume != null && <LegendRow label="V" value={values.volume} decimals={0} />}
        </>
      ) : (
        values.price != null && <LegendRow label="Px" value={values.price} />
      )}
      {values.sma != null && <LegendRow label="∅" value={values.sma} />}
    </div>
  );
}

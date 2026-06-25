import { ChartGrid } from "@/components/terminal/chart-grid";
import { MarketWatchlist } from "@/components/terminal/market-watchlist";
import { StatsCards } from "@/components/terminal/stats-cards";
import { StreamPanel } from "@/components/terminal/stream-panel";

export function ChartsView() {
  return (
    <div className="flex min-h-0 flex-1">
      <MarketWatchlist />
      <div className="flex min-h-0 min-w-0 flex-1 flex-col">
        <ChartGrid />
        <StatsCards />
      </div>
      <StreamPanel />
    </div>
  );
}

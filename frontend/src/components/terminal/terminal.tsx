import { ChartsView } from "@/components/terminal/charts-view";
import { LivestreamView } from "@/components/terminal/livestream-view";
import { OverviewView } from "@/components/terminal/overview-view";
import { PanelStreamBridges } from "@/components/terminal/panel-stream-bridge";
import { Sidebar } from "@/components/terminal/sidebar";
import { TopBar } from "@/components/terminal/top-bar";
import { useTerminal } from "@/providers/TerminalProvider";

export function Terminal() {
  const { settings } = useTerminal();

  return (
    <div className="flex h-full bg-background">
      <PanelStreamBridges />
      <Sidebar />

      <div className="flex min-h-0 min-w-0 flex-1 flex-col">
        <TopBar />

        {settings.view === "overview" && <OverviewView />}
        {settings.view === "charts" && <ChartsView />}
        {settings.view === "stream" && <LivestreamView />}
      </div>
    </div>
  );
}

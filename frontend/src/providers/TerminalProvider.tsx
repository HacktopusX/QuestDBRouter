import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import {
  createDefaultPanels,
  createPanelId,
  DEFAULT_SETTINGS,
  DEFAULT_SYMBOLS,
  layoutCapacity,
  type LayoutMode,
  type PanelState,
  type TerminalSettings,
  type TerminalView,
} from "@/types/terminal";

interface TerminalContextValue {
  settings: TerminalSettings;
  panels: PanelState[];
  setView: (view: TerminalView) => void;
  setLayout: (layout: LayoutMode) => void;
  setMode: (mode: TerminalSettings["mode"]) => void;
  setFeatures: (features: Partial<TerminalSettings["features"]>) => void;
  focusPanel: (id: string) => void;
  updatePanelTopic: (id: string, topic: string) => void;
  updatePanelStats: (
    id: string,
    stats: Partial<Pick<PanelState, "price" | "change" | "tickCount" | "live">>,
  ) => void;
  bumpPanelTick: (id: string, price: number) => void;
  setPanelReplay: (id: string, tickCount: number, price: number | null) => void;
  removePanel: (id: string) => void;
  addPanel: () => void;
  fullscreenPanelId: string | null;
  expandPanel: (id: string) => void;
  collapsePanel: () => void;
}

const TerminalContext = createContext<TerminalContextValue | null>(null);

export function TerminalProvider({ children }: { children: ReactNode }) {
  const [settings, setSettings] = useState<TerminalSettings>(DEFAULT_SETTINGS);
  const [panels, setPanels] = useState<PanelState[]>(() =>
    createDefaultPanels(DEFAULT_SETTINGS.layout),
  );
  const [fullscreenPanelId, setFullscreenPanelId] = useState<string | null>(null);

  const setView = useCallback((view: TerminalView) => {
    setSettings((s) => ({ ...s, view }));
    setFullscreenPanelId(null);
  }, []);

  const setLayout = useCallback((layout: LayoutMode) => {
    setFullscreenPanelId(null);
    setSettings((s) => ({ ...s, layout }));
    setPanels((prev) => {
      const capacity = layoutCapacity(layout);
      let next = [...prev];
      while (next.length > capacity) next = next.slice(0, -1);
      while (next.length < capacity) {
        const i = next.length;
        next.push({
          id: createPanelId(),
          topic: DEFAULT_SYMBOLS[i] ?? `sym-${i + 1}`,
          focused: next.length === 0,
          price: null,
          change: null,
          tickCount: 0,
          live: false,
        });
      }
      if (!next.some((p) => p.focused) && next[0]) {
        next = next.map((p, idx) => ({ ...p, focused: idx === 0 }));
      }
      return next;
    });
  }, []);

  const setMode = useCallback((mode: TerminalSettings["mode"]) => {
    setSettings((s) => ({ ...s, mode }));
  }, []);

  const setFeatures = useCallback((features: Partial<TerminalSettings["features"]>) => {
    setSettings((s) => ({ ...s, features: { ...s.features, ...features } }));
  }, []);

  const focusPanel = useCallback((id: string) => {
    setPanels((prev) => prev.map((p) => ({ ...p, focused: p.id === id })));
  }, []);

  const expandPanel = useCallback((id: string) => {
    setPanels((prev) => prev.map((p) => ({ ...p, focused: p.id === id })));
    setFullscreenPanelId(id);
  }, []);

  const collapsePanel = useCallback(() => {
    setFullscreenPanelId(null);
  }, []);

  const updatePanelTopic = useCallback((id: string, topic: string) => {
    setPanels((prev) =>
      prev.map((p) =>
        p.id === id ? { ...p, topic, price: null, change: null, tickCount: 0, live: false } : p,
      ),
    );
  }, []);

  const bumpPanelTick = useCallback((id: string, price: number) => {
    setPanels((prev) =>
      prev.map((p) =>
        p.id === id ? { ...p, price, tickCount: p.tickCount + 1, live: true } : p,
      ),
    );
  }, []);

  const setPanelReplay = useCallback((id: string, tickCount: number, price: number | null) => {
    setPanels((prev) =>
      prev.map((p) =>
        p.id === id ? { ...p, tickCount, price: price ?? p.price, live: true } : p,
      ),
    );
  }, []);

  const updatePanelStats = useCallback(
    (id: string, stats: Partial<Pick<PanelState, "price" | "change" | "tickCount" | "live">>) => {
      setPanels((prev) => prev.map((p) => (p.id === id ? { ...p, ...stats } : p)));
    },
    [],
  );

  const removePanel = useCallback((id: string) => {
    setFullscreenPanelId((fs) => (fs === id ? null : fs));
    setPanels((prev) => {
      if (prev.length <= 1) return prev;
      const next = prev.filter((p) => p.id !== id);
      if (!next.some((p) => p.focused) && next[0]) {
        return next.map((p, idx) => ({ ...p, focused: idx === 0 }));
      }
      return next;
    });
  }, []);

  const addPanel = useCallback(() => {
    setPanels((prev) => {
      const capacity = layoutCapacity(settings.layout);
      if (prev.length >= capacity) return prev;
      const i = prev.length;
      return [
        ...prev,
        {
          id: createPanelId(),
          topic: DEFAULT_SYMBOLS[i] ?? `sym-${i + 1}`,
          focused: false,
          price: null,
          change: null,
          tickCount: 0,
          live: false,
        },
      ];
    });
  }, [settings.layout]);

  const value = useMemo(
    () => ({
      settings,
      panels,
      setView,
      setLayout,
      setMode,
      setFeatures,
      focusPanel,
      updatePanelTopic,
      updatePanelStats,
      bumpPanelTick,
      setPanelReplay,
      removePanel,
      addPanel,
      fullscreenPanelId,
      expandPanel,
      collapsePanel,
    }),
    [
      settings,
      panels,
      setView,
      setLayout,
      setMode,
      setFeatures,
      focusPanel,
      updatePanelTopic,
      updatePanelStats,
      bumpPanelTick,
      setPanelReplay,
      removePanel,
      addPanel,
      fullscreenPanelId,
      expandPanel,
      collapsePanel,
    ],
  );

  return <TerminalContext.Provider value={value}>{children}</TerminalContext.Provider>;
}

export function useTerminal() {
  const ctx = useContext(TerminalContext);
  if (!ctx) throw new Error("useTerminal must be used within TerminalProvider");
  return ctx;
}

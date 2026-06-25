import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { StreamClient } from "@/lib/stream";
import { lastPriceFromRow } from "@/lib/ilp/types";
import type { IlpRow } from "@/lib/ilp/types";
import type { StreamLogEntry } from "@/types/terminal";

const MAX_LOG = 300;

interface StreamContextValue {
  client: StreamClient;
  connected: boolean;
  statusDetail: string;
  tickLog: StreamLogEntry[];
  clearTickLog: () => void;
}

const StreamContext = createContext<StreamContextValue | null>(null);

function resolveWsUrl(): string {
  return (
    import.meta.env.VITE_STREAM_WS ??
    (import.meta.env.DEV
      ? `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws`
      : `${location.protocol === "https:" ? "wss" : "ws"}://${location.hostname}:8080/ws`)
  );
}

export function StreamProvider({ children }: { children: ReactNode }) {
  const [connected, setConnected] = useState(false);
  const [statusDetail, setStatusDetail] = useState("Disconnected");
  const [tickLog, setTickLog] = useState<StreamLogEntry[]>([]);
  const clientRef = useRef<StreamClient | null>(null);
  const logId = useRef(0);

  const appendLog = useCallback((topic: string, row: IlpRow) => {
    const price = lastPriceFromRow(row);
    const time = row.timestamp_ns ?? Date.now() * 1_000_000;
    setTickLog((prev) => {
      const entry: StreamLogEntry = {
        id: `tick-${++logId.current}`,
        topic,
        time,
        price,
        at: Date.now(),
      };
      return [entry, ...prev].slice(0, MAX_LOG);
    });
  }, []);

  if (!clientRef.current) {
    clientRef.current = new StreamClient({
      url: resolveWsUrl(),
      onStatus: (isConnected, detail) => {
        setConnected(isConnected);
        setStatusDetail(detail ?? (isConnected ? "Live" : "Disconnected"));
      },
      onGlobalRow: appendLog,
    });
  }

  const clearTickLog = useCallback(() => setTickLog([]), []);

  const value = useMemo(
    () => ({
      client: clientRef.current!,
      connected,
      statusDetail,
      tickLog,
      clearTickLog,
    }),
    [connected, statusDetail, tickLog, clearTickLog],
  );

  return <StreamContext.Provider value={value}>{children}</StreamContext.Provider>;
}

export function useStream() {
  const ctx = useContext(StreamContext);
  if (!ctx) throw new Error("useStream must be used within StreamProvider");
  return ctx;
}

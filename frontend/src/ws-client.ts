import { decode } from "@msgpack/msgpack";
import type { IlpRow, ReplayOut } from "./types";

export type RowHandler = (row: IlpRow) => void;
export type ReplayHandler = (payload: ReplayOut) => void;
export type StatusHandler = (connected: boolean, detail?: string) => void;

export interface StreamClientOptions {
  url: string;
  onRow: RowHandler;
  onReplay: ReplayHandler;
  onStatus: StatusHandler;
}

export class StreamClient {
  private ws: WebSocket | null = null;
  private topic = "";
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private shouldReconnect = false;

  constructor(private readonly opts: StreamClientOptions) {}

  connect(topic: string): void {
    this.disconnect(false);
    this.topic = topic;
    this.shouldReconnect = true;
    this.open();
  }

  disconnect(userInitiated = true): void {
    if (userInitiated) this.shouldReconnect = false;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    if (this.ws) {
      this.ws.onclose = null;
      this.ws.close();
      this.ws = null;
    }
    this.opts.onStatus(false);
  }

  private open(): void {
    this.opts.onStatus(false, "Connecting…");
    const ws = new WebSocket(this.opts.url);
    ws.binaryType = "arraybuffer";
    this.ws = ws;

    ws.onopen = () => {
      this.opts.onStatus(true);
      ws.send(JSON.stringify({ op: "subscribe", topics: [this.topic] }));
      ws.send(JSON.stringify({ op: "replay", topic: this.topic, last_n: 500 }));
    };

    ws.onmessage = (ev) => {
      if (!(ev.data instanceof ArrayBuffer)) return;
      const payload = decode(new Uint8Array(ev.data)) as IlpRow | ReplayOut;
      if (payload && typeof payload === "object" && "op" in payload && payload.op === "replay") {
        this.opts.onReplay(payload);
        return;
      }
      this.opts.onRow(payload as IlpRow);
    };

    ws.onerror = () => {
      this.opts.onStatus(false, "Connection error");
    };

    ws.onclose = () => {
      this.ws = null;
      this.opts.onStatus(false, "Disconnected");
      if (this.shouldReconnect) {
        this.reconnectTimer = setTimeout(() => this.open(), 2000);
      }
    };
  }
}

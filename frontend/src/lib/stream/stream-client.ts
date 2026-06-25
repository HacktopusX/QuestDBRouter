import { decode } from "@msgpack/msgpack";
import { deriveTopicFromRow, normalizeTopic } from "@/lib/stream/topic";
import type { IlpRow, ReplayOut } from "@/lib/ilp/types";

export type RowHandler = (row: IlpRow) => void;
export type ReplayHandler = (payload: ReplayOut) => void;
export type StatusHandler = (connected: boolean, detail?: string) => void;

export interface TopicHandlers {
  onRow: RowHandler;
  onReplay: ReplayHandler;
}

export type GlobalRowHandler = (topic: string, row: IlpRow) => void;

export interface StreamClientOptions {
  url: string;
  onStatus: StatusHandler;
  onGlobalRow?: GlobalRowHandler;
}

export class StreamClient {
  private ws: WebSocket | null = null;
  private readonly topics = new Map<string, TopicHandlers>();
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private shouldReconnect = false;

  constructor(private readonly opts: StreamClientOptions) {}

  get topicCount(): number {
    return this.topics.size;
  }

  addTopic(topic: string, handlers: TopicHandlers): void {
    const key = normalizeTopic(topic);
    if (!key) return;
    this.topics.set(key, handlers);
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.sendSubscribe(key);
      this.sendReplay(key);
    } else if (!this.ws) {
      this.shouldReconnect = true;
      this.open();
    }
  }

  updateTopic(oldTopic: string, newTopic: string, handlers: TopicHandlers): void {
    const oldKey = normalizeTopic(oldTopic);
    const newKey = normalizeTopic(newTopic);
    if (!newKey) return;

    if (oldKey && oldKey !== newKey) {
      this.topics.delete(oldKey);
    }
    this.topics.set(newKey, handlers);
    if (this.ws?.readyState === WebSocket.OPEN) {
      if (oldKey && oldKey !== newKey) {
        this.ws.send(JSON.stringify({ op: "unsubscribe", topics: [oldKey] }));
      }
      this.sendSubscribe(newKey);
      this.sendReplay(newKey);
    } else if (!this.ws) {
      this.shouldReconnect = true;
      this.open();
    }
  }

  removeTopic(topic: string, disconnectIfEmpty = true): void {
    const key = normalizeTopic(topic);
    if (!key) return;
    this.topics.delete(key);
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({ op: "unsubscribe", topics: [key] }));
    }
    if (disconnectIfEmpty && this.topics.size === 0) {
      this.disconnect(true);
    }
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
      for (const topic of this.topics.keys()) {
        this.sendSubscribe(topic);
        this.sendReplay(topic);
      }
      this.opts.onStatus(true, `Live · ${this.topics.size} symbols`);
    };

    ws.onmessage = (ev) => {
      if (!(ev.data instanceof ArrayBuffer)) return;
      const payload = decode(new Uint8Array(ev.data)) as IlpRow | ReplayOut;
      if (payload && typeof payload === "object" && "op" in payload && payload.op === "replay") {
        const handlers = this.topics.get(normalizeTopic(payload.topic));
        handlers?.onReplay(payload);
        return;
      }
      const row = payload as IlpRow;
      const topic = normalizeTopic(deriveTopicFromRow(row));
      this.opts.onGlobalRow?.(topic, row);
      this.topics.get(topic)?.onRow(row);
    };

    ws.onerror = () => {
      this.opts.onStatus(false, "Connection error");
    };

    ws.onclose = () => {
      this.ws = null;
      this.opts.onStatus(false, "Disconnected");
      if (this.shouldReconnect && this.topics.size > 0) {
        this.reconnectTimer = setTimeout(() => this.open(), 2000);
      }
    };
  }

  private sendSubscribe(topic: string): void {
    this.ws?.send(JSON.stringify({ op: "subscribe", topics: [topic] }));
  }

  private sendReplay(topic: string): void {
    this.ws?.send(JSON.stringify({ op: "replay", topic, last_n: 500 }));
  }
}

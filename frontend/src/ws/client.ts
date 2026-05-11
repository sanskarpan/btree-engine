// WebSocket client with typed event dispatch and exponential backoff reconnect.
import type { EngineEvent, EventType } from "../api/types";

type Handler = (evt: EngineEvent) => void;

export class WSClient {
  private ws: WebSocket | null = null;
  private handlers: Map<EventType | "*", Handler[]> = new Map();
  private backoff = 1000;
  private closed = false;
  private url: string;

  constructor(url = defaultWSURL()) {
    this.url = url;
    this.connect();
  }

  private connect() {
    if (this.closed) return;
    this.ws = new WebSocket(this.url);

    this.ws.onopen = () => {
      this.backoff = 1000; // reset on successful connection
    };

    this.ws.onmessage = (msg) => {
      try {
        const evt: EngineEvent = JSON.parse(msg.data);
        this.dispatch(evt);
      } catch {
        // ignore malformed messages
      }
    };

    this.ws.onclose = () => {
      if (!this.closed) {
        setTimeout(() => this.connect(), this.backoff);
        this.backoff = Math.min(this.backoff * 2, 30_000);
      }
    };

    this.ws.onerror = () => {
      this.ws?.close();
    };
  }

  private dispatch(evt: EngineEvent) {
    // typed handlers
    const typed = this.handlers.get(evt.Type) ?? [];
    for (const h of typed) h(evt);
    // wildcard handlers
    const wild = this.handlers.get("*") ?? [];
    for (const h of wild) h(evt);
  }

  /** Subscribe to a specific event type. Returns unsubscribe function. */
  on(type: EventType, handler: Handler): () => void {
    const list = this.handlers.get(type) ?? [];
    list.push(handler);
    this.handlers.set(type, list);
    return () => {
      const updated = (this.handlers.get(type) ?? []).filter((h) => h !== handler);
      this.handlers.set(type, updated);
    };
  }

  /** Subscribe to all events. Returns unsubscribe function. */
  onAll(handler: Handler): () => void {
    return this.on("*" as EventType, handler);
  }

  close() {
    this.closed = true;
    this.ws?.close();
  }
}

// Singleton instance
let _client: WSClient | null = null;
export function getWSClient(): WSClient {
  if (!_client) _client = new WSClient();
  return _client;
}

function defaultWSURL(): string {
  if (typeof window === "undefined") {
    return "ws://localhost:3001/ws";
  }
  const url = new URL("/ws", window.location.origin);
  url.protocol = url.protocol === "https:" ? "wss:" : "ws:";
  return url.toString();
}

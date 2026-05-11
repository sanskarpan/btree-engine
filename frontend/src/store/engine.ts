// Reactive engine state store driven by WebSocket events.
import type { TreeNode, EngineEvent, BufferStats, WALRecord, FrameInfo } from "../api/types";
import { getWSClient } from "../ws/client";

export interface EngineState {
  treeRoot: TreeNode | null;
  bufferStats: BufferStats;
  walRecords: WALRecord[];
  frames: FrameInfo[];
  recentEvents: EngineEvent[];
  activeTxns: Set<number>;
  splitCount: number;
  mergeCount: number;
}

type Listener = (state: EngineState) => void;

class EngineStore {
  private state: EngineState = {
    treeRoot: null,
    bufferStats: { buffer_hit_rate: 0, dirty_pages: 0, total_frames: 0, active_txns: 0 },
    walRecords: [],
    frames: [],
    recentEvents: [],
    activeTxns: new Set(),
    splitCount: 0,
    mergeCount: 0,
  };

  private listeners: Listener[] = [];
  private ws = getWSClient();

  constructor() {
    this.ws.onAll((evt) => this.handleEvent(evt));
  }

  private handleEvent(evt: EngineEvent) {
    const s = { ...this.state };

    // Track recent events (ring buffer of last 100)
    s.recentEvents = [evt, ...s.recentEvents].slice(0, 100);

    switch (evt.Type) {
      case "txn_begin":
        s.activeTxns = new Set(s.activeTxns).add(evt.Extra.txn_id as number);
        break;
      case "txn_commit":
      case "txn_abort": {
        const next = new Set(s.activeTxns);
        next.delete(evt.Extra.txn_id as number);
        s.activeTxns = next;
        break;
      }
      case "tree_split":
        s.splitCount++;
        break;
      case "tree_merge":
        s.mergeCount++;
        break;
      case "buffer_hit":
      case "buffer_miss":
        s.bufferStats = {
          ...s.bufferStats,
          active_txns: s.activeTxns.size,
        };
        break;
      case "wal_append":
        // New WAL record — keep last 200
        s.walRecords = [
          {
            lsn: evt.Extra.lsn as number,
            prev_lsn: 0,
            txn_id: evt.Extra.txn_id as number,
            page_id: 0,
            type: String(evt.Extra.type) as any,
          },
          ...s.walRecords,
        ].slice(0, 200);
        break;
    }

    this.state = s;
    this.notify();
  }

  private notify() {
    for (const l of this.listeners) l(this.state);
  }

  getState(): EngineState {
    return this.state;
  }

  subscribe(listener: Listener): () => void {
    this.listeners.push(listener);
    return () => {
      this.listeners = this.listeners.filter((l) => l !== listener);
    };
  }

  updateTree(root: TreeNode) {
    this.state = { ...this.state, treeRoot: root };
    this.notify();
  }

  updateBufferStats(stats: BufferStats) {
    this.state = { ...this.state, bufferStats: stats };
    this.notify();
  }

  updateFrames(frames: FrameInfo[]) {
    this.state = { ...this.state, frames };
    this.notify();
  }
}

export const engineStore = new EngineStore();

// TypeScript interfaces for all B+Tree engine API types.

// -------- Engine Events (32 types) --------

export type EventType =
  | "page_read" | "page_write" | "page_evict" | "page_dirty"
  | "page_split" | "page_merge" | "page_alloc" | "page_free" | "page_compact"
  | "tree_insert" | "tree_search" | "tree_delete" | "tree_split" | "tree_merge"
  | "txn_begin" | "txn_commit" | "txn_abort"
  | "tuple_insert" | "tuple_delete" | "tuple_visible" | "tuple_hidden"
  | "snapshot_taken" | "write_skew"
  | "wal_append" | "wal_flush" | "wal_checkpoint"
  | "recovery_start" | "recovery_analysis" | "recovery_redo" | "recovery_undo" | "recovery_done"
  | "buffer_hit" | "buffer_miss"
  | "vacuum_start" | "vacuum_tuple" | "vacuum_page" | "vacuum_done";

export interface EngineEvent {
  Type: EventType;
  Extra: Record<string, unknown>;
}

// -------- Page --------

export interface PageHeader {
  magic: number;
  type: "internal" | "leaf" | "meta" | "free";
  num_slots: number;
  free_start: number;
  free_end: number;
  page_lsn: number;
  flags: number;
  prev_sib: number;
  next_sib: number;
  checksum: number;
}

export interface SlotInfo {
  slot_idx: number;
  xmin: number;
  xmax: number;
  key: string;
  value: string;
  xmin_status: "active" | "committed" | "aborted";
  xmax_status: "" | "active" | "committed" | "aborted";
}

export interface PageContents {
  page_id: number;
  type: "internal" | "leaf" | "meta" | "free";
  num_slots: number;
  free_space: number;
  page_lsn: number;
  prev_sib: number;
  next_sib: number;
  slots: SlotInfo[];
}

// -------- Tree --------

export interface TreeNode {
  page_id: number;
  type: "internal" | "leaf";
  num_slots: number;
  fill_pct: number;
  prev_sib?: number;
  next_sib?: number;
  keys?: string[];
  children?: TreeNode[];
}

// -------- MVCC / Transaction --------

export type TxnStatus = "active" | "committed" | "aborted";

export interface Snapshot {
  xmin_snapshot: number;
  xmax_snapshot: number;
  xip_list: number[];
  creator_txn_id: number;
}

export interface Transaction {
  id: number;
  status: TxnStatus;
  iso_level: "snapshot" | "read_committed";
  snapshot: Snapshot;
  started_at: string;
}

export interface MVCCTuple {
  xmin: number;
  xmax: number;
  key: string;
  value: string;
  flags: number;
}

export interface VersionInfo {
  xmin: number;
  xmax: number;
  value: string;
  xmin_status: TxnStatus;
  xmax_status: "" | TxnStatus;
}

// -------- Buffer Pool --------

export interface FrameInfo {
  page_id: number;
  dirty: boolean;
  pin_count: number;
  rec_lsn: number;
  empty: boolean;
}

export interface BufferStats {
  buffer_hit_rate: number;
  dirty_pages: number;
  total_frames: number;
  active_txns: number;
}

// -------- WAL --------

export type WALRecordType =
  | "BEGIN" | "COMMIT" | "ABORT"
  | "INSERT" | "DELETE" | "UPDATE"
  | "SPLIT" | "MERGE" | "CLR"
  | "CHECKPOINT" | "PAGE_ALLOC" | "PAGE_FREE" | "ROOT_CHANGE";

export interface WALRecord {
  lsn: number;
  prev_lsn: number;
  txn_id: number;
  page_id: number;
  type: WALRecordType;
}

// -------- Engine Stats --------

export interface EngineStats {
  buffer_hit_rate: number;
  dirty_pages: number;
  total_frames: number;
  active_txns: number;
}

// -------- Scenarios --------

export interface StepResult {
  step: string;
  description: string;
  ok: boolean;
  detail?: string;
}

export interface ScenarioResult {
  name: string;
  steps: StepResult[];
  stats: Record<string, unknown>;
}

// -------- API Responses --------

export interface TxnBeginResponse {
  txn_id: number;
  iso_level: string;
}

export interface GetResponse {
  key: string;
  value: string;
}

export interface ScanResponse {
  results: Array<{ key: string; value: string }>;
  count: number;
}

export interface MVCCVersionsResponse {
  key: string;
  versions: VersionInfo[];
}

export interface WALTailResponse {
  records: WALRecord[];
  count: number;
}

export interface BufferFramesResponse {
  frames: FrameInfo[];
  count: number;
}

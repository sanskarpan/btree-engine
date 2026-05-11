// API client for the B+Tree engine gateway (via BFF).
import type {
  EngineStats,
  TreeNode,
  PageContents,
  VersionInfo,
  BufferStats,
  FrameInfo,
  WALRecord,
  TxnBeginResponse,
  GetResponse,
  ScanResponse,
  ScenarioResult,
} from "./types";

const BASE =
  typeof window !== "undefined"
    ? `${window.location.origin}/api/v1`
    : "http://localhost:3001/api/v1";

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) throw new Error(`GET ${path}: ${res.status}`);
  return res.json();
}

async function post<T>(path: string, body?: unknown): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) throw new Error(`POST ${path}: ${res.status}`);
  return res.json();
}

async function del<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`, { method: "DELETE" });
  if (!res.ok) throw new Error(`DELETE ${path}: ${res.status}`);
  return res.json();
}

// -------- Engine --------
export const engine = {
  stats: () => get<EngineStats>("/engine/stats"),
  crash: () => post<{ status: string }>("/engine/crash"),
  recover: () => post<{ status: string }>("/engine/recover"),
};

// -------- Transactions --------
export const txn = {
  begin: (isoLevel: "snapshot" | "read_committed" = "snapshot") =>
    post<TxnBeginResponse>("/txn/begin", { iso_level: isoLevel }),
  commit: (id: number) => post<{ status: string }>(`/txn/${id}/commit`),
  abort: (id: number) => post<{ status: string }>(`/txn/${id}/abort`),
  put: (id: number, key: string, value: string) =>
    post<{ status: string }>(`/txn/${id}/put`, { key, value }),
  get: (id: number, key: string) =>
    get<GetResponse>(`/txn/${id}/get?key=${encodeURIComponent(key)}`),
  delete: (id: number, key: string) =>
    del<{ status: string }>(`/txn/${id}/delete?key=${encodeURIComponent(key)}`),
  scan: (id: number, start?: string, end?: string) => {
    const params = new URLSearchParams();
    if (start) params.set("start", start);
    if (end) params.set("end", end);
    return get<ScanResponse>(`/txn/${id}/scan?${params}`);
  },
};

// -------- Tree Inspection --------
export const tree = {
  structure: () => get<TreeNode>("/tree/structure"),
  page: (id: number) => get<PageContents>(`/tree/page/${id}`),
};

// -------- MVCC Inspection --------
export const mvcc = {
  versions: (key: string) =>
    get<{ key: string; versions: VersionInfo[] }>(
      `/mvcc/versions?key=${encodeURIComponent(key)}`
    ),
  visibility: (key: string, txnId: number) =>
    get<unknown>(
      `/mvcc/visibility?key=${encodeURIComponent(key)}&txnID=${txnId}`
    ),
};

// -------- Buffer Pool --------
export const buffer = {
  stats: () => get<BufferStats>("/buffer/stats"),
  frames: () => get<{ frames: FrameInfo[]; count: number }>("/buffer/frames"),
};

// -------- WAL --------
export const wal = {
  tail: (n = 50) =>
    get<{ records: WALRecord[]; count: number }>(`/wal/tail?n=${n}`),
  checkpoint: () => post<{ status: string }>("/wal/checkpoint"),
};

// -------- Scenarios --------
export const scenarios = {
  list: () => get<{ scenarios: string[] }>("/scenarios"),
  run: (name: string) => post<ScenarioResult>(`/scenarios/${name}/run`),
};

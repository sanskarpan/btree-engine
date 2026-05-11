// BFF (Backend For Frontend) — Elysia proxy + WebSocket fan-out for the B+Tree engine.
import { Elysia } from "elysia";
import { dirname, extname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { existsSync, readFileSync, statSync } from "node:fs";

const BACKEND = process.env.BACKEND_URL ?? "http://localhost:8080";
const BFF_PORT = Number(process.env.BFF_PORT ?? "3001");
const BACKEND_API_KEY = process.env.BACKEND_API_KEY?.trim() ?? "";
const SERVER_DIR = dirname(fileURLToPath(import.meta.url));
const FRONTEND_ROOT = join(SERVER_DIR, "..");
const INDEX_HTML = join(FRONTEND_ROOT, "index.html");
const DIST_DIR = join(FRONTEND_ROOT, "dist");
const DIST_ROOT = resolve(DIST_DIR);

function serveStatic(path: string, contentType: string): Response {
  return new Response(readFileSync(path), {
    headers: { "content-type": contentType },
  });
}

function contentTypeFor(path: string): string {
  switch (extname(path)) {
    case ".css":
      return "text/css; charset=utf-8";
    case ".html":
      return "text/html; charset=utf-8";
    case ".json":
      return "application/json; charset=utf-8";
    case ".svg":
      return "image/svg+xml";
    case ".js":
      return "text/javascript; charset=utf-8";
    default:
      return "application/octet-stream";
  }
}

export function resolveDistPath(requestPath: string): string | null {
  const normalized = requestPath.replace(/^\/+/, "");
  const candidate = resolve(DIST_ROOT, normalized);
  if (!candidate.startsWith(`${DIST_ROOT}/`) && candidate !== DIST_ROOT) {
    return null;
  }
  if (!existsSync(candidate)) {
    return null;
  }
  if (!statSync(candidate).isFile()) {
    return null;
  }
  return candidate;
}

function proxyURL(path: string, requestURL: string): string {
  const reqURL = new URL(requestURL);
  const target = new URL(`/api/${path}`, BACKEND);
  target.search = reqURL.search;
  return target.toString();
}

function backendWSURL(): string {
  const target = new URL(BACKEND);
  target.protocol = target.protocol === "https:" ? "wss:" : "ws:";
  target.pathname = "/ws";
  target.search = "";
  return target.toString();
}

function backendAuthHeader(incomingAuth?: string | null): string | undefined {
  if (incomingAuth?.trim()) {
    return incomingAuth;
  }
  if (!BACKEND_API_KEY) {
    return undefined;
  }
  return BACKEND_API_KEY.startsWith("Bearer ")
    ? BACKEND_API_KEY
    : `Bearer ${BACKEND_API_KEY}`;
}

function proxyHeaders(request: Request): Headers {
  const headers = new Headers();
  const auth = backendAuthHeader(request.headers.get("authorization"));
  if (auth) {
    headers.set("authorization", auth);
  }
  const accept = request.headers.get("accept");
  if (accept) {
    headers.set("accept", accept);
  }
  return headers;
}

function headersToObject(headers: Headers): Record<string, string> {
  const out: Record<string, string> = {};
  headers.forEach((value, key) => {
    out[key] = value;
  });
  return out;
}

type BackendWebSocket = new (
  url: string,
  init?: { headers?: HeadersInit },
) => WebSocket;

const BackendWS = WebSocket as unknown as BackendWebSocket;

type BrowserSocket = {
  close: () => void;
  send: (data: string | ArrayBufferLike | Blob | ArrayBufferView) => void;
};

type BackendSocket = {
  close: () => void;
  onclose: null | ((event: CloseEvent) => void);
  onerror: null | ((event: Event) => void);
  onmessage: null | ((event: MessageEvent) => void);
};

export function wireBackendWebSocket(browser: BrowserSocket, backend: BackendSocket) {
  backend.onclose = (_event) => {
    try {
      browser.close();
    } catch {
      // browser client already gone
    }
  };
  backend.onmessage = (message) => {
    try {
      browser.send(message.data);
    } catch {
      // browser client disconnected
    }
  };
  backend.onerror = (_event) => backend.close();
}

export const app = new Elysia()
  .get("/", () => serveStatic(INDEX_HTML, "text/html; charset=utf-8"))
  .get("/index.html", () => serveStatic(INDEX_HTML, "text/html; charset=utf-8"))
  .get("/dist/*", ({ params }) => {
    const path = (params as Record<string, string>)["*"] ?? "";
    const resolvedPath = resolveDistPath(path);
    if (!resolvedPath) {
      return new Response("not found", { status: 404 });
    }
    return serveStatic(resolvedPath, contentTypeFor(resolvedPath));
  })
  // Proxy all GET /api/* requests to the backend
  .get("/api/*", ({ params, request }) => {
    const path = (params as Record<string, string>)["*"] ?? "";
    return fetch(proxyURL(path, request.url), {
      method: "GET",
      headers: proxyHeaders(request),
    });
  })
  // Proxy all PUT /api/* requests
  .put("/api/*", ({ params, body, request }) => {
    const path = (params as Record<string, string>)["*"] ?? "";
    return fetch(proxyURL(path, request.url), {
      method: "PUT",
      headers: {
        ...headersToObject(proxyHeaders(request)),
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    });
  })
  // Proxy all POST /api/* requests
  .post("/api/*", ({ params, body, request }) => {
    const path = (params as Record<string, string>)["*"] ?? "";
    return fetch(proxyURL(path, request.url), {
      method: "POST",
      headers: {
        ...headersToObject(proxyHeaders(request)),
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    });
  })
  // Proxy all PATCH /api/* requests
  .patch("/api/*", ({ params, body, request }) => {
    const path = (params as Record<string, string>)["*"] ?? "";
    return fetch(proxyURL(path, request.url), {
      method: "PATCH",
      headers: {
        ...headersToObject(proxyHeaders(request)),
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    });
  })
  // Proxy all DELETE /api/* requests
  .delete("/api/*", ({ params, request }) => {
    const path = (params as Record<string, string>)["*"] ?? "";
    return fetch(proxyURL(path, request.url), {
      method: "DELETE",
      headers: proxyHeaders(request),
    });
  })
  // Proxy all OPTIONS /api/* requests
  .options("/api/*", ({ params, request }) => {
    const path = (params as Record<string, string>)["*"] ?? "";
    return fetch(proxyURL(path, request.url), {
      method: "OPTIONS",
      headers: proxyHeaders(request),
    });
  })
  // Health check
  .get("/health", async () => {
    let backendStatus = "down";
    try {
      const resp = await fetch(new URL("/health", BACKEND));
      backendStatus = resp.ok ? "ok" : "degraded";
    } catch {
      backendStatus = "down";
    }
    return { status: "ok", backend: BACKEND, backend_status: backendStatus };
  })
  // WebSocket fan-out: connect to backend WS and forward all messages to browser clients
  .ws("/ws", {
    open(ws) {
      const auth = backendAuthHeader(ws.data.request.headers.get("authorization"));
      const bk = new BackendWS(backendWSURL(), {
        headers: auth ? { authorization: auth } : undefined,
      });
      wireBackendWebSocket(ws, bk);
      (ws as { data: { bk?: WebSocket } }).data.bk = bk;
    },
    close(ws) {
      (ws as { data: { bk?: WebSocket } }).data.bk?.close();
    },
  });

export type App = typeof app;

if (import.meta.main) {
  app.listen(BFF_PORT);
  console.log(`BFF listening on http://localhost:${BFF_PORT} -> ${BACKEND}`);
}

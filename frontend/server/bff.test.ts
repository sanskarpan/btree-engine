import { describe, expect, it } from "bun:test";

import { resolveDistPath, wireBackendWebSocket } from "./bff";

describe("resolveDistPath", () => {
  it("resolves bundled assets inside dist", () => {
    const resolved = resolveDistPath("main.js");
    expect(resolved).not.toBeNull();
    expect(resolved?.endsWith("/frontend/dist/main.js")).toBe(true);
  });

  it("rejects traversal outside dist", () => {
    expect(resolveDistPath("../index.html")).toBeNull();
    expect(resolveDistPath("../../README.md")).toBeNull();
  });

  it("returns null for missing assets", () => {
    expect(resolveDistPath("missing.js")).toBeNull();
  });

  it("rejects directory targets", () => {
    expect(resolveDistPath("")).toBeNull();
  });
});

describe("wireBackendWebSocket", () => {
  it("closes the browser socket when the backend socket closes", () => {
    let browserClosed = false;
    const browser = {
      close: () => {
        browserClosed = true;
      },
      send: () => {},
    };
    const backend = {
      close: () => {},
      onclose: null as null | (() => void),
      onerror: null as null | (() => void),
      onmessage: null as null | ((event: MessageEvent) => void),
    };

    wireBackendWebSocket(browser, backend);
    backend.onclose?.();

    expect(browserClosed).toBe(true);
  });

  it("forwards backend messages to the browser socket", () => {
    const forwarded: string[] = [];
    const browser = {
      close: () => {},
      send: (data: string | ArrayBufferLike | Blob | ArrayBufferView) => {
        forwarded.push(String(data));
      },
    };
    const backend = {
      close: () => {},
      onclose: null as null | (() => void),
      onerror: null as null | (() => void),
      onmessage: null as null | ((event: MessageEvent) => void),
    };

    wireBackendWebSocket(browser, backend);
    backend.onmessage?.({ data: "evt" } as MessageEvent);

    expect(forwarded).toEqual(["evt"]);
  });
});

// Panel 6: WAL + Recovery Visualizer — scrolling log + crash/recover buttons.
import * as d3 from "d3";
import type { WALRecord } from "../../api/types";
import { wal as walApi, engine as engineApi } from "../../api/client";
import { getWSClient } from "../../ws/client";

const TYPE_COLOR: Record<string, string> = {
  BEGIN: "#42a5f5",
  INSERT: "#66bb6a",
  DELETE: "#ef5350",
  COMMIT: "#26c6da",
  ABORT: "#ffa726",
  CLR: "#ab47bc",
  SPLIT: "#ff7043",
  MERGE: "#8d6e63",
  CHECKPOINT: "#78909c",
  ROOT_CHANGE: "#ec407a",
  UPDATE: "#9ccc65",
  PAGE_ALLOC: "#26a69a",
  PAGE_FREE: "#78909c",
};

export class WALPanel {
  private container: d3.Selection<HTMLElement, unknown, null, undefined>;
  private records: WALRecord[] = [];

  constructor(el: HTMLElement) {
    this.container = d3.select(el);
    this.buildUI();

    const ws = getWSClient();
    ws.on("wal_append", () => this.refresh());
    ws.on("recovery_done", () => this.refresh());

    this.refresh();
  }

  private buildUI() {
    this.container.html(`
      <div style="margin-bottom:8px;display:flex;gap:8px;align-items:center">
        <button id="btn-crash" style="background:#e53935;color:white;border:none;padding:6px 12px;border-radius:4px;cursor:pointer">Simulate Crash</button>
        <button id="btn-recover" style="background:#4caf50;color:white;border:none;padding:6px 12px;border-radius:4px;cursor:pointer">ARIES Recover</button>
        <button id="btn-checkpoint" style="background:#78909c;color:white;border:none;padding:6px 12px;border-radius:4px;cursor:pointer">Checkpoint</button>
        <span id="recovery-status" style="font-size:12px;color:#666"></span>
      </div>
      <div id="wal-log" style="font-family:monospace;font-size:11px;max-height:400px;overflow-y:auto;border:1px solid #ddd;padding:4px"></div>
    `);

    this.container.select("#btn-crash").on("click", async () => {
      const status = this.container.select("#recovery-status");
      status.text("Crashing...");
      await engineApi.crash().catch(() => {});
      status.text("Crashed. Click Recover to run ARIES.");
    });

    this.container.select("#btn-recover").on("click", async () => {
      const status = this.container.select("#recovery-status");
      status.text("Running ARIES recovery...");
      await engineApi.recover().catch(() => {});
      status.text("Recovery complete.");
      this.refresh();
    });

    this.container.select("#btn-checkpoint").on("click", async () => {
      await walApi.checkpoint().catch(() => {});
      this.refresh();
    });
  }

  async refresh() {
    try {
      const { records } = await walApi.tail(100);
      this.records = records;
      this.renderLog();
    } catch {
      // engine may be unavailable
    }
  }

  private renderLog() {
    const el = this.container.select("#wal-log");
    el.html("");

    if (!this.records?.length) {
      el.html("<em>No WAL records.</em>");
      return;
    }

    this.records.forEach((r) => {
      const color = TYPE_COLOR[r.type] ?? "#333";
      el.append("div")
        .style("padding", "2px 4px")
        .style("border-left", `4px solid ${color}`)
        .style("margin", "1px 0")
        .html(
          `<span style="color:${color};font-weight:bold">${r.type}</span> LSN=${r.lsn} TxnID=${r.txn_id} PageID=${r.page_id} PrevLSN=${r.prev_lsn}`
        );
    });
  }
}

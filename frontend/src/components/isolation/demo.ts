// Panel 4: Isolation Level & Anomaly Demo — RC vs SI split screen.
import type { StepResult } from "../../api/types";
import { scenarios as scenariosApi } from "../../api/client";

export class IsolationDemo {
  private container: HTMLElement;

  constructor(el: HTMLElement) {
    this.container = el;
    this.buildUI();
  }

  private buildUI() {
    this.container.innerHTML = `
      <div style="display:flex;gap:24px;flex-wrap:wrap">
        <div style="flex:1;min-width:300px">
          <h3>Read Committed</h3>
          <button id="run-rc">Run RC Scenario</button>
          <div id="rc-steps" style="margin-top:8px;font-size:12px"></div>
        </div>
        <div style="flex:1;min-width:300px">
          <h3>Snapshot Isolation</h3>
          <button id="run-si">Run SI + Write Skew</button>
          <div id="si-steps" style="margin-top:8px;font-size:12px"></div>
        </div>
      </div>
    `;

    this.container.querySelector("#run-rc")?.addEventListener("click", async () => {
      const result = await scenariosApi.run("read-committed").catch((e) => ({ steps: [{ step: "error", description: String(e), ok: false }], name: "error", stats: {} }));
      this.renderSteps("#rc-steps", result.steps);
    });

    this.container.querySelector("#run-si")?.addEventListener("click", async () => {
      const result = await scenariosApi.run("write-skew").catch((e) => ({ steps: [{ step: "error", description: String(e), ok: false }], name: "error", stats: {} }));
      this.renderSteps("#si-steps", result.steps);
      const ws = (result.stats as Record<string, unknown>)?.["write_skew_occurred"];
      if (ws) {
        const banner = document.createElement("div");
        banner.style.cssText = "background:#e53935;color:white;padding:8px;border-radius:4px;margin-top:8px;font-weight:bold";
        banner.textContent = "⚠ WRITE SKEW DETECTED — Snapshot Isolation allowed both doctors to clock out!";
        this.container.querySelector("#si-steps")?.appendChild(banner);
      }
    });
  }

  private renderSteps(selector: string, steps: StepResult[]) {
    const el = this.container.querySelector(selector);
    if (!el) return;
    el.innerHTML = steps
      .map(
        (s) =>
          `<div style="padding:4px;border-left:3px solid ${s.ok ? "#4caf50" : "#e53935"};margin:2px 0;background:${s.ok ? "#f1f8e9" : "#ffebee"}">
            <strong>${s.step}</strong>: ${s.description}
            ${s.detail ? `<br><em style="color:#666">${s.detail}</em>` : ""}
          </div>`
      )
      .join("");
  }
}

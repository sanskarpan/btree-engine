// Panel 7: Scenarios & Metrics — run pre-built scenarios and view live metrics.
import * as d3 from "d3";
import type { ScenarioResult } from "../../api/types";
import { scenarios as scenariosApi, engine as engineApi, buffer as bufferApi } from "../../api/client";
import { engineStore } from "../../store/engine";
import { getWSClient } from "../../ws/client";

export class ScenariosPanel {
  private container: HTMLElement;
  private metricsTimer: ReturnType<typeof setInterval> | null = null;
  private metricsEl: HTMLElement | null = null;

  constructor(el: HTMLElement) {
    this.container = el;
    this.buildUI();
    this.startMetrics();
  }

  destroy() {
    if (this.metricsTimer) clearInterval(this.metricsTimer);
  }

  private async buildUI() {
    // Load scenario list
    let scenarioList: string[] = [];
    try {
      const resp = await scenariosApi.list();
      scenarioList = resp.scenarios;
    } catch {
      scenarioList = ["insert-and-search", "mvcc-basic", "write-skew", "crash-recovery"];
    }

    this.container.innerHTML = `
      <div style="display:flex;gap:24px;flex-wrap:wrap">
        <div style="flex:1;min-width:300px">
          <h3>Run Scenario</h3>
          <select id="scenario-select" style="width:200px;margin-right:8px">
            ${scenarioList.map((n) => `<option value="${n}">${n}</option>`).join("")}
          </select>
          <button id="run-scenario" style="background:#1976d2;color:white;border:none;padding:6px 12px;border-radius:4px;cursor:pointer">Run</button>
          <div id="scenario-output" style="margin-top:12px;font-size:12px;max-height:300px;overflow-y:auto"></div>
        </div>
        <div style="flex:1;min-width:280px">
          <h3>Live Metrics</h3>
          <div id="metrics-panel" style="font-size:12px"></div>
        </div>
      </div>
      <div id="metrics-charts" style="margin-top:16px"></div>
    `;

    this.metricsEl = this.container.querySelector("#metrics-panel");

    this.container.querySelector("#run-scenario")?.addEventListener("click", async () => {
      const select = this.container.querySelector("#scenario-select") as HTMLSelectElement;
      const name = select.value;
      const output = this.container.querySelector("#scenario-output")!;
      output.innerHTML = `<em>Running ${name}...</em>`;
      try {
        const result = await scenariosApi.run(name);
        this.renderResult(result);
      } catch (e) {
        output.innerHTML = `<em style="color:red">Error: ${e}</em>`;
      }
    });

    // Live event counter chart
    this.buildEventChart();
  }

  private renderResult(result: ScenarioResult) {
    const output = this.container.querySelector("#scenario-output")!;
    output.innerHTML = result.steps
      .map(
        (s) =>
          `<div style="padding:3px;border-left:3px solid ${s.ok ? "#4caf50" : "#e53935"};margin:2px 0;background:${s.ok ? "#f1f8e9" : "#ffebee"}">
            <b>${s.step}</b>: ${s.description}
          </div>`
      )
      .join("");

    if (Object.keys(result.stats).length > 0) {
      output.innerHTML +=
        `<div style="margin-top:8px;background:#f5f5f5;padding:6px;border-radius:4px"><b>Stats:</b><pre style="margin:0;font-size:11px">${JSON.stringify(result.stats, null, 2)}</pre></div>`;
    }
  }

  private startMetrics() {
    const update = async () => {
      try {
        const [stats, bufStats] = await Promise.all([engineApi.stats(), bufferApi.stats()]);
        engineStore.updateBufferStats(bufStats);
        if (this.metricsEl) {
          const state = engineStore.getState();
          this.metricsEl.innerHTML = `
            <table style="border-collapse:collapse;width:100%">
              <tr><td style="padding:3px 8px"><b>Buffer Hit Rate</b></td><td>${(stats.buffer_hit_rate * 100).toFixed(1)}%</td></tr>
              <tr><td style="padding:3px 8px"><b>Dirty Pages</b></td><td>${stats.dirty_pages}</td></tr>
              <tr><td style="padding:3px 8px"><b>Total Frames</b></td><td>${stats.total_frames}</td></tr>
              <tr><td style="padding:3px 8px"><b>Active Txns</b></td><td>${state.activeTxns.size}</td></tr>
              <tr><td style="padding:3px 8px"><b>Tree Splits</b></td><td>${state.splitCount}</td></tr>
              <tr><td style="padding:3px 8px"><b>Tree Merges</b></td><td>${state.mergeCount}</td></tr>
              <tr><td style="padding:3px 8px"><b>Recent Events</b></td><td>${state.recentEvents.length}</td></tr>
            </table>
          `;
        }
      } catch {
        // engine unavailable
      }
    };
    update();
    this.metricsTimer = setInterval(update, 2000);
  }

  private buildEventChart() {
    const chartEl = this.container.querySelector("#metrics-charts") as HTMLElement;
    if (!chartEl) return;

    const counts: Record<string, number> = {};
    const ws = getWSClient();
    ws.onAll((evt) => {
      counts[evt.Type] = (counts[evt.Type] ?? 0) + 1;
      if (Object.keys(counts).length > 0) redraw();
    });

    const svg = d3.select(chartEl).append("svg").attr("width", 700).attr("height", 120);
    svg.append("text").attr("x", 10).attr("y", 15).attr("font-size", "12px").attr("fill", "#555").text("Event counts (live)");

    const g = svg.append("g").attr("transform", "translate(10,25)");

    function redraw() {
      const entries = Object.entries(counts).sort((a, b) => b[1] - a[1]).slice(0, 12);
      if (!entries.length) return;
      const max = Math.max(...entries.map((e) => e[1]));
      const barH = 12;
      const w = 600;

      g.selectAll<SVGGElement, [string, number]>(".bar-g")
        .data(entries, ([k]) => k)
        .join(
          (enter) => {
            const bg = enter.append("g").attr("class", "bar-g").attr("transform", (_d, i) => `translate(0,${i * (barH + 4)})`);
            bg.append("rect").attr("height", barH).attr("rx", 2).attr("fill", "#42a5f5");
            bg.append("text").attr("x", 4).attr("y", barH - 2).attr("font-size", "9px").attr("fill", "white");
            bg.append("text").attr("class", "label").attr("x", -2).attr("text-anchor", "end").attr("y", barH - 2).attr("font-size", "9px").attr("fill", "#555");
            return bg;
          },
          (update) => update.attr("transform", (_d, i) => `translate(0,${i * (barH + 4)})`)
        )
        .each(function ([k, v]) {
          const barW = (v / max) * w;
          d3.select(this).select("rect").attr("width", barW);
          d3.select(this).select("text").text(`${k}: ${v}`);
          d3.select(this).select(".label").text(k);
        });
    }
  }
}

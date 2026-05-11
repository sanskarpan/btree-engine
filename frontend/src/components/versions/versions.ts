// Panel 3: MVCC Version Chain Viewer — timeline of all versions for a key.
import * as d3 from "d3";
import type { VersionInfo } from "../../api/types";
import { mvcc as mvccApi } from "../../api/client";

const COLORS = {
  live: "#4caf50",
  dead: "#e53935",
  active: "#ff9800",
  aborted: "#9e9e9e",
};

export class VersionChainViewer {
  private container: d3.Selection<HTMLElement, unknown, null, undefined>;

  constructor(el: HTMLElement) {
    this.container = d3.select(el);
    this.buildUI();
  }

  private buildUI() {
    this.container.html("");

    const controls = this.container.append("div").style("margin-bottom", "8px");
    controls.append("label").text("Key: ");
    const input = controls
      .append("input")
      .attr("type", "text")
      .attr("placeholder", "enter key...")
      .style("width", "160px");

    controls
      .append("button")
      .text("Show versions")
      .style("margin-left", "6px")
      .on("click", async () => {
        const key = (input.node() as HTMLInputElement)?.value;
        if (key) await this.showVersions(key);
      });

    this.container.append("div").attr("id", "version-chart");
  }

  async showVersions(key: string) {
    try {
      const { versions } = await mvccApi.versions(key);
      this.render(key, versions);
    } catch (e) {
      this.container.select("#version-chart").html(`<em>${e}</em>`);
    }
  }

  private render(key: string, versions: VersionInfo[]) {
    const el = this.container.select("#version-chart");
    el.html("");

    if (!versions?.length) {
      el.html(`<em>No versions found for "${key}"</em>`);
      return;
    }

    const allXids = versions.flatMap((v) => [v.xmin, v.xmax || v.xmin + 10]);
    const xScale = d3
      .scaleLinear()
      .domain([Math.min(...allXids) - 1, Math.max(...allXids) + 2])
      .range([60, 700]);

    const svg = el
      .append("svg")
      .attr("width", 760)
      .attr("height", versions.length * 40 + 60);

    // X axis (transaction IDs)
    const xAxis = d3.axisBottom(xScale).ticks(8).tickFormat(d3.format("d"));
    svg
      .append("g")
      .attr("transform", `translate(0,${versions.length * 40 + 20})`)
      .call(xAxis);

    svg
      .append("text")
      .attr("x", 380)
      .attr("y", versions.length * 40 + 55)
      .attr("text-anchor", "middle")
      .attr("font-size", "11px")
      .text("Transaction ID");

    versions.forEach((v, i) => {
      const y = i * 40 + 30;
      const x1 = xScale(v.xmin);
      const x2 = v.xmax ? xScale(v.xmax) : 720;

      let color = COLORS.live;
      if (v.xmax && v.xmax_status === "committed") color = COLORS.dead;
      else if (v.xmin_status === "active") color = COLORS.active;
      else if (v.xmin_status === "aborted") color = COLORS.aborted;

      svg
        .append("rect")
        .attr("x", x1)
        .attr("y", y - 12)
        .attr("width", Math.max(8, x2 - x1))
        .attr("height", 24)
        .attr("rx", 4)
        .attr("fill", color)
        .attr("opacity", 0.85);

      svg
        .append("text")
        .attr("x", x1 + 4)
        .attr("y", y + 4)
        .attr("font-size", "10px")
        .attr("fill", "white")
        .text(`v=${v.value} xmin=${v.xmin} xmax=${v.xmax || "∞"}`);
    });
  }
}

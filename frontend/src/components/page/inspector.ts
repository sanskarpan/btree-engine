// Panel 2: Page Inspector — hex-dump-style view of a selected 4096-byte page.
import * as d3 from "d3";
import type { PageContents, SlotInfo } from "../../api/types";
import { tree as treeApi } from "../../api/client";

const STATUS_COLOR: Record<string, string> = {
  committed: "#4caf50",
  active: "#ff9800",
  aborted: "#9e9e9e",
  "": "#ccc",
};

export class PageInspector {
  private container: d3.Selection<HTMLElement, unknown, null, undefined>;
  private currentPageID = 1;

  constructor(el: HTMLElement) {
    this.container = d3.select(el);
    this.buildUI();
  }

  private buildUI() {
    this.container.html(""); // clear

    // Page ID input
    const controls = this.container.append("div").style("margin-bottom", "8px");
    controls.append("label").text("Page ID: ");
    const input = controls
      .append("input")
      .attr("type", "number")
      .attr("min", "0")
      .attr("value", String(this.currentPageID))
      .style("width", "60px");

    controls
      .append("button")
      .text("Inspect")
      .style("margin-left", "6px")
      .on("click", () => {
        this.currentPageID = +(input.node()?.value ?? "1");
        this.loadPage(this.currentPageID);
      });

    this.container.append("div").attr("id", "page-content");
    this.loadPage(this.currentPageID);
  }

  async loadPage(id: number) {
    try {
      const page = await treeApi.page(id);
      this.renderPage(page);
    } catch (e) {
      this.container.select("#page-content").html(`<em>Error: ${e}</em>`);
    }
  }

  private renderPage(page: PageContents) {
    const el = this.container.select("#page-content");
    el.html("");

    // Header summary
    const header = el.append("div").style("margin-bottom", "12px");
    header.append("strong").text(`Page ${page.page_id} `);
    header.append("span").text(`[${page.type}] slots=${page.num_slots} free=${page.free_space}B lsn=${page.page_lsn}`);

    // Fill factor bar
    const fill = Math.max(0, Math.min(100, 100 - (page.free_space / 4096) * 100));
    const bar = el.append("div").style("margin-bottom", "12px");
    bar.append("div").text(`Fill: ${fill.toFixed(1)}%`).style("font-size", "12px");
    const track = bar
      .append("div")
      .style("width", "400px")
      .style("height", "10px")
      .style("background", "#eee")
      .style("border-radius", "5px");
    track
      .append("div")
      .style("width", `${fill}%`)
      .style("height", "100%")
      .style("background", fill > 80 ? "#e53935" : fill > 50 ? "#ff9800" : "#4caf50")
      .style("border-radius", "5px");

    // Slot table
    if (!page.slots?.length) {
      el.append("p").text("No slots.");
      return;
    }

    const table = el
      .append("table")
      .style("border-collapse", "collapse")
      .style("font-size", "12px");

    const thead = table.append("thead");
    const hrow = thead.append("tr");
    ["Slot", "Xmin", "Xmax", "Key", "Value", "Xmin Status", "Xmax Status"].forEach((h) => {
      hrow
        .append("th")
        .text(h)
        .style("border", "1px solid #ddd")
        .style("padding", "4px 8px")
        .style("background", "#f5f5f5");
    });

    const tbody = table.append("tbody");
    (page.slots as SlotInfo[]).forEach((slot) => {
      const tr = tbody.append("tr");
      [
        String(slot.slot_idx),
        String(slot.xmin),
        slot.xmax ? String(slot.xmax) : "—",
        slot.key,
        slot.value,
        slot.xmin_status,
        slot.xmax_status || "—",
      ].forEach((val, i) => {
        const td = tr
          .append("td")
          .text(val)
          .style("border", "1px solid #ddd")
          .style("padding", "4px 8px");
        if (i === 5)
          td.style("color", STATUS_COLOR[slot.xmin_status] ?? "#333");
        if (i === 6)
          td.style("color", STATUS_COLOR[slot.xmax_status] ?? "#ccc");
      });
    });
  }
}

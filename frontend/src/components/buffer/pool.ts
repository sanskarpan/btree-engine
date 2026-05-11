// Panel 5: Buffer Pool Visualizer — grid of N frames with clock hand animation.
import * as d3 from "d3";
import type { FrameInfo } from "../../api/types";
import { buffer as bufferApi } from "../../api/client";
import { getWSClient } from "../../ws/client";

const FRAME_SIZE = 40;
const COLS = 16;
const REFRESH_MS = 1000;

const FRAME_COLOR = (f: FrameInfo): string => {
  if (f.empty) return "#f5f5f5";
  if (f.dirty && f.pin_count > 0) return "#e53935";
  if (f.dirty) return "#ff9800";
  return "#42a5f5";
};

export class BufferPoolPanel {
  private svg: d3.Selection<SVGSVGElement, unknown, null, undefined>;
  private frames: FrameInfo[] = [];
  private hitRate = 0;
  private timer: ReturnType<typeof setInterval> | null = null;

  constructor(container: HTMLElement) {
    const width = COLS * (FRAME_SIZE + 4) + 20;
    this.svg = d3.select(container).append("svg").attr("width", width).attr("height", 600);

    // Subscribe to buffer events for live updates
    const ws = getWSClient();
    ws.on("buffer_hit", () => this.refresh());
    ws.on("buffer_miss", () => this.refresh());
    ws.on("page_evict", () => this.refresh());
    ws.on("page_dirty", () => this.refresh());

    this.timer = setInterval(() => this.refresh(), REFRESH_MS);
    this.refresh();
  }

  destroy() {
    if (this.timer) clearInterval(this.timer);
  }

  async refresh() {
    try {
      const [stats, framesResp] = await Promise.all([bufferApi.stats(), bufferApi.frames()]);
      this.hitRate = stats.buffer_hit_rate;
      this.frames = framesResp.frames;
      this.render();
    } catch {
      // ignore
    }
  }

  private render() {
    this.svg.selectAll("*").remove();

    // Hit rate gauge
    const gaugeY = 20;
    this.svg.append("text").attr("x", 10).attr("y", gaugeY).attr("font-size", "13px").text(`Hit Rate: ${(this.hitRate * 100).toFixed(1)}%`);

    const rows = Math.ceil(this.frames.length / COLS);
    const gridY = gaugeY + 20;

    const frameGroups = this.svg
      .selectAll(".frame")
      .data(this.frames)
      .join("g")
      .attr("class", "frame")
      .attr("transform", (_d, i) => {
        const col = i % COLS;
        const row = Math.floor(i / COLS);
        return `translate(${10 + col * (FRAME_SIZE + 4)},${gridY + row * (FRAME_SIZE + 4)})`;
      });

    frameGroups
      .append("rect")
      .attr("width", FRAME_SIZE)
      .attr("height", FRAME_SIZE)
      .attr("rx", 3)
      .attr("fill", FRAME_COLOR)
      .attr("stroke", "#ccc")
      .attr("stroke-width", 1);

    frameGroups
      .append("text")
      .attr("x", FRAME_SIZE / 2)
      .attr("y", FRAME_SIZE / 2 + 4)
      .attr("text-anchor", "middle")
      .attr("font-size", "9px")
      .attr("fill", "#333")
      .text((d) => (d.empty ? "" : `P${d.page_id}`));

    // Legend
    const legendY = gridY + rows * (FRAME_SIZE + 4) + 20;
    const legend = [
      { color: "#f5f5f5", label: "empty" },
      { color: "#42a5f5", label: "clean" },
      { color: "#ff9800", label: "dirty" },
      { color: "#e53935", label: "pinned+dirty" },
    ];
    legend.forEach((l, i) => {
      this.svg.append("rect").attr("x", 10 + i * 110).attr("y", legendY).attr("width", 14).attr("height", 14).attr("fill", l.color).attr("stroke", "#ccc");
      this.svg.append("text").attr("x", 28 + i * 110).attr("y", legendY + 11).attr("font-size", "11px").text(l.label);
    });
  }
}

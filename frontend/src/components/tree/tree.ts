// Panel 1: B+Tree Structure Visualizer using D3 hierarchy layout.
import * as d3 from "d3";
import type { TreeNode } from "../../api/types";
import { tree as treeApi } from "../../api/client";
import { getWSClient } from "../../ws/client";

const WIDTH = 960;
const HEIGHT = 600;
const NODE_W = 120;
const NODE_H = 40;

export class BTreeVisualizer {
  private svg: d3.Selection<SVGSVGElement, unknown, null, undefined>;
  private g: d3.Selection<SVGGElement, unknown, null, undefined>;
  private treeLayout = d3.tree<TreeNode>().nodeSize([NODE_W + 20, NODE_H + 60]);

  constructor(container: HTMLElement) {
    this.svg = d3
      .select(container)
      .append("svg")
      .attr("width", WIDTH)
      .attr("height", HEIGHT)
      .attr("viewBox", `0 0 ${WIDTH} ${HEIGHT}`);

    // Zoom + pan
    const zoom = d3.zoom<SVGSVGElement, unknown>().scaleExtent([0.3, 3]).on("zoom", (e) => {
      this.g.attr("transform", e.transform.toString());
    });
    this.svg.call(zoom);

    this.g = this.svg.append("g").attr("transform", `translate(${WIDTH / 2},60)`);

    // Subscribe to structural and tuple-level events for live updates.
    const ws = getWSClient();
    ws.on("tree_split", () => this.refresh());
    ws.on("tree_merge", () => this.refresh());
    ws.on("tuple_insert", () => this.refresh());
    ws.on("tuple_delete", () => this.refresh());

    this.refresh();
  }

  async refresh() {
    try {
      const root = await treeApi.structure();
      this.render(root);
    } catch {
      // engine may be unavailable
    }
  }

  private render(rootData: TreeNode) {
    const root = d3.hierarchy<TreeNode>(rootData, (d) => d.children ?? []);
    const treeData = this.treeLayout(root);

    this.g.selectAll("*").remove();

    // Links
    this.g
      .selectAll(".link")
      .data(treeData.links())
      .join("path")
      .attr("class", "link")
      .attr("fill", "none")
      .attr("stroke", "#999")
      .attr("stroke-width", 1.5)
      .attr(
        "d",
        d3
          .linkVertical<d3.HierarchyPointLink<TreeNode>, d3.HierarchyPointNode<TreeNode>>()
          .x((d) => d.x)
          .y((d) => d.y)
      );

    // Leaf sibling links (horizontal)
    const leaves = treeData.leaves();
    for (let i = 0; i < leaves.length - 1; i++) {
      const a = leaves[i];
      const b = leaves[i + 1];
      if (a.data.next_sib === b.data.page_id) {
        this.g
          .append("line")
          .attr("stroke", "#6ab7ff")
          .attr("stroke-width", 1)
          .attr("stroke-dasharray", "4 2")
          .attr("x1", a.x + NODE_W / 2)
          .attr("y1", a.y)
          .attr("x2", b.x - NODE_W / 2)
          .attr("y2", b.y);
      }
    }

    // Nodes
    const node = this.g
      .selectAll(".node")
      .data(treeData.descendants())
      .join("g")
      .attr("class", "node")
      .attr("transform", (d) => `translate(${d.x},${d.y})`)
      .style("cursor", "pointer")
      .on("click", (_e, d) => this.showPageDetail(d.data));

    node
      .append("rect")
      .attr("x", -NODE_W / 2)
      .attr("y", -NODE_H / 2)
      .attr("width", NODE_W)
      .attr("height", NODE_H)
      .attr("rx", 4)
      .attr("fill", (d) => (d.data.type === "internal" ? "#9e9e9e" : "#42a5f5"))
      .attr("stroke", "#333")
      .attr("stroke-width", 1.5);

    node
      .append("text")
      .attr("text-anchor", "middle")
      .attr("dy", "-4")
      .attr("font-size", "11px")
      .attr("fill", "white")
      .text((d) => `P${d.data.page_id} (${d.data.type})`);

    node
      .append("text")
      .attr("text-anchor", "middle")
      .attr("dy", "10")
      .attr("font-size", "10px")
      .attr("fill", "#eee")
      .text((d) => `slots=${d.data.num_slots} fill=${d.data.fill_pct.toFixed(0)}%`);
  }

  private showPageDetail(node: TreeNode) {
    alert(
      `Page ${node.page_id}\nType: ${node.type}\nSlots: ${node.num_slots}\nFill: ${node.fill_pct.toFixed(1)}%\nKeys: ${(node.keys ?? []).slice(0, 3).join(", ")}${(node.keys ?? []).length > 3 ? "..." : ""}`
    );
  }
}

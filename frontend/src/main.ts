// Main entry point — mounts all 7 panels into the page.
import { BTreeVisualizer } from "./components/tree/tree";
import { PageInspector } from "./components/page/inspector";
import { VersionChainViewer } from "./components/versions/versions";
import { IsolationDemo } from "./components/isolation/demo";
import { BufferPoolPanel } from "./components/buffer/pool";
import { WALPanel } from "./components/wal/log";
import { ScenariosPanel } from "./components/scenarios/panel";

document.addEventListener("DOMContentLoaded", () => {
  const panels: Array<[string, (el: HTMLElement) => void]> = [
    ["panel-tree", (el) => new BTreeVisualizer(el)],
    ["panel-page", (el) => new PageInspector(el)],
    ["panel-versions", (el) => new VersionChainViewer(el)],
    ["panel-isolation", (el) => new IsolationDemo(el)],
    ["panel-buffer", (el) => new BufferPoolPanel(el)],
    ["panel-wal", (el) => new WALPanel(el)],
    ["panel-scenarios", (el) => new ScenariosPanel(el)],
  ];

  for (const [id, init] of panels) {
    const el = document.getElementById(id);
    if (el) init(el);
  }
});

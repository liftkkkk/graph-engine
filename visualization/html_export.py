"""
可视化模块
输出交互式HTML图谱（基于vis.js，无需服务器，本地直接打开）
"""

from __future__ import annotations
import json
from pathlib import Path
from core.graph_engine import GraphEngine


# 节点类型 → 颜色映射
NODE_COLORS = {
    "company":     {"background": "#4A90D9", "border": "#2171B5", "font": "#FFFFFF"},
    "person":      {"background": "#E8A838", "border": "#B07A1A", "font": "#FFFFFF"},
    "concept":     {"background": "#52B788", "border": "#2D6A4F", "font": "#FFFFFF"},
    "institution": {"background": "#9B72CF", "border": "#6A3EA1", "font": "#FFFFFF"},
    "industry":    {"background": "#E07A5F", "border": "#A0432A", "font": "#FFFFFF"},
}

EDGE_COLORS = {
    "holds":      "#E8A838",
    "supplies":   "#4A90D9",
    "belongs_to": "#52B788",
    "employs":    "#9B72CF",
    "competes":   "#E07A5F",
}


def export_html_graph(
    engine: GraphEngine,
    output_path: str,
    title: str = "关系图谱",
    node_ids: list[str] | None = None,
    max_nodes: int = 200,
) -> None:
    """
    将图谱导出为可直接在浏览器中打开的交互式HTML

    Args:
        engine: 图引擎实例
        output_path: 输出HTML文件路径
        title: 页面标题
        node_ids: 指定导出的节点（None则导出全部，受max_nodes限制）
        max_nodes: 最大节点数
    """
    # 收集节点和边
    if node_ids:
        selected = set(node_ids)
        # 加入一跳邻居
        for nid in list(selected):
            for edge in engine._adj.get(nid, []):
                selected.add(edge.target_id)
            for edge in engine._radj.get(nid, []):
                selected.add(edge.source_id)
    else:
        selected = set(list(engine._nodes.keys())[:max_nodes])

    vis_nodes = []
    for nid in selected:
        node = engine.get_node(nid)
        if not node:
            continue
        color = NODE_COLORS.get(node.node_type, NODE_COLORS["company"])
        vis_nodes.append({
            "id": nid,
            "label": node.label,
            "group": node.node_type,
            "color": {"background": color["background"], "border": color["border"]},
            "font": {"color": color["font"], "size": 12},
            "title": f"<b>{node.label}</b><br>类型: {node.node_type}<br>"
                     + "<br>".join(f"{k}: {v}" for k, v in node.properties.items()),
        })

    vis_edges = []
    edge_set = set()
    for nid in selected:
        for edge in engine._adj.get(nid, []):
            if edge.target_id not in selected:
                continue
            key = (edge.source_id, edge.target_id, edge.edge_type)
            if key in edge_set:
                continue
            edge_set.add(key)
            color = EDGE_COLORS.get(edge.edge_type, "#AAAAAA")
            label = ""
            if edge.edge_type == "holds":
                label = f"{edge.weight*100:.1f}%"
            vis_edges.append({
                "from": edge.source_id,
                "to": edge.target_id,
                "label": label,
                "color": {"color": color, "opacity": 0.8},
                "arrows": "to",
                "title": f"关系: {edge.edge_type}<br>权重: {edge.weight}",
                "font": {"size": 10, "color": "#555"},
            })

    nodes_json = json.dumps(vis_nodes, ensure_ascii=False)
    edges_json = json.dumps(vis_edges, ensure_ascii=False)

    stats = engine.stats()

    html = f"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<title>{title}</title>
<script src="https://cdnjs.cloudflare.com/ajax/libs/vis/4.21.0/vis.min.js"></script>
<link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/vis/4.21.0/vis.min.css">
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ font-family: -apple-system, "PingFang SC", sans-serif; background: #0f1117; color: #e0e0e0; }}
  #header {{ padding: 16px 24px; background: #1a1d27; border-bottom: 1px solid #2d3148; display: flex; align-items: center; gap: 16px; }}
  #header h1 {{ font-size: 18px; font-weight: 500; color: #fff; }}
  .stat {{ font-size: 12px; color: #888; background: #252836; padding: 4px 10px; border-radius: 12px; }}
  #controls {{ padding: 10px 24px; background: #13151f; border-bottom: 1px solid #2d3148; display: flex; gap: 12px; align-items: center; flex-wrap: wrap; }}
  #controls label {{ font-size: 13px; color: #aaa; }}
  #search {{ background: #1e2130; border: 1px solid #3a3f5c; color: #e0e0e0; padding: 6px 12px; border-radius: 6px; font-size: 13px; width: 200px; }}
  #search:focus {{ outline: none; border-color: #4A90D9; }}
  .filter-btn {{ background: #1e2130; border: 1px solid #3a3f5c; color: #aaa; padding: 5px 12px; border-radius: 6px; font-size: 12px; cursor: pointer; transition: all .15s; }}
  .filter-btn:hover, .filter-btn.active {{ background: #4A90D9; border-color: #4A90D9; color: #fff; }}
  #graph {{ width: 100%; height: calc(100vh - 120px); }}
  #legend {{ position: fixed; bottom: 20px; left: 20px; background: rgba(26,29,39,0.95); border: 1px solid #2d3148; border-radius: 10px; padding: 14px 16px; font-size: 12px; }}
  #legend h3 {{ font-size: 12px; color: #888; margin-bottom: 8px; text-transform: uppercase; letter-spacing: 0.5px; }}
  .legend-item {{ display: flex; align-items: center; gap: 8px; margin-bottom: 5px; color: #ccc; }}
  .dot {{ width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0; }}
  #info-panel {{ position: fixed; top: 130px; right: 20px; width: 280px; background: rgba(26,29,39,0.97); border: 1px solid #2d3148; border-radius: 10px; padding: 16px; display: none; font-size: 13px; }}
  #info-panel h3 {{ font-size: 15px; font-weight: 500; color: #fff; margin-bottom: 10px; }}
  #info-content p {{ color: #aaa; margin-bottom: 5px; line-height: 1.5; }}
  #info-content strong {{ color: #e0e0e0; }}
  #close-info {{ float: right; cursor: pointer; color: #666; font-size: 16px; }}
</style>
</head>
<body>
<div id="header">
  <h1>{title}</h1>
  <span class="stat">节点 {stats['node_count']}</span>
  <span class="stat">关系 {stats['edge_count']}</span>
  <span class="stat">本次展示 {len(vis_nodes)} 节点</span>
</div>
<div id="controls">
  <label>搜索:</label>
  <input id="search" placeholder="输入公司名或代码..." oninput="filterNodes(this.value)">
  <label>显示类型:</label>
  {"".join(f'<button class="filter-btn active" onclick="toggleType(this,\'{t}\')">{t}</button>'
    for t in ["company","person","concept","institution"])}
  <button class="filter-btn" onclick="network.fit()">重置视图</button>
  <button class="filter-btn" onclick="network.stabilize()">重新布局</button>
</div>
<div id="graph"></div>

<div id="legend">
  <h3>图例</h3>
  {"".join(f'<div class="legend-item"><div class="dot" style="background:{c["background"]}"></div>{t}</div>'
    for t, c in NODE_COLORS.items())}
  <h3 style="margin-top:10px">关系类型</h3>
  {"".join(f'<div class="legend-item"><div style="width:20px;height:2px;background:{c}"></div>{et}</div>'
    for et, c in EDGE_COLORS.items())}
</div>

<div id="info-panel">
  <span id="close-info" onclick="closeInfo()">✕</span>
  <h3 id="info-title">节点详情</h3>
  <div id="info-content"></div>
</div>

<script>
const allNodes = new vis.DataSet({nodes_json});
const allEdges = new vis.DataSet({edges_json});
let hiddenTypes = new Set();

const options = {{
  nodes: {{
    shape: "dot",
    size: 16,
    borderWidth: 2,
    shadow: false,
    font: {{ size: 12, face: "PingFang SC, sans-serif" }},
  }},
  edges: {{
    width: 1.5,
    smooth: {{ type: "continuous", roundness: 0.3 }},
    font: {{ size: 10, align: "middle" }},
  }},
  physics: {{
    stabilization: {{ iterations: 150 }},
    barnesHut: {{ gravitationalConstant: -8000, springConstant: 0.04, springLength: 120 }},
  }},
  interaction: {{ hover: true, tooltipDelay: 200 }},
}};

const container = document.getElementById("graph");
const network = new vis.Network(container, {{ nodes: allNodes, edges: allEdges }}, options);

network.on("click", params => {{
  if (params.nodes.length > 0) {{
    const nid = params.nodes[0];
    const node = allNodes.get(nid);
    showInfo(node);
  }} else {{
    closeInfo();
  }}
}});

function showInfo(node) {{
  const panel = document.getElementById("info-panel");
  document.getElementById("info-title").textContent = node.label;
  const div = document.getElementById("info-content");
  div.innerHTML = `<p><strong>ID:</strong> ${{node.id}}</p><p><strong>类型:</strong> ${{node.group}}</p>`;
  panel.style.display = "block";
}}

function closeInfo() {{
  document.getElementById("info-panel").style.display = "none";
}}

function filterNodes(query) {{
  if (!query) {{
    allNodes.forEach(n => allNodes.update({{ id: n.id, hidden: hiddenTypes.has(n.group) }}));
    return;
  }}
  const q = query.toLowerCase();
  allNodes.forEach(n => {{
    const match = n.label.toLowerCase().includes(q);
    allNodes.update({{ id: n.id, hidden: !match || hiddenTypes.has(n.group) }});
  }});
}}

function toggleType(btn, nodeType) {{
  btn.classList.toggle("active");
  if (hiddenTypes.has(nodeType)) {{
    hiddenTypes.delete(nodeType);
  }} else {{
    hiddenTypes.add(nodeType);
  }}
  allNodes.forEach(n => {{
    if (n.group === nodeType) {{
      allNodes.update({{ id: n.id, hidden: hiddenTypes.has(nodeType) }});
    }}
  }});
}}
</script>
</body>
</html>"""

    Path(output_path).write_text(html, encoding="utf-8")
    print(f"图谱HTML已导出至: {output_path}")

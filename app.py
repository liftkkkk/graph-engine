"""
图谱引擎 - Web服务
运行: python app.py
访问: http://localhost:5000
"""

import sys
import json
from pathlib import Path
from flask import Flask, request, jsonify, send_from_directory

ROOT = Path(__file__).parent
sys.path.insert(0, str(ROOT))

from core.graph_engine import GraphEngine
from data.loader import AStockDataLoader
from data.llm_data_generator import generate_all
from modules.analyzers import (
    ConceptPropagationAnalyzer,
    SupplyChainAnalyzer,
    ShareholderNetworkAnalyzer,
    ExecutiveNetworkAnalyzer,
)
from modules.hidden_connections import HiddenConnectionFinder, compute_influence_rank

import os, io, traceback
import csv as csv_module
from werkzeug.utils import secure_filename

app = Flask(__name__, static_folder="static")
app.config["MAX_CONTENT_LENGTH"] = 10 * 1024 * 1024  # 10MB

# ── 数据目录 ───────────────────────────────
USER_DATA_DIR = ROOT / "data" / "user"
USER_DATA_DIR.mkdir(parents=True, exist_ok=True)

# 五类数据文件的规范：字段名、说明、示例行
DATA_SCHEMAS = {
    "companies": {
        "label": "上市公司",
        "fields": ["code", "name", "industry", "market_cap", "sector"],
        "required": ["code", "name"],
        "desc": "上市公司基础信息",
        "example": "300750,宁德时代,锂电池,12000,新能源",
    },
    "shareholding": {
        "label": "股权关系",
        "fields": ["holder_id", "holder_name", "holder_type", "company_code", "ratio"],
        "required": ["holder_id", "holder_name", "holder_type", "company_code", "ratio"],
        "desc": "股东持股关系，ratio为持股比例（0~1）",
        "example": "person_zyq,曾毓群,person,300750,0.243",
    },
    "supply_chain": {
        "label": "供应链关系",
        "fields": ["supplier_code", "customer_code", "relation_type", "importance"],
        "required": ["supplier_code", "customer_code", "relation_type"],
        "desc": "供应链上下游关系，importance为重要性（0~1）",
        "example": "002460,300750,supplies,0.9",
    },
    "concepts": {
        "label": "概念板块",
        "fields": ["concept_id", "concept_name", "stock_code", "weight"],
        "required": ["concept_id", "concept_name", "stock_code"],
        "desc": "股票所属概念板块，weight为代表性权重（0~1）",
        "example": "battery,锂电池,300750,1.0",
    },
    "executives": {
        "label": "高管任职",
        "fields": ["person_id", "person_name", "company_code", "title", "start_date", "end_date"],
        "required": ["person_id", "person_name", "company_code", "title"],
        "desc": "高管任职记录，end_date为空表示在任",
        "example": "zyq,曾毓群,300750,董事长,2011-01-01,",
    },
}

# ── 图谱状态 ───────────────────────────────
engine = GraphEngine()
_data_source = "demo"   # "demo" | "user"
_load_errors = []

def _get_data_dir():
    """优先用用户数据，否则用Demo数据"""
    user_files = list(USER_DATA_DIR.glob("*.csv"))
    if user_files:
        return USER_DATA_DIR, "user"
    return ROOT / "data" / "sample_v2", "demo"

def rebuild_engine():
    """清空并重建图谱"""
    global engine, _data_source, _load_errors
    engine = GraphEngine()
    loader = AStockDataLoader(engine)
    data_dir, source = _get_data_dir()
    _data_source = source
    _load_errors = []

    file_map = {
        "companies":    (loader.load_companies,    "companies.csv"),
        "shareholding": (loader.load_shareholding, "shareholding.csv"),
        "supply_chain": (loader.load_supply_chain, "supply_chain.csv"),
        "concepts":     (loader.load_concepts,     "concepts.csv"),
        "executives":   (loader.load_executives,   "executives.csv"),
    }
    loaded = {}
    for key, (fn, fname) in file_map.items():
        fpath = data_dir / fname
        if fpath.exists():
            try:
                loaded[key] = fn(str(fpath))
            except Exception as e:
                _load_errors.append({"file": fname, "error": str(e)})
                loaded[key] = 0
        else:
            loaded[key] = 0
    return loaded

def init():
    data_dir = ROOT / "data" / "sample_v2"
    generate_all(str(data_dir))
    counts = rebuild_engine()
    print(f"图谱就绪 [{_data_source}]: {engine.stats()}")

# ── 工具函数 ───────────────────────────────

def node_to_dict(node):
    if not node:
        return None
    return {
        "id": node.id,
        "label": node.label,
        "type": node.node_type,
        **node.properties,
    }

def find_company(query: str):
    """按名称或代码模糊搜索公司"""
    query = query.strip()
    results = []
    for node in engine._nodes.values():
        if node.node_type != "company":
            continue
        code = node.properties.get("code", "")
        if query in node.label or query == code:
            results.append(node)
    return results

# ── API 路由 ───────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")

@app.route("/data.html")
def data_page():
    return send_from_directory("static", "data.html")

@app.route("/api/search")
def search():
    """搜索公司"""
    q = request.args.get("q", "").strip()
    if not q:
        return jsonify([])
    results = find_company(q)[:10]
    return jsonify([node_to_dict(n) for n in results])

@app.route("/api/stats")
def stats():
    """图谱统计"""
    return jsonify(engine.stats())

@app.route("/api/company/<company_id>")
def company_detail(company_id):
    """公司基础信息 + 一跳邻居"""
    node = engine.get_node(company_id)
    if not node:
        return jsonify({"error": "未找到"}), 404

    # 邻居节点和边
    neighbors = []
    for edge in engine._adj.get(company_id, [])[:20]:
        t = engine.get_node(edge.target_id)
        if t:
            neighbors.append({
                "node": node_to_dict(t),
                "edge_type": edge.edge_type,
                "weight": edge.weight,
                "direction": "out",
            })
    for edge in engine._radj.get(company_id, [])[:20]:
        s = engine.get_node(edge.source_id)
        if s:
            neighbors.append({
                "node": node_to_dict(s),
                "edge_type": edge.edge_type,
                "weight": edge.weight,
                "direction": "in",
            })

    return jsonify({
        "node": node_to_dict(node),
        "neighbors": neighbors,
    })

@app.route("/api/path")
def find_path():
    """两点之间的关联路径"""
    src = request.args.get("from", "").strip()
    dst = request.args.get("to", "").strip()
    edge_types = request.args.get("edge_types", "").strip()

    if not src or not dst:
        return jsonify({"error": "请输入起点和终点"}), 400

    # 支持按名称或ID搜索
    def resolve(q):
        node = engine.get_node(q)
        if node:
            return node.id
        results = find_company(q)
        return results[0].id if results else None

    src_id = resolve(src)
    dst_id = resolve(dst)

    if not src_id:
        return jsonify({"error": f"找不到公司：{src}"}), 404
    if not dst_id:
        return jsonify({"error": f"找不到公司：{dst}"}), 404

    types = [t.strip() for t in edge_types.split(",")] if edge_types else None
    path = engine.shortest_path(src_id, dst_id, edge_types=types, max_hops=6)

    if not path:
        return jsonify({"found": False, "message": f"未找到 {src} 到 {dst} 的关联路径"})

    path_nodes = [node_to_dict(engine.get_node(nid)) for nid in path.nodes]
    path_edges = [
        {"source": e.source_id, "target": e.target_id,
         "type": e.edge_type, "weight": e.weight}
        for e in path.edges
    ]
    return jsonify({
        "found": True,
        "hop_count": path.hop_count,
        "nodes": path_nodes,
        "edges": path_edges,
    })

@app.route("/api/supply_chain/<company_id>")
def supply_chain(company_id):
    """供应链穿透分析"""
    node = engine.get_node(company_id)
    if not node:
        # 尝试按名称找
        results = find_company(company_id)
        if not results:
            return jsonify({"error": "未找到公司"}), 404
        company_id = results[0].id
        node = results[0]

    analyzer = SupplyChainAnalyzer(engine)
    result = analyzer.analyze(company_id, max_hops=3)

    return jsonify({
        "company": node_to_dict(node),
        "upstream": result.upstream,
        "downstream": result.downstream,
        "bottleneck_nodes": [
            node_to_dict(engine.get_node(n)) for n in result.bottleneck_nodes
        ],
        "key_beneficiaries": result.key_beneficiaries,
    })

@app.route("/api/shareholders/<company_id>")
def shareholders(company_id):
    """股权穿透分析"""
    node = engine.get_node(company_id)
    if not node:
        results = find_company(company_id)
        if not results:
            return jsonify({"error": "未找到公司"}), 404
        company_id = results[0].id

    analyzer = ShareholderNetworkAnalyzer(engine)
    result = analyzer.analyze(company_id)

    return jsonify({
        "direct_shareholders": result.direct_shareholders,
        "ultimate_controllers": result.ultimate_controllers,
        "concerted_actors": result.concerted_actors,
    })

@app.route("/api/concept/<concept_id>")
def concept_propagation(concept_id):
    """概念传导分析"""
    analyzer = ConceptPropagationAnalyzer(engine)
    try:
        result = analyzer.analyze(concept_id, top_n=15)
    except ValueError as e:
        return jsonify({"error": str(e)}), 404

    trigger_names = [
        engine.get_node(s).label for s in result.trigger_stocks
        if engine.get_node(s)
    ]
    return jsonify({
        "concept_id": concept_id,
        "concept_name": engine.get_node(concept_id).label if engine.get_node(concept_id) else concept_id,
        "trigger_stocks": trigger_names,
        "ranked_stocks": result.ranked_stocks,
    })

@app.route("/api/concepts")
def list_concepts():
    """列出所有概念板块"""
    concepts = [
        {"id": n.id, "name": n.label}
        for n in engine._nodes.values()
        if n.node_type == "concept"
    ]
    return jsonify(sorted(concepts, key=lambda x: x["name"]))

@app.route("/api/hidden")
def hidden_connections():
    """隐性关联发现"""
    company_id = request.args.get("company_id", "").strip()
    focus_ids = [company_id] if company_id else None

    finder = HiddenConnectionFinder(engine)
    report = finder.find_all(focus_ids=focus_ids, top_n=8)
    all_findings = report.all_sorted()

    return jsonify({
        "summary": report.summary(),
        "findings": [
            {
                "type": c.type,
                "title": c.title,
                "entity_names": c.entity_names,
                "evidence": c.evidence,
                "score": c.score,
                "signal": c.signal,
            }
            for c in all_findings
        ],
    })

@app.route("/api/influence")
def influence_rank():
    """PageRank影响力排名"""
    ranks = compute_influence_rank(engine, edge_types=["supplies", "holds"])
    company_ranks = {
        nid: score for nid, score in ranks.items()
        if engine.get_node(nid) and engine.get_node(nid).node_type == "company"
    }
    top20 = sorted(company_ranks.items(), key=lambda x: x[1], reverse=True)[:20]
    result = []
    for rank, (nid, score) in enumerate(top20, 1):
        node = engine.get_node(nid)
        result.append({
            "rank": rank,
            "id": nid,
            "name": node.label,
            "score": score,
            "industry": node.properties.get("industry", ""),
            "market_cap": node.properties.get("market_cap", 0),
        })
    return jsonify(result)

@app.route("/api/graph_data")
def graph_data():
    """返回完整图谱数据供前端可视化"""
    company_id = request.args.get("company_id", "").strip()

    if company_id:
        # 子图：目标公司 + 两跳内邻居
        node = engine.get_node(company_id)
        if not node:
            results = find_company(company_id)
            if results:
                company_id = results[0].id
        selected = {company_id}
        for edge in engine._adj.get(company_id, []):
            selected.add(edge.target_id)
            for e2 in engine._adj.get(edge.target_id, []):
                selected.add(e2.target_id)
        for edge in engine._radj.get(company_id, []):
            selected.add(edge.source_id)
    else:
        selected = set(engine._nodes.keys())

    nodes = []
    for nid in selected:
        n = engine.get_node(nid)
        if n:
            nodes.append({
                "id": n.id, "label": n.label,
                "type": n.node_type,
                "industry": n.properties.get("industry", ""),
                "market_cap": n.properties.get("market_cap", 0),
            })

    edges = []
    seen = set()
    for nid in selected:
        for edge in engine._adj.get(nid, []):
            if edge.target_id not in selected:
                continue
            key = (edge.source_id, edge.target_id, edge.edge_type)
            if key in seen:
                continue
            seen.add(key)
            edges.append({
                "source": edge.source_id,
                "target": edge.target_id,
                "type": edge.edge_type,
                "weight": edge.weight,
            })

    return jsonify({"nodes": nodes, "edges": edges})


# ═══════════════════════════════════════════
# 数据管理 API
# ═══════════════════════════════════════════

@app.route("/api/data/status")
def data_status():
    data_dir, source = _get_data_dir()
    files = {}
    for key, schema in DATA_SCHEMAS.items():
        fpath = data_dir / f"{key}.csv"
        if fpath.exists():
            with open(fpath, encoding="utf-8") as f:
                rows = sum(1 for _ in f) - 1
            files[key] = {"exists": True, "rows": rows, "size": fpath.stat().st_size, "label": schema["label"], "source": source}
        else:
            files[key] = {"exists": False, "rows": 0, "size": 0, "label": schema["label"], "source": source}
    return jsonify({"source": _data_source, "stats": engine.stats(), "files": files, "errors": _load_errors})

@app.route("/api/data/schema/<data_type>")
def data_schema(data_type):
    if data_type not in DATA_SCHEMAS: return jsonify({"error": "未知类型"}), 404
    return jsonify(DATA_SCHEMAS[data_type])

@app.route("/api/data/preview/<data_type>")
def data_preview(data_type):
    if data_type not in DATA_SCHEMAS: return jsonify({"error": "未知类型"}), 404
    data_dir, _ = _get_data_dir()
    fpath = data_dir / f"{data_type}.csv"
    if not fpath.exists(): return jsonify({"headers": [], "rows": [], "total": 0})
    rows, headers = [], []
    with open(fpath, encoding="utf-8") as f:
        reader = csv_module.DictReader(f)
        headers = reader.fieldnames or []
        for i, row in enumerate(reader):
            if i >= 20: break
            rows.append(dict(row))
    with open(fpath, encoding="utf-8") as f:
        total = sum(1 for _ in f) - 1
    return jsonify({"headers": headers, "rows": rows, "total": total})

@app.route("/api/data/upload/<data_type>", methods=["POST"])
def upload_data(data_type):
    if data_type not in DATA_SCHEMAS: return jsonify({"error": "未知类型"}), 404
    schema = DATA_SCHEMAS[data_type]
    csv_text = None
    if "file" in request.files:
        f = request.files["file"]
        if not f.filename.endswith(".csv"): return jsonify({"error": "请上传CSV文件"}), 400
        csv_text = f.read().decode("utf-8-sig")
    elif request.json and "csv_text" in request.json:
        csv_text = request.json["csv_text"]
    else:
        return jsonify({"error": "请上传文件或提供CSV文本"}), 400
    try:
        reader = csv_module.DictReader(io.StringIO(csv_text))
        headers = reader.fieldnames or []
        rows = list(reader)
    except Exception as e:
        return jsonify({"error": f"CSV格式错误: {e}"}), 400
    missing = [f for f in schema["required"] if f not in headers]
    if missing: return jsonify({"error": f"缺少字段: {', '.join(missing)}", "required": schema["required"]}), 400
    if not rows: return jsonify({"error": "文件内容为空"}), 400
    USER_DATA_DIR.mkdir(parents=True, exist_ok=True)
    (USER_DATA_DIR / f"{data_type}.csv").write_text(csv_text, encoding="utf-8")
    rebuild_engine()
    return jsonify({"success": True, "message": f"已导入 {len(rows)} 行，图谱已重建", "rows_imported": len(rows), "graph_stats": engine.stats(), "errors": _load_errors})

@app.route("/api/data/delete/<data_type>", methods=["DELETE"])
def delete_data(data_type):
    if data_type not in DATA_SCHEMAS: return jsonify({"error": "未知类型"}), 404
    fpath = USER_DATA_DIR / f"{data_type}.csv"
    if fpath.exists(): fpath.unlink()
    rebuild_engine()
    return jsonify({"success": True, "graph_stats": engine.stats()})

@app.route("/api/data/reset", methods=["POST"])
def reset_data():
    for f in USER_DATA_DIR.glob("*.csv"): f.unlink()
    rebuild_engine()
    return jsonify({"success": True, "message": "已恢复Demo数据", "graph_stats": engine.stats()})

@app.route("/api/data/download/<data_type>")
def download_template(data_type):
    if data_type not in DATA_SCHEMAS: return jsonify({"error": "未知类型"}), 404
    schema = DATA_SCHEMAS[data_type]
    csv_content = ",".join(schema["fields"]) + "\n" + schema["example"]
    from flask import Response
    return Response(csv_content, mimetype="text/csv", headers={"Content-Disposition": f"attachment; filename={data_type}_template.csv"})


if __name__ == "__main__":
    init()
    print("\n启动Web服务: http://localhost:5000\n")
    app.run(debug=False, port=5000)

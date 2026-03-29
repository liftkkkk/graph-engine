"""
关系图谱引擎 v2 - 主程序
新增：隐性关联发现、PageRank影响力、更大规模模拟数据
"""

import sys
from pathlib import Path

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
from visualization.html_export import export_html_graph


def build_graph() -> GraphEngine:
    engine = GraphEngine()
    loader = AStockDataLoader(engine)
    data_dir = ROOT / "data" / "sample_v2"

    print("生成模拟数据...")
    generate_all(str(data_dir))

    print("\n构建图谱...")
    loader.load_companies(str(data_dir / "companies.csv"))
    loader.load_shareholding(str(data_dir / "shareholding.csv"))
    loader.load_supply_chain(str(data_dir / "supply_chain.csv"))
    loader.load_concepts(str(data_dir / "concepts.csv"))
    loader.load_executives(str(data_dir / "executives.csv"))

    stats = engine.stats()
    print(f"\n图谱: {stats['node_count']} 节点, {stats['edge_count']} 关系")
    print(f"节点分布: {stats['node_types']}")
    print(f"关系分布: {stats['edge_types']}\n")
    return engine


def demo_hidden_connections(engine: GraphEngine):
    print("=" * 55)
    print("【核心功能】隐性关联发现")
    print("=" * 55)
    finder = HiddenConnectionFinder(engine)
    report = finder.find_all(top_n=5)

    print(f"\n发现统计: {report.summary()}\n")

    all_findings = report.all_sorted()
    for i, conn in enumerate(all_findings[:8], 1):
        signal_icon = {"positive": "↑", "negative": "⚠", "neutral": "○"}.get(conn.signal, "○")
        print(f"{i}. [{signal_icon}] {conn.title}")
        print(f"   评分: {conn.score:.2f}")
        for ev in conn.evidence:
            print(f"   • {ev}")
        print()


def demo_influence_rank(engine: GraphEngine):
    print("=" * 55)
    print("【PageRank】图谱影响力排名")
    print("=" * 55)
    ranks = compute_influence_rank(engine, edge_types=["supplies", "holds"])
    company_ranks = {
        nid: score for nid, score in ranks.items()
        if engine.get_node(nid) and engine.get_node(nid).node_type == "company"
    }
    top10 = sorted(company_ranks.items(), key=lambda x: x[1], reverse=True)[:10]
    print("\n供应链+股权图中影响力最高的公司：")
    for rank, (nid, score) in enumerate(top10, 1):
        node = engine.get_node(nid)
        print(f"  {rank:2d}. {node.label:<12} 影响力={score:.4f}  行业={node.properties.get('industry','')}")
    print()


def demo_concept_propagation(engine: GraphEngine):
    print("=" * 55)
    print("【概念传导】碳中和板块启动 → 受益标的")
    print("=" * 55)
    analyzer = ConceptPropagationAnalyzer(engine)
    result = analyzer.analyze("concept_carbon_neutral", decay=0.65, top_n=8)
    print(f"\n触发概念: 碳中和")
    print(f"龙头种子: {[engine.get_node(s).label for s in result.trigger_stocks if engine.get_node(s)]}")
    print("\n传导受益标的:")
    for s in result.ranked_stocks:
        print(f"  {s['name']:<10} ({s['code']})  分数={s['score']:.4f}  行业={s['industry']}")
    print()


def demo_supply_chain(engine: GraphEngine):
    print("=" * 55)
    print("【供应链穿透】宁德时代完整产业链")
    print("=" * 55)
    analyzer = SupplyChainAnalyzer(engine)
    result = analyzer.analyze("stock_300750", max_hops=3)
    print(f"\n上游供应商 ({len(result.upstream)} 家):")
    for u in result.upstream:
        bar = "█" * int(u["importance"] * 10)
        print(f"  {u['name']:<10} L{u['hops']}层  重要性 {bar} {u['importance']:.2f}")
    print(f"\n下游客户: {[d['name'] for d in result.downstream]}")
    print(f"瓶颈节点: {[engine.get_node(n).label for n in result.bottleneck_nodes if engine.get_node(n)]}")
    print(f"核心受益: {[b['name'] for b in result.key_beneficiaries]}")
    print()


def demo_shareholder(engine: GraphEngine):
    print("=" * 55)
    print("【股权穿透】宁德时代 → 实控人识别")
    print("=" * 55)
    analyzer = ShareholderNetworkAnalyzer(engine)
    result = analyzer.analyze("stock_300750")
    print("\n直接股东:")
    for sh in result.direct_shareholders:
        print(f"  {sh['name']:<14} {sh['type']:<12} {sh['ratio']*100:.1f}%")
    print("\n穿透实控人:")
    for ctrl in result.ultimate_controllers:
        print(f"  {ctrl['name']:<14} 穿透持股 {ctrl['penetrated_ratio']*100:.1f}%  深度={ctrl['depth']}层")
    print(f"\n疑似一致行动人: {len(result.concerted_actors)} 组")
    for ca in result.concerted_actors[:3]:
        print(f"  {ca['name_a']} + {ca['name_b']}  相似度={ca['similarity_score']}")
    print()


def demo_executive(engine: GraphEngine):
    print("=" * 55)
    print("【高管网络】关键人物跨公司影响圈")
    print("=" * 55)
    analyzer = ExecutiveNetworkAnalyzer(engine)

    # 找离职并去竞争对手的高管
    result = analyzer.analyze_person("person_chenwh")
    node = engine.get_node("person_chenwh")
    print(f"\n人物: {node.label if node else 'unknown'}")
    print("历史任职:")
    for r in result.historical_roles:
        print(f"  {r['company_name']:<12} {r['title']:<12} {r['start_date']}~{r['end_date']}")
    print("当前任职:")
    for r in result.current_roles:
        print(f"  {r['company_name']:<12} {r['title']:<12} {r['start_date']}~")
    print(f"关键信号: {[s['signal'] for s in result.key_signals]}")
    print()


def main():
    engine = build_graph()

    demo_hidden_connections(engine)
    demo_influence_rank(engine)
    demo_concept_propagation(engine)
    demo_supply_chain(engine)
    demo_shareholder(engine)
    demo_executive(engine)

    # 导出
    output_dir = ROOT / "output"
    output_dir.mkdir(exist_ok=True)
    export_html_graph(engine, str(output_dir / "graph_v2.html"), title="关系图谱 v2")
    engine.export_json(str(output_dir / "graph_v2.json"))
    engine.save(str(output_dir / "graph_v2.pkl"))
    print(f"输出: {output_dir}/graph_v2.html  （用浏览器打开查看交互图谱）")


if __name__ == "__main__":
    main()

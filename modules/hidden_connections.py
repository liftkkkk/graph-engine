"""
隐性关联发现引擎
这是整个产品最核心的差异化功能：
发现人工分析和大模型都给不出的、隐藏在图结构里的关联信号

四类隐性关联：
1. 隐性股权桥梁 — 两家看似无关的公司，通过三层以上股权共同联系到同一实控人
2. 供应链单点风险 — 某个小公司是多家龙头的唯一供应商，但市场不知道
3. 高管隐性网络 — 同一个人在多家公司任职，形成隐性信息通道
4. 跨板块概念共振 — 两个不同行业的股票，共同属于多个小众概念，往往会同涨同跌
"""

from __future__ import annotations
from dataclasses import dataclass, field
from collections import defaultdict
from core.graph_engine import GraphEngine


# ─────────────────────────────────────────
# 结果数据结构
# ─────────────────────────────────────────

@dataclass
class HiddenConnection:
    """单条隐性关联"""
    type: str                    # equity_bridge / supply_risk / exec_network / concept_resonance
    title: str                   # 一句话描述
    entities: list[str]          # 涉及的节点ID
    entity_names: list[str]      # 涉及的节点名称
    evidence: list[str]          # 证据链（可解释）
    score: float                 # 重要性评分 0~1
    signal: str                  # 投资信号方向：positive / negative / neutral


@dataclass
class HiddenConnectionReport:
    """完整的隐性关联发现报告"""
    equity_bridges: list[HiddenConnection]
    supply_risks: list[HiddenConnection]
    exec_networks: list[HiddenConnection]
    concept_resonances: list[HiddenConnection]

    def all_sorted(self) -> list[HiddenConnection]:
        """返回所有发现，按评分排序"""
        all_items = (
            self.equity_bridges
            + self.supply_risks
            + self.exec_networks
            + self.concept_resonances
        )
        return sorted(all_items, key=lambda x: x.score, reverse=True)

    def summary(self) -> dict:
        return {
            "equity_bridges": len(self.equity_bridges),
            "supply_risks": len(self.supply_risks),
            "exec_networks": len(self.exec_networks),
            "concept_resonances": len(self.concept_resonances),
            "total": len(self.all_sorted()),
        }


# ─────────────────────────────────────────
# 隐性关联发现引擎
# ─────────────────────────────────────────

class HiddenConnectionFinder:
    """
    这是其他工具给不出的答案：
    - 同花顺/Wind：给你知道的标签关系
    - 大模型：给你常识性推断
    - 本引擎：给你数据里真实存在但人眼看不到的结构性关联
    """

    def __init__(self, engine: GraphEngine):
        self.engine = engine

    def find_all(
        self,
        focus_ids: list[str] | None = None,
        top_n: int = 10,
    ) -> HiddenConnectionReport:
        """
        全图扫描，发现所有类型的隐性关联

        Args:
            focus_ids: 聚焦的节点（None则全图扫描）
            top_n: 每类发现取Top N
        """
        return HiddenConnectionReport(
            equity_bridges=self._find_equity_bridges(focus_ids, top_n),
            supply_risks=self._find_supply_risks(top_n),
            exec_networks=self._find_exec_networks(focus_ids, top_n),
            concept_resonances=self._find_concept_resonances(focus_ids, top_n),
        )

    # ── 1. 隐性股权桥梁 ──────────────────────

    def _find_equity_bridges(
        self,
        focus_ids: list[str] | None,
        top_n: int,
    ) -> list[HiddenConnection]:
        """
        发现：两家公司共享隐性股权桥梁
        逻辑：A和B的实控人在3层以上股权穿透后，指向同一个自然人或机构
        但在直接股东列表里，它们没有任何重叠

        这是监管合规分析的核心场景：
        很多"独立"公司其实是同一实控人的关联方
        """
        results = []

        # 获取所有公司节点
        companies = [
            n for n in self.engine._nodes.values()
            if n.node_type == "company"
        ]
        if focus_ids:
            companies = [n for n in companies if n.id in focus_ids]

        # 为每家公司构建穿透后的实控人集合
        company_controllers: dict[str, set[str]] = {}
        for company in companies:
            penetration = self.engine.ownership_penetration(company.id, min_ratio=0.03, max_depth=5)
            controllers = {
                hid for hid, info in penetration.items()
                if self.engine.get_node(hid) and
                   self.engine.get_node(hid).node_type in ("person", "institution")
                   and info["ratio"] > 0.05
            }
            if controllers:
                company_controllers[company.id] = controllers

        # 找出直接股东不重叠、但穿透实控人重叠的公司对
        company_ids = list(company_controllers.keys())
        for i in range(len(company_ids)):
            for j in range(i + 1, len(company_ids)):
                a, b = company_ids[i], company_ids[j]

                # 直接股东
                direct_a = {e.source_id for e in self.engine._radj.get(a, []) if e.edge_type == "holds"}
                direct_b = {e.source_id for e in self.engine._radj.get(b, []) if e.edge_type == "holds"}
                direct_overlap = direct_a & direct_b

                # 穿透后共同实控人
                ctrl_overlap = company_controllers[a] & company_controllers[b]
                hidden_ctrl = ctrl_overlap - direct_overlap  # 直接看不出来的部分

                if not hidden_ctrl:
                    continue

                node_a = self.engine.get_node(a)
                node_b = self.engine.get_node(b)

                for ctrl_id in hidden_ctrl:
                    ctrl_node = self.engine.get_node(ctrl_id)
                    if not ctrl_node:
                        continue

                    score = min(0.95, 0.5 + len(ctrl_overlap) * 0.1)
                    evidence = [
                        f"{node_a.label} 和 {node_b.label} 直接股东无重叠",
                        f"股权穿透后，共同受 {ctrl_node.label} 影响",
                        f"穿透持股比例均超过5%",
                    ]
                    results.append(HiddenConnection(
                        type="equity_bridge",
                        title=f"隐性股权桥梁：{node_a.label} ↔ {node_b.label}（通过{ctrl_node.label}）",
                        entities=[a, b, ctrl_id],
                        entity_names=[node_a.label, node_b.label, ctrl_node.label],
                        evidence=evidence,
                        score=score,
                        signal="neutral",
                    ))

        return sorted(results, key=lambda x: x.score, reverse=True)[:top_n]

    # ── 2. 供应链单点风险 ────────────────────

    def _find_supply_risks(self, top_n: int) -> list[HiddenConnection]:
        """
        发现：隐性供应链单点故障
        逻辑：某个市值小、知名度低的公司，是多家龙头的唯一或主要供应商
        如果它出问题，整条产业链都会受影响
        这类公司往往被市场低估，也是最容易爆发黑天鹅的地方
        """
        results = []

        # 统计每家公司有多少客户，以及其中多少是它的唯一供应商
        supplier_stats: dict[str, dict] = {}

        for node in self.engine._nodes.values():
            if node.node_type != "company":
                continue

            # 该节点作为供应商的所有客户
            customers = [
                e for e in self.engine._adj.get(node.id, [])
                if e.edge_type == "supplies"
            ]
            if len(customers) < 2:
                continue

            # 对每个客户，检查它还有没有其他供应商
            sole_supply_customers = []  # 该节点是唯一供应商的客户
            major_supply_customers = [] # 该节点是最重要供应商的客户

            for e in customers:
                customer_id = e.target_id
                # 该客户的所有供应商
                all_suppliers_of_customer = [
                    re for re in self.engine._radj.get(customer_id, [])
                    if re.edge_type == "supplies"
                ]
                other_suppliers = [s for s in all_suppliers_of_customer if s.source_id != node.id]

                if not other_suppliers:
                    sole_supply_customers.append(customer_id)
                elif e.weight >= 0.8:
                    major_supply_customers.append(customer_id)

            if not sole_supply_customers and not major_supply_customers:
                continue

            node_market_cap = node.properties.get("market_cap", 999999)
            all_risk_customers = sole_supply_customers + major_supply_customers

            # 计算被影响的总市值
            total_affected_cap = sum(
                self.engine.get_node(cid).properties.get("market_cap", 0)
                for cid in all_risk_customers
                if self.engine.get_node(cid)
            )

            # 评分：小市值供应商 × 影响大市值客户 = 高风险
            if node_market_cap > 0:
                risk_ratio = total_affected_cap / (node_market_cap + 1)
                score = min(0.95, 0.3 + risk_ratio * 0.005)
            else:
                score = 0.5

            customer_names = [
                self.engine.get_node(cid).label
                for cid in all_risk_customers
                if self.engine.get_node(cid)
            ]

            evidence = [
                f"{node.label}（市值约{node_market_cap}亿）是以下公司的关键供应商",
                f"关键客户: {', '.join(customer_names)}",
                f"受影响总市值约 {total_affected_cap} 亿",
                f"风险放大比: {risk_ratio:.1f}x（每1元市值对应{risk_ratio:.1f}元客户市值暴露）",
            ]

            results.append(HiddenConnection(
                type="supply_risk",
                title=f"供应链单点风险：{node.label} 是 {len(all_risk_customers)} 家龙头的关键供应商",
                entities=[node.id] + all_risk_customers,
                entity_names=[node.label] + customer_names,
                evidence=evidence,
                score=score,
                signal="negative",  # 风险信号
            ))

        return sorted(results, key=lambda x: x.score, reverse=True)[:top_n]

    # ── 3. 高管隐性网络 ──────────────────────

    def _find_exec_networks(
        self,
        focus_ids: list[str] | None,
        top_n: int,
    ) -> list[HiddenConnection]:
        """
        发现：高管隐性信息网络
        逻辑：同一个人在多家公司担任职务，形成隐性的信息和决策通道
        特别是：同一人跨越不同行业、不同股权结构的公司担任职务，
        往往是资本运作的信号
        """
        results = []

        # 找出所有在2家以上公司任职的人物
        person_companies: dict[str, list[dict]] = defaultdict(list)

        for node in self.engine._nodes.values():
            if node.node_type != "person":
                continue
            for edge in self.engine._adj.get(node.id, []):
                if edge.edge_type != "employs":
                    continue
                company = self.engine.get_node(edge.target_id)
                if company:
                    person_companies[node.id].append({
                        "company_id": edge.target_id,
                        "company_name": company.label,
                        "title": edge.properties.get("title", ""),
                        "is_current": edge.properties.get("is_current", True),
                        "industry": company.properties.get("industry", ""),
                        "sector": company.properties.get("sector", ""),
                    })

        for person_id, roles in person_companies.items():
            if len(roles) < 2:
                continue

            person_node = self.engine.get_node(person_id)
            if not person_node:
                continue

            # 检查是否跨行业（更有信号价值）
            sectors = set(r["sector"] for r in roles if r["sector"])
            industries = set(r["industry"] for r in roles if r["industry"])
            is_cross_sector = len(sectors) > 1

            current_roles = [r for r in roles if r["is_current"]]
            historical_roles = [r for r in roles if not r["is_current"]]

            company_ids = [r["company_id"] for r in roles]
            company_names = [r["company_name"] for r in roles]

            score = 0.4
            score += len(roles) * 0.08
            if is_cross_sector:
                score += 0.2
            if historical_roles:
                score += 0.1  # 有离职记录，信号更强
            score = min(0.92, score)

            signal = "positive" if is_cross_sector else "neutral"

            evidence = [
                f"{person_node.label} 在 {len(roles)} 家公司有任职记录",
                f"当前任职: {', '.join(r['company_name'] + '(' + r['title'] + ')' for r in current_roles)}",
            ]
            if historical_roles:
                evidence.append(
                    f"历史任职: {', '.join(r['company_name'] for r in historical_roles)}"
                )
            if is_cross_sector:
                evidence.append(f"跨越行业: {', '.join(sectors)}（跨行业任职是资本运作信号）")

            results.append(HiddenConnection(
                type="exec_network",
                title=f"高管隐性网络：{person_node.label} 连接 {' + '.join(company_names)}",
                entities=[person_id] + company_ids,
                entity_names=[person_node.label] + company_names,
                evidence=evidence,
                score=score,
                signal=signal,
            ))

        return sorted(results, key=lambda x: x.score, reverse=True)[:top_n]

    # ── 4. 跨板块概念共振 ────────────────────

    def _find_concept_resonances(
        self,
        focus_ids: list[str] | None,
        top_n: int,
    ) -> list[HiddenConnection]:
        """
        发现：跨板块概念共振对
        逻辑：两只表面上不相关的股票（不同行业、不同供应链），
        但它们共同属于3个以上相同的小众概念板块
        历史上这类股票往往会出现联动行情

        这是最难被人工发现的隐性关联，也是最有投资价值的发现
        """
        results = []

        # 构建每只股票的概念集
        stock_concepts: dict[str, dict[str, float]] = defaultdict(dict)
        for node in self.engine._nodes.values():
            if node.node_type != "company":
                continue
            for edge in self.engine._adj.get(node.id, []):
                if edge.edge_type == "belongs_to":
                    stock_concepts[node.id][edge.target_id] = edge.weight

        stocks = list(stock_concepts.keys())
        if focus_ids:
            stocks = [s for s in stocks if s in focus_ids] + [
                s for s in stocks if s not in focus_ids
            ]

        # 检查供应链和直接股权关系（排除已知关联）
        def is_directly_related(a: str, b: str) -> bool:
            for edge in self.engine._adj.get(a, []):
                if edge.target_id == b and edge.edge_type in ("supplies", "holds", "competes"):
                    return True
            for edge in self.engine._adj.get(b, []):
                if edge.target_id == a and edge.edge_type in ("supplies", "holds", "competes"):
                    return True
            return False

        seen_pairs = set()
        for i in range(len(stocks)):
            for j in range(i + 1, len(stocks)):
                a, b = stocks[i], stocks[j]
                pair_key = (min(a, b), max(a, b))
                if pair_key in seen_pairs:
                    continue
                seen_pairs.add(pair_key)

                # 排除已有直接供应链关系的（那是已知关联，不是隐性的）
                if is_directly_related(a, b):
                    continue

                # 共同概念
                concepts_a = set(stock_concepts[a].keys())
                concepts_b = set(stock_concepts[b].keys())
                common_concepts = concepts_a & concepts_b

                if len(common_concepts) < 2:
                    continue

                node_a = self.engine.get_node(a)
                node_b = self.engine.get_node(b)
                if not node_a or not node_b:
                    continue

                # 检查是否跨行业
                sector_a = node_a.properties.get("sector", "")
                sector_b = node_b.properties.get("sector", "")
                is_cross_sector = sector_a != sector_b and sector_a and sector_b

                if not is_cross_sector and len(common_concepts) < 3:
                    continue

                # 计算概念重叠的加权相似度
                weight_sum = sum(
                    (stock_concepts[a].get(c, 0) + stock_concepts[b].get(c, 0)) / 2
                    for c in common_concepts
                )
                similarity = weight_sum / max(len(concepts_a), len(concepts_b))

                score = min(0.9, 0.3 + len(common_concepts) * 0.12 + similarity * 0.2)
                if is_cross_sector:
                    score += 0.15
                score = min(0.92, score)

                concept_names = [
                    self.engine.get_node(cid).label
                    for cid in common_concepts
                    if self.engine.get_node(cid)
                ]

                evidence = [
                    f"{node_a.label}（{sector_a}）与 {node_b.label}（{sector_b}）表面无关联",
                    f"共同属于 {len(common_concepts)} 个概念板块: {', '.join(concept_names)}",
                    f"概念重叠相似度: {similarity:.2f}",
                ]
                if is_cross_sector:
                    evidence.append("跨行业共振——当相关政策或资金驱动时，两者可能联动")

                results.append(HiddenConnection(
                    type="concept_resonance",
                    title=f"跨板块共振：{node_a.label} ↔ {node_b.label}（{len(common_concepts)}个共同概念）",
                    entities=[a, b] + list(common_concepts),
                    entity_names=[node_a.label, node_b.label] + concept_names,
                    evidence=evidence,
                    score=score,
                    signal="positive",
                ))

        return sorted(results, key=lambda x: x.score, reverse=True)[:top_n]


# ─────────────────────────────────────────
# PageRank 影响力评分
# ─────────────────────────────────────────

def compute_influence_rank(
    engine: GraphEngine,
    edge_types: list[str] | None = None,
    damping: float = 0.85,
    iterations: int = 30,
) -> dict[str, float]:
    """
    图谱PageRank：计算每个节点在整张图中的影响力
    被越多重要节点连接的节点，影响力越高
    用于：识别产业链核心节点、关键枢纽公司

    Args:
        damping: 阻尼系数（标准PageRank=0.85）
        iterations: 迭代次数
    """
    nodes = list(engine._nodes.keys())
    n = len(nodes)
    if n == 0:
        return {}

    rank = {nid: 1.0 / n for nid in nodes}

    # 预计算出度
    out_degree: dict[str, int] = {}
    for nid in nodes:
        edges = engine._adj.get(nid, [])
        if edge_types:
            edges = [e for e in edges if e.edge_type in edge_types]
        out_degree[nid] = max(len(edges), 1)

    for _ in range(iterations):
        new_rank: dict[str, float] = {nid: (1 - damping) / n for nid in nodes}
        for nid in nodes:
            edges = engine._adj.get(nid, [])
            if edge_types:
                edges = [e for e in edges if e.edge_type in edge_types]
            for edge in edges:
                new_rank[edge.target_id] = new_rank.get(edge.target_id, 0) + \
                    damping * rank[nid] / out_degree[nid]
        rank = new_rank

    # 归一化到 0~1
    max_rank = max(rank.values()) if rank else 1
    return {nid: round(v / max_rank, 4) for nid, v in rank.items()}

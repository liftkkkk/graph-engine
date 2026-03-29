"""
四大分析模块
1. 概念传导分析
2. 供应链图谱穿透
3. 股东关系网络
4. 高管关系图谱
"""

from __future__ import annotations
from dataclasses import dataclass
from core.graph_engine import GraphEngine, Path


# ─────────────────────────────────────────
# 1. 概念传导分析
# ─────────────────────────────────────────

@dataclass
class ConceptPropagationResult:
    trigger_concept: str
    trigger_stocks: list[str]           # 直接触发的龙头股
    propagation_map: dict[str, float]   # {stock_id: 传导分数}
    ranked_stocks: list[dict]           # 排序后的受益标的


class ConceptPropagationAnalyzer:
    """
    概念传导分析
    逻辑：某个概念板块启动 → 通过共同概念归属 → 传导到关联标的
    """

    def __init__(self, engine: GraphEngine):
        self.engine = engine

    def analyze(
        self,
        concept_id: str,
        seed_stock_ids: list[str] | None = None,
        decay: float = 0.65,
        max_hops: int = 2,
        top_n: int = 20,
    ) -> ConceptPropagationResult:
        """
        分析概念传导路径和受益标的

        Args:
            concept_id: 触发概念ID（如 concept_battery）
            seed_stock_ids: 手动指定龙头股（None则自动取概念下权重最高的股票）
            decay: 传导衰减系数
            max_hops: 最大传导跳数
            top_n: 返回Top N标的
        """
        cnode = self.engine.get_node(concept_id)
        if not cnode:
            raise ValueError(f"概念节点 {concept_id} 不存在")

        # 获取该概念下所有成员股及其权重
        concept_members: dict[str, float] = {}
        for edge in self.engine._radj.get(concept_id, []):
            if edge.edge_type == "belongs_to":
                concept_members[edge.source_id] = edge.weight

        if not concept_members:
            return ConceptPropagationResult(concept_id, [], {}, [])

        # 确定种子（龙头股）
        if seed_stock_ids:
            seeds = {sid: concept_members.get(sid, 0.5) for sid in seed_stock_ids}
        else:
            # 自动取权重最高的前3只
            sorted_members = sorted(concept_members.items(), key=lambda x: x[1], reverse=True)
            seeds = dict(sorted_members[:3])

        trigger_stocks = list(seeds.keys())

        # 通过概念节点传导：股票 → 概念 → 其他股票
        # 第一跳：找出与种子股同属其他概念的股票
        related_concepts: dict[str, float] = {}
        for sid, s_weight in seeds.items():
            for edge in self.engine._adj.get(sid, []):
                if edge.edge_type == "belongs_to" and edge.target_id != concept_id:
                    cid = edge.target_id
                    related_concepts[cid] = max(
                        related_concepts.get(cid, 0),
                        s_weight * edge.weight
                    )

        # 第二跳：找出这些概念下的其他股票
        propagation_map: dict[str, float] = {}
        for cid, c_score in related_concepts.items():
            for edge in self.engine._radj.get(cid, []):
                if edge.edge_type == "belongs_to":
                    stock_id = edge.source_id
                    if stock_id in seeds:
                        continue
                    score = c_score * decay * edge.weight
                    propagation_map[stock_id] = max(
                        propagation_map.get(stock_id, 0), score
                    )

        # 构建排序结果
        ranked = []
        for stock_id, score in sorted(propagation_map.items(), key=lambda x: x[1], reverse=True)[:top_n]:
            node = self.engine.get_node(stock_id)
            if node:
                ranked.append({
                    "stock_id": stock_id,
                    "name": node.label,
                    "score": round(score, 4),
                    "code": node.properties.get("code", ""),
                    "industry": node.properties.get("industry", ""),
                })

        return ConceptPropagationResult(
            trigger_concept=concept_id,
            trigger_stocks=trigger_stocks,
            propagation_map=propagation_map,
            ranked_stocks=ranked,
        )


# ─────────────────────────────────────────
# 2. 供应链图谱穿透
# ─────────────────────────────────────────

@dataclass
class SupplyChainResult:
    target_company: str
    upstream: list[dict]       # 上游供应商
    downstream: list[dict]     # 下游客户
    bottleneck_nodes: list[str]  # 瓶颈节点（唯一供应商）
    key_beneficiaries: list[dict]  # 核心受益标的


class SupplyChainAnalyzer:
    """
    供应链图谱穿透分析
    识别上下游关系、瓶颈节点、核心受益标的
    """

    def __init__(self, engine: GraphEngine):
        self.engine = engine

    def analyze(
        self,
        company_id: str,
        max_hops: int = 3,
    ) -> SupplyChainResult:
        """
        分析目标公司的供应链图谱

        Args:
            company_id: 目标公司ID
            max_hops: 穿透层级
        """
        node = self.engine.get_node(company_id)
        if not node:
            raise ValueError(f"公司节点 {company_id} 不存在")

        # 上游：谁供应给目标
        upstream_reach = self.engine.reachability(
            company_id,
            edge_types=["supplies"],
            max_hops=max_hops,
        )
        # 反向：目标供应给谁
        downstream_ids = set()
        for edge in self.engine._adj.get(company_id, []):
            if edge.edge_type == "supplies":
                downstream_ids.add(edge.target_id)

        # 构建上游列表
        upstream = []
        for nid, hops in sorted(upstream_reach.items(), key=lambda x: x[1]):
            n = self.engine.get_node(nid)
            if n and n.node_type == "company":
                # 找到连接边的重要性
                importance = 0.0
                for edge in self.engine._adj.get(nid, []):
                    if edge.edge_type == "supplies":
                        importance = max(importance, edge.weight)
                upstream.append({
                    "id": nid,
                    "name": n.label,
                    "code": n.properties.get("code", ""),
                    "hops": hops,
                    "importance": round(importance, 3),
                })

        # 构建下游列表
        downstream = []
        for nid in downstream_ids:
            n = self.engine.get_node(nid)
            if n:
                downstream.append({
                    "id": nid,
                    "name": n.label,
                    "code": n.properties.get("code", ""),
                })

        # 瓶颈识别：在上游中，某节点是唯一供应商的
        bottleneck_nodes = []
        for up in upstream:
            suppliers_of_up = [
                e.source_id for e in self.engine._radj.get(up["id"], [])
                if e.edge_type == "supplies"
            ]
            if len(suppliers_of_up) <= 1:
                bottleneck_nodes.append(up["id"])

        # 核心受益标的：上游中importance最高的
        key_beneficiaries = sorted(
            [u for u in upstream if u["hops"] == 1],
            key=lambda x: x["importance"],
            reverse=True,
        )[:5]

        return SupplyChainResult(
            target_company=company_id,
            upstream=upstream,
            downstream=downstream,
            bottleneck_nodes=bottleneck_nodes,
            key_beneficiaries=key_beneficiaries,
        )


# ─────────────────────────────────────────
# 3. 股东关系网络
# ─────────────────────────────────────────

@dataclass
class ShareholderNetworkResult:
    company_id: str
    direct_shareholders: list[dict]
    ultimate_controllers: list[dict]   # 实控人（穿透后持股>5%）
    concerted_actors: list[dict]       # 疑似一致行动人
    all_paths: list[dict]             # 所有关联路径


class ShareholderNetworkAnalyzer:
    """
    股东关系网络分析
    股权穿透、实控人识别、一致行动人发现
    """

    def __init__(self, engine: GraphEngine):
        self.engine = engine

    def analyze(
        self,
        company_id: str,
        control_threshold: float = 0.05,
    ) -> ShareholderNetworkResult:
        """
        分析目标公司的股东网络

        Args:
            company_id: 目标公司ID
            control_threshold: 实控人认定最低持股比例
        """
        # 直接股东
        direct_shareholders = []
        for edge in self.engine._radj.get(company_id, []):
            if edge.edge_type == "holds":
                holder = self.engine.get_node(edge.source_id)
                if holder:
                    direct_shareholders.append({
                        "id": edge.source_id,
                        "name": holder.label,
                        "type": holder.node_type,
                        "ratio": edge.weight,
                    })
        direct_shareholders.sort(key=lambda x: x["ratio"], reverse=True)

        # 股权穿透，找实控人
        penetration = self.engine.ownership_penetration(
            company_id, min_ratio=control_threshold
        )
        ultimate_controllers = []
        for holder_id, info in sorted(
            penetration.items(), key=lambda x: x[1]["ratio"], reverse=True
        ):
            node = self.engine.get_node(holder_id)
            if node and node.node_type == "person":  # 自然人才是终极实控人
                ultimate_controllers.append({
                    "id": holder_id,
                    "name": node.label,
                    "penetrated_ratio": info["ratio"],
                    "depth": info["depth"],
                    "path": info["path"],
                })

        # 疑似一致行动人：持有同一标的且互有关联的股东
        concerted_actors = self._find_concerted_actors(company_id, direct_shareholders)

        # 路径汇总
        all_paths = []
        for ctrl in ultimate_controllers[:3]:
            path = self.engine.shortest_path(ctrl["id"], company_id, edge_types=["holds"])
            if path:
                all_paths.append(path.to_dict())

        return ShareholderNetworkResult(
            company_id=company_id,
            direct_shareholders=direct_shareholders,
            ultimate_controllers=ultimate_controllers,
            concerted_actors=concerted_actors,
            all_paths=all_paths,
        )

    def _find_concerted_actors(
        self,
        company_id: str,
        shareholders: list[dict],
    ) -> list[dict]:
        """
        发现疑似一致行动人
        判断逻辑：两个股东同时持有3家以上相同公司
        """
        holder_portfolios: dict[str, set[str]] = {}
        for sh in shareholders:
            owned = set()
            for edge in self.engine._adj.get(sh["id"], []):
                if edge.edge_type == "holds":
                    owned.add(edge.target_id)
            if owned:
                holder_portfolios[sh["id"]] = owned

        concerted = []
        holders = list(holder_portfolios.keys())
        for i in range(len(holders)):
            for j in range(i + 1, len(holders)):
                a, b = holders[i], holders[j]
                common = holder_portfolios[a] & holder_portfolios[b]
                if len(common) >= 2:
                    node_a = self.engine.get_node(a)
                    node_b = self.engine.get_node(b)
                    concerted.append({
                        "holder_a": a,
                        "name_a": node_a.label if node_a else a,
                        "holder_b": b,
                        "name_b": node_b.label if node_b else b,
                        "common_holdings": list(common),
                        "similarity_score": round(
                            len(common) / max(len(holder_portfolios[a]), len(holder_portfolios[b])), 3
                        ),
                    })
        return sorted(concerted, key=lambda x: x["similarity_score"], reverse=True)


# ─────────────────────────────────────────
# 4. 高管关系图谱
# ─────────────────────────────────────────

@dataclass
class ExecutiveNetworkResult:
    person_id: str
    current_roles: list[dict]
    historical_roles: list[dict]
    influence_circle: list[dict]       # 影响圈（通过任职关联的公司）
    key_signals: list[dict]            # 关键信号（如离职、新任）


class ExecutiveNetworkAnalyzer:
    """
    高管关系图谱分析
    任职网络、影响圈、人事变动信号
    """

    def __init__(self, engine: GraphEngine):
        self.engine = engine

    def analyze_person(self, person_id: str) -> ExecutiveNetworkResult:
        """分析特定高管的关系网络"""
        node = self.engine.get_node(person_id)
        if not node:
            raise ValueError(f"人物节点 {person_id} 不存在")

        current_roles = []
        historical_roles = []

        for edge in self.engine._adj.get(person_id, []):
            if edge.edge_type != "employs":
                continue
            company = self.engine.get_node(edge.target_id)
            if not company:
                continue
            role_info = {
                "company_id": edge.target_id,
                "company_name": company.label,
                "code": company.properties.get("code", ""),
                "title": edge.properties.get("title", ""),
                "start_date": edge.properties.get("start_date", ""),
                "end_date": edge.properties.get("end_date", ""),
            }
            if edge.properties.get("is_current", True):
                current_roles.append(role_info)
            else:
                historical_roles.append(role_info)

        # 影响圈：通过历史任职，找出相关联的其他公司
        influence_circle = []
        visited_companies = {r["company_id"] for r in current_roles + historical_roles}
        for company_id in visited_companies:
            # 找出该公司的供应链关联
            reachable = self.engine.reachability(company_id, edge_types=["supplies"], max_hops=1)
            for related_id in reachable:
                if related_id not in visited_companies:
                    related = self.engine.get_node(related_id)
                    if related:
                        influence_circle.append({
                            "company_id": related_id,
                            "name": related.label,
                            "via": company_id,
                            "relation": "supply_chain",
                        })

        # 关键信号：近期离职
        key_signals = []
        for role in historical_roles:
            if role["end_date"]:
                key_signals.append({
                    "type": "departure",
                    "person": node.label,
                    "from_company": role["company_name"],
                    "date": role["end_date"],
                    "signal": f"{node.label} 于 {role['end_date']} 离开 {role['company_name']}",
                })

        return ExecutiveNetworkResult(
            person_id=person_id,
            current_roles=current_roles,
            historical_roles=historical_roles,
            influence_circle=influence_circle[:10],
            key_signals=key_signals,
        )

    def find_connected_executives(
        self,
        company_a: str,
        company_b: str,
    ) -> list[dict]:
        """
        查找两家公司之间的高管关联
        用于：识别隐性关联、一致行动人背景核查
        """
        execs_a = {
            e.source_id for e in self.engine._radj.get(company_a, [])
            if e.edge_type == "employs"
        }
        execs_b = {
            e.source_id for e in self.engine._radj.get(company_b, [])
            if e.edge_type == "employs"
        }
        common = execs_a & execs_b
        result = []
        for pid in common:
            node = self.engine.get_node(pid)
            if node:
                result.append({"person_id": pid, "name": node.label})
        return result

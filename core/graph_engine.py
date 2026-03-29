"""
关系图谱引擎 - 核心图计算模块
确定性图推理，本地部署，无大模型依赖
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Optional
from collections import defaultdict, deque
import heapq
import json
import pickle
from pathlib import Path


# ─────────────────────────────────────────
# 数据模型
# ─────────────────────────────────────────

@dataclass
class Node:
    """图节点：公司、人物、概念板块等"""
    id: str
    label: str                        # 显示名称
    node_type: str                    # company / person / concept / industry
    properties: dict[str, Any] = field(default_factory=dict)

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return isinstance(other, Node) and self.id == other.id


@dataclass
class Edge:
    """图边：持股、供应、任职、概念归属等"""
    source_id: str
    target_id: str
    edge_type: str                    # holds / supplies / employs / belongs_to / competes
    weight: float = 1.0              # 权重，如持股比例
    properties: dict[str, Any] = field(default_factory=dict)

    def __hash__(self):
        return hash((self.source_id, self.target_id, self.edge_type))


@dataclass
class Path:
    """推理路径结果"""
    nodes: list[str]                 # 节点ID序列
    edges: list[Edge]                # 边序列
    total_weight: float              # 路径总权重
    hop_count: int                   # 跳数

    def to_dict(self) -> dict:
        return {
            "nodes": self.nodes,
            "edges": [
                {
                    "source": e.source_id,
                    "target": e.target_id,
                    "type": e.edge_type,
                    "weight": e.weight,
                }
                for e in self.edges
            ],
            "total_weight": round(self.total_weight, 4),
            "hop_count": self.hop_count,
        }


# ─────────────────────────────────────────
# 图引擎
# ─────────────────────────────────────────

class GraphEngine:
    """
    确定性图计算引擎
    支持：多类型节点/边、有向/无向、加权图
    核心算法：BFS、Dijkstra、DFS、影响力传播
    """

    def __init__(self):
        self._nodes: dict[str, Node] = {}
        self._adj: dict[str, list[Edge]] = defaultdict(list)   # 正向邻接表
        self._radj: dict[str, list[Edge]] = defaultdict(list)  # 反向邻接表
        self._type_index: dict[str, set[str]] = defaultdict(set)  # 按类型索引

    # ── 图构建 ──────────────────────────────

    def add_node(self, node: Node) -> None:
        self._nodes[node.id] = node
        self._type_index[node.node_type].add(node.id)

    def add_edge(self, edge: Edge) -> None:
        # 确保节点存在
        for nid in (edge.source_id, edge.target_id):
            if nid not in self._nodes:
                raise ValueError(f"节点 {nid} 不存在，请先添加节点")
        self._adj[edge.source_id].append(edge)
        self._radj[edge.target_id].append(edge)

    def remove_node(self, node_id: str) -> None:
        if node_id not in self._nodes:
            return
        node = self._nodes.pop(node_id)
        self._type_index[node.node_type].discard(node_id)
        self._adj.pop(node_id, None)
        self._radj.pop(node_id, None)
        for edges in self._adj.values():
            edges[:] = [e for e in edges if e.target_id != node_id]
        for edges in self._radj.values():
            edges[:] = [e for e in edges if e.source_id != node_id]

    def update_node_properties(self, node_id: str, props: dict) -> None:
        if node_id in self._nodes:
            self._nodes[node_id].properties.update(props)

    # ── 查询 ────────────────────────────────

    def get_node(self, node_id: str) -> Optional[Node]:
        return self._nodes.get(node_id)

    def get_nodes_by_type(self, node_type: str) -> list[Node]:
        return [self._nodes[nid] for nid in self._type_index.get(node_type, [])]

    def get_neighbors(self, node_id: str, edge_types: list[str] | None = None) -> list[Node]:
        edges = self._adj.get(node_id, [])
        if edge_types:
            edges = [e for e in edges if e.edge_type in edge_types]
        return [self._nodes[e.target_id] for e in edges if e.target_id in self._nodes]

    def get_edges(self, source_id: str, edge_types: list[str] | None = None) -> list[Edge]:
        edges = self._adj.get(source_id, [])
        if edge_types:
            edges = [e for e in edges if e.edge_type in edge_types]
        return edges

    def node_count(self) -> int:
        return len(self._nodes)

    def edge_count(self) -> int:
        return sum(len(v) for v in self._adj.values())

    # ── 核心算法 ────────────────────────────

    def shortest_path(
        self,
        source_id: str,
        target_id: str,
        edge_types: list[str] | None = None,
        max_hops: int = 6,
    ) -> Optional[Path]:
        """
        BFS最短路径（按跳数）
        用于：关联关系追溯、股权穿透路径
        """
        if source_id not in self._nodes or target_id not in self._nodes:
            return None
        if source_id == target_id:
            return Path([source_id], [], 0.0, 0)

        visited = {source_id}
        queue = deque([(source_id, [source_id], [], 0.0)])

        while queue:
            cur, path_nodes, path_edges, total_w = queue.popleft()
            if len(path_nodes) - 1 >= max_hops:
                continue

            for edge in self._adj.get(cur, []):
                if edge_types and edge.edge_type not in edge_types:
                    continue
                nxt = edge.target_id
                if nxt in visited:
                    continue
                new_nodes = path_nodes + [nxt]
                new_edges = path_edges + [edge]
                new_w = total_w + edge.weight
                if nxt == target_id:
                    return Path(new_nodes, new_edges, new_w, len(new_nodes) - 1)
                visited.add(nxt)
                queue.append((nxt, new_nodes, new_edges, new_w))
        return None

    def all_paths(
        self,
        source_id: str,
        target_id: str,
        edge_types: list[str] | None = None,
        max_hops: int = 4,
        max_results: int = 10,
    ) -> list[Path]:
        """
        DFS枚举所有路径（限制跳数和结果数）
        用于：一致行动人识别、多路径关联分析
        """
        results = []
        visited = set()

        def dfs(cur, path_nodes, path_edges, total_w):
            if len(results) >= max_results:
                return
            if cur == target_id and len(path_nodes) > 1:
                results.append(Path(path_nodes[:], path_edges[:], total_w, len(path_nodes) - 1))
                return
            if len(path_nodes) - 1 >= max_hops:
                return
            for edge in self._adj.get(cur, []):
                if edge_types and edge.edge_type not in edge_types:
                    continue
                nxt = edge.target_id
                if nxt in visited:
                    continue
                visited.add(nxt)
                path_nodes.append(nxt)
                path_edges.append(edge)
                dfs(nxt, path_nodes, path_edges, total_w + edge.weight)
                path_nodes.pop()
                path_edges.pop()
                visited.discard(nxt)

        visited.add(source_id)
        dfs(source_id, [source_id], [], 0.0)
        return sorted(results, key=lambda p: p.hop_count)

    def reachability(
        self,
        source_id: str,
        edge_types: list[str] | None = None,
        max_hops: int = 3,
    ) -> dict[str, int]:
        """
        BFS可达性分析：从source出发，返回{节点ID: 跳数}
        用于：影响圈分析、概念传导范围
        """
        if source_id not in self._nodes:
            return {}
        visited = {source_id: 0}
        queue = deque([(source_id, 0)])
        while queue:
            cur, hops = queue.popleft()
            if hops >= max_hops:
                continue
            for edge in self._adj.get(cur, []):
                if edge_types and edge.edge_type not in edge_types:
                    continue
                nxt = edge.target_id
                if nxt not in visited:
                    visited[nxt] = hops + 1
                    queue.append((nxt, hops + 1))
        del visited[source_id]
        return visited

    def propagate_score(
        self,
        seed_scores: dict[str, float],
        edge_types: list[str] | None = None,
        decay: float = 0.6,
        max_hops: int = 3,
    ) -> dict[str, float]:
        """
        带衰减的分数传播算法
        用于：风险传导评分、概念热度扩散
        decay: 每跳衰减系数（0~1）
        """
        scores: dict[str, float] = dict(seed_scores)
        queue = deque([(nid, score, 0) for nid, score in seed_scores.items()])
        visited = set(seed_scores.keys())

        while queue:
            cur, score, hops = queue.popleft()
            if hops >= max_hops:
                continue
            for edge in self._adj.get(cur, []):
                if edge_types and edge.edge_type not in edge_types:
                    continue
                nxt = edge.target_id
                propagated = score * decay * edge.weight
                if nxt not in visited:
                    visited.add(nxt)
                    scores[nxt] = propagated
                    queue.append((nxt, propagated, hops + 1))
                else:
                    scores[nxt] = max(scores[nxt], propagated)

        # 排除种子节点，归一化
        for seed in seed_scores:
            scores.pop(seed, None)
        return dict(sorted(scores.items(), key=lambda x: x[1], reverse=True))

    def ownership_penetration(
        self,
        company_id: str,
        min_ratio: float = 0.05,
        max_depth: int = 5,
    ) -> dict[str, dict]:
        """
        股权穿透计算：计算间接持股比例
        返回：{股东ID: {ratio: 穿透比例, depth: 层级, path: 路径}}
        """
        result = {}

        def dfs(cur_id, accumulated_ratio, depth, path):
            if depth > max_depth:
                return
            for edge in self._radj.get(cur_id, []):
                if edge.edge_type != "holds":
                    continue
                holder_id = edge.source_id
                ratio = accumulated_ratio * edge.weight
                if ratio < min_ratio:
                    continue
                holder_path = path + [holder_id]
                if holder_id not in result or result[holder_id]["ratio"] < ratio:
                    result[holder_id] = {
                        "ratio": round(ratio, 4),
                        "depth": depth,
                        "path": holder_path,
                    }
                dfs(holder_id, ratio, depth + 1, holder_path)

        dfs(company_id, 1.0, 1, [company_id])
        return result

    # ── 持久化 ──────────────────────────────

    def save(self, path: str) -> None:
        with open(path, "wb") as f:
            pickle.dump({"nodes": self._nodes, "adj": dict(self._adj), "radj": dict(self._radj)}, f)

    def load(self, path: str) -> None:
        with open(path, "rb") as f:
            data = pickle.load(f)
        self._nodes = data["nodes"]
        self._adj = defaultdict(list, data["adj"])
        self._radj = defaultdict(list, data["radj"])
        self._type_index = defaultdict(set)
        for nid, node in self._nodes.items():
            self._type_index[node.node_type].add(nid)

    def export_json(self, path: str) -> None:
        """导出为JSON，供前端可视化使用"""
        nodes = [
            {"id": n.id, "label": n.label, "type": n.node_type, **n.properties}
            for n in self._nodes.values()
        ]
        edges = [
            {
                "source": e.source_id,
                "target": e.target_id,
                "type": e.edge_type,
                "weight": e.weight,
                **e.properties,
            }
            for edges in self._adj.values()
            for e in edges
        ]
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"nodes": nodes, "edges": edges}, f, ensure_ascii=False, indent=2)

    def stats(self) -> dict:
        type_counts = {t: len(ids) for t, ids in self._type_index.items()}
        edge_type_counts: dict[str, int] = defaultdict(int)
        for edges in self._adj.values():
            for e in edges:
                edge_type_counts[e.edge_type] += 1
        return {
            "node_count": self.node_count(),
            "edge_count": self.edge_count(),
            "node_types": type_counts,
            "edge_types": dict(edge_type_counts),
        }

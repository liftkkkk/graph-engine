"""
数据加载模块
支持：CSV / JSON 格式数据导入
数据来源：公开年报、Wind导出、东方财富等
"""

from __future__ import annotations
import csv
import json
from pathlib import Path
from core.graph_engine import GraphEngine, Node, Edge


class AStockDataLoader:
    """
    图谱数据加载器
    将结构化数据文件转换为图节点和边
    """

    def __init__(self, engine: GraphEngine):
        self.engine = engine

    # ── 公司基础数据 ────────────────────────

    def load_companies(self, filepath: str) -> int:
        """
        加载上市公司基础信息
        CSV格式：code,name,industry,market_cap,list_date
        """
        count = 0
        with open(filepath, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                node = Node(
                    id=f"stock_{row['code']}",
                    label=row["name"],
                    node_type="company",
                    properties={
                        "code": row["code"],
                        "industry": row.get("industry", ""),
                        "market_cap": float(row.get("market_cap", 0)),
                        "list_date": row.get("list_date", ""),
                    },
                )
                self.engine.add_node(node)
                count += 1
        return count

    # ── 股权关系 ────────────────────────────

    def load_shareholding(self, filepath: str) -> int:
        """
        加载股权关系数据
        CSV格式：holder_id,holder_name,holder_type,company_code,ratio
        holder_type: company / person / institution
        """
        count = 0
        with open(filepath, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                ratio = float(row["ratio"])
                holder_id = row["holder_id"]
                holder_type = row.get("holder_type", "person")

                # 自动创建持有方节点（如果不存在）
                if not self.engine.get_node(holder_id):
                    self.engine.add_node(Node(
                        id=holder_id,
                        label=row["holder_name"],
                        node_type=holder_type,
                        properties={"name": row["holder_name"]},
                    ))

                company_id = f"stock_{row['company_code']}"
                if self.engine.get_node(company_id):
                    self.engine.add_edge(Edge(
                        source_id=holder_id,
                        target_id=company_id,
                        edge_type="holds",
                        weight=ratio,
                        properties={"ratio": ratio},
                    ))
                    count += 1
        return count

    # ── 供应链关系 ──────────────────────────

    def load_supply_chain(self, filepath: str) -> int:
        """
        加载供应链关系
        CSV格式：supplier_code,customer_code,relation_type,importance
        relation_type: supplies / competes / partners
        importance: 0~1
        """
        count = 0
        with open(filepath, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                src = f"stock_{row['supplier_code']}"
                dst = f"stock_{row['customer_code']}"
                if self.engine.get_node(src) and self.engine.get_node(dst):
                    self.engine.add_edge(Edge(
                        source_id=src,
                        target_id=dst,
                        edge_type=row.get("relation_type", "supplies"),
                        weight=float(row.get("importance", 0.5)),
                        properties={"source": "supply_chain"},
                    ))
                    count += 1
        return count

    # ── 概念板块 ────────────────────────────

    def load_concepts(self, filepath: str) -> int:
        """
        加载概念板块归属关系
        CSV格式：concept_id,concept_name,stock_code,weight
        weight: 该股票在板块中的权重/代表性
        """
        count = 0
        concepts_seen: set[str] = set()

        with open(filepath, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                cid = f"concept_{row['concept_id']}"
                if cid not in concepts_seen:
                    self.engine.add_node(Node(
                        id=cid,
                        label=row["concept_name"],
                        node_type="concept",
                    ))
                    concepts_seen.add(cid)

                stock_id = f"stock_{row['stock_code']}"
                if self.engine.get_node(stock_id):
                    self.engine.add_edge(Edge(
                        source_id=stock_id,
                        target_id=cid,
                        edge_type="belongs_to",
                        weight=float(row.get("weight", 1.0)),
                    ))
                    count += 1
        return count

    # ── 高管任职 ────────────────────────────

    def load_executives(self, filepath: str) -> int:
        """
        加载高管任职关系
        CSV格式：person_id,person_name,company_code,title,start_date,end_date
        """
        count = 0
        persons_seen: set[str] = set()

        with open(filepath, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                pid = f"person_{row['person_id']}"
                if pid not in persons_seen:
                    self.engine.add_node(Node(
                        id=pid,
                        label=row["person_name"],
                        node_type="person",
                        properties={"name": row["person_name"]},
                    ))
                    persons_seen.add(pid)

                company_id = f"stock_{row['company_code']}"
                if self.engine.get_node(company_id):
                    is_current = not row.get("end_date", "").strip()
                    self.engine.add_edge(Edge(
                        source_id=pid,
                        target_id=company_id,
                        edge_type="employs",
                        weight=1.0,
                        properties={
                            "title": row.get("title", ""),
                            "start_date": row.get("start_date", ""),
                            "end_date": row.get("end_date", ""),
                            "is_current": is_current,
                        },
                    ))
                    count += 1
        return count

    # ── 示例数据生成 ─────────────────────────

    @staticmethod
    def create_sample_data(output_dir: str) -> None:
        """生成用于测试的示例数据集（新能源产业链）"""
        d = Path(output_dir)
        d.mkdir(parents=True, exist_ok=True)

        # 公司
        companies = [
            ["code", "name", "industry", "market_cap"],
            ["300750", "宁德时代", "新能源", "12000"],
            ["002594", "比亚迪", "新能源汽车", "8000"],
            ["601899", "紫金矿业", "有色金属", "3500"],
            ["002460", "赣锋锂业", "锂资源", "2000"],
            ["000100", "TCL科技", "消费电子", "1800"],
            ["688036", "传音控股", "消费电子", "900"],
            ["300014", "亿纬锂能", "锂电池", "1500"],
            ["603659", "璞泰来", "锂电材料", "800"],
            ["300438", "鹏辉能源", "储能", "600"],
            ["002074", "国轩高科", "锂电池", "700"],
        ]

        # 股权关系
        shareholding = [
            ["holder_id", "holder_name", "holder_type", "company_code", "ratio"],
            ["person_wcy", "王传福", "person", "002594", "0.172"],
            ["inst_hk", "香港中央结算", "institution", "300750", "0.086"],
            ["inst_hk", "香港中央结算", "institution", "002594", "0.072"],
            ["company_catl_hk", "宁德时代香港子公司", "company", "002074", "0.238"],
            ["person_zengyq", "曾毓群", "person", "300750", "0.243"],
            ["inst_fund1", "易方达基金", "institution", "300750", "0.032"],
            ["inst_fund1", "易方达基金", "institution", "002594", "0.021"],
        ]

        # 供应链
        supply_chain = [
            ["supplier_code", "customer_code", "relation_type", "importance"],
            ["002460", "300750", "supplies", "0.9"],   # 赣锋 → 宁德
            ["002460", "002594", "supplies", "0.7"],   # 赣锋 → 比亚迪
            ["601899", "002460", "supplies", "0.8"],   # 紫金 → 赣锋（上游）
            ["300750", "002594", "supplies", "0.6"],   # 宁德 → 比亚迪（电池）
            ["603659", "300750", "supplies", "0.85"],  # 璞泰来 → 宁德（涂覆膜）
            ["603659", "300014", "supplies", "0.75"],  # 璞泰来 → 亿纬
            ["300014", "002594", "supplies", "0.5"],   # 亿纬 → 比亚迪
        ]

        # 概念板块
        concepts = [
            ["concept_id", "concept_name", "stock_code", "weight"],
            ["nev", "新能源汽车", "002594", "1.0"],
            ["nev", "新能源汽车", "300750", "0.9"],
            ["nev", "新能源汽车", "300014", "0.7"],
            ["nev", "新能源汽车", "002074", "0.6"],
            ["battery", "锂电池", "300750", "1.0"],
            ["battery", "锂电池", "300014", "0.85"],
            ["battery", "锂电池", "002074", "0.8"],
            ["battery", "锂电池", "300438", "0.7"],
            ["lithium", "锂资源", "002460", "1.0"],
            ["lithium", "锂资源", "601899", "0.7"],
            ["energy_storage", "储能", "300438", "1.0"],
            ["energy_storage", "储能", "300014", "0.8"],
            ["energy_storage", "储能", "300750", "0.75"],
        ]

        # 高管
        executives = [
            ["person_id", "person_name", "company_code", "title", "start_date", "end_date"],
            ["wcy", "王传福", "002594", "董事长", "1995-01-01", ""],
            ["zyq", "曾毓群", "300750", "董事长", "2011-01-01", ""],
            ["lhj", "李河君", "000100", "董事长", "2012-01-01", "2019-06-01"],
            ["lxy", "李想", "002594", "前产品总监", "2010-01-01", "2015-06-01"],
        ]

        def write_csv(name, rows):
            with open(d / name, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerows(rows)

        write_csv("companies.csv", companies)
        write_csv("shareholding.csv", shareholding)
        write_csv("supply_chain.csv", supply_chain)
        write_csv("concepts.csv", concepts)
        write_csv("executives.csv", executives)
        print(f"示例数据已生成至 {output_dir}")

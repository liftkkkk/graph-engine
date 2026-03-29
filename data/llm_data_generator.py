"""
大模型模拟数据生成器
用LLM生成高真实感的市场数据：公司、股权、供应链、概念、高管
无需真实数据源，用于产品Demo和功能验证
"""

from __future__ import annotations
import json
import csv
import random
from pathlib import Path


# ─────────────────────────────────────────
# 硬编码的高质量模拟数据
# 覆盖：新能源、半导体、医药、消费、金融五大板块
# 数据参考真实市场结构，具备足够的真实感
# ─────────────────────────────────────────

COMPANIES = [
    # 新能源产业链
    {"code": "300750", "name": "宁德时代", "industry": "锂电池", "market_cap": 12000, "sector": "新能源"},
    {"code": "002594", "name": "比亚迪",   "industry": "新能源汽车", "market_cap": 8500, "sector": "新能源"},
    {"code": "002460", "name": "赣锋锂业", "industry": "锂资源", "market_cap": 2100, "sector": "新能源"},
    {"code": "601899", "name": "紫金矿业", "industry": "有色金属", "market_cap": 3500, "sector": "新能源"},
    {"code": "300014", "name": "亿纬锂能", "industry": "锂电池", "market_cap": 1500, "sector": "新能源"},
    {"code": "603659", "name": "璞泰来",   "industry": "锂电材料", "market_cap": 820,  "sector": "新能源"},
    {"code": "002074", "name": "国轩高科", "industry": "锂电池", "market_cap": 720,  "sector": "新能源"},
    {"code": "300438", "name": "鹏辉能源", "industry": "储能", "market_cap": 610,  "sector": "新能源"},
    {"code": "688567", "name": "孚能科技", "industry": "动力电池", "market_cap": 480,  "sector": "新能源"},
    {"code": "002709", "name": "天赐材料", "industry": "电解液", "market_cap": 560,  "sector": "新能源"},
    {"code": "301358", "name": "华盛锂电", "industry": "电解液", "market_cap": 290,  "sector": "新能源"},
    {"code": "688336", "name": "七彩化学", "industry": "锂电材料", "market_cap": 210,  "sector": "新能源"},

    # 半导体产业链
    {"code": "603501", "name": "韦尔股份", "industry": "半导体设计", "market_cap": 1800, "sector": "半导体"},
    {"code": "002049", "name": "紫光国微", "industry": "半导体", "market_cap": 1200, "sector": "半导体"},
    {"code": "688981", "name": "中芯国际", "industry": "晶圆代工", "market_cap": 4200, "sector": "半导体"},
    {"code": "300661", "name": "圣邦股份", "industry": "模拟芯片", "market_cap": 780,  "sector": "半导体"},
    {"code": "688012", "name": "中微公司", "industry": "半导体设备", "market_cap": 1100, "sector": "半导体"},
    {"code": "688041", "name": "海光信息", "industry": "CPU芯片", "market_cap": 2300, "sector": "半导体"},
    {"code": "688396", "name": "华润微",   "industry": "功率半导体", "market_cap": 890,  "sector": "半导体"},
    {"code": "300782", "name": "卓胜微",   "industry": "射频芯片", "market_cap": 650,  "sector": "半导体"},
    {"code": "688185", "name": "康希通信", "industry": "射频器件", "market_cap": 320,  "sector": "半导体"},
    {"code": "688819", "name": "天岳先进", "industry": "碳化硅", "market_cap": 280,  "sector": "半导体"},

    # 医药产业链
    {"code": "600276", "name": "恒瑞医药", "industry": "创新药", "market_cap": 2800, "sector": "医药"},
    {"code": "300760", "name": "迈瑞医疗", "industry": "医疗器械", "market_cap": 3200, "sector": "医药"},
    {"code": "688180", "name": "君实生物", "industry": "生物药", "market_cap": 680,  "sector": "医药"},
    {"code": "688389", "name": "普蕊斯",   "industry": "CRO", "market_cap": 210,  "sector": "医药"},
    {"code": "300347", "name": "泰格医药", "industry": "CRO", "market_cap": 890,  "sector": "医药"},
    {"code": "603259", "name": "药明康德", "industry": "CDMO", "market_cap": 2100, "sector": "医药"},
    {"code": "300601", "name": "康泰生物", "industry": "疫苗", "market_cap": 780,  "sector": "医药"},
    {"code": "688321", "name": "荣昌生物", "industry": "生物药", "market_cap": 460,  "sector": "医药"},

    # 消费互联网
    {"code": "600519", "name": "贵州茅台", "industry": "白酒", "market_cap": 22000, "sector": "消费"},
    {"code": "000858", "name": "五粮液",   "industry": "白酒", "market_cap": 6800, "sector": "消费"},
    {"code": "600887", "name": "伊利股份", "industry": "乳制品", "market_cap": 1800, "sector": "消费"},
    {"code": "603288", "name": "海天味业", "industry": "调味品", "market_cap": 2100, "sector": "消费"},
    {"code": "002304", "name": "洋河股份", "industry": "白酒", "market_cap": 1700, "sector": "消费"},
    {"code": "000568", "name": "泸州老窖", "industry": "白酒", "market_cap": 1900, "sector": "消费"},

    # 金融
    {"code": "601318", "name": "中国平安", "industry": "保险", "market_cap": 8800, "sector": "金融"},
    {"code": "600036", "name": "招商银行", "industry": "银行", "market_cap": 9200, "sector": "金融"},
    {"code": "601688", "name": "华泰证券", "industry": "证券", "market_cap": 1800, "sector": "金融"},
    {"code": "600030", "name": "中信证券", "industry": "证券", "market_cap": 2400, "sector": "金融"},

    # AI/科技
    {"code": "002415", "name": "海康威视", "industry": "智能安防", "market_cap": 3200, "sector": "科技"},
    {"code": "300750", "name": "科大讯飞", "industry": "AI语音", "market_cap": 980,  "sector": "科技"},
    {"code": "688111", "name": "金山办公", "industry": "办公软件", "market_cap": 1200, "sector": "科技"},
    {"code": "300459", "name": "汤姆猫",   "industry": "移动游戏", "market_cap": 320,  "sector": "科技"},
]

# 去重（code唯一）
_seen_codes = set()
_deduped = []
for c in COMPANIES:
    if c["code"] not in _seen_codes:
        _seen_codes.add(c["code"])
        _deduped.append(c)
COMPANIES = _deduped


SHAREHOLDERS = [
    # 新能源核心人物
    {"holder_id": "person_zyq",  "holder_name": "曾毓群",  "holder_type": "person",      "company_code": "300750", "ratio": 0.243},
    {"holder_id": "person_wcy",  "holder_name": "王传福",  "holder_type": "person",      "company_code": "002594", "ratio": 0.172},
    {"holder_id": "person_lhm",  "holder_name": "李春梅",  "holder_type": "person",      "company_code": "002460", "ratio": 0.089},
    {"holder_id": "person_clh",  "holder_name": "陈李花",  "holder_type": "person",      "company_code": "002460", "ratio": 0.043},
    # 机构持股
    {"holder_id": "inst_hk",     "holder_name": "香港中央结算", "holder_type": "institution", "company_code": "300750", "ratio": 0.086},
    {"holder_id": "inst_hk",     "holder_name": "香港中央结算", "holder_type": "institution", "company_code": "002594", "ratio": 0.072},
    {"holder_id": "inst_hk",     "holder_name": "香港中央结算", "holder_type": "institution", "company_code": "688981", "ratio": 0.091},
    {"holder_id": "inst_hk",     "holder_name": "香港中央结算", "holder_type": "institution", "company_code": "600519", "ratio": 0.095},
    {"holder_id": "inst_efunda", "holder_name": "易方达基金",   "holder_type": "institution", "company_code": "300750", "ratio": 0.032},
    {"holder_id": "inst_efunda", "holder_name": "易方达基金",   "holder_type": "institution", "company_code": "002594", "ratio": 0.021},
    {"holder_id": "inst_efunda", "holder_name": "易方达基金",   "holder_type": "institution", "company_code": "600519", "ratio": 0.041},
    {"holder_id": "inst_efunda", "holder_name": "易方达基金",   "holder_type": "institution", "company_code": "600276", "ratio": 0.028},
    {"holder_id": "inst_harvest","holder_name": "嘉实基金",     "holder_type": "institution", "company_code": "688981", "ratio": 0.018},
    {"holder_id": "inst_harvest","holder_name": "嘉实基金",     "holder_type": "institution", "company_code": "603501", "ratio": 0.024},
    {"holder_id": "inst_harvest","holder_name": "嘉实基金",     "holder_type": "institution", "company_code": "300760", "ratio": 0.031},
    # 公司间持股（交叉持股/战略投资）
    {"holder_id": "stock_300750","holder_name": "宁德时代",     "holder_type": "company",     "company_code": "002074", "ratio": 0.238},
    {"holder_id": "stock_300750","holder_name": "宁德时代",     "holder_type": "company",     "company_code": "688567", "ratio": 0.156},
    {"holder_id": "stock_002594","holder_name": "比亚迪",       "holder_type": "company",     "company_code": "301358", "ratio": 0.112},
    # 半导体
    {"holder_id": "person_yuzj", "holder_name": "虞仁荣",  "holder_type": "person",      "company_code": "300782", "ratio": 0.412},
    {"holder_id": "person_zhangf","holder_name": "张汝京", "holder_type": "person",      "company_code": "688981", "ratio": 0.002},
    {"holder_id": "inst_sd",     "holder_name": "国家大基金二期","holder_type": "institution","company_code": "688981", "ratio": 0.089},
    {"holder_id": "inst_sd",     "holder_name": "国家大基金二期","holder_type": "institution","company_code": "688012", "ratio": 0.124},
    {"holder_id": "inst_sd",     "holder_name": "国家大基金二期","holder_type": "institution","company_code": "688396", "ratio": 0.098},
    {"holder_id": "inst_sd",     "holder_name": "国家大基金二期","holder_type": "institution","company_code": "688819", "ratio": 0.076},
    # 医药
    {"holder_id": "person_sunp", "holder_name": "孙飘扬",  "holder_type": "person",      "company_code": "600276", "ratio": 0.218},
    {"holder_id": "person_liy",  "holder_name": "李宇",    "holder_type": "person",      "company_code": "603259", "ratio": 0.045},
    # 消费
    {"holder_id": "inst_moutai_gov","holder_name": "茅台集团","holder_type": "institution","company_code": "600519", "ratio": 0.541},
    {"holder_id": "person_ding",  "holder_name": "丁世家",  "holder_type": "person",      "company_code": "603288", "ratio": 0.368},
]


SUPPLY_CHAIN = [
    # 新能源完整链条：矿 → 材料 → 电池 → 整车
    {"supplier_code": "601899", "customer_code": "002460", "relation_type": "supplies", "importance": 0.82},
    {"supplier_code": "601899", "customer_code": "002709", "relation_type": "supplies", "importance": 0.45},
    {"supplier_code": "002460", "customer_code": "300750", "relation_type": "supplies", "importance": 0.91},
    {"supplier_code": "002460", "customer_code": "002594", "relation_type": "supplies", "importance": 0.68},
    {"supplier_code": "002460", "customer_code": "300014", "relation_type": "supplies", "importance": 0.55},
    {"supplier_code": "002709", "customer_code": "300750", "relation_type": "supplies", "importance": 0.88},
    {"supplier_code": "002709", "customer_code": "300014", "relation_type": "supplies", "importance": 0.72},
    {"supplier_code": "002709", "customer_code": "002074", "relation_type": "supplies", "importance": 0.61},
    {"supplier_code": "603659", "customer_code": "300750", "relation_type": "supplies", "importance": 0.87},
    {"supplier_code": "603659", "customer_code": "300014", "relation_type": "supplies", "importance": 0.76},
    {"supplier_code": "603659", "customer_code": "002074", "relation_type": "supplies", "importance": 0.58},
    {"supplier_code": "688336", "customer_code": "603659", "relation_type": "supplies", "importance": 0.71},
    {"supplier_code": "300750", "customer_code": "002594", "relation_type": "supplies", "importance": 0.65},
    {"supplier_code": "300014", "customer_code": "002594", "relation_type": "supplies", "importance": 0.42},
    {"supplier_code": "300014", "customer_code": "688567", "relation_type": "supplies", "importance": 0.38},
    {"supplier_code": "002074", "customer_code": "002594", "relation_type": "supplies", "importance": 0.31},
    {"supplier_code": "300438", "customer_code": "002594", "relation_type": "supplies", "importance": 0.29},
    {"supplier_code": "688567", "customer_code": "002594", "relation_type": "supplies", "importance": 0.44},
    {"supplier_code": "301358", "customer_code": "300750", "relation_type": "supplies", "importance": 0.52},
    {"supplier_code": "301358", "customer_code": "002594", "relation_type": "supplies", "importance": 0.61},

    # 半导体链条：材料 → 设备 → 代工 → 设计
    {"supplier_code": "688819", "customer_code": "688981", "relation_type": "supplies", "importance": 0.76},
    {"supplier_code": "688012", "customer_code": "688981", "relation_type": "supplies", "importance": 0.89},
    {"supplier_code": "688396", "customer_code": "688041", "relation_type": "supplies", "importance": 0.54},
    {"supplier_code": "688981", "customer_code": "603501", "relation_type": "supplies", "importance": 0.72},
    {"supplier_code": "688981", "customer_code": "300661", "relation_type": "supplies", "importance": 0.68},
    {"supplier_code": "688981", "customer_code": "002049", "relation_type": "supplies", "importance": 0.61},
    {"supplier_code": "688981", "customer_code": "300782", "relation_type": "supplies", "importance": 0.55},
    {"supplier_code": "688185", "customer_code": "300782", "relation_type": "supplies", "importance": 0.82},

    # 医药链条：CRO/CDMO → 药企
    {"supplier_code": "603259", "customer_code": "600276", "relation_type": "supplies", "importance": 0.58},
    {"supplier_code": "603259", "customer_code": "688180", "relation_type": "supplies", "importance": 0.72},
    {"supplier_code": "603259", "customer_code": "688321", "relation_type": "supplies", "importance": 0.65},
    {"supplier_code": "300347", "customer_code": "600276", "relation_type": "supplies", "importance": 0.45},
    {"supplier_code": "300347", "customer_code": "688180", "relation_type": "supplies", "importance": 0.51},
    {"supplier_code": "688389", "customer_code": "300347", "relation_type": "supplies", "importance": 0.38},

    # 竞争关系
    {"supplier_code": "300750", "customer_code": "300014", "relation_type": "competes", "importance": 0.85},
    {"supplier_code": "300750", "customer_code": "002074", "relation_type": "competes", "importance": 0.78},
    {"supplier_code": "600519", "customer_code": "000858", "relation_type": "competes", "importance": 0.92},
    {"supplier_code": "600519", "customer_code": "002304", "relation_type": "competes", "importance": 0.88},
    {"supplier_code": "688981", "customer_code": "002049", "relation_type": "competes", "importance": 0.71},
]


CONCEPTS = [
    # 新能源
    ("nev",          "新能源汽车",  [("002594",1.0),("300750",0.92),("300014",0.75),("002074",0.68),("688567",0.61),("300438",0.55)]),
    ("battery",      "锂电池",      [("300750",1.0),("300014",0.88),("002074",0.82),("300438",0.74),("688567",0.71),("002594",0.65)]),
    ("lithium",      "锂资源",      [("002460",1.0),("601899",0.78),("688336",0.52)]),
    ("cathode",      "正极材料",    [("603659",1.0),("688336",0.82),("301358",0.71)]),
    ("electrolyte",  "电解液",      [("002709",1.0),("301358",0.86)]),
    ("energy_storage","储能",       [("300438",1.0),("300014",0.85),("300750",0.78),("002074",0.72)]),
    ("solid_battery","固态电池",    [("300750",0.92),("688567",0.88),("300014",0.81)]),
    # 半导体
    ("chip",         "芯片",        [("688981",1.0),("688041",0.91),("002049",0.85),("603501",0.82),("300782",0.78)]),
    ("sic",          "碳化硅",      [("688819",1.0),("688396",0.88),("002049",0.72)]),
    ("semi_equip",   "半导体设备",  [("688012",1.0)]),
    ("analog_chip",  "模拟芯片",    [("300661",1.0),("603501",0.72)]),
    ("rf_chip",      "射频芯片",    [("300782",1.0),("688185",0.91)]),
    # 医药
    ("innovative_drug","创新药",    [("600276",1.0),("688180",0.88),("688321",0.82),("300601",0.61)]),
    ("cro",          "CRO",         [("603259",1.0),("300347",0.91),("688389",0.78)]),
    ("medical_device","医疗器械",   [("300760",1.0)]),
    ("vaccine",      "疫苗",        [("300601",1.0),("688180",0.72)]),
    # 消费
    ("baijiu",       "白酒",        [("600519",1.0),("000858",0.88),("002304",0.82),("000568",0.79)]),
    ("condiment",    "调味品",      [("603288",1.0)]),
    # AI/科技
    ("ai",           "人工智能",    [("002415",0.88),("688111",0.72),("300750",0.65)]),
    # 跨板块联动概念（关键：用于发现隐性关联）
    ("carbon_neutral","碳中和",     [("300750",0.91),("002594",0.88),("300438",0.85),("601899",0.72),("002460",0.68)]),
    ("data_security","数据安全",    [("002049",0.88),("002415",0.82),("688111",0.71)]),
    ("state_owned",  "央国企改革",  [("601318",0.92),("600036",0.88),("601899",0.85),("688981",0.78)]),
]


EXECUTIVES = [
    # 新能源
    {"person_id": "zyq",   "person_name": "曾毓群", "company_code": "300750", "title": "董事长兼CEO", "start_date": "2011-01-01", "end_date": ""},
    {"person_id": "wcy",   "person_name": "王传福", "company_code": "002594", "title": "董事长", "start_date": "1995-01-01", "end_date": ""},
    {"person_id": "lhm",   "person_name": "李春梅", "company_code": "002460", "title": "董事长", "start_date": "2016-03-01", "end_date": ""},
    {"person_id": "wxy",   "person_name": "吴晓宇", "company_code": "300750", "title": "CFO", "start_date": "2018-06-01", "end_date": ""},
    {"person_id": "wxy",   "person_name": "吴晓宇", "company_code": "002074", "title": "独立董事", "start_date": "2021-01-01", "end_date": ""},
    # 关键：同一人在多家公司任职（隐性关联信号）
    {"person_id": "zhangl","person_name": "张磊（高瓴）","company_code": "300750", "title": "董事", "start_date": "2021-06-01", "end_date": "2023-06-01"},
    {"person_id": "zhangl","person_name": "张磊（高瓴）","company_code": "688180", "title": "董事", "start_date": "2020-01-01", "end_date": ""},
    {"person_id": "zhangl","person_name": "张磊（高瓴）","company_code": "603259", "title": "董事", "start_date": "2019-06-01", "end_date": ""},
    # 半导体
    {"person_id": "yuzj",  "person_name": "虞仁荣", "company_code": "300782", "title": "董事长", "start_date": "2012-01-01", "end_date": ""},
    {"person_id": "yuzj",  "person_name": "虞仁荣", "company_code": "688185", "title": "实控人关联", "start_date": "2020-06-01", "end_date": ""},
    {"person_id": "liuqh", "person_name": "刘庆华", "company_code": "688012", "title": "董事长", "start_date": "2014-01-01", "end_date": ""},
    {"person_id": "liuqh", "person_name": "刘庆华", "company_code": "688819", "title": "独立董事", "start_date": "2022-03-01", "end_date": ""},
    # 医药
    {"person_id": "sunp",  "person_name": "孙飘扬", "company_code": "600276", "title": "董事长", "start_date": "1995-01-01", "end_date": ""},
    {"person_id": "sunp",  "person_name": "孙飘扬", "company_code": "688321", "title": "战略顾问", "start_date": "2021-01-01", "end_date": ""},
    {"person_id": "liy",   "person_name": "李宇",   "company_code": "603259", "title": "联席CEO", "start_date": "2017-01-01", "end_date": ""},
    {"person_id": "liy",   "person_name": "李宇",   "company_code": "300347", "title": "独立董事", "start_date": "2020-06-01", "end_date": ""},
    # 消费
    {"person_id": "ding",  "person_name": "丁世家", "company_code": "603288", "title": "董事长", "start_date": "2000-01-01", "end_date": ""},
    # 离职高管（用于人事变动信号）
    {"person_id": "chenwh","person_name": "陈伟鸿", "company_code": "300750", "title": "前副总裁", "start_date": "2015-01-01", "end_date": "2022-09-01"},
    {"person_id": "chenwh","person_name": "陈伟鸿", "company_code": "688567", "title": "CEO", "start_date": "2022-10-01", "end_date": ""},
]


def generate_all(output_dir: str) -> None:
    """生成所有模拟数据CSV文件"""
    d = Path(output_dir)
    d.mkdir(parents=True, exist_ok=True)

    # companies.csv
    with open(d / "companies.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["code","name","industry","market_cap","sector"])
        w.writeheader()
        w.writerows(COMPANIES)

    # shareholding.csv
    with open(d / "shareholding.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["holder_id","holder_name","holder_type","company_code","ratio"])
        w.writeheader()
        w.writerows(SHAREHOLDERS)

    # supply_chain.csv
    with open(d / "supply_chain.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["supplier_code","customer_code","relation_type","importance"])
        w.writeheader()
        w.writerows(SUPPLY_CHAIN)

    # concepts.csv
    rows = []
    for cid, cname, members in CONCEPTS:
        for code, weight in members:
            rows.append({"concept_id": cid, "concept_name": cname, "stock_code": code, "weight": weight})
    with open(d / "concepts.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["concept_id","concept_name","stock_code","weight"])
        w.writeheader()
        w.writerows(rows)

    # executives.csv
    with open(d / "executives.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["person_id","person_name","company_code","title","start_date","end_date"])
        w.writeheader()
        w.writerows(EXECUTIVES)

    counts = {
        "公司": len(COMPANIES),
        "股权关系": len(SHAREHOLDERS),
        "供应链关系": len(SUPPLY_CHAIN),
        "概念归属": sum(len(m) for _,_,m in CONCEPTS),
        "高管任职": len(EXECUTIVES),
    }
    print("模拟数据已生成:")
    for k, v in counts.items():
        print(f"  {k}: {v} 条")
    return counts


if __name__ == "__main__":
    generate_all("sample_v2")

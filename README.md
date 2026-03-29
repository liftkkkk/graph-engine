# 关系图谱引擎（v2）

基于确定性图推理的关系图谱分析系统。本地运行、结果可解释，可用于快速做概念传导、供应链穿透、股权穿透、高管网络，以及“隐性关联”结构信号挖掘。

## 功能概览

- 图谱构建：多类型节点/边（公司/人物/机构/概念），支持加权有向图
- 四大分析模块：
  - 概念传导：板块/概念启动后的受益标的排序
  - 供应链穿透：上下游多跳扩散、瓶颈节点识别、关键受益方
  - 股权穿透：实控人识别、穿透持股计算、一致行动人线索
  - 高管网络：跨公司任职链条、关键人物影响圈
- 核心差异化：隐性关联发现（股权桥梁 / 供应链单点风险 / 高管隐性网络 / 跨板块概念共振）
- 可视化：
  - 导出离线交互式 HTML（浏览器直接打开）
  - Web 服务（Flask + D3 前端），支持查询/路径/板块/隐性关联/数据管理

## 目录结构

```
astock_graph/
├── app.py                  # Web 服务入口（Flask）
├── main_v2.py               # v2 主程序（生成数据 + 构建图谱 + demo + 导出）
├── core/
│   └── graph_engine.py      # 图引擎核心（节点/边/算法）
├── data/
│   ├── loader.py            # CSV 数据加载
│   ├── llm_data_generator.py# Demo 数据生成（写入 data/sample_v2）
│   ├── sample_v2/           # v2 demo 数据（运行时会自动生成/刷新）
│   └── user/                # 用户自定义数据（Web 上传后写入）
├── modules/
│   ├── analyzers.py         # 四大分析模块
│   └── hidden_connections.py# 隐性关联发现 + 影响力(PageRank)等
├── visualization/
│   └── html_export.py       # HTML 图谱导出
├── static/                  # Web 前端静态资源（D3）
└── output/                  # 输出（HTML/JSON/PKL）
```

## 快速开始（命令行 / 离线导出）

环境要求：Python 3.9+（图引擎与分析核心为纯标准库实现）

在项目根目录运行：

```bash
python main_v2.py
```

运行后会：

- 生成/刷新 demo 数据到 `data/sample_v2/`
- 构建图谱并打印统计信息
- 依次演示：隐性关联、影响力排名、概念传导、供应链穿透、股权穿透、高管网络
- 导出结果到 `output/`：
  - `output/graph_v2.html`（浏览器打开交互查看）
  - `output/graph_v2.json`
  - `output/graph_v2.pkl`

## 启动 Web 服务（交互查询 + 数据管理）

Web 服务依赖 Flask（图引擎本身不依赖第三方库）：

```bash
pip install flask
python app.py
```

访问：

- `http://localhost:5000/`：图谱可视化与查询
- `http://localhost:5000/data.html`：数据管理（上传/预览/删除/重置）

## 自定义数据（CSV）

Web 模式下支持导入自定义 CSV，文件会保存到 `data/user/`，并自动重建图谱；若 `data/user/` 下存在任意 `*.csv`，系统会优先使用用户数据，否则使用 `data/sample_v2/` 的 demo 数据。

五类数据文件（文件名必须匹配）：

- `companies.csv`：上市公司基础信息  
  必填：`code,name`  
  字段：`code,name,industry,market_cap,sector`
- `shareholding.csv`：股权关系（holder → company）  
  必填：`holder_id,holder_name,holder_type,company_code,ratio`
- `supply_chain.csv`：供应链关系（supplier → customer）  
  必填：`supplier_code,customer_code,relation_type`
- `concepts.csv`：概念板块归属（stock → concept）  
  必填：`concept_id,concept_name,stock_code`
- `executives.csv`：高管任职（person → company）  
  必填：`person_id,person_name,company_code,title`

也可通过 API 下载模板与导入（节选）：

- `GET /api/data/download/<data_type>`：下载 CSV 模板
- `POST /api/data/upload/<data_type>`：上传（multipart file 或 JSON csv_text）
- `DELETE /api/data/delete/<data_type>`：删除某类用户数据
- `POST /api/data/reset`：清空用户数据并回退到 demo

## 常用 API（节选）

- `GET /api/search?q=关键字`：按公司名/代码搜索
- `GET /api/company/<company_id>`：公司信息 + 一跳邻居
- `GET /api/path?from=...&to=...&edge_types=supplies,holds`：两点关联路径
- `GET /api/concepts`：概念列表
- `GET /api/concept/<concept_id>`：概念传导分析
- `GET /api/supply_chain/<company_id>`：供应链穿透分析
- `GET /api/shareholders/<company_id>`：股权穿透分析
- `GET /api/hidden?company_id=<可选>`：隐性关联发现
- `GET /api/influence`：影响力(PageRank)排名（基于 supplies + holds）

## 进一步阅读

更完整的原理、数据模型与算法说明见：[技术实现文档.md](file:///c:/Users/z1881/Downloads/astock_graph_v6/astock_graph/技术实现文档.md)

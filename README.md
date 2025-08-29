# Text2SQL

基于RAG架构的自然语言转SQL系统，使用离线处理构建知识库，在线处理实现查询转换和SQL生成。

## 特性

- **RAG架构**: 使用检索增强生成，提高SQL生成准确性
- **离线处理**: 自动同步数据库元数据，构建向量知识库
- **在线处理**: 实时查询解析、上下文检索、SQL生成和验证
- **自我修正**: SQL执行失败时自动修正
- **多模态输入**: 支持CLI、API和交互式界面
- **中文优化**: 使用BGE-large-zh中文嵌入模型

## 技术栈

- **Python 3.10+**
- **LLM**: Ollama (deepseek-r1:32b)
- **嵌入模型**: BGE-large-zh
- **向量数据库**: ChromaDB
- **框架**: LangChain, langgraph
- **SQL解析**: sqlglot
- **Web框架**: FastAPI

## 快速开始

### 1. 安装依赖

```bash
pip install -e .
```

### 2. 配置环境

复制环境变量模板：
```bash
cp .env.example .env
```

编辑 `.env` 文件，配置数据库连接等信息：
```env
# 数据库配置
DB_TYPE=mysql
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=your_password
DB_NAME=your_database

# LLM配置
LLM_MODEL_NAME=deepseek-r1:32b
LLM_BASE_URL=http://localhost:11434
```

### 3. 启动Ollama服务

确保Ollama已安装并启动：
```bash
# 启动Ollama服务
ollama serve

# 下载模型
ollama pull deepseek-r1:32b
```

### 4. 构建知识库

```bash
# 从数据库构建知识库
text2sql build

# 强制重建
text2sql build --force

# 添加业务规则
text2sql build --business-rules business_rules.json
```

### 5. 查询转换

```bash
# CLI查询
text2sql query "上周客单价最高的门店是哪个？"

# 显示中间结果
text2sql query "统计每个城市的用户数量" --show-intermediate

# 交互模式
text2sql interactive
```

## 使用示例

### CLI使用

```bash
# 查看数据库schema
text2sql schema

# 查看特定表
text2sql schema --table-name orders

# 验证SQL
text2sql validate "SELECT * FROM users WHERE id = 1"

# 添加业务规则
text2sql add-rule "客单价" "SUM(payment_amount) / COUNT(DISTINCT user_id)"

# 导出知识库
text2sql export knowledge_backup.json

# 查看系统状态
text2sql stats
```

### API使用

启动API服务：
```bash
# 使用uvicorn启动
uvicorn text2sql.api.main:app --reload

# 或使用CLI
text2sql api
```

API文档访问：http://localhost:8000/docs

Python客户端示例：
```python
import requests

# 查询转换
response = requests.post(
    "http://localhost:8000/query",
    json={"query": "上周销售额最高的产品是什么？"}
)
result = response.json()
print(result["sql"])

# 构建知识库
requests.post("http://localhost:8000/build", json={"force_rebuild": True})
```

### Python库使用

```python
from text2sql import Text2SQL

# 初始化
text2sql = Text2SQL(database_url="mysql://user:pass@localhost/db")

# 构建知识库
text2sql.build_knowledge_base()

# 查询转换
result = text2sql.query_to_sql("统计每个订单的平均金额")
print(result["sql"])
print(result["execution_results"])

# 添加业务规则
text2sql.add_business_rule("GMV", "SUM(payment_amount)")
```

## 架构说明

### 离线处理

1. **元数据同步**: 自动提取数据库表结构、字段、注释、关系等信息
2. **知识库构建**: 将元数据分块、向量化，存储到向量数据库

### 在线处理

1. **查询解析**: 提取查询意图、实体、时间范围等信息
2. **RAG检索**: 根据查询检索相关的表结构信息
3. **提示构建**: 构建包含上下文的提示词
4. **SQL生成**: 使用LLM生成SQL查询
5. **验证修正**: 验证SQL语法和执行，失败时自动修正

## 配置说明

### 业务规则示例 (business_rules.json)

```json
{
  "general_terms": {
    "客单价": "平均每个用户的消费金额",
    "GMV": "商品交易总额"
  },
  "metrics": {
    "客单价": "SUM(payment_amount) / COUNT(DISTINCT user_id)",
    "GMV": "SUM(payment_amount)"
  },
  "table_terms": {
    "orders": {
      "支付成功": "order_status = 1",
      "已取消": "order_status = 3"
    }
  },
  "calculations": {
    "同比增长率": "(本期值 - 上期值) / 上期值 * 100"
  }
}
```

## 开发指南

### 项目结构

```
text2sql/
├── text2sql/
│   ├── core/          # 核心组件 (LLM, 嵌入, 向量DB)
│   ├── offline/       # 离线处理 (元数据同步, 知识库构建)
│   ├── online/        # 在线处理 (查询解析, RAG, SQL生成)
│   ├── api/           # Web API
│   └── cli.py         # 命令行界面
├── tests/             # 测试用例
└── examples/          # 示例代码
```

### 运行测试

```bash
# 安装测试依赖
pip install -e ".[dev]"

# 运行测试
pytest

# 运行特定测试
pytest tests/test_query.py -v
```

## 性能优化

1. **向量检索优化**: 调整 `RAG_TOP_K` 和 `RAG_SCORE_THRESHOLD`
2. **LLM配置**: 根据需求调整温度和最大输出长度
3. **缓存策略**: 启用查询结果缓存
4. **批量处理**: 批量构建知识库

## 常见问题

### Q: 如何处理大型数据库？

A: 可以通过以下方式优化：
- 只同步必要的表
- 调整分块大小
- 使用更强大的向量数据库

### Q: SQL生成不准确怎么办？

A: 可以：
- 添加更详细的表注释
- 定义明确的业务规则
- 提供few-shot示例
- 增加修正尝试次数

### Q: 支持哪些数据库？

A: 目前支持MySQL和PostgreSQL，可以扩展支持其他SQL数据库。

## 许可证

MIT License

## 贡献

欢迎提交Issue和Pull Request！
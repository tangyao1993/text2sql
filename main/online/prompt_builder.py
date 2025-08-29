import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

from ..config import settings


class PromptBuilder:
    """Builds optimized prompts for SQL generation."""
    
    def __init__(self):
        """Initialize prompt builder."""
        self.logger = logging.getLogger(__name__)
        
        # Predefined few-shot examples
        self.few_shot_examples = [
            {
                'question': '查询上周的总销售额',
                'sql': 'SELECT SUM(payment_amount) FROM orders WHERE created_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY);',
                'context': 'orders表包含payment_amount和created_at字段'
            },
            {
                'question': '统计每个城市的用户数量',
                'sql': 'SELECT city, COUNT(*) as user_count FROM users GROUP BY city;',
                'context': 'users表包含city字段'
            },
            {
                'question': '找出订单金额最高的前10个用户',
                'sql': 'SELECT u.user_id, u.username, SUM(o.payment_amount) as total_amount FROM users u JOIN orders o ON u.user_id = o.user_id GROUP BY u.user_id, u.username ORDER BY total_amount DESC LIMIT 10;',
                'context': 'users和orders表通过user_id关联'
            }
        ]
    
    def build_sql_generation_prompt(
        self,
        query: str,
        schema_context: Dict[str, Any],
        business_rules: Optional[str] = None,
        few_shot_examples: Optional[List[Dict[str, Any]]] = None,
        include_constraints: bool = True
    ) -> str:
        """
        Build a comprehensive prompt for SQL generation.
        
        Args:
            query: User's natural language query
            schema_context: Retrieved schema information
            business_rules: Optional business rules
            few_shot_examples: Optional custom examples
            include_constraints: Whether to include SQL constraints
            
        Returns:
            Complete prompt string
        """
        prompt_parts = []
        
        # 1. Role and instructions
        prompt_parts.append(self._build_role_instruction())
        
        # 2. Schema context
        prompt_parts.append(self._build_schema_context(schema_context))
        
        # 3. Business rules
        if business_rules:
            prompt_parts.append(self._build_business_context(business_rules))
        
        # 4. Few-shot examples
        examples = few_shot_examples or self._select_relevant_examples(query, schema_context)
        if examples:
            prompt_parts.append(self._build_few_shot_examples(examples))
        
        # 5. Query-specific context
        prompt_parts.append(self._build_query_context(query, schema_context))
        
        # 6. Constraints
        if include_constraints:
            prompt_parts.append(self._build_constraints())
        
        # 7. Final instruction
        prompt_parts.append(f'\n现在，请为以下问题生成SQL查询：\n"{query}"')
        
        # Join all parts
        complete_prompt = '\n\n'.join(prompt_parts)
        
        self.logger.debug(f"Built prompt with {len(complete_prompt)} characters")
        return complete_prompt
    
    def build_correction_prompt(
        self,
        original_query: str,
        error_sql: str,
        error_message: str,
        schema_context: Dict[str, Any],
        attempt_count: int
    ) -> str:
        """
        Build prompt for SQL correction.
        
        Args:
            original_query: Original user query
            error_sql: SQL that caused error
            error_message: Database error message
            schema_context: Schema information
            attempt_count: Current correction attempt
            
        Returns:
            Correction prompt
        """
        prompt = f"""你之前生成的SQL执行时出错了。请根据错误信息修正SQL。

原始问题: {original_query}

错误的SQL:
```sql
{error_sql}
```

数据库错误信息: {error_message}

请根据上面的错误信息，生成正确的SQL查询。注意：
1. 仔细检查表名和字段名是否正确
2. 确保SQL语法正确
3. 注意表之间的关联关系
4. 只使用提供的表结构信息

这是第{attempt_count}次修正尝试，请确保生成的SQL是正确的。

请将SQL代码包裹在```sql ... ```中。"""

        return prompt
    
    def _build_role_instruction(self) -> str:
        """Build role and instruction section."""
        return """你是一个世界级的数据库专家和SQL工程师。你的任务是根据用户的问题和提供的数据表结构，生成准确、高效的SQL查询。

请严格遵循以下规则：
1. 只使用提供的表和字段，不要编造任何不存在的字段
2. 注意表之间的关联关系，正确使用JOIN
3. 对于复杂的计算逻辑，使用CTE（WITH语句）提高可读性
4. 确保生成的SQL语法正确且可执行
5. 根据查询需求选择合适的聚合函数"""
    
    def _build_schema_context(self, schema_context: Dict[str, Any]) -> str:
        """Build schema context section."""
        context_parts = ["数据表结构信息："]
        
        # Add each table's schema
        for table in schema_context.get('tables', []):
            table_name = table['name']
            context_parts.append(f"\n--- 表: {table_name} ---")
            
            # Extract CREATE TABLE from document
            doc = table['document']
            if '```sql' in doc:
                # Extract SQL block
                start = doc.find('```sql') + 6
                end = doc.find('```', start)
                if end > start:
                    sql_part = doc[start:end].strip()
                    context_parts.append(sql_part)
            else:
                # Fallback: include the whole document
                context_parts.append(doc)
        
        # Add relationships if any
        relationships = schema_context.get('relationships', [])
        if relationships:
            context_parts.append("\n表关系：")
            for rel in relationships:
                context_parts.append(
                    f"- {rel['from_table']}.{rel['from_column']} -> {rel['to_table']}.{rel['to_column']}"
                )
        
        return '\n'.join(context_parts)
    
    def _build_business_context(self, business_rules: str) -> str:
        """Build business rules context section."""
        return f"\n业务规则和定义：\n{business_rules}"
    
    def _build_few_shot_examples(self, examples: List[Dict[str, Any]]) -> str:
        """Build few-shot examples section."""
        examples_text = ["示例："]
        
        for i, example in enumerate(examples, 1):
            examples_text.append(f"\n示例 {i}:")
            examples_text.append(f"问题: {example['question']}")
            examples_text.append(f"SQL: ```sql\n{example['sql']}\n```")
            if 'context' in example:
                examples_text.append(f"说明: {example['context']}")
        
        return '\n'.join(examples_text)
    
    def _build_query_context(self, query: str, schema_context: Dict[str, Any]) -> str:
        """Build query-specific context."""
        context_parts = []
        
        # Extract query characteristics
        query_lower = query.lower()
        
        # Time-based queries
        if any(time_word in query_lower for time_word in ['上周', '昨天', '本月', '今年']):
            context_parts.append("这是一个时间范围查询，请使用适当的日期函数。")
        
        # Aggregation queries
        if any(agg_word in query_lower for agg_word in ['统计', '总数', '平均', '总计']):
            context_parts.append("这是一个聚合查询，请使用GROUP BY和聚合函数。")
        
        # Ranking queries
        if any(rank_word in query_lower for rank_word in ['最高', '最低', '前', '排名']):
            context_parts.append("这是一个排名查询，请使用ORDER BY和LIMIT。")
        
        # Multiple tables
        if len(schema_context.get('tables', [])) > 1:
            context_parts.append("查询涉及多个表，请正确使用JOIN。")
        
        if context_parts:
            return "\n查询提示：\n" + '\n'.join(f"- {ctx}" for ctx in context_parts)
        
        return ""
    
    def _build_constraints(self) -> str:
        """Build SQL constraints section."""
        return """\nSQL约束：
1. 使用标准SQL语法
2. 确保所有表名和字段名都存在
3. 正确处理NULL值
4. 使用适当的索引提示（如果需要）
5. 避免使用SELECT *，明确指定需要的字段

请将最终的SQL代码包裹在```sql ... ```中。"""
    
    def _select_relevant_examples(
        self, 
        query: str, 
        schema_context: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Select relevant few-shot examples based on query."""
        query_lower = query.lower()
        relevant_examples = []
        
        # Table names in context
        table_names = [t['name'] for t in schema_context.get('tables', [])]
        
        # Query characteristics
        has_aggregation = any(word in query_lower for word in ['统计', '总数', '平均', '总计'])
        has_join = len(table_names) > 1
        has_time_filter = any(word in query_lower for word in ['上周', '昨天', '本月', '今年'])
        has_ranking = any(word in query_lower for word in ['最高', '最低', '前', '排名'])
        
        # Select examples based on characteristics
        for example in self.few_shot_examples:
            example_relevant = False
            
            # Check for aggregation
            if has_aggregation and 'SUM(' in example['sql']:
                example_relevant = True
            
            # Check for JOIN
            if has_join and 'JOIN' in example['sql']:
                example_relevant = True
            
            # Check for time filter
            if has_time_filter and 'WHERE' in example['sql']:
                example_relevant = True
            
            # Check for ranking
            if has_ranking and ('ORDER BY' in example['sql'] and 'LIMIT' in example['sql']):
                example_relevant = True
            
            if example_relevant:
                relevant_examples.append(example)
        
        # Return up to 2 relevant examples
        return relevant_examples[:2]
    
    def build_validation_prompt(self, sql: str, schema_context: Dict[str, Any]) -> str:
        """Build prompt for SQL validation."""
        return f"""请验证以下SQL查询的正确性：

SQL查询:
```sql
{sql}
```

可用的表结构：
{self._build_schema_context(schema_context)}

请检查：
1. 语法是否正确
2. 表名和字段名是否存在
3. JOIN条件是否正确
4. 聚合函数使用是否恰当
5. WHERE条件是否有效

如果有错误，请指出具体的错误位置和修正建议。"""
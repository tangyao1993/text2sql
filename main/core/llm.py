import logging
from typing import Optional, Dict, Any
from langchain_community.llms import Ollama
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser

from ..config import settings


class LLMManager:
    """Manages LLM interactions for SQL generation."""
    
    def __init__(self):
        """Initialize the LLM manager."""
        self.logger = logging.getLogger(__name__)
        self.llm = self._initialize_llm()
        self.output_parser = StrOutputParser()
        
    def _initialize_llm(self) -> Ollama:
        """Initialize the Ollama LLM."""
        try:
            llm = Ollama(
                model=settings.llm_model_name,
                base_url=settings.llm_base_url,
                temperature=settings.llm_temperature,
                num_predict=settings.llm_max_tokens,
            )
            self.logger.info(f"LLM initialized with model: {settings.llm_model_name}")
            return llm
        except Exception as e:
            self.logger.error(f"Failed to initialize LLM: {e}")
            raise
    
    def generate_sql(
        self, 
        prompt: str, 
        schema_context: str,
        business_context: Optional[str] = None,
        few_shot_examples: Optional[str] = None
    ) -> str:
        """
        Generate SQL from natural language query.
        
        Args:
            prompt: User's natural language query
            schema_context: Retrieved schema information
            business_context: Optional business rules and definitions
            few_shot_examples: Optional few-shot examples
            
        Returns:
            Generated SQL query
        """
        # Build the complete prompt
        complete_prompt = self._build_sql_generation_prompt(
            prompt, schema_context, business_context, few_shot_examples
        )
        
        # Generate SQL
        try:
            result = self.llm.invoke(complete_prompt)
            sql = self._extract_sql_from_response(result)
            self.logger.info(f"Generated SQL: {sql}")
            return sql
        except Exception as e:
            self.logger.error(f"Error generating SQL: {e}")
            raise
    
    def correct_sql(
        self, 
        original_prompt: str, 
        error_sql: str, 
        error_message: str,
        schema_context: str
    ) -> str:
        """
        Correct SQL based on error message.
        
        Args:
            original_prompt: Original user query
            error_sql: SQL that caused the error
            error_message: Database error message
            schema_context: Schema information
            
        Returns:
            Corrected SQL query
        """
        correction_prompt = f"""
        你上次生成的SQL执行时出错了。请根据下面的错误信息修正你的SQL。
        
        原始问题: {original_prompt}
        
        错误的SQL: 
        ```sql
        {error_sql}
        ```
        
        数据库报错: {error_message}
        
        请重新生成正确的SQL，只返回SQL代码，不要包含其他解释。
        """
        
        complete_prompt = self._build_sql_generation_prompt(
            correction_prompt, schema_context
        )
        
        try:
            result = self.llm.invoke(complete_prompt)
            corrected_sql = self._extract_sql_from_response(result)
            self.logger.info(f"Corrected SQL: {corrected_sql}")
            return corrected_sql
        except Exception as e:
            self.logger.error(f"Error correcting SQL: {e}")
            raise
    
    def _build_sql_generation_prompt(
        self,
        prompt: str,
        schema_context: str,
        business_context: Optional[str] = None,
        few_shot_examples: Optional[str] = None
    ) -> str:
        """Build the complete prompt for SQL generation."""
        template = f"""
        你是一个世界级的数据库专家和SQL工程师。你的任务是根据用户的问题和提供的数据表结构，生成一段准确、高效的SQL查询。
        
        请严格遵循以下规则：
        1. 只使用提供的表和字段。
        2. 注意表之间的关联关系。
        3. 如果问题的计算逻辑复杂，请使用CTE（WITH语句）来保证SQL的可读性。
        4. 不要编造任何不存在的字段。
        5. 返回的SQL必须语法正确且可执行。
        
        数据表结构：
        {schema_context}
        
        {business_context or ''}
        
        {few_shot_examples or ''}
        
        现在，请根据以上信息，为以下问题生成SQL：
        "{prompt}"
        
        请将SQL代码包裹在```sql ... ```中。
        """
        
        return template.strip()
    
    def _extract_sql_from_response(self, response: str) -> str:
        """Extract SQL code from LLM response."""
        import re
        
        # Try to extract SQL from code blocks
        sql_pattern = r"```sql\s*(.*?)\s*```"
        matches = re.findall(sql_pattern, response, re.DOTALL)
        
        if matches:
            return matches[0].strip()
        
        # If no code blocks, try to find SQL-like content
        lines = response.split('\n')
        sql_lines = []
        in_sql = False
        
        for line in lines:
            line = line.strip()
            if line.upper().startswith(('SELECT', 'WITH', 'INSERT', 'UPDATE', 'DELETE')):
                in_sql = True
            if in_sql:
                sql_lines.append(line)
        
        if sql_lines:
            return '\n'.join(sql_lines)
        
        # Fallback: return the whole response
        return response.strip()
"""
Text2SQL: Main orchestrator for RAG-based Text-to-SQL system.
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
import json
from datetime import datetime

from .config import settings
from .core import LLMManager, EmbeddingManager, VectorDBManager
from .offline import MetadataSync, KnowledgeBaseBuilder
from .online import QueryParser, RAGRetriever, PromptBuilder, SQLValidator


class Text2SQL:
    """Main Text2SQL orchestrator."""
    
    def __init__(self, database_url: str = None):
        """
        Initialize Text2SQL system.
        
        Args:
            database_url: Database connection URL
        """
        self.logger = logging.getLogger(__name__)
        self.database_url = database_url or settings.database_url
        
        # Initialize components
        self.llm_manager = LLMManager()
        self.embedding_manager = EmbeddingManager()
        self.vector_db = VectorDBManager()
        
        # Offline processing
        self.metadata_sync = MetadataSync()
        self.knowledge_base = KnowledgeBaseBuilder()
        
        # Online processing
        self.query_parser = QueryParser()
        self.rag_retriever = RAGRetriever()
        self.prompt_builder = PromptBuilder()
        self.sql_validator = SQLValidator()
        
        # Initialize database connections
        self._initialize_connections()
    
    def _initialize_connections(self):
        """Initialize database connections."""
        try:
            self.metadata_sync.connect(self.database_url)
            self.sql_validator.connect(self.database_url)
        except Exception as e:
            self.logger.warning(f"Could not connect to database: {e}")
            self.logger.info("Running in offline mode")
    
    def build_knowledge_base(
        self,
        business_rules: Optional[Dict[str, Any]] = None,
        force_rebuild: bool = False
    ) -> None:
        """
        Build the knowledge base from database metadata.
        
        Args:
            business_rules: Optional business rules and definitions
            force_rebuild: Whether to force rebuild even if exists
        """
        try:
            # Check if knowledge base exists
            if not force_rebuild and self.vector_db.count_documents() > 0:
                self.logger.info("Knowledge base already exists. Use force_rebuild=True to rebuild.")
                return
            
            self.logger.info("Building knowledge base...")
            
            # Extract metadata and build knowledge base
            self.knowledge_base.build_from_database(
                database_url=self.database_url,
                business_rules=business_rules
            )
            
            self.logger.info("Knowledge base built successfully!")
            
        except Exception as e:
            self.logger.error(f"Error building knowledge base: {e}")
            raise
    
    def query_to_sql(
        self,
        query: str,
        max_correction_attempts: int = None,
        return_intermediate: bool = False
    ) -> Dict[str, Any]:
        """
        Convert natural language query to SQL.
        
        Args:
            query: Natural language query
            max_correction_attempts: Maximum correction attempts
            return_intermediate: Whether to return intermediate results
            
        Returns:
            Dictionary with results
        """
        if max_correction_attempts is None:
            max_correction_attempts = settings.max_correction_attempts
        
        result = {
            'query': query,
            'sql': None,
            'is_valid': False,
            'execution_results': None,
            'correction_attempts': 0,
            'intermediate': {} if return_intermediate else None
        }
        
        try:
            # 1. Parse query
            parsed_query = self.query_parser.parse_query(query)
            if return_intermediate:
                result['intermediate']['parsed_query'] = parsed_query
            
            # 2. Retrieve relevant context
            context = self.rag_retriever.retrieve_context(query)
            if return_intermediate:
                result['intermediate']['retrieved_context'] = {
                    'tables': [t['name'] for t in context['tables']],
                    'relationships': context['relationships']
                }
            
            # 3. Generate initial SQL
            sql = self._generate_sql(query, context, parsed_query)
            result['sql'] = sql
            
            # 4. Validate and execute
            is_valid, fixed_sql, error = self.sql_validator.validate_and_fix(
                sql, context
            )
            
            if is_valid:
                result['sql'] = fixed_sql
                result['is_valid'] = True
                
                # Execute the query
                success, execution_results, exec_error = self.sql_validator.execute_query(fixed_sql)
                if success:
                    result['execution_results'] = execution_results
                else:
                    result['execution_error'] = exec_error
            else:
                # 5. Try to correct if validation failed
                for attempt in range(1, max_correction_attempts + 1):
                    self.logger.info(f"Correction attempt {attempt}/{max_correction_attempts}")
                    
                    # Generate correction prompt
                    correction_prompt = self.prompt_builder.build_correction_prompt(
                        query, sql, error, context, attempt
                    )
                    
                    # Generate corrected SQL
                    corrected_sql = self.llm_manager.correct_sql(
                        query, sql, error, context['schema_text']
                    )
                    
                    # Validate corrected SQL
                    is_valid, fixed_sql, error = self.sql_validator.validate_and_fix(
                        corrected_sql, context
                    )
                    
                    if is_valid:
                        result['sql'] = fixed_sql
                        result['is_valid'] = True
                        result['correction_attempts'] = attempt
                        
                        # Execute corrected query
                        success, execution_results, exec_error = self.sql_validator.execute_query(fixed_sql)
                        if success:
                            result['execution_results'] = execution_results
                        else:
                            result['execution_error'] = exec_error
                        break
                    else:
                        sql = corrected_sql
                
                if not result['is_valid']:
                    result['error'] = f"Failed after {max_correction_attempts} correction attempts. Last error: {error}"
            
        except Exception as e:
            self.logger.error(f"Error in query_to_sql: {e}")
            result['error'] = str(e)
        
        return result
    
    def _generate_sql(
        self,
        query: str,
        context: Dict[str, Any],
        parsed_query: Dict[str, Any]
    ) -> str:
        """Generate SQL from query and context."""
        # Build prompt
        prompt = self.prompt_builder.build_sql_generation_prompt(
            query=query,
            schema_context=context,
            business_rules=context.get('business_rules')
        )
        
        # Generate SQL
        sql = self.llm_manager.generate_sql(
            prompt=prompt,
            schema_context=context['schema_text'],
            business_context=context.get('business_rules')
        )
        
        return sql
    
    def explain_sql(self, sql: str) -> Optional[Dict[str, Any]]:
        """
        Get explanation for SQL query.
        
        Args:
            sql: SQL query to explain
            
        Returns:
            Execution plan information
        """
        return self.sql_validator.explain_query(sql)
    
    def get_schema_info(self, table_name: str = None) -> Dict[str, Any]:
        """
        Get schema information.
        
        Args:
            table_name: Specific table name (optional)
            
        Returns:
            Schema information
        """
        if table_name:
            return self.knowledge_base.get_table_schema(table_name)
        else:
            # Return summary of all tables
            documents = self.vector_db.list_documents()
            tables = [
                {
                    'name': doc['metadata']['table_name'],
                    'columns': len(doc['metadata']['columns']),
                    'row_count': doc['metadata']['row_count']
                }
                for doc in documents
                if doc['metadata']['type'] == 'table'
            ]
            return {'tables': tables}
    
    def add_business_rule(self, rule_name: str, rule_definition: str) -> None:
        """
        Add a business rule to the knowledge base.
        
        Args:
            rule_name: Name of the rule
            rule_definition: Rule definition
        """
        self.knowledge_base.add_business_rule(rule_name, rule_definition)
    
    def export_knowledge_base(self, file_path: str) -> None:
        """
        Export knowledge base to file.
        
        Args:
            file_path: Output file path
        """
        try:
            # Get all documents
            documents = self.vector_db.list_documents()
            
            # Export as JSON
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(documents, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Knowledge base exported to {file_path}")
            
        except Exception as e:
            self.logger.error(f"Error exporting knowledge base: {e}")
            raise
    
    def import_knowledge_base(self, file_path: str) -> None:
        """
        Import knowledge base from file.
        
        Args:
            file_path: Input file path
        """
        try:
            # Load documents
            with open(file_path, 'r', encoding='utf-8') as f:
                documents = json.load(f)
            
            # Clear existing collection
            self.vector_db.clear_collection()
            
            # Add documents
            docs = [doc.get('document', '') for doc in documents]
            metadatas = [doc.get('metadata', {}) for doc in documents]
            ids = [doc.get('id', str(i)) for i, doc in enumerate(documents)]
            
            self.vector_db.add_documents(docs, metadatas, ids)
            
            self.logger.info(f"Knowledge base imported from {file_path}")
            
        except Exception as e:
            self.logger.error(f"Error importing knowledge base: {e}")
            raise
    
    def get_stats(self) -> Dict[str, Any]:
        """Get system statistics."""
        return {
            'knowledge_base_size': self.vector_db.count_documents(),
            'last_updated': datetime.now().isoformat(),
            'database_type': settings.db_type,
            'llm_model': settings.llm_model_name,
            'embedding_model': settings.embedding_model_name
        }
import logging
from typing import Dict, List, Any, Optional
import re

from ..core import VectorDBManager
from .query_parser import QueryParser
from ..config import settings


class RAGRetriever:
    """Retrieves relevant schema information using RAG."""
    
    def __init__(self):
        """Initialize RAG retriever."""
        self.logger = logging.getLogger(__name__)
        self.vector_db = VectorDBManager()
        self.query_parser = QueryParser()
        
    def retrieve_context(
        self, 
        query: str,
        top_k: int = None,
        score_threshold: float = None,
        use_hybrid_search: bool = True
    ) -> Dict[str, Any]:
        """
        Retrieve relevant context for the query.
        
        Args:
            query: User's natural language query
            top_k: Number of results to retrieve
            score_threshold: Minimum similarity score
            use_hybrid_search: Whether to use hybrid search
            
        Returns:
            Dictionary with retrieved context
        """
        try:
            # Parse query to extract entities and enhance search
            parsed_query = self.query_parser.parse_query(query)
            
            if use_hybrid_search:
                # Use both original and enhanced queries
                enhanced_query = self.query_parser.enhance_search_query(query, parsed_query)
                search_queries = [query, enhanced_query]
            else:
                search_queries = [query]
            
            # Set defaults
            if top_k is None:
                top_k = settings.rag_top_k
            if score_threshold is None:
                score_threshold = settings.rag_score_threshold
            
            # Retrieve results for each query
            all_results = []
            for search_query in search_queries:
                results = self.vector_db.search(
                    query=search_query,
                    top_k=top_k,
                    where={'type': 'table'}  # Only search table schemas
                )
                
                # Filter by score threshold
                filtered_results = [
                    r for r in results 
                    if r['distance'] <= (1 - score_threshold)
                ]
                
                all_results.extend(filtered_results)
            
            # Deduplicate results
            unique_results = self._deduplicate_results(all_results)
            
            # Extract related tables and build context
            context = self._build_context(unique_results, parsed_query)
            
            self.logger.info(f"Retrieved context with {len(unique_results)} tables")
            return context
            
        except Exception as e:
            self.logger.error(f"Error retrieving context: {e}")
            raise
    
    def retrieve_table_schemas(self, table_names: List[str]) -> Dict[str, str]:
        """
        Retrieve schemas for specific tables.
        
        Args:
            table_names: List of table names
            
        Returns:
            Dictionary mapping table names to their schema documents
        """
        schemas = {}
        
        for table_name in table_names:
            try:
                result = self.vector_db.get_document(f"table_{table_name}")
                if result:
                    schemas[table_name] = result['document']
            except Exception as e:
                self.logger.warning(f"Could not retrieve schema for {table_name}: {e}")
        
        return schemas
    
    def retrieve_business_rules(self) -> Optional[str]:
        """Retrieve business rules from knowledge base."""
        try:
            result = self.vector_db.get_document('business_rules')
            if result:
                return result['document']
            return None
        except Exception as e:
            self.logger.warning(f"Could not retrieve business rules: {e}")
            return None
    
    def find_related_tables(self, query: str, current_tables: List[str]) -> List[str]:
        """
        Find additional tables that might be related to the query.
        
        Args:
            query: User query
            current_tables: Already identified tables
            
        Returns:
            List of additional table names
        """
        try:
            # Extract entities from query
            parsed_query = self.query_parser.parse_query(query)
            entities = parsed_query['entities']
            
            # Search for tables containing these entities
            additional_tables = []
            
            for entity in entities:
                # Look for tables with entity in name or columns
                results = self.vector_db.search(
                    query=entity,
                    top_k=5,
                    where={'type': 'table'}
                )
                
                for result in results:
                    table_name = result['metadata']['table_name']
                    if table_name not in current_tables and table_name not in additional_tables:
                        additional_tables.append(table_name)
            
            return additional_tables[:2]  # Limit to 2 additional tables
            
        except Exception as e:
            self.logger.error(f"Error finding related tables: {e}")
            return []
    
    def _deduplicate_results(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove duplicate results based on document ID."""
        seen_ids = set()
        unique_results = []
        
        for result in results:
            if result['id'] not in seen_ids:
                seen_ids.add(result['id'])
                unique_results.append(result)
        
        return unique_results
    
    def _build_context(
        self, 
        results: List[Dict[str, Any]], 
        parsed_query: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Build structured context from retrieved results."""
        context = {
            'tables': [],
            'relationships': [],
            'schema_text': '',
            'business_rules': None,
            'metadata': {
                'query_type': parsed_query['intent'],
                'aggregation_type': parsed_query['aggregation_type'],
                'time_range': parsed_query['time_range']
            }
        }
        
        # Process each table
        table_names = []
        for result in results:
            table_name = result['metadata']['table_name']
            table_names.append(table_name)
            
            # Add table info
            context['tables'].append({
                'name': table_name,
                'document': result['document'],
                'metadata': result['metadata'],
                'score': result['distance']
            })
            
            # Build schema text for LLM
            context['schema_text'] += f"\n\n--- Table: {table_name} ---\n"
            context['schema_text'] += result['document']
        
        # Extract relationships between tables
        if len(table_names) > 1:
            context['relationships'] = self._extract_relationships(context['tables'])
        
        # Get business rules if available
        business_rules = self.retrieve_business_rules()
        if business_rules:
            context['business_rules'] = business_rules
        
        return context
    
    def _extract_relationships(self, tables: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract foreign key relationships between tables."""
        relationships = []
        
        # Build a map of tables by name
        table_map = {table['name']: table for table in tables}
        
        # Check each table for foreign keys
        for table in tables:
            foreign_keys = table['metadata'].get('foreign_keys', [])
            
            for fk in foreign_keys:
                referenced_table = fk['references']['table']
                
                # Only include relationships to tables in our result set
                if referenced_table in table_map:
                    relationships.append({
                        'from_table': table['name'],
                        'from_column': fk['column'],
                        'to_table': referenced_table,
                        'to_column': fk['references']['column']
                    })
        
        return relationships
    
    def get_relevant_columns(self, query: str, table_name: str) -> List[str]:
        """
        Get columns from a table that are relevant to the query.
        
        Args:
            query: User query
            table_name: Table name
            
        Returns:
            List of relevant column names
        """
        try:
            # Get table schema
            table_doc = self.vector_db.get_document(f"table_{table_name}")
            if not table_doc:
                return []
            
            # Extract column information
            metadata = table_doc['metadata']
            columns = metadata['columns']
            
            # Parse query to get entities and metrics
            parsed_query = self.query_parser.parse_query(query)
            entities = parsed_query['entities']
            metrics = parsed_query['metrics']
            
            # Find relevant columns
            relevant_columns = []
            
            # Check time columns
            if parsed_query['time_range']['relative_time']:
                time_columns = [col for col in columns if any(
                    time_word in col.lower() 
                    for time_word in ['time', 'date', 'created', 'updated']
                )]
                relevant_columns.extend(time_columns)
            
            # Check metric columns
            for metric in metrics:
                metric_columns = [col for col in columns if metric in col.lower()]
                relevant_columns.extend(metric_columns)
            
            # Check entity columns
            for entity in entities:
                entity_columns = [col for col in columns if entity in col.lower()]
                relevant_columns.extend(entity_columns)
            
            # Always include primary keys
            pk_columns = metadata.get('primary_keys', [])
            relevant_columns.extend(pk_columns)
            
            # Remove duplicates
            return list(set(relevant_columns))
            
        except Exception as e:
            self.logger.error(f"Error getting relevant columns: {e}")
            return columns if 'columns' in locals() else []
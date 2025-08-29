import logging
from typing import Dict, List, Any, Optional
import json
import re
from datetime import datetime

from .metadata_sync import MetadataSync
from ..core import VectorDBManager, EmbeddingManager


class KnowledgeBaseBuilder:
    """Builds and manages the knowledge base for RAG."""
    
    def __init__(self):
        """Initialize knowledge base builder."""
        self.logger = logging.getLogger(__name__)
        self.metadata_sync = MetadataSync()
        self.vector_db = VectorDBManager()
        self.embedding_manager = EmbeddingManager()
        
    def build_from_database(
        self, 
        database_url: str = None,
        business_rules: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Build knowledge base from database metadata.
        
        Args:
            database_url: Database connection URL
            business_rules: Optional business rules and definitions
        """
        try:
            # Extract metadata
            self.metadata_sync.connect(database_url)
            metadata = self.metadata_sync.extract_metadata()
            
            # Process into chunks
            chunks = self._create_chunks(metadata, business_rules)
            
            # Generate embeddings and store
            self._store_chunks(chunks)
            
            self.logger.info(f"Knowledge base built with {len(chunks)} chunks")
            
        except Exception as e:
            self.logger.error(f"Error building knowledge base: {e}")
            raise
    
    def build_from_metadata_file(
        self, 
        metadata_file: str,
        business_rules: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Build knowledge base from metadata JSON file.
        
        Args:
            metadata_file: Path to metadata JSON file
            business_rules: Optional business rules and definitions
        """
        try:
            # Load metadata
            metadata = self.metadata_sync.load_metadata(metadata_file)
            
            # Process into chunks
            chunks = self._create_chunks(metadata, business_rules)
            
            # Generate embeddings and store
            self._store_chunks(chunks)
            
            self.logger.info(f"Knowledge base built with {len(chunks)} chunks")
            
        except Exception as e:
            self.logger.error(f"Error building knowledge base from file: {e}")
            raise
    
    def _create_chunks(
        self, 
        metadata: Dict[str, Any],
        business_rules: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Create document chunks from metadata.
        
        Args:
            metadata: Database metadata
            business_rules: Business rules and definitions
            
        Returns:
            List of chunk dictionaries
        """
        chunks = []
        
        # Create one chunk per table
        for table_name, table_info in metadata['tables'].items():
            chunk = self._create_table_chunk(table_name, table_info, business_rules)
            chunks.append(chunk)
        
        # Create chunks for business terms if provided
        if business_rules:
            business_chunk = self._create_business_chunk(business_rules)
            chunks.append(business_chunk)
        
        return chunks
    
    def _create_table_chunk(
        self, 
        table_name: str, 
        table_info: Dict[str, Any],
        business_rules: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Create a chunk for a single table."""
        # Generate DDL
        ddl = self._generate_table_ddl(table_info)
        
        # Create document content
        content = f"""# Table: {table_name}

## Description
{table_info['comment'] or f'表 {table_name}'}

## Schema
```sql
{ddl}
```

## Columns
"""
        
        # Add column details
        for col in table_info['columns']:
            content += f"- **{col['name']}**: {col['type']}"
            if col['comment']:
                content += f" - {col['comment']}"
            if col['is_primary']:
                content += " (主键)"
            if col['is_foreign']:
                content += f" (外键 -> {col['references']['table']}.{col['references']['column']})"
            content += "\n"
        
        # Add business terms if available
        if business_rules and table_name in business_rules.get('table_terms', {}):
            content += "\n## Business Terms\n"
            for term, definition in business_rules['table_terms'][table_name].items():
                content += f"- **{term}**: {definition}\n"
        
        # Add enum values if available
        enum_values = self._extract_enum_values(table_info)
        if enum_values:
            content += "\n## Enum Values\n"
            for col_name, values in enum_values.items():
                content += f"- **{col_name}**: {values}\n"
        
        # Add synonyms
        synonyms = self._generate_synonyms(table_name, table_info)
        if synonyms:
            content += "\n## Synonyms\n"
            if synonyms.get('table'):
                content += f"- Table: {', '.join(synonyms['table'])}\n"
            if synonyms.get('columns'):
                content += "- Columns:\n"
                for col, syns in synonyms['columns'].items():
                    content += f"  - {col}: {', '.join(syns)}\n"
        
        # Create metadata
        chunk_metadata = {
            'type': 'table',
            'table_name': table_name,
            'columns': [col['name'] for col in table_info['columns']],
            'primary_keys': [col['name'] for col in table_info['columns'] if col['is_primary']],
            'foreign_keys': [
                {
                    'column': col['name'],
                    'references': col['references']
                }
                for col in table_info['columns'] if col['is_foreign']
            ],
            'row_count': table_info['row_count'],
            'created_at': datetime.now().isoformat()
        }
        
        return {
            'id': f"table_{table_name}",
            'content': content,
            'metadata': chunk_metadata
        }
    
    def _create_business_chunk(self, business_rules: Dict[str, Any]) -> Dict[str, Any]:
        """Create a chunk for business rules and terms."""
        content = "# Business Rules and Definitions\n\n"
        
        # Add general business terms
        if 'general_terms' in business_rules:
            content += "## General Terms\n"
            for term, definition in business_rules['general_terms'].items():
                content += f"- **{term}**: {definition}\n"
            content += "\n"
        
        # Add business metrics
        if 'metrics' in business_rules:
            content += "## Business Metrics\n"
            for metric, definition in business_rules['metrics'].items():
                content += f"- **{metric}**: {definition}\n"
            content += "\n"
        
        # Add calculation rules
        if 'calculations' in business_rules:
            content += "## Calculation Rules\n"
            for rule, formula in business_rules['calculations'].items():
                content += f"- **{rule}**: {formula}\n"
        
        return {
            'id': 'business_rules',
            'content': content,
            'metadata': {
                'type': 'business',
                'created_at': datetime.now().isoformat()
            }
        }
    
    def _generate_table_ddl(self, table_info: Dict[str, Any]) -> str:
        """Generate DDL for a table."""
        ddl_parts = [f"CREATE TABLE {table_info['name']} ("]
        
        # Add columns
        column_defs = []
        for col in table_info['columns']:
            col_def = f"    {col['name']} {col['type']}"
            if not col['nullable']:
                col_def += " NOT NULL"
            column_defs.append(col_def)
        
        # Add primary key
        pk_columns = [col['name'] for col in table_info['columns'] if col['is_primary']]
        if pk_columns:
            column_defs.append(f"    PRIMARY KEY ({', '.join(pk_columns)})")
        
        ddl_parts.append(',\n'.join(column_defs))
        ddl_parts.append(")")
        
        return '\n'.join(ddl_parts)
    
    def _extract_enum_values(self, table_info: Dict[str, Any]) -> Dict[str, str]:
        """Extract enum values from column comments."""
        enum_values = {}
        
        for col in table_info['columns']:
            if col['comment']:
                # Look for patterns like "status: 1=成功, 2=失败"
                enum_pattern = r'(\d+)\s*=\s*([^,\s]+)'
                matches = re.findall(enum_pattern, col['comment'])
                if matches:
                    enum_values[col['name']] = ', '.join([f"{m[0]} means '{m[1]}'" for m in matches])
        
        return enum_values
    
    def _generate_synonyms(self, table_name: str, table_info: Dict[str, Any]) -> Dict[str, Any]:
        """Generate synonyms for table and columns."""
        synonyms = {}
        
        # Table synonyms (could be enhanced with LLM or dictionary)
        table_synonyms = []
        if 'order' in table_name.lower():
            table_synonyms.extend(['订单', '交易记录'])
        if 'user' in table_name.lower():
            table_synonyms.extend(['用户', '客户'])
        if 'product' in table_name.lower():
            table_synonyms.extend(['产品', '商品'])
        
        if table_synonyms:
            synonyms['table'] = table_synonyms
        
        # Column synonyms
        column_synonyms = {}
        for col in table_info['columns']:
            syns = []
            col_name = col['name'].lower()
            
            if 'amount' in col_name or 'price' in col_name:
                syns.extend(['金额', '价格', '销售额'])
            if 'time' in col_name or 'date' in col_name or 'at' in col_name:
                syns.extend(['时间', '日期'])
            if 'status' in col_name:
                syns.extend(['状态', '情况'])
            if 'id' in col_name and col_name != 'id':
                syns.extend(['ID', '编号'])
            
            if syns:
                column_synonyms[col['name']] = syns
        
        if column_synonyms:
            synonyms['columns'] = column_synonyms
        
        return synonyms
    
    def _store_chunks(self, chunks: List[Dict[str, Any]]) -> None:
        """Store chunks in vector database."""
        try:
            # Prepare data for vector DB
            documents = [chunk['content'] for chunk in chunks]
            metadatas = [chunk['metadata'] for chunk in chunks]
            ids = [chunk['id'] for chunk in chunks]
            
            # Clear existing collection if needed
            if self.vector_db.count_documents() > 0:
                self.logger.info("Clearing existing knowledge base")
                self.vector_db.clear_collection()
            
            # Add documents to vector DB
            self.vector_db.add_documents(documents, metadatas, ids)
            
            self.logger.info(f"Stored {len(chunks)} chunks in vector database")
            
        except Exception as e:
            self.logger.error(f"Error storing chunks: {e}")
            raise
    
    def add_business_rule(self, rule_name: str, rule_definition: str) -> None:
        """Add a new business rule to the knowledge base."""
        try:
            # Get existing business rules chunk
            existing = self.vector_db.get_document('business_rules')
            
            if existing:
                # Update existing chunk
                content = existing['document']
                if "## Business Metrics" in content:
                    content = content.replace(
                        "## Business Metrics",
                        f"## Business Metrics\n- **{rule_name}**: {rule_definition}"
                    )
                else:
                    content += f"\n\n## Business Metrics\n- **{rule_name}**: {rule_definition}"
                
                self.vector_db.update_document('business_rules', document=content)
            else:
                # Create new business rules chunk
                chunk = {
                    'id': 'business_rules',
                    'content': f"# Business Rules and Definitions\n\n## Business Metrics\n- **{rule_name}**: {rule_definition}",
                    'metadata': {
                        'type': 'business',
                        'created_at': datetime.now().isoformat()
                    }
                }
                self.vector_db.add_documents(
                    [chunk['content']], 
                    [chunk['metadata']], 
                    [chunk['id']]
                )
            
            self.logger.info(f"Added business rule: {rule_name}")
            
        except Exception as e:
            self.logger.error(f"Error adding business rule: {e}")
            raise
    
    def get_table_schema(self, table_name: str) -> Optional[Dict[str, Any]]:
        """Get schema information for a specific table."""
        try:
            chunk = self.vector_db.get_document(f"table_{table_name}")
            if chunk:
                return {
                    'content': chunk['document'],
                    'metadata': chunk['metadata']
                }
            return None
        except Exception as e:
            self.logger.error(f"Error getting table schema: {e}")
            return None
import logging
from typing import Dict, List, Any, Optional, Tuple
from sqlalchemy import create_engine, inspect, MetaData, Table, text
from sqlalchemy.engine import reflection
import json

from ..config import settings


class MetadataSync:
    """Handles database metadata synchronization."""
    
    def __init__(self):
        """Initialize metadata synchronizer."""
        self.logger = logging.getLogger(__name__)
        self.engine = None
        self.inspector = None
        
    def connect(self, database_url: str = None) -> None:
        """
        Connect to the database.
        
        Args:
            database_url: Database connection URL (uses settings if None)
        """
        try:
            if database_url is None:
                database_url = settings.database_url
                
            self.engine = create_engine(database_url)
            self.inspector = inspect(self.engine)
            self.logger.info(f"Connected to database: {database_url.split('@')[1] if '@' in database_url else database_url}")
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {e}")
            raise
    
    def extract_metadata(self) -> Dict[str, Any]:
        """
        Extract complete metadata from the database.
        
        Returns:
            Dictionary containing all metadata
        """
        if not self.engine:
            self.connect()
        
        try:
            metadata = {
                'tables': {},
                'relationships': [],
                'business_terms': {}
            }
            
            # Extract table information
            table_names = self.inspector.get_table_names()
            
            for table_name in table_names:
                table_info = self._extract_table_info(table_name)
                metadata['tables'][table_name] = table_info
            
            # Extract relationships
            metadata['relationships'] = self._extract_relationships(table_names)
            
            self.logger.info(f"Extracted metadata for {len(table_names)} tables")
            return metadata
            
        except Exception as e:
            self.logger.error(f"Error extracting metadata: {e}")
            raise
    
    def _extract_table_info(self, table_name: str) -> Dict[str, Any]:
        """Extract information for a single table."""
        try:
            # Get table comment
            table_comment = self.inspector.get_table_comment(table_name)
            
            # Get columns
            columns = []
            primary_keys = self.inspector.get_pk_constraint(table_name)['constrained_columns']
            foreign_keys = self.inspector.get_foreign_keys(table_name)
            
            for column in self.inspector.get_columns(table_name):
                column_info = {
                    'name': column['name'],
                    'type': str(column['type']),
                    'nullable': column.get('nullable', True),
                    'default': column.get('default'),
                    'comment': column.get('comment', ''),
                    'is_primary': column['name'] in primary_keys,
                    'is_foreign': any(fk['constrained_columns'] == [column['name']] for fk in foreign_keys)
                }
                
                # Add foreign key reference info
                if column_info['is_foreign']:
                    fk_info = next(fk for fk in foreign_keys if fk['constrained_columns'] == [column['name']])
                    column_info['references'] = {
                        'table': fk['referred_table'],
                        'column': fk['referred_columns'][0]
                    }
                
                columns.append(column_info)
            
            # Get indexes
            indexes = self.inspector.get_indexes(table_name)
            
            table_info = {
                'name': table_name,
                'comment': table_comment.get('text', ''),
                'columns': columns,
                'indexes': indexes,
                'row_count': self._get_table_row_count(table_name)
            }
            
            return table_info
            
        except Exception as e:
            self.logger.error(f"Error extracting table info for {table_name}: {e}")
            raise
    
    def _extract_relationships(self, table_names: List[str]) -> List[Dict[str, Any]]:
        """Extract foreign key relationships between tables."""
        relationships = []
        
        for table_name in table_names:
            foreign_keys = self.inspector.get_foreign_keys(table_name)
            
            for fk in foreign_keys:
                relationship = {
                    'from_table': table_name,
                    'from_columns': fk['constrained_columns'],
                    'to_table': fk['referred_table'],
                    'to_columns': fk['referred_columns'],
                    'name': fk.get('name', f"fk_{table_name}_{'_'.join(fk['constrained_columns'])}")
                }
                relationships.append(relationship)
        
        return relationships
    
    def _get_table_row_count(self, table_name: str) -> int:
        """Get approximate row count for a table."""
        try:
            with self.engine.connect() as conn:
                if settings.db_type == "mysql":
                    result = conn.execute(text(f"SELECT TABLE_ROWS FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{settings.db_name}' AND TABLE_NAME = '{table_name}'"))
                elif settings.db_type == "postgresql":
                    result = conn.execute(text(f"SELECT reltuples::bigint FROM pg_class WHERE relname = '{table_name}'"))
                else:
                    # Fallback to exact count
                    result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                
                return result.scalar() or 0
        except Exception as e:
            self.logger.warning(f"Could not get row count for {table_name}: {e}")
            return 0
    
    def save_metadata(self, metadata: Dict[str, Any], file_path: str = "metadata.json") -> None:
        """
        Save metadata to a JSON file.
        
        Args:
            metadata: Metadata dictionary
            file_path: Output file path
        """
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Metadata saved to {file_path}")
        except Exception as e:
            self.logger.error(f"Error saving metadata: {e}")
            raise
    
    def load_metadata(self, file_path: str = "metadata.json") -> Dict[str, Any]:
        """
        Load metadata from a JSON file.
        
        Args:
            file_path: Input file path
            
        Returns:
            Metadata dictionary
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            self.logger.info(f"Metadata loaded from {file_path}")
            return metadata
        except Exception as e:
            self.logger.error(f"Error loading metadata: {e}")
            raise
    
    def update_table_comment(self, table_name: str, comment: str) -> None:
        """
        Update table comment in the database.
        
        Args:
            table_name: Table name
            comment: New comment
        """
        try:
            if settings.db_type == "mysql":
                with self.engine.connect() as conn:
                    conn.execute(text(f"ALTER TABLE {table_name} COMMENT = '{comment}'"))
                    conn.commit()
            elif settings.db_type == "postgresql":
                with self.engine.connect() as conn:
                    conn.execute(text(f"COMMENT ON TABLE {table_name} IS '{comment}'"))
                    conn.commit()
            
            self.logger.info(f"Updated comment for table {table_name}")
        except Exception as e:
            self.logger.error(f"Error updating table comment: {e}")
            raise
    
    def update_column_comment(self, table_name: str, column_name: str, comment: str) -> None:
        """
        Update column comment in the database.
        
        Args:
            table_name: Table name
            column_name: Column name
            comment: New comment
        """
        try:
            if settings.db_type == "mysql":
                with self.engine.connect() as conn:
                    conn.execute(text(f"ALTER TABLE {table_name} MODIFY COLUMN {column_name} {self._get_column_type(table_name, column_name)} COMMENT '{comment}'"))
                    conn.commit()
            elif settings.db_type == "postgresql":
                with self.engine.connect() as conn:
                    conn.execute(text(f"COMMENT ON COLUMN {table_name}.{column_name} IS '{comment}'"))
                    conn.commit()
            
            self.logger.info(f"Updated comment for {table_name}.{column_name}")
        except Exception as e:
            self.logger.error(f"Error updating column comment: {e}")
            raise
    
    def _get_column_type(self, table_name: str, column_name: str) -> str:
        """Get the SQL type definition for a column."""
        columns = self.inspector.get_columns(table_name)
        column = next(c for c in columns if c['name'] == column_name)
        return str(column['type'])
    
    def generate_schema_ddl(self, table_name: str) -> str:
        """
        Generate DDL for a specific table.
        
        Args:
            table_name: Table name
            
        Returns:
            DDL string
        """
        try:
            table_info = self.extract_metadata()['tables'][table_name]
            
            # Build CREATE TABLE statement
            ddl_parts = [f"CREATE TABLE {table_name} ("]
            
            # Add columns
            column_defs = []
            for col in table_info['columns']:
                col_def = f"    {col['name']} {col['type']}"
                if not col['nullable']:
                    col_def += " NOT NULL"
                if col['comment']:
                    col_def += f" COMMENT '{col['comment']}'"
                column_defs.append(col_def)
            
            # Add primary key
            pk_columns = [col['name'] for col in table_info['columns'] if col['is_primary']]
            if pk_columns:
                column_defs.append(f"    PRIMARY KEY ({', '.join(pk_columns)})")
            
            ddl_parts.append(',\n'.join(column_defs))
            ddl_parts.append(")")
            
            # Add table comment
            if table_info['comment']:
                ddl_parts.append(f"COMMENT = '{table_info['comment']}'")
            
            return '\n'.join(ddl_parts) + ';'
            
        except Exception as e:
            self.logger.error(f"Error generating DDL for {table_name}: {e}")
            raise
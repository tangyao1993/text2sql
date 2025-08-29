import logging
from typing import Dict, List, Any, Optional, Tuple
import sqlglot
from sqlglot import transpile
from sqlglot.errors import SqlglotError, ParseError
import time
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from ..config import settings


class SQLValidator:
    """Validates and executes SQL queries."""
    
    def __init__(self):
        """Initialize SQL validator."""
        self.logger = logging.getLogger(__name__)
        self.engine = None
        self.dialect = settings.db_type
        
    def connect(self, database_url: str = None) -> None:
        """
        Connect to the database for execution.
        
        Args:
            database_url: Database connection URL
        """
        try:
            if database_url is None:
                database_url = settings.database_url
                
            self.engine = create_engine(database_url)
            self.logger.info("Connected to database for SQL validation")
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {e}")
            raise
    
    def validate_syntax(self, sql: str) -> Tuple[bool, Optional[str]]:
        """
        Validate SQL syntax.
        
        Args:
            sql: SQL query to validate
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            # Parse SQL with sqlglot
            parsed = sqlglot.parse(sql, dialect=self.dialect)
            
            # If parsing succeeds, syntax is valid
            if parsed:
                self.logger.debug("SQL syntax validation passed")
                return True, None
            else:
                return False, "Empty SQL query"
                
        except ParseError as e:
            error_msg = f"SQL syntax error: {str(e)}"
            self.logger.warning(error_msg)
            return False, error_msg
        except Exception as e:
            error_msg = f"Unexpected error during syntax validation: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg
    
    def validate_semantics(
        self, 
        sql: str, 
        schema_context: Dict[str, Any]
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate SQL semantics against schema.
        
        Args:
            sql: SQL query to validate
            schema_context: Schema information
            
        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            # Parse SQL
            parsed = sqlglot.parse(sql, dialect=self.dialect)[0]
            
            # Extract table and column references
            tables_in_query = set()
            columns_in_query = set()
            
            # Traverse the AST
            for table in parsed.find_all(sqlglot.exp.Table):
                tables_in_query.add(table.name)
            
            for column in parsed.find_all(sqlglot.exp.Column):
                if column.table:
                    columns_in_query.add(f"{column.table.name}.{column.name}")
                else:
                    columns_in_query.add(column.name)
            
            # Check against schema
            available_tables = {t['name'] for t in schema_context.get('tables', [])}
            
            # Validate tables
            invalid_tables = tables_in_query - available_tables
            if invalid_tables:
                return False, f"Invalid table(s): {', '.join(invalid_tables)}"
            
            # Validate columns (simplified check)
            for table_name in tables_in_query:
                table_info = next(
                    (t for t in schema_context.get('tables', []) if t['name'] == table_name),
                    None
                )
                
                if table_info:
                    available_columns = set(table_info['metadata']['columns'])
                    
                    # Check columns from this table
                    table_columns = {
                        col.split('.')[1] if '.' in col else col
                        for col in columns_in_query
                        if col.startswith(table_name + '.') or '.' not in col
                    }
                    
                    invalid_columns = table_columns - available_columns
                    if invalid_columns:
                        return False, f"Invalid column(s) in table {table_name}: {', '.join(invalid_columns)}"
            
            self.logger.debug("SQL semantic validation passed")
            return True, None
            
        except Exception as e:
            error_msg = f"Error during semantic validation: {str(e)}"
            self.logger.error(error_msg)
            return False, error_msg
    
    def dry_run(self, sql: str) -> Tuple[bool, Optional[str]]:
        """
        Perform dry run execution of SQL.
        
        Args:
            sql: SQL query to dry run
            
        Returns:
            Tuple of (success, error_message)
        """
        if not self.engine:
            self.connect()
        
        try:
            # Different databases have different ways to do dry run
            if self.dialect == "mysql":
                # MySQL: Use EXPLAIN
                explain_sql = f"EXPLAIN {sql}"
                with self._execute_with_timeout(explain_sql):
                    pass
            elif self.dialect == "postgresql":
                # PostgreSQL: Use PREPARE
                prepared_sql = f"PREPARE test_plan AS {sql}"
                with self._execute_with_timeout(prepared_sql):
                    pass
            else:
                # Generic: Try to parse and validate
                return self.validate_syntax(sql)
            
            self.logger.debug("SQL dry run successful")
            return True, None
            
        except Exception as e:
            error_msg = f"Dry run failed: {str(e)}"
            self.logger.warning(error_msg)
            return False, error_msg
    
    def execute_query(
        self, 
        sql: str, 
        fetch_results: bool = True,
        timeout: int = None
    ) -> Tuple[bool, Any, Optional[str]]:
        """
        Execute SQL query.
        
        Args:
            sql: SQL query to execute
            fetch_results: Whether to fetch results
            timeout: Execution timeout in seconds
            
        Returns:
            Tuple of (success, results, error_message)
        """
        if not self.engine:
            self.connect()
        
        if timeout is None:
            timeout = settings.sql_timeout
        
        try:
            with self._execute_with_timeout(sql, timeout) as result:
                if fetch_results and result:
                    # Convert to list of dictionaries
                    columns = [col[0] for col in result.cursor.description]
                    rows = result.fetchall()
                    results = [dict(zip(columns, row)) for row in rows]
                else:
                    results = None
                
                self.logger.info(f"SQL executed successfully, returned {len(results) if results else 0} rows")
                return True, results, None
                
        except Exception as e:
            error_msg = f"Execution failed: {str(e)}"
            self.logger.error(error_msg)
            return False, None, error_msg
    
    def explain_query(self, sql: str) -> Optional[Dict[str, Any]]:
        """
        Get query execution plan.
        
        Args:
            sql: SQL query to explain
            
        Returns:
            Execution plan information
        """
        if not self.engine:
            self.connect()
        
        try:
            if self.dialect == "mysql":
                explain_sql = f"EXPLAIN FORMAT=JSON {sql}"
            elif self.dialect == "postgresql":
                explain_sql = f"EXPLAIN (ANALYZE, FORMAT JSON) {sql}"
            else:
                explain_sql = f"EXPLAIN {sql}"
            
            success, results, error = self.execute_query(explain_sql)
            
            if success and results:
                return {
                    'plan': results,
                    'estimated_cost': self._extract_cost_from_plan(results),
                    'query_type': self._classify_query_type(sql)
                }
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error explaining query: {e}")
            return None
    
    def validate_and_fix(
        self, 
        sql: str, 
        schema_context: Dict[str, Any]
    ) -> Tuple[bool, str, Optional[str]]:
        """
        Comprehensive validation with basic auto-fixing.
        
        Args:
            sql: SQL query to validate
            schema_context: Schema information
            
        Returns:
            Tuple of (is_valid, fixed_sql, error_message)
        """
        # 1. Syntax validation
        is_valid, error = self.validate_syntax(sql)
        if not is_valid:
            return False, sql, error
        
        # 2. Semantic validation
        is_valid, error = self.validate_semantics(sql, schema_context)
        if not is_valid:
            # Try basic fixes
            fixed_sql = self._apply_basic_fixes(sql, error)
            if fixed_sql != sql:
                # Re-validate fixed SQL
                is_valid, error = self.validate_syntax(fixed_sql)
                if is_valid:
                    is_valid, error = self.validate_semantics(fixed_sql, schema_context)
                    if is_valid:
                        return True, fixed_sql, None
            return False, sql, error
        
        # 3. Dry run
        is_valid, error = self.dry_run(sql)
        if not is_valid:
            return False, sql, error
        
        return True, sql, None
    
    @contextmanager
    def _execute_with_timeout(self, sql: str, timeout: int = None):
        """Execute SQL with timeout."""
        if timeout is None:
            timeout = settings.sql_timeout
        
        with self.engine.connect() as conn:
            # Set statement timeout if supported
            if self.dialect == "postgresql":
                conn.execute(text(f"SET statement_timeout TO {timeout * 1000}"))
            
            # Execute query
            result = conn.execute(text(sql))
            yield result
    
    def _apply_basic_fixes(self, sql: str, error: str) -> str:
        """Apply basic fixes to common SQL errors."""
        fixed_sql = sql
        
        # Fix missing quotes around string literals
        if "Unknown column" in error:
            # Try to identify unquoted strings
            import re
            pattern = r'= (\w+)(?:\s|$|,|\))'
            matches = re.findall(pattern, sql)
            
            for match in matches:
                if match.isdigit():
                    continue  # Don't quote numbers
                fixed_sql = fixed_sql.replace(f"= {match}", f"= '{match}'")
        
        # Fix missing table prefixes in column names
        if "Column" in error and "in field list" in error:
            # This is a simplified fix - in practice, you'd need more sophisticated logic
            pass
        
        return fixed_sql
    
    def _extract_cost_from_plan(self, plan_results: List[Dict[str, Any]]) -> Optional[float]:
        """Extract estimated cost from execution plan."""
        try:
            if self.dialect == "mysql" and plan_results:
                # MySQL EXPLAIN FORMAT=JSON
                plan_json = plan_results[0].get('EXPLAIN', {})
                return plan_json.get('query_block', {}).get('cost_info', {}).get('query_cost')
            
            elif self.dialect == "postgresql" and plan_results:
                # PostgreSQL EXPLAIN ANALYZE
                plan_json = plan_results[0].get('QUERY PLAN', [{}])[0]
                return plan_json.get('Plan', {}).get('Total Cost')
            
            return None
            
        except Exception as e:
            self.logger.warning(f"Could not extract cost from plan: {e}")
            return None
    
    def _classify_query_type(self, sql: str) -> str:
        """Classify the type of SQL query."""
        sql_upper = sql.upper().strip()
        
        if sql_upper.startswith('SELECT'):
            if 'GROUP BY' in sql_upper:
                return 'aggregation'
            elif 'JOIN' in sql_upper:
                return 'join'
            else:
                return 'simple_select'
        elif sql_upper.startswith('INSERT'):
            return 'insert'
        elif sql_upper.startswith('UPDATE'):
            return 'update'
        elif sql_upper.startswith('DELETE'):
            return 'delete'
        else:
            return 'other'
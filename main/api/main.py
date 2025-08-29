"""
FastAPI web interface for Text2SQL.
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Dict, List, Any, Optional
import logging
from datetime import datetime

from ..text2sql import Text2SQL
from ..config import settings

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Text2SQL
text2sql = Text2SQL()

# FastAPI app
app = FastAPI(
    title="Text2SQL API",
    description="RAG-based Text-to-SQL API",
    version="0.1.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models
class QueryRequest(BaseModel):
    query: str = Field(..., description="Natural language query")
    max_corrections: int = Field(default=3, description="Maximum correction attempts")
    show_intermediate: bool = Field(default=False, description="Show intermediate results")

class QueryResponse(BaseModel):
    query: str
    sql: Optional[str]
    is_valid: bool
    execution_results: Optional[List[Dict[str, Any]]]
    correction_attempts: int
    error: Optional[str]
    intermediate: Optional[Dict[str, Any]]

class BuildRequest(BaseModel):
    force_rebuild: bool = Field(default=False, description="Force rebuild even if exists")
    business_rules: Optional[Dict[str, Any]] = Field(default=None, description="Business rules")

class BusinessRuleRequest(BaseModel):
    rule_name: str = Field(..., description="Name of the business rule")
    rule_definition: str = Field(..., description="Definition of the business rule")

class ValidateRequest(BaseModel):
    sql: str = Field(..., description="SQL query to validate")

class ValidateResponse(BaseModel):
    is_valid: bool
    error: Optional[str]
    fixed_sql: Optional[str]

@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "Text2SQL API",
        "version": "0.1.0",
        "docs": "/docs"
    }

@app.post("/query", response_model=QueryResponse)
async def query_to_sql(request: QueryRequest):
    """Convert natural language query to SQL."""
    try:
        result = text2sql.query_to_sql(
            query=request.query,
            max_correction_attempts=request.max_corrections,
            return_intermediate=request.show_intermediate
        )
        return QueryResponse(**result)
    except Exception as e:
        logger.error(f"Error in query_to_sql: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/build")
async def build_knowledge_base(request: BuildRequest, background_tasks: BackgroundTasks):
    """Build knowledge base from database metadata."""
    try:
        # Run in background for long operations
        background_tasks.add_task(
            text2sql.build_knowledge_base,
            business_rules=request.business_rules,
            force_rebuild=request.force_rebuild
        )
        return {"message": "Knowledge base building started"}
    except Exception as e:
        logger.error(f"Error building knowledge base: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/schema")
async def get_schema(table_name: Optional[str] = None):
    """Get database schema information."""
    try:
        schema_info = text2sql.get_schema_info(table_name)
        return schema_info
    except Exception as e:
        logger.error(f"Error getting schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/business-rules")
async def add_business_rule(request: BusinessRuleRequest):
    """Add a business rule to the knowledge base."""
    try:
        text2sql.add_business_rule(
            rule_name=request.rule_name,
            rule_definition=request.rule_definition
        )
        return {"message": "Business rule added successfully"}
    except Exception as e:
        logger.error(f"Error adding business rule: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/validate", response_model=ValidateResponse)
async def validate_sql(request: ValidateRequest):
    """Validate SQL query."""
    try:
        # For validation, we need schema context
        # This is a simplified version
        is_valid, error = text2sql.sql_validator.validate_syntax(request.sql)
        return ValidateResponse(
            is_valid=is_valid,
            error=error,
            fixed_sql=None
        )
    except Exception as e:
        logger.error(f"Error validating SQL: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats")
async def get_stats():
    """Get system statistics."""
    try:
        stats = text2sql.get_stats()
        return stats
    except Exception as e:
        logger.error(f"Error getting stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "0.1.0"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "text2sql.api.main:app",
        host=settings.app_host,
        port=settings.app_port,
        reload=settings.debug
    )
import os
from typing import Optional, Dict, Any
from pydantic import BaseSettings, Field


class Settings(BaseSettings):
    """Application settings with environment variable support."""
    
    # LLM Configuration
    llm_model_name: str = Field(default="deepseek-r1:32b", env="LLM_MODEL_NAME")
    llm_base_url: str = Field(default="http://localhost:11434", env="LLM_BASE_URL")
    llm_temperature: float = Field(default=0.1, env="LLM_TEMPERATURE")
    llm_max_tokens: int = Field(default=2048, env="LLM_MAX_TOKENS")
    
    # Embedding Model Configuration
    embedding_model_name: str = Field(
        default="BAAI/bge-large-zh", 
        env="EMBEDDING_MODEL_NAME"
    )
    embedding_device: str = Field(default="cpu", env="EMBEDDING_DEVICE")
    
    # Vector Database Configuration
    vector_db_path: str = Field(default="./chroma_db", env="VECTOR_DB_PATH")
    vector_db_collection_name: str = Field(
        default="text2sql_knowledge", 
        env="VECTOR_DB_COLLECTION_NAME"
    )
    
    # Database Configuration
    db_type: str = Field(default="mysql", env="DB_TYPE")
    db_host: str = Field(default="localhost", env="DB_HOST")
    db_port: int = Field(default=3306, env="DB_PORT")
    db_user: str = Field(default="root", env="DB_USER")
    db_password: str = Field(default="", env="DB_PASSWORD")
    db_name: str = Field(default="target_db", env="DB_NAME")
    
    # Application Configuration
    app_host: str = Field(default="0.0.0.0", env="APP_HOST")
    app_port: int = Field(default=8000, env="APP_PORT")
    debug: bool = Field(default=True, env="DEBUG")
    
    # RAG Configuration
    rag_top_k: int = Field(default=3, env="RAG_TOP_K")
    rag_score_threshold: float = Field(default=0.5, env="RAG_SCORE_THRESHOLD")
    rag_chunk_size: int = Field(default=1000, env="RAG_CHUNK_SIZE")
    rag_chunk_overlap: int = Field(default=200, env="RAG_CHUNK_OVERLAP")
    
    # Validation Configuration
    max_correction_attempts: int = Field(default=3, env="MAX_CORRECTION_ATTEMPTS")
    sql_timeout: int = Field(default=30, env="SQL_TIMEOUT")
    
    @property
    def database_url(self) -> str:
        """Construct database URL from components."""
        if self.db_type == "mysql":
            return f"mysql+pymysql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        elif self.db_type == "postgresql":
            return f"postgresql+psycopg2://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


# Global settings instance
settings = Settings()
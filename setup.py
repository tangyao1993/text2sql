from setuptools import setup, find_packages

setup(
    name="text2sql",
    version="0.1.0",
    description="RAG-based Text-to-SQL system",
    author="Hive Mind Collective",
    python_requires=">=3.10",
    packages=find_packages(),
    install_requires=[
        "ollama>=0.1.0",
        "chromadb>=0.4.0",
        "langchain>=0.1.0",
        "langchain-community>=0.0.20",
        "langchain-core>=0.1.0",
        "langgraph>=0.0.40",
        "sqlglot>=18.0.0",
        "sentence-transformers>=2.2.0",
        "torch>=2.0.0",
        "numpy>=1.24.0",
        "pandas>=2.0.0",
        "sqlalchemy>=2.0.0",
        "pymysql>=1.0.0",
        "psycopg2-binary>=2.9.0",
        "fastapi>=0.100.0",
        "uvicorn>=0.20.0",
        "pydantic>=2.0.0",
        "python-dotenv>=1.0.0",
        "click>=8.0.0",
        "rich>=13.0.0",
        "matplotlib>=3.7.0",
        "seaborn>=0.12.0",
        "plotly>=5.15.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-asyncio>=0.21.0",
            "httpx>=0.24.0",
        ]
    },
    entry_points={
        "console_scripts": [
            "text2sql=text2sql.cli:main",
        ],
    },
)
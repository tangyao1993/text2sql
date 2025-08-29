import logging
from typing import List, Dict, Any, Optional
import chromadb
from chromadb.config import Settings as ChromaSettings
from chromadb.utils import embedding_functions

from ..config import settings


class VectorDBManager:
    """Manages vector database operations for storing and retrieving schema information."""
    
    def __init__(self):
        """Initialize the vector database manager."""
        self.logger = logging.getLogger(__name__)
        self.client = self._initialize_client()
        self.collection = self._get_or_create_collection()
        
    def _initialize_client(self) -> chromadb.Client:
        """Initialize ChromaDB client."""
        try:
            client = chromadb.PersistentClient(
                path=settings.vector_db_path,
                settings=ChromaSettings(
                    anonymized_telemetry=False,
                    allow_reset=True
                )
            )
            self.logger.info(f"ChromaDB client initialized at: {settings.vector_db_path}")
            return client
        except Exception as e:
            self.logger.error(f"Failed to initialize ChromaDB: {e}")
            raise
    
    def _get_or_create_collection(self):
        """Get or create the collection for storing schema information."""
        try:
            collection = self.client.get_or_create_collection(
                name=settings.vector_db_collection_name,
                metadata={
                    "description": "Text2SQL knowledge base with database schemas",
                    "created_by": "text2sql"
                }
            )
            self.logger.info(f"Collection ready: {settings.vector_db_collection_name}")
            return collection
        except Exception as e:
            self.logger.error(f"Failed to get/create collection: {e}")
            raise
    
    def add_documents(
        self, 
        documents: List[str], 
        metadatas: List[Dict[str, Any]],
        ids: List[str]
    ) -> None:
        """
        Add documents to the vector database.
        
        Args:
            documents: List of document texts
            metadatas: List of metadata dictionaries
            ids: List of unique document IDs
        """
        try:
            self.collection.add(
                documents=documents,
                metadatas=metadatas,
                ids=ids
            )
            self.logger.info(f"Added {len(documents)} documents to vector DB")
        except Exception as e:
            self.logger.error(f"Error adding documents: {e}")
            raise
    
    def search(
        self, 
        query: str, 
        top_k: int = None,
        where: Optional[Dict[str, Any]] = None,
        where_document: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Search for relevant documents.
        
        Args:
            query: Search query text
            top_k: Number of results to return
            where: Metadata filter conditions
            where_document: Document content filter conditions
            
        Returns:
            List of search results with documents, metadata, and distances
        """
        try:
            if top_k is None:
                top_k = settings.rag_top_k
            
            results = self.collection.query(
                query_texts=[query],
                n_results=top_k,
                where=where,
                where_document=where_document
            )
            
            # Format results
            formatted_results = []
            for i in range(len(results['documents'][0])):
                formatted_results.append({
                    'document': results['documents'][0][i],
                    'metadata': results['metadatas'][0][i],
                    'distance': results['distances'][0][i],
                    'id': results['ids'][0][i]
                })
            
            self.logger.info(f"Found {len(formatted_results)} results for query: {query[:50]}...")
            return formatted_results
        except Exception as e:
            self.logger.error(f"Error searching documents: {e}")
            raise
    
    def get_document(self, doc_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve a specific document by ID.
        
        Args:
            doc_id: Document ID
            
        Returns:
            Document data or None if not found
        """
        try:
            results = self.collection.get(
                ids=[doc_id],
                include=['documents', 'metadatas']
            )
            
            if results['documents']:
                return {
                    'document': results['documents'][0],
                    'metadata': results['metadatas'][0],
                    'id': results['ids'][0]
                }
            return None
        except Exception as e:
            self.logger.error(f"Error retrieving document: {e}")
            return None
    
    def update_document(
        self, 
        doc_id: str, 
        document: str = None, 
        metadata: Dict[str, Any] = None
    ) -> None:
        """
        Update an existing document.
        
        Args:
            doc_id: Document ID to update
            document: New document text (optional)
            metadata: New metadata (optional)
        """
        try:
            update_kwargs = {"ids": [doc_id]}
            
            if document is not None:
                update_kwargs["documents"] = [document]
            
            if metadata is not None:
                update_kwargs["metadatas"] = [metadata]
            
            self.collection.update(**update_kwargs)
            self.logger.info(f"Updated document: {doc_id}")
        except Exception as e:
            self.logger.error(f"Error updating document: {e}")
            raise
    
    def delete_document(self, doc_id: str) -> None:
        """
        Delete a document by ID.
        
        Args:
            doc_id: Document ID to delete
        """
        try:
            self.collection.delete(ids=[doc_id])
            self.logger.info(f"Deleted document: {doc_id}")
        except Exception as e:
            self.logger.error(f"Error deleting document: {e}")
            raise
    
    def list_documents(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        List all documents in the collection.
        
        Args:
            limit: Maximum number of documents to return
            
        Returns:
            List of document metadata
        """
        try:
            results = self.collection.get(
                limit=limit,
                include=['metadatas']
            )
            
            documents = []
            for i in range(len(results['ids'])):
                documents.append({
                    'id': results['ids'][i],
                    'metadata': results['metadatas'][i]
                })
            
            return documents
        except Exception as e:
            self.logger.error(f"Error listing documents: {e}")
            return []
    
    def count_documents(self) -> int:
        """
        Get the total number of documents in the collection.
        
        Returns:
            Document count
        """
        try:
            return self.collection.count()
        except Exception as e:
            self.logger.error(f"Error counting documents: {e}")
            return 0
    
    def clear_collection(self) -> None:
        """Clear all documents from the collection."""
        try:
            # Delete and recreate collection
            self.client.delete_collection(settings.vector_db_collection_name)
            self.collection = self._get_or_create_collection()
            self.logger.info("Collection cleared and recreated")
        except Exception as e:
            self.logger.error(f"Error clearing collection: {e}")
            raise
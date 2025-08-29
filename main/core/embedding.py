import logging
from typing import List, Dict, Any
from sentence_transformers import SentenceTransformer
import numpy as np

from ..config import settings


class EmbeddingManager:
    """Manages text embeddings for the RAG system."""
    
    def __init__(self):
        """Initialize the embedding manager."""
        self.logger = logging.getLogger(__name__)
        self.model = self._initialize_model()
        
    def _initialize_model(self) -> SentenceTransformer:
        """Initialize the sentence transformer model."""
        try:
            model = SentenceTransformer(
                settings.embedding_model_name,
                device=settings.embedding_device
            )
            self.logger.info(f"Embedding model loaded: {settings.embedding_model_name}")
            return model
        except Exception as e:
            self.logger.error(f"Failed to load embedding model: {e}")
            raise
    
    def embed_documents(self, documents: List[str]) -> List[List[float]]:
        """
        Generate embeddings for a list of documents.
        
        Args:
            documents: List of text documents
            
        Returns:
            List of embedding vectors
        """
        try:
            embeddings = self.model.encode(
                documents,
                convert_to_numpy=True,
                normalize_embeddings=True
            )
            return embeddings.tolist()
        except Exception as e:
            self.logger.error(f"Error generating embeddings: {e}")
            raise
    
    def embed_query(self, query: str) -> List[float]:
        """
        Generate embedding for a single query.
        
        Args:
            query: Query text
            
        Returns:
            Query embedding vector
        """
        try:
            embedding = self.model.encode(
                query,
                convert_to_numpy=True,
                normalize_embeddings=True
            )
            return embedding.tolist()
        except Exception as e:
            self.logger.error(f"Error generating query embedding: {e}")
            raise
    
    def similarity_search(
        self, 
        query_embedding: List[float], 
        document_embeddings: List[List[float]],
        top_k: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Find most similar documents using cosine similarity.
        
        Args:
            query_embedding: Query embedding vector
            document_embeddings: List of document embedding vectors
            top_k: Number of top results to return
            
        Returns:
            List of dictionaries with index and similarity score
        """
        try:
            # Convert to numpy arrays
            query_vec = np.array(query_embedding)
            doc_vecs = np.array(document_embeddings)
            
            # Calculate cosine similarity
            similarities = np.dot(doc_vecs, query_vec)
            
            # Get top-k results
            top_indices = np.argsort(similarities)[::-1][:top_k]
            
            results = [
                {
                    "index": int(idx),
                    "score": float(similarities[idx])
                }
                for idx in top_indices
            ]
            
            return results
        except Exception as e:
            self.logger.error(f"Error in similarity search: {e}")
            raise
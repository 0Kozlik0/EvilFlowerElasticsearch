from elasticsearch import AsyncElasticsearch
from typing import Dict, Any, Optional
from decouple import config

class ElasticsearchClient:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance.initialize()
        return cls._instance

    def initialize(self):
        elasticsearch_url = config('ELASTICSEARCH_URL', default="http://elasticsearch:9200")
        self.client = AsyncElasticsearch([elasticsearch_url])

    async def index_document(self, index_name: str, document: Dict[str, Any], document_id: Optional[str] = None) -> Dict:
        """Index a document in Elasticsearch"""
        response = await self.client.index(
            index=index_name,
            document=document,
            id=document_id,
            refresh=True
        )
        return response

    async def search_documents(self, index_name: str, query: Dict) -> Dict:
        """Search for documents in Elasticsearch"""
        response = await self.client.search(
            index=index_name,
            body=query
        )
        return response

    async def close(self):
        """Close the Elasticsearch connection"""
        await self.client.close()
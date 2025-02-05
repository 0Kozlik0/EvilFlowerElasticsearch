from elasticsearch import AsyncElasticsearch
from typing import Dict, Any, Optional
from decouple import config

class ElasticsearchClient:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        elasticsearch_url = config('ELASTICSEARCH_URL', default="http://elasticsearch:9200")
        self.client = AsyncElasticsearch([elasticsearch_url])

    async def check_connection(self) -> bool:
        """Check if the connection to Elasticsearch is alive"""
        try:
            await self.client.info()
            return True
        except Exception:
            return False
    
    async def close(self):
        """Close the Elasticsearch connection"""
        await self.client.close()

    async def save_extracted_text_to_elasticsearch(self) -> Dict:
        pass

    async def save_extracted_image_to_elasticsearch(self) -> Dict:
        pass

    async def save_extracted_video_to_elasticsearch(self) -> Dict:
        pass

    async def save_extracted_equations_to_elasticsearch(self) -> Dict:
        pass
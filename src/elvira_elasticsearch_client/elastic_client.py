from elasticsearch import AsyncElasticsearch
from typing import Tuple, List, Dict, Any, Optional
from decouple import config
from datetime import datetime

class ElasticsearchClient:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        elasticsearch_url = config('ELASTICSEARCH_URL', default="http://localhost:9200")
        print(elasticsearch_url)
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

    async def save_extracted_metadata_to_elasticsearch(self) -> Dict:
        pass

    async def save_extracted_text_to_elasticsearch(self, document_id: str, text_data: List[str], metadata: Optional[dict] = None) -> Dict:
        """Save extracted text data to Elasticsearch using approach with multiple indices"""

        # Check connection to Elasticsearch
        if not await self.check_connection():
            return {"status": "error", "message": "Cannot connect to Elasticsearch"}
        
        try:
            pages = text_data[0]  # Get all pages as text
            paragraphs = text_data[1]  # Get all paragraphs by page
            sentences = text_data[2]  # Get all sentences by paragraph
            
            bulk_operations = [] # Array for bulk operations
            
            # Create document in "documents" index
            doc_metadata = {
                "document_id": document_id,
                # "full_text": " ".join(pages),
                "timestamp": datetime.now().isoformat()
            }
            
            # Add metadata if provided
            if metadata:
                doc_metadata.update(metadata)
            
            # Add document to bulk operations
            bulk_operations.append({"index": {"_index": "documents", "_id": document_id}})
            bulk_operations.append(doc_metadata)
            
            # Add pages to "pages" index
            for page_idx, page_text in enumerate(pages):
                page_id = f"{document_id}_p{page_idx + 1}"
                page_doc = {
                    "page_id": page_id,
                    "document_id": document_id,
                    "page_number": page_idx + 1,
                    "text": page_text,
                    "timestamp": datetime.now().isoformat()
                }
                
                bulk_operations.append({"index": {"_index": "pages", "_id": page_id}})
                bulk_operations.append(page_doc)
            
            # Add paragraphs to "paragraphs" index
            for page_idx, page_paragraphs in enumerate(paragraphs):
                if not page_paragraphs:
                    continue
                    
                page_id = f"{document_id}_p{page_idx + 1}"
                
                for para_idx, paragraph_text in enumerate(page_paragraphs):
                    para_id = f"{page_id}_par{para_idx + 1}"
                    para_doc = {
                        "paragraph_id": para_id,
                        "page_id": page_id,
                        "document_id": document_id,
                        "paragraph_number": para_idx + 1,
                        "text": paragraph_text,
                        "timestamp": datetime.now().isoformat()
                    }
                    
                    bulk_operations.append({"index": {"_index": "paragraphs", "_id": para_id}})
                    bulk_operations.append(para_doc)
            
            # Add sentences to "sentences" index
            for page_idx, page_sentences in enumerate(sentences):
                if not page_sentences:
                    continue
                    
                page_id = f"{document_id}_p{page_idx + 1}"
                
                for para_idx, paragraph_sentences in enumerate(page_sentences):
                    if not paragraph_sentences:
                        continue
                        
                    para_id = f"{page_id}_par{para_idx + 1}"
                    
                    for sent_idx, sentence_text in enumerate(paragraph_sentences):
                        sent_id = f"{para_id}_s{sent_idx + 1}"
                        sent_doc = {
                            "sentence_id": sent_id,
                            "paragraph_id": para_id,
                            "page_id": page_id,
                            "document_id": document_id,
                            "position": sent_idx + 1,
                            "text": sentence_text,
                            "timestamp": datetime.now().isoformat()
                        }
                        
                        bulk_operations.append({"index": {"_index": "sentences", "_id": sent_id}})
                        bulk_operations.append(sent_doc)
            
            # Execute all bulk operations in a single request
            if bulk_operations:
                result = await self.client.bulk(operations=bulk_operations, refresh=True)
                
                # Check if the operations were successful or not
                if result.get("errors", False):
                    errors = [item["index"]["error"] for item in result["items"] if "error" in item.get("index", {})]
                    return {
                        "status": "partial_success" if len(errors) < len(bulk_operations) // 2 else "error",
                        "document_id": document_id,
                        "message": f"Encountered {len(errors)} errors during indexing",
                        "errors": errors
                    }
                
                # Get numbers of items added to indices
                pages_count = len(pages)
                paragraphs_count = sum(len(page_paragraphs) for page_paragraphs in paragraphs if page_paragraphs)
                sentences_count = sum(
                    sum(len(paragraph_sentences) for paragraph_sentences in page_sentences if paragraph_sentences)
                    for page_sentences in sentences if page_sentences
                )
                
                return {
                    "status": "success",
                    "document_id": document_id,
                    "indexed_items": {
                        "documents": 1,
                        "pages": pages_count,
                        "paragraphs": paragraphs_count,
                        "sentences": sentences_count
                    }
                }
            
            return {"status": "warning", "message": "No data to save into index"}
            
        except Exception as e:
            return {
                "status": "error",
                "document_id": document_id,
                "message": str(e)
            }

    async def save_extracted_image_to_elasticsearch(self, document_id: str, images_data: List[Tuple[str, str]], page_numbers: Optional[List[int]] = None) -> Dict:
        """Save extracted image metadata (captions and labels) to Elasticsearch"""

        # Check connection to Elasticsearch
        if not await self.check_connection():
            return {"status": "error", "message": "Cannot connect to Elasticsearch"}
        
        try:
            bulk_operations = [] # Array for bulk operations
            
            # Add each image to "images" index
            for idx, (caption, labels) in enumerate(images_data):
                image_id = f"{document_id}_img_{idx+1}"
                
                labels_list = [label.strip() for label in labels.split(',')] if labels else [] # Create list of labels from comma-separated string
                
                # Create index record for image
                image_doc = {
                    "image_id": image_id,
                    "document_id": document_id,
                    "position": idx + 1,
                    "caption": caption,
                    "labels": labels_list,
                    "timestamp": datetime.now().isoformat()
                }
                
                # Add page number if available
                if page_numbers and idx < len(page_numbers):
                    image_doc["page_number"] = page_numbers[idx]
                
                # Add to bulk operations
                bulk_operations.append({"index": {"_index": "images", "_id": image_id}})
                bulk_operations.append(image_doc)
            
            # Execute all bulk operations in a single request
            if bulk_operations:
                result = await self.client.bulk(operations=bulk_operations, refresh=True)
                
                # Check if the operations were successful or not
                if result.get("errors", False):
                    errors = [item["index"]["error"] for item in result["items"] if "error" in item.get("index", {})]
                    return {
                        "status": "partial_success" if len(errors) < len(bulk_operations) // 2 else "error",
                        "document_id": document_id,
                        "message": f"Encountered {len(errors)} errors during indexing",
                        "errors": errors
                    }
                
                # Return success response
                return {
                    "status": "success",
                    "document_id": document_id,
                    "images_indexed": len(images_data),
                }
            
            else:
                return {
                    "status": "warning",
                    "document_id": document_id,
                    "message": "No image data to save into index"
                }
            
        except Exception as e:
            return {
                "status": "error",
                "document_id": document_id,
                "message": str(e)
            }
    
    async def save_extracted_video_to_elasticsearch(self) -> Dict:
        pass

    async def save_extracted_equations_to_elasticsearch(self) -> Dict:
        pass

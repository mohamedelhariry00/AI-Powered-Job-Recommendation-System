import json
import logging
from typing import List
import boto3
import time

logger = logging.getLogger(__name__)

class EmbeddingService:
    def __init__(self):
        self.region = 'us-east-1'  # Titan is available in us-east-1
        self.bedrock_client = boto3.client('bedrock-runtime', region_name=self.region)
        
        # Try different model IDs in order of preference
        self.model_ids = [
            'amazon.titan-embed-text-v2:0',  # Latest version
            'amazon.titan-embed-text-v1'     # Fallback
        ]
        
        self.current_model = None
        self.embedding_dims = None
        self._initialize_model()
        
    def _initialize_model(self):
        """Initialize and validate the embedding model"""
        for model_id in self.model_ids:
            try:
                # Test the model with a simple text
                test_response = self._call_bedrock(model_id, "test")
                if test_response:
                    self.current_model = model_id
                    self.embedding_dims = len(test_response)
                    logger.info(f"Successfully initialized model: {model_id} with {self.embedding_dims} dimensions")
                    return
            except Exception as e:
                logger.warning(f"Failed to initialize model {model_id}: {str(e)}")
                continue
        
        # If all models fail
        logger.error("Failed to initialize any Titan embedding model")
        raise Exception("No Titan embedding model available")
    
    def _call_bedrock(self, model_id: str, text: str, max_retries: int = 3) -> List[float]:
        """Call Bedrock API with retry logic and enhanced debugging"""
        for attempt in range(max_retries):
            try:
                # Prepare request based on model version
                if 'v2' in model_id:
                    request_body = {
                        "inputText": text,
                        "dimensions": 1024,  # v2 supports different dimensions
                        "normalize": True
                    }
                else:
                    request_body = {
                        "inputText": text
                    }
                
                logger.debug(f"Bedrock request for model {model_id}: {json.dumps(request_body, default=str)[:200]}")
                
                # Call Bedrock API
                response = self.bedrock_client.invoke_model(
                    modelId=model_id,
                    body=json.dumps(request_body),
                    contentType='application/json',
                    accept='application/json'
                )
                
                # Parse response
                response_body = json.loads(response['body'].read())
                logger.debug(f"Bedrock response keys: {list(response_body.keys())}")
                
                embedding = response_body.get('embedding')
                
                # Enhanced validation with detailed logging
                if embedding is None:
                    logger.error(f"Bedrock returned None embedding. Full response: {response_body}")
                    raise ValueError("Bedrock returned None embedding")
                
                if not isinstance(embedding, list):
                    logger.error(f"Embedding is not a list: {type(embedding)}")
                    raise ValueError(f"Embedding is not a list: {type(embedding)}")
                
                if len(embedding) == 0:
                    logger.error("Embedding is empty list")
                    raise ValueError("Embedding is empty list")
                
                # Check for None values in the embedding
                none_indices = [i for i, val in enumerate(embedding) if val is None]
                if none_indices:
                    logger.error(f"Embedding contains None values at indices: {none_indices[:10]}...")
                    raise ValueError(f"Embedding contains {len(none_indices)} None values")
                
                # Check for non-numeric values
                non_numeric_indices = [i for i, val in enumerate(embedding) if not isinstance(val, (int, float))]
                if non_numeric_indices:
                    logger.error(f"Embedding contains non-numeric values at indices: {non_numeric_indices[:10]}...")
                    raise ValueError(f"Embedding contains {len(non_numeric_indices)} non-numeric values")
                
                logger.debug(f"Generated valid embedding with {len(embedding)} dimensions using {model_id}")
                logger.debug(f"Sample embedding values: {embedding[:5]} ... {embedding[-5:]}")
                return embedding
                
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed for model {model_id}: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"All attempts failed for model {model_id}")
                    raise e
    
    def generate_embedding(self, text: str) -> List[float]:
        """Generate embedding for given text with enhanced debugging"""
        try:
            if not text or len(text.strip()) < 5:
                raise ValueError("Text too short for embedding generation")
            
            # Clean and truncate text if necessary
            text = text.strip()
            
            # Titan has token limits, truncate if too long
            if len(text) > 8000:  # Conservative limit
                text = text[:8000] + "..."
                logger.warning("Text truncated for embedding generation")
            
            logger.info(f"Generating embedding for text length: {len(text)} characters")
            logger.debug(f"Text preview: {text[:100]}...")
            
            # Generate embedding using current model
            embedding = self._call_bedrock(self.current_model, text)
            
            # Final validation before returning
            if not embedding:
                logger.error("Final embedding validation failed: embedding is falsy")
                raise ValueError("Generated embedding is falsy")
            
            if not isinstance(embedding, list):
                logger.error(f"Final embedding validation failed: not a list, type={type(embedding)}")
                raise ValueError(f"Generated embedding is not a list: {type(embedding)}")
            
            if len(embedding) == 0:
                logger.error("Final embedding validation failed: empty list")
                raise ValueError("Generated embedding is empty")
            
            # Check dimensions match expected
            if self.embedding_dims and len(embedding) != self.embedding_dims:
                logger.warning(f"Embedding dimension mismatch: got {len(embedding)}, expected {self.embedding_dims}")
            
            # Final check for None values
            if any(val is None for val in embedding):
                none_count = sum(1 for val in embedding if val is None)
                logger.error(f"Final validation failed: {none_count} None values in embedding")
                raise ValueError(f"Generated embedding contains {none_count} None values")
            
            # Final check for non-numeric values
            non_numeric_vals = [val for val in embedding if not isinstance(val, (int, float))]
            if non_numeric_vals:
                logger.error(f"Final validation failed: non-numeric values: {non_numeric_vals[:5]}")
                raise ValueError(f"Generated embedding contains non-numeric values")
            
            logger.info(f"Successfully generated and validated embedding with {len(embedding)} dimensions")
            return embedding
            
        except Exception as e:
            logger.error(f"Error generating embedding: {str(e)}")
            logger.error(f"Text that caused error (first 200 chars): {text[:200] if text else 'None'}")
            raise e
    
    def get_model_info(self) -> dict:
        """Get information about the current model"""
        return {
            'current_model': self.current_model,
            'embedding_dimensions': self.embedding_dims,
            'available_models': self.model_ids,
            'region': self.region
        }
    
    def test_service(self) -> dict:
        """Test the embedding service with detailed diagnostics"""
        try:
            test_texts = [
                "This is a test for the embedding service.",
                "Software engineer with Python experience",
                "Data analyst position at tech company"
            ]
            
            results = []
            for i, test_text in enumerate(test_texts):
                try:
                    embedding = self.generate_embedding(test_text)
                    
                    results.append({
                        'test_case': i + 1,
                        'text_length': len(test_text),
                        'embedding_dimensions': len(embedding),
                        'has_nulls': any(val is None for val in embedding),
                        'all_numeric': all(isinstance(val, (int, float)) for val in embedding),
                        'min_value': min(embedding),
                        'max_value': max(embedding),
                        'status': 'success'
                    })
                except Exception as e:
                    results.append({
                        'test_case': i + 1,
                        'text_length': len(test_text),
                        'error': str(e),
                        'status': 'failed'
                    })
            
            overall_success = all(result['status'] == 'success' for result in results)
            
            return {
                'overall_status': 'success' if overall_success else 'partial_failure',
                'model_used': self.current_model,
                'expected_dimensions': self.embedding_dims,
                'test_results': results
            }
            
        except Exception as e:
            return {
                'overall_status': 'failed',
                'error': str(e),
                'model_attempted': self.current_model
            }
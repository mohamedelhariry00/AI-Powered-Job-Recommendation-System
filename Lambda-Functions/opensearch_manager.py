import json
import logging
import os
from typing import Dict, Any
import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class OpenSearchManager:
    def __init__(self):
        self.region = os.environ.get("AWS_REGION", "us-east-1")
        self.host = self._get_domain_host()
        self.cv_index = 'cv-index'
        self.job_index = 'job-index'
        
        # Set up AWS authentication for OpenSearch
        session = boto3.Session()
        credentials = session.get_credentials()
        self.awsauth = AWS4Auth(
            credentials.access_key,
            credentials.secret_key, 
            self.region,
            'es',
            session_token=credentials.token
        )
        
        # Initialize OpenSearch client with proper authentication
        self.client = OpenSearch(
            hosts=[{'host': self.host, 'port': 443}],
            http_auth=self.awsauth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            timeout=60,
            max_retries=3,
            retry_on_timeout=True
        )
        
        logger.info(f"Initialized OpenSearchManager with host: {self.host}")
    
    def _get_domain_host(self) -> str:
        """Get OpenSearch domain host (without https://) with robust error handling"""
        try:
            # Method 1: Environment variable
            endpoint = os.environ.get('OPENSEARCH_ENDPOINT')
            if endpoint and len(endpoint) < 200:  # Sanity check for clean endpoint
                host = endpoint.strip().replace('https://', '').replace('http://', '')
                if '.' in host and 'amazonaws.com' in host:  # Basic validation
                    logger.info(f"Using OpenSearch host from env var: {host}")
                    return host
            
            # Method 2: AWS OpenSearch API
            domain_name = os.environ.get('OPENSEARCH_DOMAIN_NAME', 'new-job-recommendationdomain')
            
            try:
                client = boto3.client('opensearch', region_name=self.region)
                response = client.describe_domain(DomainName=domain_name)
                endpoint = response['DomainStatus']['Endpoint']
                host = endpoint.strip().replace('https://', '').replace('http://', '')
                logger.info(f"Retrieved OpenSearch host from AWS OpenSearch API: {host}")
                return host
            except Exception as e:
                logger.warning(f"OpenSearch API failed: {str(e)}")
            
            # Method 3: AWS ES API (legacy fallback)
            try:
                client = boto3.client('es', region_name=self.region)
                response = client.describe_elasticsearch_domain(DomainName=domain_name)
                endpoint = response['DomainStatus']['Endpoint']
                host = endpoint.strip().replace('https://', '').replace('http://', '')
                logger.info(f"Retrieved OpenSearch host from AWS ES API: {host}")
                return host
            except Exception as e:
                logger.warning(f"ES API failed: {str(e)}")
            
            # Method 4: Hardcoded fallback
            fallback_host = "search-new-job-recommendationdomain-equlis5ogx733rohqkaxrlabu4.us-east-1.es.amazonaws.com"
            logger.warning(f"Using hardcoded fallback host: {fallback_host}")
            return fallback_host
            
        except Exception as e:
            logger.error(f"Error getting OpenSearch host: {str(e)}")
            # Final emergency fallback
            return "search-new-job-recommendationdomain-equlis5ogx733rohqkaxrlabu4.us-east-1.es.amazonaws.com"
    
    def index_cv_document(self, user_id: str, document: Dict[str, Any]) -> Dict:
        """Index a CV document in OpenSearch"""
        try:
            # Validate document has required fields
            required_fields = ['user_id', 'cv_text']
            for field in required_fields:
                if field not in document:
                    raise ValueError(f"Missing required field: {field}")
            
            # Handle cv_embedding - only include if valid
            if 'cv_embedding' in document:
                embedding = document.get('cv_embedding')
                if not embedding or len(embedding) == 0:
                    logger.warning(f"CV embedding is empty for user {user_id}, removing from document")
                    document = {k: v for k, v in document.items() if k != 'cv_embedding'}
            
            response = self.client.index(
                index=self.cv_index, 
                id=user_id, 
                body=document, 
                refresh=True
            )
            
            logger.info(f"Successfully indexed CV document for user {user_id}")
            return response
            
        except Exception as e:
            logger.error(f"Error indexing CV document for user {user_id}: {str(e)}")
            raise e
    
    def get_cv_by_user_id(self, user_id: str) -> Dict:
        """Get CV document by user_id"""
        try:
            if not self.client.indices.exists(index=self.cv_index):
                logger.warning(f"CV index {self.cv_index} does not exist")
                return None
            
            # Search for CV document by user_id
            search_body = {
                "query": {
                    "term": {
                        "user_id": user_id
                    }
                },
                "size": 1,
                "_source": {
                    "exclude": ["cv_text"]  # Exclude large text field, keep embedding and metadata
                }
            }
            
            result = self.client.search(index=self.cv_index, body=search_body)
            
            hits = result.get('hits', {}).get('hits', [])
            if not hits:
                logger.info(f"No CV found for user_id: {user_id}")
                return None
            
            cv_document = hits[0]['_source']
            logger.info(f"Retrieved CV for user {user_id} with {len(cv_document.get('cv_embedding', []))} embedding dimensions")
            
            return cv_document
            
        except Exception as e:
            logger.error(f"Error retrieving CV for user {user_id}: {str(e)}")
            return None
    
    def index_job_document(self, job_id: str, document: Dict[str, Any]) -> Dict:
        """Index a job document in OpenSearch - FIXED VERSION"""
        try:
            # Create a deep copy of the document to avoid modifying the original
            import copy
            doc_to_index = copy.deepcopy(document)
            
            # Debug logging
            logger.info(f"Attempting to index job {job_id}")
            logger.info(f"Document keys: {list(doc_to_index.keys())}")
            
            # Handle job_embedding field very carefully
            if 'job_embedding' in doc_to_index:
                embedding = doc_to_index.get('job_embedding')
                logger.info(f"Job {job_id} embedding info: type={type(embedding)}, is_none={embedding is None}")
                
                if embedding is not None:
                    logger.info(f"Job {job_id} embedding length: {len(embedding) if hasattr(embedding, '__len__') else 'no length'}")
                
                # Comprehensive validation
                embedding_is_valid = (
                    embedding is not None and 
                    isinstance(embedding, list) and 
                    len(embedding) > 0 and 
                    all(isinstance(x, (int, float)) and x is not None for x in embedding)
                )
                
                if not embedding_is_valid:
                    logger.warning(f"Job {job_id} has invalid embedding, removing field. Details:")
                    logger.warning(f"  - embedding is None: {embedding is None}")
                    logger.warning(f"  - is list: {isinstance(embedding, list) if embedding is not None else False}")
                    logger.warning(f"  - length > 0: {len(embedding) > 0 if hasattr(embedding, '__len__') else False}")
                    
                    if isinstance(embedding, list) and len(embedding) > 0:
                        none_count = sum(1 for x in embedding if x is None)
                        non_numeric_count = sum(1 for x in embedding if not isinstance(x, (int, float)))
                        logger.warning(f"  - null values in embedding: {none_count}")
                        logger.warning(f"  - non-numeric values: {non_numeric_count}")
                    
                    # Remove the embedding field entirely
                    del doc_to_index['job_embedding']
                    
                    # Update status
                    doc_to_index['embedding_status'] = 'invalid_embedding_removed'
                else:
                    logger.info(f"Job {job_id} has valid embedding with {len(embedding)} dimensions")
            
            logger.info(f"Final document keys for job {job_id}: {list(doc_to_index.keys())}")
            
            # Index the document
            response = self.client.index(
                index=self.job_index, 
                id=job_id, 
                body=doc_to_index, 
                refresh=True
            )
            
            embedding_info = "with embedding" if 'job_embedding' in doc_to_index else "without embedding"
            logger.info(f"Successfully indexed job document {job_id} {embedding_info}")
            return response
            
        except Exception as e:
            logger.error(f"Error indexing job document {job_id}: {str(e)}")
            
            # Additional retry without embedding field if the error seems embedding-related
            if "job_embedding" in str(e) or "knn_vector" in str(e) or "null" in str(e):
                try:
                    logger.warning(f"Retrying job {job_id} without embedding field due to embedding-related error")
                    doc_retry = {k: v for k, v in document.items() if k != 'job_embedding'}
                    doc_retry['embedding_status'] = 'removed_due_to_indexing_error'
                    
                    response = self.client.index(
                        index=self.job_index, 
                        id=job_id, 
                        body=doc_retry, 
                        refresh=True
                    )
                    
                    logger.info(f"Successfully indexed job document {job_id} on retry (without embedding)")
                    return response
                except Exception as retry_error:
                    logger.error(f"Retry also failed for job {job_id}: {str(retry_error)}")
                    raise retry_error
            else:
                raise e
    
    def search_similar_jobs(self, cv_embedding: list, size: int = 10) -> Dict:
        """Search for similar jobs using CV embedding"""
        try:
            if not cv_embedding or len(cv_embedding) == 0:
                raise ValueError("CV embedding is empty")
            
            # Check OpenSearch version to determine search method
            cluster_info = self.client.info()
            version = cluster_info.get('version', {}).get('number', '1.0.0')
            
            if version.startswith('2.') or version.startswith('3.'):
                # Use KNN search for OpenSearch 2.x+
                search_body = {
                    "size": size,
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "knn": {
                                        "job_embedding": {
                                            "vector": cv_embedding,
                                            "k": size
                                        }
                                    }
                                }
                            ],
                            "filter": [
                                {"exists": {"field": "job_embedding"}}  # Only return jobs with embeddings
                            ]
                        }
                    },
                    "_source": {
                        "exclude": ["job_embedding"]
                    }
                }
            else:
                # Fallback to basic search for older versions
                search_body = {
                    "size": size,
                    "query": {
                        "bool": {
                            "must": [
                                {"exists": {"field": "job_embedding"}}
                            ]
                        }
                    },
                    "_source": {
                        "exclude": ["job_embedding"]
                    }
                }
            
            return self.client.search(index=self.job_index, body=search_body)
            
        except Exception as e:
            logger.error(f"Error searching similar jobs: {str(e)}")
            # Fallback to basic search without embedding requirements
            try:
                basic_search = {
                    "size": size,
                    "query": {"match_all": {}},
                    "_source": {"exclude": ["job_embedding"]}
                }
                logger.info("Falling back to basic job search without embedding similarity")
                return self.client.search(index=self.job_index, body=basic_search)
            except Exception as fallback_error:
                logger.error(f"Fallback search also failed: {str(fallback_error)}")
                raise e
    
    def get_jobs_without_embeddings(self, size: int = 10) -> Dict:
        """Get jobs that don't have embeddings for debugging purposes"""
        try:
            search_body = {
                "size": size,
                "query": {
                    "bool": {
                        "must_not": [
                            {"exists": {"field": "job_embedding"}}
                        ]
                    }
                },
                "_source": {
                    "include": ["job_id", "title", "company", "embedding_status"]
                }
            }
            
            return self.client.search(index=self.job_index, body=search_body)
            
        except Exception as e:
            logger.error(f"Error getting jobs without embeddings: {str(e)}")
            return {"hits": {"hits": []}}
    
    def test_connection(self) -> Dict:
        """Test OpenSearch connection and return status"""
        try:
            # Test cluster health
            health = self.client.cluster.health()
            
            # Test index existence
            cv_exists = self.client.indices.exists(index=self.cv_index)
            job_exists = self.client.indices.exists(index=self.job_index)
            
            # Get document counts
            cv_count = 0
            job_count = 0
            jobs_with_embeddings = 0
            jobs_without_embeddings = 0
            
            if cv_exists:
                try:
                    cv_stats = self.client.count(index=self.cv_index)
                    cv_count = cv_stats.get('count', 0)
                except Exception as e:
                    logger.warning(f"Could not get CV count: {str(e)}")
            
            if job_exists:
                try:
                    job_stats = self.client.count(index=self.job_index)
                    job_count = job_stats.get('count', 0)
                    
                    # Count jobs with embeddings
                    with_embeddings = self.client.count(
                        index=self.job_index,
                        body={"query": {"exists": {"field": "job_embedding"}}}
                    )
                    jobs_with_embeddings = with_embeddings.get('count', 0)
                    jobs_without_embeddings = job_count - jobs_with_embeddings
                    
                except Exception as e:
                    logger.warning(f"Could not get job counts: {str(e)}")
            
            logger.info(f"OpenSearch connection successful - Cluster: {health.get('status', 'unknown')}")
            
            return {
                'status': 'connected',
                'host': self.host,
                'cluster_health': health.get('status', 'unknown'),
                'cluster_name': health.get('cluster_name', 'unknown'),
                'indices': {
                    'cv_index_exists': cv_exists,
                    'job_index_exists': job_exists,
                    'cv_document_count': cv_count,
                    'job_document_count': job_count,
                    'jobs_with_embeddings': jobs_with_embeddings,
                    'jobs_without_embeddings': jobs_without_embeddings
                }
            }
            
        except Exception as e:
            logger.error(f"OpenSearch connection test failed: {str(e)}")
            return {
                'status': 'failed', 
                'host': self.host,
                'error': str(e)
            }
    
    def get_index_stats(self) -> Dict:
        """Get statistics about the indices"""
        try:
            stats = {}
            
            # CV Index stats
            if self.client.indices.exists(index=self.cv_index):
                cv_stats = self.client.count(index=self.cv_index)
                stats['cv_documents'] = cv_stats.get('count', 0)
                
                # Get sample document
                try:
                    sample = self.client.search(index=self.cv_index, body={"size": 1})
                    stats['cv_sample_available'] = len(sample['hits']['hits']) > 0
                except:
                    stats['cv_sample_available'] = False
            else:
                stats['cv_documents'] = 0
                stats['cv_sample_available'] = False
            
            # Job Index stats
            if self.client.indices.exists(index=self.job_index):
                job_stats = self.client.count(index=self.job_index)
                stats['job_documents'] = job_stats.get('count', 0)
                
                # Count jobs with embeddings
                try:
                    with_embeddings = self.client.count(
                        index=self.job_index,
                        body={"query": {"exists": {"field": "job_embedding"}}}
                    )
                    stats['jobs_with_embeddings'] = with_embeddings.get('count', 0)
                    stats['jobs_without_embeddings'] = stats['job_documents'] - stats['jobs_with_embeddings']
                except Exception as e:
                    logger.warning(f"Could not get embedding counts: {str(e)}")
                    stats['jobs_with_embeddings'] = 'unknown'
                    stats['jobs_without_embeddings'] = 'unknown'
                
                # Get sample document
                try:
                    sample = self.client.search(index=self.job_index, body={"size": 1})
                    stats['job_sample_available'] = len(sample['hits']['hits']) > 0
                except:
                    stats['job_sample_available'] = False
            else:
                stats['job_documents'] = 0
                stats['jobs_with_embeddings'] = 0
                stats['jobs_without_embeddings'] = 0
                stats['job_sample_available'] = False
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting index stats: {str(e)}")
            return {
                'cv_documents': 0, 
                'job_documents': 0, 
                'jobs_with_embeddings': 0,
                'jobs_without_embeddings': 0,
                'error': str(e)
            }
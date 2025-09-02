import json
import logging
import time
from opensearchpy import OpenSearch, RequestsHttpConnection
import boto3
import os
from requests_aws4auth import AWS4Auth
from cv_processor import CVProcessor
from job_scraper import JobScraper
from opensearch_manager import OpenSearchManager
from embedding_service import EmbeddingService

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ====== OpenSearch Config ======
region = os.environ.get("APP_REGION", "us-east-1")

def get_opensearch_endpoint():
    """Get OpenSearch domain endpoint with proper error handling"""
    try:
        # Method 1: Try environment variable first
        endpoint = os.environ.get("OPENSEARCH_ENDPOINT")
        if endpoint:
            host = endpoint.replace('https://', '').replace('http://', '').strip()
            logger.info(f"Using OpenSearch host from environment: {host}")
            return host
        
        # Method 2: Get from AWS OpenSearch API
        domain_name = os.environ.get("OPENSEARCH_DOMAIN_NAME", "new-job-recommendationdomain")
        
        try:
            opensearch_client = boto3.client('opensearch', region_name=region)
            response = opensearch_client.describe_domain(DomainName=domain_name)
            endpoint = response['DomainStatus']['Endpoint']
            host = endpoint.replace('https://', '').replace('http://', '').strip()
            logger.info(f"Retrieved OpenSearch host from AWS API: {host}")
            return host
        except Exception as e:
            logger.warning(f"Could not retrieve from AWS API: {str(e)}")
        
        # Method 3: Hardcoded fallback based on your domain
        fallback_host = "search-new-job-recommendationdomain-equlis5ogx733rohqkaxrlabu4.us-east-1.es.amazonaws.com"
        logger.info(f"Using hardcoded fallback host: {fallback_host}")
        return fallback_host
        
    except Exception as e:
        logger.error(f"Error getting OpenSearch endpoint: {str(e)}")
        # Final fallback
        return "search-new-job-recommendationdomain-equlis5ogx733rohqkaxrlabu4.us-east-1.es.amazonaws.com"

# Get the clean host
host = get_opensearch_endpoint()
logger.info(f"Final OpenSearch host: {host}")

cv_index = "cv-index"
job_index = "job-index"

# Initialize AWS auth
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key,
                   credentials.secret_key,
                   region,
                   "es",
                   session_token=credentials.token)

# Initialize OpenSearch client
client = OpenSearch(
    hosts=[{'host': host, 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
    timeout=60,
    max_retries=3,
    retry_on_timeout=True
)

def lambda_handler(event, context):
    """
    Enhanced Lambda handler supporting API Gateway requests and direct queries
    """
    try:
        logger.info(f"Received event: {json.dumps(event, default=str)}")

        # Handle direct OpenSearch query requests FIRST
        if 'query' in event and isinstance(event['query'], dict):
            logger.info("Detected OpenSearch query in event")
            return handle_opensearch_query(event, context)

        # Handle API Gateway requests
        elif 'httpMethod' in event:
            return handle_api_gateway_request(event, context)
        
        # Handle S3 events (CV processing)
        elif 'Records' in event and event['Records']:
            return handle_cv_processing(event, context)
        
        # Handle CloudWatch events (job scraping)
        elif 'source' in event and event['source'] == 'aws.events':
            return handle_job_scraping(event, context)
        
        # Handle manual invocation with task
        elif 'task' in event:
            return handle_manual_invoke(event, context)
        
        # Handle unknown events - could be query-related
        else:
            logger.warning(f"Unrecognized event format: {list(event.keys())}")
            # Check if this could be a malformed query
            if any(key in event for key in ['success', 'total_hits', 'results']):
                logger.info("Event looks like query results, treating as test data")
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': 'Received what appears to be query results',
                        'event_type': 'query_results',
                        'event_data': event
                    }, default=str)
                }
            else:
                return handle_manual_invoke(event, context)

    except Exception as e:
        logger.error(f"Error in lambda_handler: {str(e)}", exc_info=True)
        return create_api_response(500, {'error': str(e)})

def handle_opensearch_query(event, context):
    """Handle direct OpenSearch query execution"""
    try:
        query = event.get('query')
        index_name = event.get('index', job_index)  # Default to job-index
        size = event.get('size', 10)  # Default size
        from_param = event.get('from', 0)  # Default from
        
        logger.info(f"Executing OpenSearch query on index '{index_name}': {json.dumps(query)}")
        
        # Build the search body
        search_body = {
            'query': query,
            'size': size,
            'from': from_param
        }
        
        # Add sorting if specified
        if 'sort' in event:
            search_body['sort'] = event['sort']
        else:
            # Default sort by relevance score
            search_body['sort'] = [{"_score": {"order": "desc"}}]
            
        # Add filters to exclude jobs with empty descriptions if requested
        filters = event.get('filters', {})
        if filters.get('exclude_empty_descriptions', False):
            search_body['query'] = {
                "bool": {
                    "must": [query],
                    "must_not": [
                        {"term": {"description": ""}},
                        {"bool": {"must_not": {"exists": {"field": "description"}}}}
                    ]
                }
            }
            
        # Add source filtering to exclude embeddings from response (too large)
        search_body['_source'] = {
            "excludes": ["job_embedding", "cv_embedding"]
        }
        
        # Execute the search
        start_time = time.time()
        response = client.search(
            index=index_name,
            body=search_body
        )
        execution_time = time.time() - start_time
        
        # Process results
        hits = response.get('hits', {})
        total_hits = hits.get('total', {}).get('value', 0)
        results = []
        
        for hit in hits.get('hits', []):
            result = {
                'id': hit.get('_id'),
                'score': hit.get('_score'),
                'source': hit.get('_source', {})
            }
            results.append(result)
        
        # Build response
        query_response = {
            'success': True,
            'query': query,
            'index': index_name,
            'total_hits': total_hits,
            'returned_hits': len(results),
            'execution_time_seconds': round(execution_time, 3),
            'opensearch_took_ms': response.get('took', 0),
            'results': results,
            'aggregations': response.get('aggregations', {}),
            'metadata': {
                'max_score': hits.get('max_score'),
                'timed_out': response.get('timed_out', False),
                'shards': response.get('_shards', {})
            }
        }
        
        logger.info(f"Query executed successfully. Found {total_hits} total hits, returned {len(results)} results")
        
        return {
            'statusCode': 200,
            'body': json.dumps(query_response, default=str)
        }
        
    except Exception as e:
        logger.error(f"Error executing OpenSearch query: {str(e)}")
        error_response = {
            'success': False,
            'error': str(e),
            'query': event.get('query', {}),
            'index': event.get('index', job_index)
        }
        
        return {
            'statusCode': 500,
            'body': json.dumps(error_response)
        }

def execute_opensearch_aggregation(event, context):
    """Execute OpenSearch aggregation queries"""
    try:
        query = event.get('query', {"match_all": {}})
        aggregations = event.get('aggregations', {})
        index_name = event.get('index', job_index)
        
        logger.info(f"Executing aggregation query on index '{index_name}'")
        
        search_body = {
            'query': query,
            'aggs': aggregations,
            'size': 0  # We only want aggregations, not documents
        }
        
        response = client.search(
            index=index_name,
            body=search_body
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'success': True,
                'aggregations': response.get('aggregations', {}),
                'total_documents': response.get('hits', {}).get('total', {}).get('value', 0),
                'took_ms': response.get('took', 0)
            }, default=str)
        }
        
    except Exception as e:
        logger.error(f"Error executing aggregation: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'success': False,
                'error': str(e)
            })
        }

def handle_api_gateway_request(event, context):
    """Handle API Gateway HTTP requests"""
    try:
        http_method = event.get('httpMethod', '')
        path = event.get('path', '')
        
        logger.info(f"API Gateway request: {http_method} {path}")
        
        # Parse request body
        body = {}
        if event.get('body'):
            try:
                body = json.loads(event['body']) if isinstance(event['body'], str) else event['body']
            except json.JSONDecodeError:
                return create_api_response(400, {'error': 'Invalid JSON in request body'})
        
        # Route requests
        if path == '/recommendations' and http_method == 'POST':
            return handle_recommendation_request(body, context)
        
        elif path == '/search' and http_method == 'POST':
            # Direct search endpoint
            return handle_opensearch_query(body, context)
        
        elif path == '/aggregations' and http_method == 'POST':
            # Aggregation endpoint
            return execute_opensearch_aggregation(body, context)
        
        elif path == '/status' and http_method == 'GET':
            return handle_status_request()
        
        elif path == '/test' and http_method == 'POST':
            return handle_test_request(body)
        
        else:
            return create_api_response(404, {'error': f'Endpoint not found: {http_method} {path}'})
            
    except Exception as e:
        logger.error(f"Error handling API Gateway request: {str(e)}")
        return create_api_response(500, {'error': str(e)})

def handle_recommendation_request(body, context):
    """Handle job recommendation requests"""
    try:
        # Extract user_id from request
        user_id = body.get('user_id')
        if not user_id:
            return create_api_response(400, {'error': 'user_id is required'})
        
        logger.info(f"Getting recommendations for user: {user_id}")
        
        # Initialize services
        opensearch_manager = OpenSearchManager()
        
        # Step 1: Get user's CV embedding from OpenSearch
        cv_result = opensearch_manager.get_cv_by_user_id(user_id)
        
        if not cv_result:
            return create_api_response(404, {
                'error': 'CV not found for this user. Please upload your CV first.',
                'user_id': user_id
            })
        
        cv_embedding = cv_result.get('cv_embedding')
        if not cv_embedding:
            return create_api_response(400, {
                'error': 'CV embedding not available. Please re-upload your CV.',
                'user_id': user_id
            })
        
        # Step 2: Search for similar jobs
        similar_jobs = opensearch_manager.search_similar_jobs(cv_embedding, size=10)
        
        if not similar_jobs or not similar_jobs.get('hits', {}).get('hits'):
            return create_api_response(404, {
                'message': 'No matching jobs found at the moment.',
                'user_id': user_id,
                'recommendations': []
            })
        
        # Step 3: Format recommendations
        recommendations = []
        for hit in similar_jobs['hits']['hits']:
            job_data = hit['_source']
            similarity_score = hit.get('_score', 0)
            
            # Calculate match percentage (normalize score to 0-100)
            match_percentage = min(100, max(0, int(similarity_score * 10)))  # Adjust multiplier as needed
            
            recommendation = {
                'job_id': job_data.get('job_id', hit['_id']),
                'title': job_data.get('title', 'Job Title Not Available'),
                'company': job_data.get('company', 'Company Name Not Available'),
                'description': job_data.get('description', 'No description available'),
                'location': job_data.get('location', 'Location not specified'),
                'job_url': job_data.get('job_url', ''),
                'skills_required': job_data.get('skills_required', []),
                'experience_level': job_data.get('experience_level', 'Not specified'),
                'salary_range': job_data.get('salary_range', 'Not specified'),
                'match_percentage': match_percentage,
                'similarity_score': similarity_score,
                'scraped_date': job_data.get('scraped_date')
            }
            
            recommendations.append(recommendation)
        
        # Step 4: Get user's CV metadata for personalization
        cv_metadata = {
            'skills_extracted': cv_result.get('skills_extracted', []),
            'experience_years': cv_result.get('experience_years', 0),
            'job_title': cv_result.get('job_title', 'Not specified')
        }
        
        response_data = {
            'user_id': user_id,
            'total_recommendations': len(recommendations),
            'user_profile': cv_metadata,
            'recommendations': recommendations,
            'search_metadata': {
                'total_jobs_in_database': similar_jobs.get('hits', {}).get('total', {}).get('value', 0),
                'search_took_ms': similar_jobs.get('took', 0)
            }
        }
        
        logger.info(f"Successfully generated {len(recommendations)} recommendations for user {user_id}")
        return create_api_response(200, response_data)
        
    except Exception as e:
        logger.error(f"Error getting recommendations: {str(e)}")
        return create_api_response(500, {'error': f'Failed to get recommendations: {str(e)}'})

def handle_status_request():
    """Handle status check requests"""
    try:
        opensearch_manager = OpenSearchManager()
        status = opensearch_manager.test_connection()
        
        return create_api_response(200, {
            'system_status': 'operational',
            'opensearch_status': status,
            'lambda_version': '1.0.0',
            'timestamp': int(time.time() * 1000)
        })
        
    except Exception as e:
        logger.error(f"Error checking status: {str(e)}")
        return create_api_response(500, {'error': str(e)})

def handle_test_request(body):
    """Handle test requests"""
    try:
        test_type = body.get('test_type', 'connection')
        
        if test_type == 'connection':
            opensearch_manager = OpenSearchManager()
            result = opensearch_manager.test_connection()
            
        elif test_type == 'embedding':
            embedding_service = EmbeddingService()
            result = embedding_service.test_service()
            
        else:
            return create_api_response(400, {'error': f'Unknown test type: {test_type}'})
        
        return create_api_response(200, {
            'test_type': test_type,
            'result': result
        })
        
    except Exception as e:
        logger.error(f"Error running test: {str(e)}")
        return create_api_response(500, {'error': str(e)})

def handle_manual_invoke(event, context):
    """Handle manual Lambda invocations and test events"""
    try:
        logger.info("Processing manual invocation")
        
        # Get task from event
        task = event.get('task', 'test')
        
        if task == 'test_small_scrape':
            # Test job scraping functionality
            logger.info("Running test job scraping")
            job_scraper = JobScraper()
            result = job_scraper.scrape_small_batch()  # Implement this method in JobScraper
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Test scraping completed',
                    'task': task,
                    'result': result
                }, default=str)
            }
            
        elif task == 'test_connection':
            # Test OpenSearch connection
            logger.info("Testing OpenSearch connection")
            opensearch_manager = OpenSearchManager()
            status = opensearch_manager.test_connection()
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Connection test completed',
                    'task': task,
                    'opensearch_status': status
                }, default=str)
            }
            
        elif task == 'process_pending_cvs':
            # Process any pending CVs
            logger.info("Processing pending CVs")
            cv_processor = CVProcessor()
            result = cv_processor.process_pending()  # Implement this method
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'CV processing completed',
                    'task': task,
                    'result': result
                }, default=str)
            }
            
        elif task == 'health_check':
            # Comprehensive health check
            logger.info("Running health check")
            
            health_status = {
                'lambda_status': 'healthy',
                'timestamp': int(time.time() * 1000)
            }
            
            try:
                opensearch_manager = OpenSearchManager()
                health_status['opensearch_status'] = opensearch_manager.test_connection()
            except Exception as e:
                health_status['opensearch_status'] = f'error: {str(e)}'
            
            try:
                embedding_service = EmbeddingService()
                health_status['embedding_service'] = embedding_service.test_service()
            except Exception as e:
                health_status['embedding_service'] = f'error: {str(e)}'
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Health check completed',
                    'task': task,
                    'health_status': health_status
                }, default=str)
            }
            
        else:
            # Default test response
            logger.info(f"Unknown task '{task}', returning default test response")
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Lambda function is working',
                    'task': task,
                    'event_received': event,
                    'available_tasks': [
                        'test_small_scrape',
                        'test_connection', 
                        'process_pending_cvs',
                        'health_check'
                    ]
                }, default=str)
            }
            
    except Exception as e:
        logger.error(f"Error in handle_manual_invoke: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'task': event.get('task', 'unknown')
            })
        }

def handle_cv_processing(event, context):
    """Handle S3 CV file uploads"""
    try:
        logger.info("Processing CV upload from S3")
        
        processed_files = []
        
        for record in event['Records']:
            # Extract S3 information
            s3_bucket = record['s3']['bucket']['name']
            s3_key = record['s3']['object']['key']
            
            logger.info(f"Processing file: {s3_key} from bucket: {s3_bucket}")
            
            # Initialize CV processor
            cv_processor = CVProcessor()
            
            # Process the CV
            result = cv_processor.process_s3_file(s3_bucket, s3_key)
            processed_files.append({
                'file': s3_key,
                'bucket': s3_bucket,
                'result': result
            })
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Processed {len(processed_files)} CV files',
                'processed_files': processed_files
            }, default=str)
        }
        
    except Exception as e:
        logger.error(f"Error processing CV: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def handle_job_scraping(event, context):
    """Handle scheduled job scraping from CloudWatch Events"""
    try:
        logger.info("Starting scheduled job scraping")
        
        # Initialize job scraper
        job_scraper = JobScraper()
        
        # Run the scraping process
        result = job_scraper.run_scheduled_scrape()
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Job scraping completed',
                'result': result
            }, default=str)
        }
        
    except Exception as e:
        logger.error(f"Error in job scraping: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }

def create_api_response(status_code, body):
    """Create standardized API Gateway response"""
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',  # Configure this properly for production
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, Authorization'
        },
        'body': json.dumps(body, default=str)
    }
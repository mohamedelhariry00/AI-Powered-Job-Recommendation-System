import json
import logging
import time
import re
from typing import Dict, List, Optional
import boto3
from opensearch_manager import OpenSearchManager
from embedding_service import EmbeddingService
from utils import extract_user_id_from_key, clean_text

logger = logging.getLogger(__name__)

class CVProcessor:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.opensearch = OpenSearchManager()
        self.embedding_service = EmbeddingService()
    
    def process_cv_record(self, record: Dict) -> Dict:
        """Process a single S3 CV record"""
        try:
            # Extract S3 details
            bucket_name = record['s3']['bucket']['name']
            object_key = record['s3']['object']['key']
            
            logger.info(f"Processing CV file: s3://{bucket_name}/{object_key}")
            
            # Extract user_id from path: structured/{user_id}/extracted_text.txt
            user_id = self._extract_user_id_from_structured_path(object_key)
            if not user_id:
                raise ValueError(f"Cannot extract user_id from key: {object_key}")
            
            # Read CV text from S3
            cv_text = self._read_cv_text_from_s3(bucket_name, object_key)
            if not cv_text or len(cv_text.strip()) < 50:
                raise ValueError("CV text is too short or empty")
            
            # Generate embeddings
            cv_embedding = self.embedding_service.generate_embedding(cv_text)
            
            # Extract metadata
            metadata = self._extract_cv_metadata(cv_text)
            
            # Prepare document
            cv_document = {
                'user_id': user_id,
                'cv_text': cv_text,
                'cv_embedding': cv_embedding,
                'timestamp': int(time.time() * 1000),
                'skills_extracted': metadata['skills'],
                'experience_years': metadata['experience_years'],
                'job_title': metadata['job_title'],
                's3_bucket': bucket_name,
                's3_key': object_key,
                'text_length': len(cv_text)
            }
            
            # Store in OpenSearch
            result = self.opensearch.index_cv_document(user_id, cv_document)
            
            logger.info(f"Successfully processed CV for user {user_id}")
            return {
                'user_id': user_id,
                'status': 'success',
                'embedding_dimensions': len(cv_embedding),
                'skills_count': len(metadata['skills']),
                'opensearch_result': result.get('result', 'unknown')
            }
            
        except Exception as e:
            logger.error(f"Error processing CV record: {str(e)}", exc_info=True)
            return {
                'status': 'error',
                'error': str(e),
                'object_key': record.get('s3', {}).get('object', {}).get('key', 'unknown')
            }
    
    def _extract_user_id_from_structured_path(self, object_key: str) -> Optional[str]:
        """Extract user_id from structured path: structured/{user_id}/extracted_text.txt"""
        try:
            # Pattern: structured/user123/extracted_text.txt
            parts = object_key.split('/')
            if len(parts) >= 3 and parts[0] == 'structured':
                return parts[1]  # user_id
            
            # Fallback: try to extract any reasonable identifier
            for part in parts:
                if part and len(part) > 3 and part != 'structured' and not part.endswith('.txt'):
                    return part
                    
            return None
        except Exception:
            return None
    
    def _read_cv_text_from_s3(self, bucket_name: str, object_key: str) -> str:
        """Read CV text content from S3"""
        try:
            response = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)
            cv_text = response['Body'].read().decode('utf-8')
            
            # Clean and normalize
            cv_text = clean_text(cv_text)
            
            # Limit text length (Titan has token limits)
            if len(cv_text) > 8000:
                cv_text = cv_text[:8000] + "..."
                logger.warning(f"CV text truncated to 8000 characters for embedding")
            
            logger.info(f"Successfully read CV text: {len(cv_text)} characters")
            return cv_text
            
        except Exception as e:
            logger.error(f"Error reading from S3: {str(e)}")
            raise e
    
    def _extract_cv_metadata(self, cv_text: str) -> Dict:
        """Extract metadata from CV text"""
        return {
            'skills': self._extract_skills(cv_text),
            'experience_years': self._extract_experience_years(cv_text),
            'job_title': self._extract_job_title(cv_text)
        }
    
    def _extract_skills(self, cv_text: str) -> List[str]:
        """Extract technical skills from CV"""
        skill_keywords = [
            # Programming Languages
            'python', 'java', 'javascript', 'typescript', 'c++', 'c#', 'php', 'ruby',
            'go', 'rust', 'kotlin', 'swift', 'scala', 'r', 'matlab', 'sql',
            
            # Web Technologies
            'html', 'css', 'react', 'angular', 'vue', 'node.js', 'express', 'django',
            'flask', 'spring', 'laravel', 'bootstrap', 'jquery',
            
            # Cloud & DevOps
            'aws', 'azure', 'gcp', 'docker', 'kubernetes', 'jenkins', 'git',
            'terraform', 'ansible',
            
            # Databases
            'mysql', 'postgresql', 'mongodb', 'redis', 'elasticsearch', 'oracle',
            
            # Data Science
            'machine learning', 'deep learning', 'tensorflow', 'pytorch',
            'pandas', 'numpy', 'scikit-learn', 'tableau', 'power bi',
            
            # Design & Marketing
            'photoshop', 'illustrator', 'figma', 'sketch', 'adobe', 'canva',
            'seo', 'sem', 'google analytics', 'social media',
            
            # Business & Finance
            'excel', 'powerpoint', 'salesforce', 'crm', 'erp', 'sap',
            'accounting', 'finance', 'project management'
        ]
        
        found_skills = []
        cv_text_lower = cv_text.lower()
        
        for skill in skill_keywords:
            # Use word boundaries for better matching
            pattern = r'\b' + re.escape(skill.lower()) + r'\b'
            if re.search(pattern, cv_text_lower):
                found_skills.append(skill.title())
        
        return found_skills[:20]  # Limit to 20 skills
    
    def _extract_experience_years(self, cv_text: str) -> int:
        """Extract years of experience"""
        patterns = [
            r'(\d+)\+?\s*years?\s*(?:of\s*)?experience',
            r'experience\s*[:\-]?\s*(\d+)\+?\s*years?',
            r'(\d+)\+?\s*years?\s*in\s*\w+',
            r'(\d+)\+?\s*yrs?\s*(?:of\s*)?experience'
        ]
        
        years = []
        text_lower = cv_text.lower()
        
        for pattern in patterns:
            matches = re.findall(pattern, text_lower)
            for match in matches:
                try:
                    year_val = int(match)
                    if 0 <= year_val <= 50:  # Reasonable range
                        years.append(year_val)
                except ValueError:
                    continue
        
        return max(years) if years else 0
    
    def _extract_job_title(self, cv_text: str) -> str:
        """Extract job title from CV"""
        lines = cv_text.split('\n')[:15]  # Check first 15 lines
        
        title_keywords = [
            'engineer', 'developer', 'manager', 'analyst', 'specialist',
            'consultant', 'architect', 'designer', 'scientist', 'lead',
            'director', 'coordinator', 'administrator', 'technician',
            'representative', 'executive', 'officer', 'assistant'
        ]
        
        # Look for lines that seem like job titles
        for line in lines:
            line_clean = line.strip()
            if len(line_clean) > 5 and len(line_clean) < 100:
                line_lower = line_clean.lower()
                if any(keyword in line_lower for keyword in title_keywords):
                    # Avoid lines that are clearly not titles
                    if not any(word in line_lower for word in ['email', 'phone', 'address', 'linkedin', 'github']):
                        return line_clean
        
        return "Not specified"
    
    def test_processing(self):
        """Test function for manual testing"""
        try:
            # Test OpenSearch connection
            opensearch_status = self.opensearch.test_connection()
            
            # Test embedding service
            embedding_status = self.embedding_service.test_service()
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'CV Processor is ready',
                    'opensearch_status': opensearch_status,
                    'embedding_service_status': embedding_status
                })
            }
        except Exception as e:
            return {
                'statusCode': 500,
                'body': json.dumps({
                    'message': 'CV Processor test failed',
                    'error': str(e)
                })
            }
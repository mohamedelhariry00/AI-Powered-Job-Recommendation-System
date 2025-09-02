import json
import logging
import time
import re
import hashlib
from typing import Dict, List
import requests
from bs4 import BeautifulSoup
from opensearch_manager import OpenSearchManager
from embedding_service import EmbeddingService
from utils import clean_text

logger = logging.getLogger(__name__)

class JobScraper:
    def __init__(self):
        self.opensearch = OpenSearchManager()
        self.embedding_service = EmbeddingService()
        self.base_url = "https://wuzzuf.net"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def scrape_small_batch(self, max_jobs: int = 10) -> Dict:
        """Scrape a small batch of jobs for testing purposes"""
        try:
            logger.info(f"Starting small batch scraping (max {max_jobs} jobs)")
            
            # Use only one search URL for testing
            test_url = "https://wuzzuf.net/search/jobs/?a=hpb&q=software%20engineer"
            
            jobs = self._scrape_from_url(test_url, max_jobs)
            logger.info(f"Scraped {len(jobs)} jobs from test URL")
            
            # Process a smaller subset for testing
            processed_jobs = []
            embedding_success = 0
            embedding_failures = 0
            
            for job in jobs[:5]:  # Process only first 5 jobs
                try:
                    processed_job = self._process_and_embed_job(job)
                    if processed_job:
                        processed_jobs.append(processed_job)
                        
                        # Track embedding success
                        if processed_job.get('embedding_status') == 'success':
                            embedding_success += 1
                        else:
                            embedding_failures += 1
                        
                        # Store in OpenSearch
                        result = self.opensearch.index_job_document(processed_job['job_id'], processed_job)
                        logger.info(f"Indexed job {processed_job['job_id']}: {result.get('result', 'unknown')}")
                        
                except Exception as e:
                    logger.error(f"Error processing job in small batch: {str(e)}")
                    embedding_failures += 1
                    continue
            
            return {
                'total_scraped': len(jobs),
                'successfully_processed': len(processed_jobs),
                'embedding_success': embedding_success,
                'embedding_failures': embedding_failures,
                'processed_job_ids': [job['job_id'] for job in processed_jobs],
                'status': 'success'
            }
            
        except Exception as e:
            logger.error(f"Error in scrape_small_batch: {str(e)}")
            return {
                'total_scraped': 0,
                'successfully_processed': 0,
                'embedding_success': 0,
                'embedding_failures': 1,
                'error': str(e),
                'status': 'failed'
            }
    
    def run_scheduled_scrape(self) -> Dict:
        """Run the scheduled scraping process for CloudWatch Events"""
        try:
            logger.info("Starting scheduled job scraping")
            
            # Use the main scraping method with a reasonable limit
            result = self.scrape_and_embed_jobs(max_jobs=100)
            
            logger.info(f"Scheduled scraping completed: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error in scheduled scraping: {str(e)}")
            return {
                'total_scraped': 0,
                'successfully_processed': 0,
                'embedding_success': 0,
                'embedding_failures': 1,
                'error': str(e),
                'status': 'failed'
            }
    
    def scrape_and_embed_jobs(self, max_jobs: int = 50) -> Dict:
        """Main function to scrape jobs and create embeddings"""
        try:
            logger.info(f"Starting job scraping (max {max_jobs} jobs)")
            
            # Updated URLs based on current Wuzzuf structure
            search_urls = [
                "https://wuzzuf.net/search/jobs/?a=hpb&q=software%20engineer",
                "https://wuzzuf.net/search/jobs/?a=hpb&q=developer",
                "https://wuzzuf.net/search/jobs/?a=hpb&q=data%20analyst",
                "https://wuzzuf.net/search/jobs/?a=hpb&q=marketing",
                "https://wuzzuf.net/search/jobs/?a=hpb&q=sales"
            ]
            
            all_jobs = []
            jobs_per_search = max_jobs // len(search_urls)
            
            for i, url in enumerate(search_urls):
                try:
                    search_term = url.split('q=')[1].replace('%20', ' ')
                    logger.info(f"Scraping jobs for: {search_term}")
                    
                    jobs = self._scrape_from_url(url, jobs_per_search)
                    all_jobs.extend(jobs)
                    logger.info(f"Scraped {len(jobs)} jobs for '{search_term}'")
                    
                    # Rate limiting
                    time.sleep(3)
                    
                except Exception as e:
                    logger.error(f"Error scraping URL {url}: {str(e)}")
                    continue
            
            # Process and embed jobs
            processed_jobs = []
            embedding_success = 0
            embedding_failures = 0
            
            for job in all_jobs:
                try:
                    processed_job = self._process_and_embed_job(job)
                    if processed_job:
                        processed_jobs.append(processed_job)
                        
                        # Track embedding success
                        if processed_job.get('embedding_status') == 'success':
                            embedding_success += 1
                        else:
                            embedding_failures += 1
                        
                        # Store in OpenSearch
                        self.opensearch.index_job_document(processed_job['job_id'], processed_job)
                        
                except Exception as e:
                    logger.error(f"Error processing job: {str(e)}")
                    embedding_failures += 1
                    continue
            
            logger.info(f"Successfully processed {len(processed_jobs)} jobs")
            logger.info(f"Embedding success: {embedding_success}, failures: {embedding_failures}")
            
            return {
                'total_scraped': len(all_jobs),
                'successfully_processed': len(processed_jobs),
                'embedding_success': embedding_success,
                'embedding_failures': embedding_failures,
                'search_terms': [url.split('q=')[1].replace('%20', ' ') for url in search_urls],
                'status': 'success'
            }
            
        except Exception as e:
            logger.error(f"Error in scrape_and_embed_jobs: {str(e)}")
            raise e
    
    def _scrape_from_url(self, url: str, max_jobs: int) -> List[Dict]:
        """Scrape jobs from a specific URL"""
        jobs = []
        page = 0
        
        while len(jobs) < max_jobs and page < 3:  # Max 3 pages per search
            try:
                # Add page parameter
                page_url = f"{url}&start={page * 15}"  # Wuzzuf typically shows 15 jobs per page
                logger.info(f"Fetching: {page_url}")
                
                response = self.session.get(page_url, timeout=30)
                
                if response.status_code != 200:
                    logger.warning(f"Failed to fetch {page_url} - Status: {response.status_code}")
                    break
                
                soup = BeautifulSoup(response.content, 'html.parser')
                page_jobs = self._parse_jobs_from_page(soup)
                
                if not page_jobs:
                    logger.info(f"No jobs found on page {page}")
                    break
                
                jobs.extend(page_jobs)
                page += 1
                time.sleep(2)  # Rate limiting between pages
                
            except Exception as e:
                logger.error(f"Error scraping page {page}: {str(e)}")
                break
        
        return jobs[:max_jobs]
    
    def _parse_jobs_from_page(self, soup) -> List[Dict]:
        """Parse job listings from a page - Updated selectors"""
        jobs = []
        
        # Try multiple selectors as Wuzzuf structure may vary
        possible_selectors = [
            # New structure
            'div[data-testid="job-card"]',
            'div.css-1gatmva',
            'div.css-pkv5jc',
            # Fallback selectors
            '.job-card',
            '.job-listing',
            'article',
            # Generic fallback
            'div:has(h2 a)'
        ]
        
        job_cards = []
        for selector in possible_selectors:
            try:
                job_cards = soup.select(selector)
                if job_cards:
                    logger.info(f"Found {len(job_cards)} job cards using selector: {selector}")
                    break
            except Exception as e:
                continue
        
        if not job_cards:
            # Last resort: find any divs containing job-like content
            job_cards = soup.find_all('div', string=re.compile(r'.*(engineer|developer|analyst|manager).*', re.I))
            logger.info(f"Fallback: Found {len(job_cards)} potential job containers")
        
        for card in job_cards[:20]:  # Limit to avoid too much processing
            try:
                job = self._extract_job_from_card(card)
                if job and job.get('title'):
                    jobs.append(job)
            except Exception as e:
                logger.error(f"Error extracting job from card: {str(e)}")
                continue
        
        logger.info(f"Successfully parsed {len(jobs)} jobs from page")
        return jobs
    
    def _extract_job_from_card(self, card) -> Dict:
        """Extract job details from a job card - More flexible extraction"""
        try:
            # Try to find title
            title = None
            title_selectors = [
                'h2 a', 'h3 a', 'a[data-testid="job-title"]',
                '.css-o171kl', '.css-17s97q8',
                'a:contains("engineer")', 'a:contains("developer")'
            ]
            
            for selector in title_selectors:
                try:
                    title_elem = card.select_one(selector)
                    if title_elem:
                        title = title_elem.get_text(strip=True)
                        job_url = title_elem.get('href', '')
                        break
                except:
                    continue
            
            if not title:
                # Fallback: any link in the card
                link = card.find('a')
                if link:
                    title = link.get_text(strip=True)
                    job_url = link.get('href', '')
            
            if not title:
                return None
            
            # Ensure full URL
            if job_url and job_url.startswith('/'):
                job_url = self.base_url + job_url
            
            # Try to find company
            company = "Unknown Company"
            company_selectors = [
                '[data-testid="job-company"]',
                '.css-1gatmva a', '.css-17s97q8',
                'span:contains("Company")',
                'div:contains("at")'
            ]
            
            for selector in company_selectors:
                try:
                    company_elem = card.select_one(selector)
                    if company_elem:
                        company_text = company_elem.get_text(strip=True)
                        if company_text and len(company_text) < 100:
                            company = company_text
                            break
                except:
                    continue
            
            # Try to find description
            description = ""
            desc_selectors = [
                '[data-testid="job-description"]',
                '.css-y4udm8', '.css-1ubo9m8',
                'p', '.job-description'
            ]
            
            for selector in desc_selectors:
                try:
                    desc_elem = card.select_one(selector)
                    if desc_elem:
                        description = desc_elem.get_text(strip=True)
                        if description:
                            break
                except:
                    continue
            
            # Generate unique job_id
            job_id = self._generate_job_id(title, company, job_url or title)
            
            return {
                'job_id': job_id,
                'title': title,
                'company': company,
                'description': description,
                'location': "Egypt",  # Default location
                'job_url': job_url or "",
                'scraped_timestamp': int(time.time() * 1000)
            }
            
        except Exception as e:
            logger.error(f"Error extracting job details: {str(e)}")
            return None
    
    def _generate_job_id(self, title: str, company: str, unique_str: str) -> str:
        """Generate unique job ID"""
        unique_string = f"{title}_{company}_{unique_str}".lower()
        return hashlib.md5(unique_string.encode()).hexdigest()[:16]
    
    def _process_and_embed_job(self, job: Dict) -> Dict:
        """Process job data and generate embeddings - FIXED VERSION"""
        try:
            # Combine title and description for embedding
            embedding_text = f"{job['title']} {job.get('description', '')}"
            embedding_text = clean_text(embedding_text)
            
            # Prepare base job document WITHOUT embedding field initially
            processed_job = {
                'job_id': job['job_id'],
                'title': job['title'],
                'company': job['company'],
                'description': job.get('description', ''),
                'skills_required': self._extract_job_skills(embedding_text),
                'experience_level': self._extract_experience_level(embedding_text),
                'location': job.get('location', 'Egypt'),
                'salary_range': "Not specified",
                'job_url': job.get('job_url', ''),
                'scraped_date': job['scraped_timestamp']
            }
            
            # Try to generate embedding only if text is long enough
            if len(embedding_text.strip()) >= 20:  # Minimum viable length
                try:
                    # Generate embedding
                    job_embedding = self.embedding_service.generate_embedding(embedding_text)
                    
                    # Thorough validation of embedding
                    if (job_embedding is not None and 
                        isinstance(job_embedding, list) and 
                        len(job_embedding) > 0 and 
                        all(isinstance(x, (int, float)) and x is not None for x in job_embedding)):
                        
                        # Only add embedding field if it's completely valid
                        processed_job['job_embedding'] = job_embedding
                        processed_job['embedding_status'] = 'success'
                        logger.info(f"Successfully generated and validated embedding for job {job['job_id']} (dim: {len(job_embedding)})")
                    else:
                        # Log details about the invalid embedding
                        embedding_info = {
                            'is_none': job_embedding is None,
                            'is_list': isinstance(job_embedding, list) if job_embedding is not None else False,
                            'length': len(job_embedding) if isinstance(job_embedding, list) else 0,
                            'has_nulls': any(x is None for x in job_embedding) if isinstance(job_embedding, list) else False
                        }
                        logger.warning(f"Invalid embedding for job {job['job_id']}: {embedding_info}")
                        processed_job['embedding_status'] = 'failed_invalid_embedding'
                        # DO NOT add job_embedding field to document
                        
                except Exception as e:
                    logger.warning(f"Failed to generate embedding for job {job['job_id']}: {str(e)}")
                    processed_job['embedding_status'] = f'failed_exception: {str(e)[:50]}'
                    # DO NOT add job_embedding field to document
            else:
                logger.warning(f"Job text too short for embedding: {job['job_id']} (length: {len(embedding_text)})")
                processed_job['embedding_status'] = 'skipped_short_text'
                # DO NOT add job_embedding field to document
            
            # Final validation - ensure no null embedding field exists
            if 'job_embedding' in processed_job and processed_job['job_embedding'] is None:
                logger.error(f"Found null job_embedding field in processed job {job['job_id']}, removing it")
                del processed_job['job_embedding']
                processed_job['embedding_status'] = 'null_field_removed'
            
            return processed_job
            
        except Exception as e:
            logger.error(f"Error processing job {job.get('job_id', 'unknown')}: {str(e)}")
            return None
    
    def _extract_job_skills(self, job_text: str) -> List[str]:
        """Extract required skills from job description"""
        skills_patterns = [
            # Programming languages
            r'\b(python|java|javascript|typescript|c\+\+|c#|php|ruby|go|kotlin|swift)\b',
            # Web technologies
            r'\b(html|css|react|angular|vue|node\.?js|express|django|flask)\b',
            # Databases
            r'\b(sql|mysql|postgresql|mongodb|redis|oracle)\b',
            # Cloud/DevOps
            r'\b(aws|azure|gcp|docker|kubernetes|jenkins|git)\b',
            # Data/Analytics
            r'\b(excel|power\s?bi|tableau|analytics|data)\b'
        ]
        
        found_skills = []
        text_lower = job_text.lower()
        
        for pattern in skills_patterns:
            matches = re.findall(pattern, text_lower, re.IGNORECASE)
            found_skills.extend(matches)
        
        # Remove duplicates and limit
        unique_skills = list(set(found_skills))
        return unique_skills[:10]
    
    def _extract_experience_level(self, job_text: str) -> str:
        """Extract experience level from job description"""
        text_lower = job_text.lower()
        
        senior_keywords = ['senior', 'lead', 'principal', '5+', '3+', 'experienced', 'expert']
        junior_keywords = ['junior', 'entry', 'graduate', 'intern', 'fresh', '0-2', 'trainee']
        
        if any(keyword in text_lower for keyword in senior_keywords):
            return 'senior'
        elif any(keyword in text_lower for keyword in junior_keywords):
            return 'junior'
        else:
            return 'mid'
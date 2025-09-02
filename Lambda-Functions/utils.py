import re
from typing import Optional

def extract_user_id_from_key(object_key: str) -> Optional[str]:
    """Extract user_id from S3 object key"""
    try:
        parts = object_key.split('/')
        if len(parts) >= 2:
            return parts[0] if parts[0] != 'structured' else parts[1] if len(parts) >= 3 else None
        return None
    except Exception:
        return None

def clean_text(text: str) -> str:
    """Clean and normalize text for processing"""
    if not text:
        return ""
    
    # Remove excessive whitespace
    text = re.sub(r'\s+', ' ', text)
    
    # Remove HTML tags if any
    text = re.sub(r'<[^>]+>', '', text)
    
    # Remove special characters but keep basic punctuation
    text = re.sub(r'[^\w\s\.\,\-\(\)\@\+\:\;\!\?]', ' ', text)
    
    # Remove extra spaces
    text = re.sub(r'\s+', ' ', text)
    
    # Remove lines with only special characters
    lines = text.split('\n')
    clean_lines = []
    for line in lines:
        if line.strip() and len(line.strip()) > 2:
            clean_lines.append(line.strip())
    
    text = ' '.join(clean_lines)
    
    return text.strip()

def validate_embedding(embedding) -> bool:
    """Validate that embedding is valid"""
    if not embedding:
        return False
    if not isinstance(embedding, list):
        return False
    if len(embedding) == 0:
        return False
    if not all(isinstance(x, (int, float)) for x in embedding):
        return False
    return True

def truncate_text(text: str, max_length: int = 8000) -> str:
    """Truncate text to maximum length while preserving word boundaries"""
    if len(text) <= max_length:
        return text
    
    # Find last space before max_length
    truncated = text[:max_length]
    last_space = truncated.rfind(' ')
    
    if last_space > max_length * 0.8:  # If space is reasonably close
        truncated = truncated[:last_space]
    
    return truncated + "..."

def extract_email_from_text(text: str) -> Optional[str]:
    """Extract email address from text"""
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    matches = re.findall(email_pattern, text)
    return matches[0] if matches else None

def extract_phone_from_text(text: str) -> Optional[str]:
    """Extract phone number from text"""
    phone_patterns = [
        r'\+?\d{1,4}[-.\s]?\(?\d{1,4}\)?[-.\s]?\d{1,4}[-.\s]?\d{1,9}',
        r'\b\d{10,15}\b',
        r'\(\d{3}\)\s*\d{3}-\d{4}'
    ]
    
    for pattern in phone_patterns:
        matches = re.findall(pattern, text)
        if matches:
            return matches[0]
    
    return None

def normalize_job_title(title: str) -> str:
    """Normalize job title for better matching"""
    if not title:
        return ""
    
    # Remove common prefixes/suffixes
    title = re.sub(r'\b(jr|sr|senior|junior|lead|principal)\b', '', title, flags=re.IGNORECASE)
    
    # Clean up extra spaces
    title = re.sub(r'\s+', ' ', title).strip()
    
    # Convert to title case
    return title.title()

def extract_years_from_date_range(text: str) -> int:
    """Extract years of experience from date ranges like '2020-2023' or '2020-Present'"""
    patterns = [
        r'(\d{4})\s*[-–]\s*(\d{4})',  # 2020-2023
        r'(\d{4})\s*[-–]\s*(present|current)',  # 2020-Present
        r'(\d{4})\s*to\s*(\d{4})',  # 2020 to 2023
        r'(\d{4})\s*to\s*(present|current)'  # 2020 to Present
    ]
    
    current_year = 2024  # Update as needed
    total_years = 0
    
    for pattern in patterns:
        matches = re.findall(pattern, text.lower())
        for match in matches:
            try:
                start_year = int(match[0])
                if match[1] in ['present', 'current']:
                    end_year = current_year
                else:
                    end_year = int(match[1])
                
                years = max(0, end_year - start_year)
                total_years += years
            except (ValueError, IndexError):
                continue
    
    return total_years
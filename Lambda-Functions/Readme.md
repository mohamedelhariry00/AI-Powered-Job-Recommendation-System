# CV-Job Recommendation Lambda System

A comprehensive AWS Lambda-based system that processes CV uploads and scrapes job listings to provide intelligent job recommendations using vector embeddings and similarity matching.

##  Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│            LAMBDA FUNCTION ARCHITECTURE                 │
├─────────────────────────────────────────────────────────┤
│  presign-cv                                             │
│ ┌─────────────────────────────────────────────────────┐ │
│ │ Frontend → Generate Presigned URL → Direct S3 Upload│ │
│ └─────────────────────────────────────────────────────┘ │
│                              │                          │
│                              ▼                          │
│             cv-job-embedding-processor                  │
│    ┌─────────────────┐    ┌─────────────────┐           │
│    │   CV Pipeline   │    │   Job Pipeline  │           │
│    │                 │    │                 │           │
│    │ S3 Event → CV   │    │ CloudWatch →    │           │
│    │ Extract → Titan │    │ Wuzzuf Scrape   │           │
│    │ Embed → Store   │    │ → Titan Embed   │           │
│    │                 │    │ → Store         │           │
│    └─────────────────┘    └─────────────────┘           │
│             │                       │                   │
│             └───────────┬───────────┘                   │
│                         ▼                               │
│                OpenSearch Storage                       │
│             cv_index     │    job_index                 │
└─────────────────────────────────────────────────────────┘
```

##  AWS Services Required

### Current Setup
- **S3 Bucket**: `cv-uploads-website-bucket` (CV uploads)
- **S3 Bucket**: `extracted-cv-text-bucket` (CV text files)
- **OpenSearch Domain**: `cv-job-recommendation-domain`
- **OpenSearch Indices**: `cv_index` and `job_index`

### Additional Services Needed
1. **AWS Lambda** - Main processing function and presign function
2. **CloudWatch Events** - Job scraping schedule
3. **IAM Roles** - Enhanced permissions
4. **Amazon Bedrock** - Titan Embeddings access
5. **API Gateway** - HTTP endpoints for presign and recommendations
6. **VPC (Optional)** - If OpenSearch is in VPC

## 📁 File Structure

```
cv-job-embedding-lambda/
├── presign-cv/
│   └── lambda_function.py     # Presign URL generator
├── cv-job-embedding-processor/
│   ├── lambda_function.py     # Main handler
│   ├── cv_processor.py        # CV embedding logic
│   ├── job_scraper.py         # Wuzzuf scraping logic
│   ├── opensearch_manager.py  # OpenSearch operations
│   ├── embedding_service.py   # Titan embeddings service
│   ├── requirements.txt       # Dependencies
│   └── utils.py               # Common utilities
```

## 🔧 Lambda Functions Overview

### **1. CV Upload Pre-signing Function (`presign-cv`)**
**Purpose**: Generates secure, temporary URLs for direct CV file uploads to S3

**Key Responsibilities**:
- **Presigned URL Generation**: Creates secure, time-limited URLs for direct S3 uploads
- **Unique Key Creation**: Generates UUID-based unique paths within `uploads/` directory
- **CORS Support**: Returns proper CORS headers for frontend integration
- **Query Parameter Processing**: Reads filename from query string parameters

**Function Details**:
- **Runtime**: Python 3.9+
- **IAM Role**: `presign-lambda-role`
- **Timeout**: 30 seconds (sufficient for URL generation)
- **Memory**: 128 MB (minimal memory required)

**Environment Variables**:
```bash
BUCKET=cv-uploads-website-bucket
```

**API Response Format**:
```json
{
  "url": "https://s3.amazonaws.com/cv-uploads-website-bucket/uploads/12345678-abcd-efgh-ijkl-123456789012/resume.pdf?AWSAccessKeyId=...&Signature=...",
  "key": "uploads/12345678-abcd-efgh-ijkl-123456789012/resume.pdf"
}
```

**Usage Example**:
```bash
curl "https://your-api-gateway-url/presign?filename=resume.pdf"
```

**How it Works**:
1. Reads `filename` from query string parameters
2. Generates unique key using UUID within `uploads/` directory
3. Creates presigned PUT URL with 1-hour expiration
4. Returns JSON with `url` and `key` along with CORS headers
5. Frontend uses the URL for direct S3 upload

### **2. CV Processing & Job Matching Function (`cv-job-embedding-processor`)**
**Purpose**: Central orchestrator and API handler for the entire CV-job matching system

**Key Responsibilities**:
- **Event Routing**: Handles multiple event types (S3, API Gateway, CloudWatch, manual invocations)
- **OpenSearch Query Execution**: Direct query interface for searching jobs/CVs
- **Job Recommendations API**: Generates personalized job recommendations by matching CV embeddings with job embeddings
- **System Health Monitoring**: Provides status checks and diagnostics
- **Error Handling**: Comprehensive error management with proper HTTP responses

**Event Types Handled**:
- S3 events → CV processing pipeline (triggered after presign upload)
- CloudWatch events → Scheduled job scraping  
- API Gateway requests → REST API endpoints
- Manual invocations → Testing and maintenance tasks

### Supporting Components

#### CVProcessor (`cv_processor.py`)
- Processes CV text files from S3
- Extracts metadata (skills, experience, job titles)
- Generates embeddings using Amazon Titan
- Stores structured CV data in OpenSearch

#### JobScraper (`job_scraper.py`)
- Scrapes job listings from Wuzzuf.net
- Extracts job details (title, company, description, requirements)
- Generates job embeddings
- Handles rate limiting and error recovery

#### OpenSearchManager (`opensearch_manager.py`)
- Manages all OpenSearch operations
- Handles CV and job document indexing
- Performs similarity searches using vector embeddings
- Provides connection testing and health checks

#### EmbeddingService (`embedding_service.py`)
- Interfaces with Amazon Bedrock Titan models
- Generates high-quality text embeddings
- Handles model fallbacks and validation
- Includes comprehensive error handling and retry logic

## 🌐 API Endpoints

### Presign Function Endpoints
| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/presign?filename=resume.pdf` | Generate presigned URL for CV upload |

### Main Processing Function Endpoints
| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/recommendations` | Get personalized job recommendations for a user |
| `POST` | `/search` | Execute direct OpenSearch queries |
| `POST` | `/aggregations` | Run OpenSearch aggregation queries |
| `GET` | `/status` | System health check and statistics |
| `POST` | `/test` | Service testing endpoints |

## 📊 Data Flow

### Complete CV Upload & Processing Pipeline
1. **Frontend Request**: User selects CV file, frontend requests presigned URL
2. **Presign Generation**: `presign-cv` Lambda generates secure upload URL
3. **Direct Upload**: Frontend uploads CV directly to S3 using presigned URL
4. **S3 Trigger**: S3 upload event triggers `cv-job-embedding-processor`
5. **Text Extraction**: Lambda extracts and processes CV content
6. **Embedding Generation**: Creates vector embeddings using Amazon Titan
7. **Storage**: Indexes CV data in OpenSearch for future recommendations

### Job Scraping Pipeline
1. **Schedule**: CloudWatch Events trigger job scraping
2. **Scrape**: Extract job listings from Wuzzuf.net
3. **Process**: Clean and structure job data
4. **Embed**: Generate job embeddings using Amazon Titan
5. **Store**: Index jobs in OpenSearch `job_index`

### Recommendation Pipeline
1. **Request**: User requests job recommendations via API
2. **Retrieve**: Fetch user's CV embedding from OpenSearch
3. **Search**: Use vector similarity to find matching jobs
4. **Rank**: Score and rank jobs by relevance
5. **Return**: Formatted job recommendations with match scores

## 🔍 Key Features

### CV Upload & Processing
- **Secure Upload**: Presigned URLs eliminate need for backend file handling
- **Direct S3 Integration**: Files go directly to S3, reducing Lambda execution time
- **Unique Path Generation**: UUID-based paths prevent file conflicts
- **Metadata Extraction**: Skills, experience years, job titles
- **Text Cleaning**: HTML removal, normalization
- **Embedding Generation**: High-dimensional vector representations
- **Validation**: Comprehensive data validation and error handling

### Job Scraping
- **Web Scraping**: Automated job listing extraction from Wuzzuf
- **Rate Limiting**: Respectful scraping with delays
- **Error Recovery**: Robust handling of network issues and parsing errors
- **Deduplication**: Prevent duplicate job entries

### Recommendation Engine
- **Vector Similarity**: Cosine similarity matching between CV and job embeddings
- **Personalization**: User-specific recommendations based on CV content
- **Scoring**: Match percentage calculation and relevance ranking
- **Real-time**: Fast API responses using pre-computed embeddings

## 🛠️ Installation & Deployment

### Prerequisites
- AWS Account with appropriate permissions
- OpenSearch domain configured
- S3 buckets for CV storage
- Bedrock access for Titan models
- API Gateway for HTTP endpoints

### Deployment Steps

#### 1. Deploy Presign Function
```bash
# Create presign function package
cd presign-cv/
zip presign-lambda.zip lambda_function.py

# Deploy presign function
aws lambda create-function \
  --function-name presign-cv \
  --runtime python3.9 \
  --role arn:aws:iam::ACCOUNT:role/presign-lambda-role \
  --handler lambda_function.lambda_handler \
  --environment Variables='{BUCKET=cv-uploads-website-bucket}' \
  --zip-file fileb://presign-lambda.zip \
  --timeout 30 \
  --memory-size 128
```

#### 2. Deploy Main Processing Function
```bash
# Create main function package
cd cv-job-embedding-processor/
pip install -r requirements.txt -t .
zip -r lambda-deployment.zip .

# Deploy main processing function
aws lambda create-function \
  --function-name cv-job-embedding-processor \
  --runtime python3.9 \
  --role arn:aws:iam::ACCOUNT:role/cv-job-embedding-lambda-role \
  --handler lambda_function.lambda_handler \
  --zip-file fileb://lambda-deployment.zip \
  --timeout 900 \
  --memory-size 1024
```

#### 3. Configure Triggers and API Gateway
```bash
# S3 bucket notification for CV processing
aws s3api put-bucket-notification-configuration \
  --bucket cv-uploads-website-bucket \
  --notification-configuration file://s3-notification.json

# CloudWatch Events for job scraping
aws events put-rule \
  --name job-scraping-schedule \
  --schedule-expression "rate(6 hours)"

# API Gateway integration (configure via console or CLI)
```

### Environment Variables

#### Presign Function
```bash
BUCKET=cv-uploads-website-bucket
```

#### Main Processing Function
```bash
OPENSEARCH_ENDPOINT=your-domain-endpoint
OPENSEARCH_DOMAIN_NAME=cv-job-recommendation-domain
AWS_REGION=us-east-1
```


### Generate Presigned URL
```bash
curl "https://your-api-gateway-url/presign?filename=resume.pdf"
```

**Response:**
```json
{
  "url": "https://s3.amazonaws.com/cv-uploads-website-bucket/uploads/12345678-abcd-efgh-ijkl-123456789012/resume.pdf?AWSAccessKeyId=...&Signature=...",
  "key": "uploads/12345678-abcd-efgh-ijkl-123456789012/resume.pdf"
}
```

### Upload CV Using Presigned URL
```javascript
// Frontend JavaScript example
fetch(presignedUrl, {
  method: 'PUT',
  body: file,
  headers: {
    'Content-Type': 'application/pdf'
  }
})
```

### Get Job Recommendations
```bash
curl -X POST https://your-api-gateway-url/recommendations \
  -H "Content-Type: application/json" \
  -d '{"user_id": "user123"}'
```

### Check System Status
```bash
curl https://your-api-gateway-url/status
```

### Search Jobs
```bash
curl -X POST https://your-api-gateway-url/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": {"match": {"title": "software engineer"}},
    "size": 10
  }'
```

## 🔧 Configuration

### Presign Function Settings
- **Runtime**: Python 3.9+
- **Memory**: 128 MB (minimal for URL generation)
- **Timeout**: 30 seconds
- **Concurrent Executions**: 100 (lightweight operation)

### Main Processing Function Settings
- **Runtime**: Python 3.9+
- **Memory**: 1024 MB (minimum for embedding processing)
- **Timeout**: 15 minutes (for scraping operations)
- **Concurrent Executions**: 10 (to avoid overwhelming external APIs)

### IAM Permissions Required

#### Presign Function Role
- S3: GetObject, PutObject on CV upload bucket
- CloudWatch: Logs permissions

#### Main Processing Function Role
- S3: GetObject, PutObject on both buckets
- OpenSearch: All index operations
- Bedrock: InvokeModel (Titan embeddings)
- CloudWatch: Logs permissions
- VPC: If OpenSearch is in VPC

## 🚨 Monitoring & Troubleshooting

### CloudWatch Metrics
- Function invocations and errors for both functions
- Presigned URL generation success rates
- CV upload and processing success rates
- Embedding generation success/failure rates
- Job scraping statistics
- OpenSearch query performance

### Common Issues

#### Presign Function Issues
1. **Invalid Filename**: Ensure filename is passed in query string
2. **S3 Permissions**: Verify IAM role has PutObject permissions
3. **CORS Issues**: Check CORS headers in response
4. **Bucket Access**: Ensure bucket exists and is accessible

#### Main Processing Function Issues
1. **Embedding Generation Failures**: Check Bedrock permissions and quotas
2. **OpenSearch Connection Issues**: Verify domain endpoint and IAM roles
3. **Job Scraping Errors**: Website structure changes or rate limiting
4. **CV Processing Failures**: Invalid file formats or S3 permissions

### Debugging Steps
```bash
# Check presign function logs
aws logs filter-log-events --log-group-name /aws/lambda/presign-cv

# Check main processing function logs
aws logs filter-log-events --log-group-name /aws/lambda/cv-job-embedding-processor

# Test presign function manually
aws lambda invoke \
  --function-name presign-cv \
  --payload '{"queryStringParameters":{"filename":"test.pdf"}}' \
  response.json
```

## 📈 Performance Optimization

### Recommendations
- **Presign Function**: Keep minimal memory allocation (128MB)
- **Main Function**: Optimize memory based on embedding processing needs
- **Batch Processing**: Process multiple CVs/jobs in single invocation
- **Caching**: Cache frequently accessed data
- **Parallel Processing**: Use async operations where possible
- **Memory Management**: Optimize for large embedding arrays

### Scaling Considerations
- Monitor Lambda concurrency limits for both functions
- Consider SQS for high-volume CV processing
- Implement circuit breakers for external API calls
- Use CloudFront for API caching if needed
- Separate presign function allows independent scaling


// Configuration - UPDATE THESE URLs TO YOUR API ENDPOINTS
const API_CONFIG = {
    RECOMMENDATION_API_URL: 'https://da0i7dpxy0.execute-api.us-east-1.amazonaws.com/prod/recommendations',
    STATUS_API_URL: 'https://da0i7dpxy0.execute-api.us-east-1.amazonaws.com/prod/status',
    CV_UPLOAD_API_URL: 'https://kfie5tn3rb.execute-api.us-east-1.amazonaws.com/CV_API_deploy/presign-cv'
};

// Global variables
let uploadedFileName = null;
let isFileUploaded = false;
let currentUserId = null;

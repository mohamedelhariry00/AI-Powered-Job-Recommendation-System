// Scroll functions
function scrollToUpload() {
    document.getElementById('upload-section').scrollIntoView({ behavior: 'smooth' });
}

function scrollToHowItWorks() {
    document.getElementById('how-it-works').scrollIntoView({ behavior: 'smooth' });
}

// Generate unique user ID from file name and timestamp
function generateUserId(fileName) {
    const timestamp = Date.now();
    const cleanFileName = fileName.replace(/[^a-zA-Z0-9]/g, '').toLowerCase();
    return `user_${cleanFileName}_${timestamp}`;
}

// File selection handling
document.addEventListener('DOMContentLoaded', function() {
    document.getElementById('cvInput').addEventListener('change', function(e) {
        const file = this.files[0];
        const fileInfo = document.getElementById('fileInfo');
        const fileLabel = document.getElementById('fileLabel');
        
        if (file) {
            const fileName = file.name;
            const fileSize = (file.size / (1024 * 1024)).toFixed(2); // MB
            
            fileInfo.innerHTML = `
                <i class="fas fa-file-alt"></i>
                <span>${fileName} (${fileSize} MB)</span>
            `;
            
            fileLabel.classList.add('file-selected');
            fileLabel.querySelector('span').textContent = 'File selected: ' + fileName;
            
            // Generate user ID for this session
            currentUserId = generateUserId(fileName);
            
            // Reset upload status
            isFileUploaded = false;
            document.getElementById('recommendBtn').style.display = 'none';
            hideMessage();
        }
    });
});

// Function to show message
function showMessage(text, type = 'info') {
    const messageEl = document.getElementById('message');
    messageEl.textContent = text;
    messageEl.className = `message ${type}`;
    messageEl.style.display = 'block';
}

// Function to show loading message
function showLoadingMessage(text) {
    const messageEl = document.getElementById('message');
    messageEl.innerHTML = `<div class="loading-spinner"></div>${text}`;
    messageEl.className = 'message loading';
    messageEl.style.display = 'block';
}

// Function to hide message
function hideMessage() {
    const messageEl = document.getElementById('message');
    messageEl.style.display = 'none';
}

// Function to handle CV upload to S3 (Updated Version)
async function uploadCV() {
    const fileInput = document.getElementById('cvInput');
    const file = fileInput.files[0];
    const uploadBtn = document.getElementById('uploadBtn');
    
    // Validate file selection
    if (!file) {
        showMessage("Please select a file first.", "error");
        return;
    }
    
    // Disable upload button and show loading
    uploadBtn.disabled = true;
    uploadBtn.innerHTML = '<div class="loading-spinner"></div>Uploading...';
    showLoadingMessage("Uploading your CV to secure cloud storage...");
    
    try {
        console.log('Starting upload for:', file.name, 'User ID:', currentUserId);
        
        // Use the original file name instead of creating structured path
        const originalFileName = file.name;
        
        // Step 1: Get a presigned URL from API Gateway
        const apiUrl = `${API_CONFIG.CV_UPLOAD_API_URL}?filename=${encodeURIComponent(originalFileName)}`;
        console.log('Requesting presigned URL...');
        
        const presignResponse = await fetch(apiUrl, {
            method: 'GET',
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            }
        });
        
        console.log('Presign response status:', presignResponse.status);
        
        if (!presignResponse.ok) {
            const errorText = await presignResponse.text();
            console.error('Presign API error:', errorText);
            throw new Error(`Failed to get upload URL: ${presignResponse.status}`);
        }
        
        const presignData = await presignResponse.json();
        console.log('Received presigned URL data');
        
        if (!presignData.url) {
            throw new Error("No upload URL received from server");
        }
        
        // Step 2: Upload the original file to S3 using the presigned URL
        console.log('Uploading to S3...');
        showLoadingMessage("Uploading file to Amazon S3...");
        
        const uploadResponse = await fetch(presignData.url, {
            method: 'PUT',
            body: file, // Upload the original file, not converted text
            headers: {
                'Content-Type': presignData.contentType || file.type
            }
        });
        
        console.log('S3 upload response status:', uploadResponse.status);
        
        if (!uploadResponse.ok) {
            const errorText = await uploadResponse.text();
            console.error('S3 upload failed:', uploadResponse.status, errorText);
            throw new Error(`Upload to S3 failed: ${uploadResponse.status} ${uploadResponse.statusText}`);
        }
        
        // Step 3: Success
        uploadedFileName = presignData.key;
        isFileUploaded = true;
        
        showMessage("CV uploaded successfully! You can now get job recommendations.", "success");
        console.log('Upload completed successfully! File stored at:', uploadedFileName);
        console.log('User ID for recommendations:', currentUserId);
        
        // Show the recommendation button
        document.getElementById('recommendBtn').style.display = 'inline-flex';
        
        // Reset upload button
        uploadBtn.disabled = false;
        uploadBtn.innerHTML = '<i class="fas fa-check-circle"></i> Uploaded Successfully';
        uploadBtn.style.backgroundColor = 'var(--success-color)';
        
    } catch (error) {
        // Handle errors
        console.error('Upload error:', error);
        showMessage(`Upload failed: ${error.message}`, "error");
        
        // Reset upload button
        uploadBtn.disabled = false;
        uploadBtn.innerHTML = '<i class="fas fa-cloud-upload-alt"></i> Upload to Cloud';
        uploadBtn.style.backgroundColor = '';
        
        isFileUploaded = false;
        document.getElementById('recommendBtn').style.display = 'none';
    }
}

// Function to get job recommendations from the API
async function getRecommendations() {
    if (!isFileUploaded || !currentUserId) {
        showMessage("Please upload your CV first.", "error");
        return;
    }
    
    const recommendBtn = document.getElementById('recommendBtn');
    
    // Disable button and show loading
    recommendBtn.disabled = true;
    recommendBtn.innerHTML = '<div class="loading-spinner"></div>AI Analyzing...';
    showLoadingMessage("Our artificial intelligence is analyzing your CV and finding the best job matches...");
    
    try {
        console.log('Requesting recommendations for user:', currentUserId);
        
        // Call the recommendation API
        const response = await fetch(API_CONFIG.RECOMMENDATION_API_URL, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            },
            body: JSON.stringify({
                user_id: currentUserId,
                action: 'get_recommendations'
            })
        });
        
        console.log('API response status:', response.status);
        
        if (!response.ok) {
            const errorData = await response.json().catch(() => ({}));
            throw new Error(errorData.error || `API request failed: ${response.status} ${response.statusText}`);
        }
        
        const recommendationData = await response.json();
        console.log('Received recommendations:', recommendationData);
        
        // Display recommendations
        displayRecommendations(recommendationData);
        
        // Show success message
        showMessage(`AI analysis complete! Found ${recommendationData.total_recommendations} job matches for you.`, "success");
        
        // Show the job recommendation section with smooth scroll
        const jobRecommendationSection = document.getElementById('job-recommendation');
        jobRecommendationSection.style.display = 'block';
        
        // Smooth scroll to recommendations
        setTimeout(() => {
            jobRecommendationSection.scrollIntoView({ behavior: 'smooth' });
        }, 500);
        
        // Reset button
        recommendBtn.disabled = false;
        recommendBtn.innerHTML = '<i class="fas fa-check-circle"></i> Recommendations Ready';
        recommendBtn.style.backgroundColor = 'var(--success-color)';
        
    } catch (error) {
        console.error('Recommendation error:', error);
        showMessage(`Failed to get recommendations: ${error.message}`, "error");
        
        // Reset button
        recommendBtn.disabled = false;
        recommendBtn.innerHTML = '<i class="fas fa-brain"></i> Get AI Recommendations';
        recommendBtn.style.backgroundColor = '';
    }
}

// Function to display job recommendations
function displayRecommendations(data) {
    console.log('Displaying recommendations:', data);
    
    // Show user profile summary
    displayUserProfile(data.user_profile);
    
    // Display job recommendations
    const container = document.getElementById('recommendations-container');
    const noRecommendationsDiv = document.getElementById('no-recommendations');
    
    if (!data.recommendations || data.recommendations.length === 0) {
        container.innerHTML = '';
        noRecommendationsDiv.style.display = 'block';
        return;
    }
    
    noRecommendationsDiv.style.display = 'none';
    container.innerHTML = '';
    
    data.recommendations.forEach((job, index) => {
        const jobCard = createJobCard(job, index === 0);
        container.appendChild(jobCard);
    });
}

// Function to display user profile summary
function displayUserProfile(userProfile) {
    const profileSummary = document.getElementById('user-profile-summary');
    const profileInfo = document.getElementById('profileInfo');
    
    if (!userProfile) {
        profileSummary.style.display = 'none';
        return;
    }
    
    const skills = userProfile.skills_extracted || [];
    const experienceYears = userProfile.experience_years || 0;
    const jobTitle = userProfile.job_title || 'Not specified';
    
    profileInfo.innerHTML = `
        <div class="profile-info-item">
            <i class="fas fa-briefcase"></i>
            <span><strong>Job Title:</strong> ${jobTitle}</span>
        </div>
        <div class="profile-info-item">
            <i class="fas fa-clock"></i>
            <span><strong>Experience:</strong> ${experienceYears} years</span>
        </div>
        <div class="profile-info-item">
            <i class="fas fa-code"></i>
            <span><strong>Skills:</strong> ${skills.length > 0 ? skills.slice(0, 5).join(', ') : 'Not extracted'}</span>
        </div>
    `;
    
    profileSummary.style.display = 'block';
}

// Function to create a job recommendation card
function createJobCard(job, isBestMatch = false) {
    const jobCard = document.createElement('div');
    jobCard.className = `job-recommendation-card ${isBestMatch ? 'best-match' : ''}`;
    
    // Determine match badge
    const matchPercentage = job.match_percentage || 0;
    let badgeClass = 'match-badge';
    let badgeText = 'Match';
    
    if (isBestMatch || matchPercentage >= 90) {
        badgeText = 'Best Match';
    } else if (matchPercentage >= 75) {
        badgeClass += ' good-match';
        badgeText = 'Good Match';
    } else if (matchPercentage >= 60) {
        badgeClass += ' fair-match';
        badgeText = 'Fair Match';
    }
    
    // Create score circle with dynamic color
    const scoreCircleColor = matchPercentage >= 90 ? '#4bb543' : 
                           matchPercentage >= 75 ? '#4895ef' : 
                           '#ffa500';
    
    const scoreCircleDegrees = (matchPercentage / 100) * 360;
    
    jobCard.innerHTML = `
        <div class="${badgeClass}">${badgeText}</div>
        <div class="job-main-info">
            <h3>${job.title || 'Job Title Not Available'}</h3>
            <h4>${job.company || 'Company Name Not Available'}</h4>
            <div class="job-meta">
                <span><i class="fas fa-map-marker-alt"></i> ${job.location || 'Location not specified'}</span>
                <span><i class="fas fa-dollar-sign"></i> ${job.salary_range || 'Salary not specified'}</span>
                <span><i class="fas fa-briefcase"></i> ${job.experience_level || 'Experience level not specified'}</span>
            </div>
            <div class="match-score">
                <div class="score-circle" style="background: conic-gradient(${scoreCircleColor} 0deg ${scoreCircleDegrees}deg, #eee ${scoreCircleDegrees}deg 360deg);">
                    <div class="score-text">${matchPercentage}%</div>
                </div>
                <span>Match Score</span>
            </div>
        </div>
        
        ${job.description ? `
        <div class="job-details">
            <h4><i class="fas fa-file-alt"></i> Job Description</h4>
            <p>${job.description.substring(0, 300)}${job.description.length > 300 ? '...' : ''}</p>
        </div>
        ` : ''}
        
        ${job.skills_required && job.skills_required.length > 0 ? `
        <div class="job-skills">
            <h4><i class="fas fa-code"></i> Required Skills</h4>
            <div class="skills-container">
                ${job.skills_required.slice(0, 8).map(skill => 
                    `<span class="skill-tag">${skill}</span>`
                ).join('')}
                ${job.skills_required.length > 8 ? '<span class="skill-tag">+' + (job.skills_required.length - 8) + ' more</span>' : ''}
            </div>
        </div>
        ` : ''}
        
        <div class="job-actions">
            ${job.job_url ? 
                `<a href="${job.job_url}" target="_blank" class="apply-btn">
                    <i class="fas fa-external-link-alt"></i> View Job
                </a>` : 
                `<button class="apply-btn" onclick="applyToJob('${job.job_id}')">
                    <i class="fas fa-paper-plane"></i> Apply Now
                </button>`
            }
            <button class="save-btn" onclick="saveJob('${job.job_id}', this)">
                <i class="fas fa-bookmark"></i> Save Job
            </button>
            <button class="share-btn" onclick="shareJob('${job.job_id}', '${job.title}', '${job.company}')">
                <i class="fas fa-share-alt"></i> Share
            </button>
        </div>
    `;
    
    return jobCard;
}

// Job action functions
function applyToJob(jobId) {
    console.log('Applying to job:', jobId);
    showMessage("This would redirect you to the job application page in a real application.", "info");
    // In a real application, this would redirect to the job application page
}

function saveJob(jobId, buttonElement) {
    console.log('Saving job:', jobId);
    
    // Visual feedback
    buttonElement.innerHTML = '<i class="fas fa-check"></i> Saved';
    buttonElement.style.backgroundColor = 'var(--success-color)';
    buttonElement.style.color = 'white';
    buttonElement.style.border = 'none';
    buttonElement.disabled = true;
    
    // Show feedback message
    setTimeout(() => {
        showMessage('Job saved to your profile!', 'success');
        setTimeout(() => hideMessage(), 3000);
    }, 300);
}

function shareJob(jobId, jobTitle, company) {
    console.log('Sharing job:', jobId, jobTitle, company);
    
    const shareData = {
        title: `${jobTitle} - ${company}`,
        text: `Check out this amazing job opportunity I found on AIpply!`,
        url: window.location.href
    };
    
    // Try Web Share API first (mobile browsers)
    if (navigator.share) {
        navigator.share(shareData).catch(console.error);
    } else {
        // Fallback to clipboard
        const shareText = `${shareData.title}\n${shareData.text}\n${shareData.url}`;
        navigator.clipboard.writeText(shareText).then(() => {
            showMessage('Job details copied to clipboard!', 'success');
            setTimeout(() => hideMessage(), 3000);
        }).catch(() => {
            // Final fallback - show share text
            alert(`Share this job:\n\n${shareText}`);
        });
    }
}

// System status check function
async function checkSystemStatus() {
    try {
        const response = await fetch(API_CONFIG.STATUS_API_URL, {
            method: 'GET',
            headers: {
                'Accept': 'application/json'
            }
        });
        
        if (response.ok) {
            const status = await response.json();
            console.log('System status:', status);
            return status;
        } else {
            console.warn('System status check failed:', response.status);
            return null;
        }
    } catch (error) {
        console.warn('System status check error:', error);
        return null;
    }
}

// Initialize page
document.addEventListener('DOMContentLoaded', function() {
    console.log('AIpply Frontend Initialized');
    console.log('API Configuration:', API_CONFIG);
    
    // Hide job recommendations initially
    document.getElementById('job-recommendation').style.display = 'none';
    
    // Check system status (optional)
    checkSystemStatus();
    
    // Add smooth scrolling to navigation links
    document.querySelectorAll('nav a[href^="#"]').forEach(anchor => {
        anchor.addEventListener('click', function (e) {
            e.preventDefault();
            const target = document.querySelector(this.getAttribute('href'));
            if (target) {
                target.scrollIntoView({ behavior: 'smooth' });
            }
        });
    });
    
    // Add animation to skill tags when they become visible
    const observerOptions = {
        threshold: 0.1,
        rootMargin: '0px 0px -50px 0px'
    };
    
    const observer = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                const skillTags = entry.target.querySelectorAll('.skill-tag');
                skillTags.forEach((tag, index) => {
                    setTimeout(() => {
                        tag.style.opacity = '1';
                        tag.style.transform = 'translateY(0)';
                    }, index * 100);
                });
            }
        });
    }, observerOptions);
    
    // Observe skills containers that get dynamically added
    const checkForSkillsContainers = () => {
        document.querySelectorAll('.skills-container:not([data-observed])').forEach(container => {
            container.setAttribute('data-observed', 'true');
            
            // Set initial styles for skill tags
            container.querySelectorAll('.skill-tag').forEach(tag => {
                tag.style.opacity = '0';
                tag.style.transform = 'translateY(20px)';
                tag.style.transition = 'all 0.3s ease';
            });
            
            observer.observe(container);
        });
    };
    
    // Check for skills containers periodically (after recommendations are loaded)
    setInterval(checkForSkillsContainers, 1000);
});

// Error handling for API configuration
window.addEventListener('load', function() {
    // Check if API URLs are configured
    if (API_CONFIG.RECOMMENDATION_API_URL.includes('YOUR_API_ID')) {
        console.warn('⚠️ API Gateway URL not configured! Please update API_CONFIG.RECOMMENDATION_API_URL');
        
        // Show warning message to user
        setTimeout(() => {
            showMessage('⚠️ System configuration incomplete. Please contact support.', 'error');
        }, 2000);
    }
});

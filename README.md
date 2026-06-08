# Infodemica

**Transform Research Into Visual Stories**

Infodemica is a Django web application that automatically converts scientific papers into engaging social media videos (TikTok/Instagram Reels format). The system uses AI (Google Gemini for text/audio generation, RunwayML for video generation) to transform complex research papers from PubMed Central into accessible, shareable video content.

## 🎯 Problem Being Solved

Scientific research is often locked behind paywalls and buried in technical jargon, making it inaccessible to the general public. Infodemica democratizes science by:

- **Breaking Down Barriers**: Converting complex research papers into engaging visual stories
- **Increasing Reach**: Videos get 10x more engagement than static PDFs
- **Accelerating Discovery**: Sharing findings faster than traditional publication timelines
- **Amplifying Impact**: Making research more visible, shareable, and impactful

## 🚀 Production Environment

- **Production URL**: https://ai-study-videos-production.up.railway.app/
- **Staging URL**: Not currently configured (can be set up as needed)

## 🛠️ Technology Stack

### Backend
- **Django 5.1.2** - Web framework
- **Celery 5.3.6** - Asynchronous task queue
- **Redis 5.0.1** - Message broker for Celery
- **PostgreSQL** - Production database (via Railway)
- **SQLite** - Local development database

### AI & APIs
- **Google Gemini API** - Text generation, scene creation, and text-to-speech
- **RunwayML API** - Video generation using Veo 3.1 model

### Storage
- **Cloudflare R2** - Cloud storage for generated videos (S3-compatible)
- **Railway Volumes** - Persistent storage option (alternative to R2)

### Infrastructure
- **Railway** - Hosting and deployment platform
- **Gunicorn** - WSGI HTTP server
- **WhiteNoise** - Static file serving

### Frontend
- **HTML/CSS/JavaScript** - Traditional web stack
- **Django Templates** - Server-side rendering

## 📋 Setup Instructions (Local Development)

### Prerequisites

- Python 3.12+
- PostgreSQL (for production) or SQLite (for local development)
- Redis (for Celery task queue)
- FFmpeg (for video processing)
- Git

### Step 1: Clone the Repository

```bash
git clone <repository-url>
cd Hidden-Hill
```

### Step 2: Create Virtual Environment

**Windows (PowerShell):**
```powershell
python -m venv venv
.\venv\Scripts\activate.ps1
```

**Linux/Mac:**
```bash
python -m venv venv
source venv/bin/activate
```

### Step 3: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 4: Environment Variables

Create a `.env` file in the project root:

```env
# Required API Keys
GEMINI_API_KEY=your-gemini-api-key-here
RUNWAYML_API_SECRET=your-runway-api-key-here

# Django Settings
SECRET_KEY=your-django-secret-key-here
DEBUG=True
VIDEO_ACCESS_CODE=your-access-code-here

# Database (optional - defaults to SQLite for local)
DATABASE_URL=sqlite:///db.sqlite3

# Celery (Redis)
CELERY_BROKER_URL=redis://localhost:6379/0
CELERY_RESULT_BACKEND=redis://localhost:6379/0

# Cloud Storage (optional for local development)
USE_CLOUD_STORAGE=False
# If using Cloudflare R2:
# AWS_ACCESS_KEY_ID=your-r2-access-key
# AWS_SECRET_ACCESS_KEY=your-r2-secret-key
# AWS_STORAGE_BUCKET_NAME=your-bucket-name
# AWS_S3_ENDPOINT_URL=https://your-account-id.r2.cloudflarestorage.com
```

**Generate Django Secret Key:**
```bash
python -c "from django.core.management.utils import get_random_secret_key; print(get_random_secret_key())"
```

### Step 5: Database Setup

```bash
python manage.py migrate
python manage.py createsuperuser  # Optional: create admin user
```

### Step 6: Start Redis (for Celery)

**Windows:**
- Download Redis from https://github.com/microsoftarchive/redis/releases
- Or use WSL/Docker

**Linux/Mac:**
```bash
# Install Redis
sudo apt-get install redis-server  # Ubuntu/Debian
brew install redis  # macOS

# Start Redis
redis-server
```

### Step 7: Start Celery Worker

**Windows (PowerShell):**
```powershell
.\scripts\start_celery_worker.ps1
```

**Linux/Mac:**
```bash
celery -A config worker --loglevel=info
```

### Step 8: Run Development Server

```bash
python manage.py runserver
```

The application will be available at `http://localhost:8000/`

### Step 9: Collect Static Files (Production-like)

```bash
python manage.py collectstatic --noinput
```

## 🚢 Deployment Information

### Production Environment

- **URL**: https://ai-study-videos-production.up.railway.app/
- **Platform**: Railway
- **Database**: PostgreSQL (managed by Railway)
- **Storage**: Cloudflare R2 (cloud storage)
- **Task Queue**: Redis (managed by Railway)

### Staging Environment

- **URL**: Not currently configured
- **Setup**: Can be configured similarly to production on Railway with a separate service

### Deployment Process

Deployment to Railway is automated via GitHub integration:

1. **Link Repository to Railway**
   - Connect GitHub repository to Railway service
   - Railway automatically detects Django projects

2. **Configure Environment Variables**
   Set the following in Railway dashboard:
   ```
   GEMINI_API_KEY=<your-key>
   RUNWAYML_API_SECRET=<your-key>
   SECRET_KEY=<your-secret-key>
   VIDEO_ACCESS_CODE=<your-access-code>
   DATABASE_URL=<railway-postgres-url>
   USE_CLOUD_STORAGE=True
   AWS_ACCESS_KEY_ID=<r2-key>
   AWS_SECRET_ACCESS_KEY=<r2-secret>
   AWS_STORAGE_BUCKET_NAME=<bucket-name>
   AWS_S3_ENDPOINT_URL=<r2-endpoint>
   CELERY_BROKER_URL=<railway-redis-url>
   CELERY_RESULT_BACKEND=<railway-redis-url>
   DEBUG=False
   ALLOWED_HOSTS=.up.railway.app
   CSRF_TRUSTED_ORIGINS=https://*.up.railway.app
   ```

3. **Attach Services**
   - PostgreSQL database (Railway managed)
   - Redis (Railway managed)
   - Optional: Railway Volume for persistent storage (alternative to R2)

4. **Deploy**
   - Railway automatically deploys on git push
   - Or manually trigger deployment from Railway dashboard

5. **Run Migrations**
   ```bash
   # Via Railway CLI or dashboard
   railway run python manage.py migrate
   railway run python manage.py collectstatic --noinput
   ```

### Deployment Commands

**Manual Deployment (if needed):**
```bash
# Install Railway CLI
npm i -g @railway/cli

# Login
railway login

# Link project
railway link

# Deploy
railway up

# Run migrations
railway run python manage.py migrate

# Collect static files
railway run python manage.py collectstatic --noinput
```

**Procfile** (automatically used by Railway):
```
web: python manage.py migrate --noinput && python manage.py collectstatic --noinput && python -m gunicorn config.wsgi:application --bind 0.0.0.0:${PORT}
worker: celery -A config worker --loglevel=info --concurrency=2 --max-tasks-per-child=50
```

## 👥 Team Member Contributions

*Note: Team member information should be added here. Please update with actual team member names and their contributions.*

### Example Format:
- **Team Member 1** - Backend development, pipeline architecture
- **Team Member 2** - Frontend development, UI/UX design
- **Team Member 3** - DevOps, deployment, infrastructure
- **Team Member 4** - Testing, QA, documentation

## 🧪 A/B Test Endpoint

### How to Compute and Access the A/B Test Endpoint

The A/B test endpoint uses the first 7 characters of the SHA-1 hash of the team nickname "hidden-hill".

#### Step 1: Compute SHA-1 Hash

**Python:**
```python
import hashlib

team_nickname = "hidden-hill"
hash_object = hashlib.sha1(team_nickname.encode())
hex_dig = hash_object.hexdigest()
first_7_chars = hex_dig[:7]
print(first_7_chars)  # Output: "e9ec8bb"
```

**Command Line (Linux/Mac):**
```bash
echo -n "hidden-hill" | sha1sum | cut -c1-7
# Output: e9ec8bb
```

**Command Line (Windows PowerShell):**
```powershell
$teamNickname = "hidden-hill"
$sha1 = [System.Security.Cryptography.SHA1]::Create()
$hash = $sha1.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($teamNickname))
$hexString = ($hash | ForEach-Object { $_.ToString("x2") }) -join ""
$first7Chars = $hexString.Substring(0, 7)
Write-Host $first7Chars  # Output: e9ec8bb
```

#### Step 2: Access the Endpoint

The A/B test endpoint is live and accessible at:

**Production:**
```
https://ai-study-videos-production.up.railway.app/e9ec8bb/
```

**Local Development:**
```
http://localhost:8000/e9ec8bb/
```

### Implementation Details

#### Features

✅ **Publicly Accessible** - No authentication required  
✅ **Team Nickname Display** - Shows list of all team member nicknames  
✅ **A/B Testing** - Button text alternates between two variants:
   - **Variant A**: "kudos"
   - **Variant B**: "thanks"
✅ **Analytics Tracking** - Tracks impressions and clicks
✅ **Session-Based** - Consistent variant assignment per session (via cookie)

#### Team Nicknames

The endpoint displays the following team member nicknames:
- charming-leopard
- Vicent (ID unknown)
- Kenza (ID unknown)
- Omar (ID unknown)
- Nora (ID unknown)
- Skel (ID unknown)

#### How It Works

1. **Variant Assignment**: Users are assigned to Variant A or B based on a hash of their session ID (50/50 split)
2. **Impression Tracking**: Each page view automatically tracks an impression event
3. **Click Tracking**: Button clicks are tracked via AJAX to `/analytics/track-click/`
4. **Session Persistence**: Session ID is stored in a cookie for consistent variant assignment

#### Analytics Data

The A/B test data is stored in the `ABTestEvent` model with the following fields:
- `event_type`: "impression" or "click"
- `variant`: "A" or "B"
- `session_id`: Unique session identifier
- `ip_address`: Client IP address
- `user_agent`: Browser user agent
- `created_at`: Timestamp

#### View Analytics

Analytics data can be viewed:
- **Django Admin**: `/admin/web/abtestevent/`
- **Management Command**: `python manage.py analytics_summary`

#### API Endpoints

- **A/B Test Page**: `GET /e9ec8bb/` - Displays team nicknames and A/B test button
- **Click Tracking**: `POST /analytics/track-click/` - Tracks button clicks
  ```json
  {
    "session_id": "uuid",
    "variant": "A" or "B"
  }
  ```

## 📚 Additional Documentation

- **[API Documentation](./docs/API_README.md)** - Complete REST API reference
- **[Pipeline Guide](./docs/PIPELINE_README.md)** - Video generation pipeline details
- **[Next Steps](./docs/NEXT_STEPS.md)** - Roadmap and future features
- **[Testing Guide](./docs/TESTING.md)** - Testing procedures and scripts
- **[Railway Setup](./docs/RAILWAY_VOLUMES_SETUP.md)** - Persistent storage configuration

## 🔧 Development

### Running Tests

```bash
# Run Django tests
python manage.py test

# Run pipeline tests
cd tests
.\test_status.ps1  # Windows
```

### Project Structure

```
Hidden-Hill/
├── config/          # Django configuration
├── web/             # Main Django app
│   ├── models.py    # Database models
│   ├── views.py     # View handlers
│   ├── tasks.py     # Celery tasks
│   └── templates/   # HTML templates
├── pipeline/        # Video generation pipeline
│   ├── main.py      # CLI entry point
│   ├── pipeline.py  # Core pipeline logic
│   ├── pubmed.py    # PubMed integration
│   ├── scenes.py    # Scene generation
│   ├── audio.py     # Audio generation
│   └── video.py    # Video generation
├── docs/            # Documentation
├── scripts/         # Utility scripts
└── tests/           # Test scripts
```

## 📝 License

*Add license information here*

## 🤝 Contributing

*Add contribution guidelines here*

---

**Last Updated**: 2025-01-28  
**Project Status**: Production-ready, core features complete

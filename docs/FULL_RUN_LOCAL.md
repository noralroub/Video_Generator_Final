# Full run locally (real video generation)

Use this to run the full pipeline on your machine: Django + Redis + Celery + API keys.

---

## 1. Prerequisites

- **Python 3.12** and project venv already set up (you did this when you got the UI running).
- **Redis** – message queue for Celery.
- **FFmpeg** – used by the pipeline for audio/video.
- **API keys** – Gemini and Runway (see below).

### Install Redis (macOS)

```bash
brew install redis
```

Start Redis (run in a separate terminal, or in background):

```bash
redis-server
```

Leave it running. To run in background: `redis-server &`.

### Install FFmpeg (macOS)

```bash
brew install ffmpeg
```

### Get API keys

1. **Gemini**: https://aistudio.google.com/apikey – create an API key.
2. **Runway**: https://runwayml.com – sign up and get an API secret from account/settings.

---

## 2. Configure .env for full run

Edit your `.env` in the project root (or copy from `.env.example` and fill in):

```env
DEBUG=True
SECRET_KEY=dev-secret-not-for-production
VIDEO_ACCESS_CODE=local

# Required for real video generation
GEMINI_API_KEY=your-actual-gemini-key
RUNWAYML_API_SECRET=your-actual-runway-secret
SIMULATION_MODE=False

# Optional; defaults to this if unset
CELERY_BROKER_URL=redis://localhost:6379/0
CELERY_RESULT_BACKEND=redis://localhost:6379/0
```

- Replace `your-actual-gemini-key` and `your-actual-runway-secret` with your real keys.
- Access code you’ll type in the UI is **`local`** (same as `VIDEO_ACCESS_CODE`).

---

## 3. Start services (three things)

Use **three terminals** (or run Redis in background).

**Terminal 1 – Redis** (if not already running):

```bash
redis-server
```

**Terminal 2 – Celery worker** (from project root, with venv activated):

```bash
cd /Users/Noralroub/Video_Generator_Final
source venv/bin/activate
celery -A config worker --loglevel=info
```

**Terminal 3 – Django** (from project root):

```bash
cd /Users/Noralroub/Video_Generator_Final
source venv/bin/activate
python manage.py runserver 8080
```

(Use `runserver` with no port if you prefer port 8000.)

Check:

- Redis: no errors in Terminal 1.
- Celery: you see “celery@... ready” in Terminal 2.
- Django: “Starting development server at http://127.0.0.1:8080/” in Terminal 3.

---

## 4. Run a full generation (walkthrough)

1. **Open the app**  
   Go to: **http://127.0.0.1:8080/**

2. **Go to Upload**  
   Click the link that takes you to the upload page (e.g. “Upload” or “Generate video”).

3. **Submit a paper**  
   - **PubMed ID or PMCID**: use a valid **PMCID** (e.g. `PMC10979640`) or **PMID** (e.g. `33963468`).  
     Papers must be from **PubMed Central** (open access) so the pipeline can fetch the full text.
   - **Access code**: type **`local`** (must match `VIDEO_ACCESS_CODE` in `.env`).
   - Click the submit button.

4. **Status page**  
   You’ll be redirected to a status page for that job. It will update as the pipeline runs:
   - Fetch paper → Generate script → Generate audio → Generate videos.

5. **Result and video**  
   When it finishes, you can open the result page and play or download the generated video. If something fails, the status/result page and Celery terminal will show errors.

---

## 5. Troubleshooting

- **“Invalid access code”** – Use exactly **`local`** (same as `VIDEO_ACCESS_CODE` in `.env`).
- **Job stays “pending” / nothing happens** – Celery isn’t running or can’t connect to Redis. Start Redis, then start the Celery worker (Terminals 1 and 2).
- **Pipeline error about GEMINI_API_KEY or RUNWAYML_API_SECRET** – Add the keys to `.env` and **restart the Celery worker** (and runserver if you changed `.env` before starting it).
- **FFmpeg not found** – Install with `brew install ffmpeg` and ensure `ffmpeg` is on your PATH.
- **Paper not found / PMC only** – Use a PMCID (e.g. `PMC10979640`) or a PMID that has a full-text article on PubMed Central.

---

## Quick checklist

- [ ] Redis installed and `redis-server` running
- [ ] FFmpeg installed (`brew install ffmpeg`)
- [ ] `.env` has `GEMINI_API_KEY`, `RUNWAYML_API_SECRET`, `VIDEO_ACCESS_CODE=local`, `SIMULATION_MODE=False`
- [ ] Celery worker running: `celery -A config worker --loglevel=info`
- [ ] Django running: `python manage.py runserver 8080`
- [ ] Upload with a PMCID (e.g. `PMC10979640`) and access code **`local`**

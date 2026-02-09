# Local testing – step by step

How to set up your environment and run end-to-end tests (CLI and/or web), and where to put API keys.

---

## 1. Where to save API keys

**Use a `.env` file in the project root** (same folder as `manage.py`).

- Copy the example: `cp .env.example .env`
- Edit `.env` and replace placeholder values.

**Required for pipeline (script + audio + presentation):**

```env
GEMINI_API_KEY=your-actual-gemini-key
```

Get a key: https://aistudio.google.com/apikey

**Required only for video mode (Veo/Runway):**

```env
RUNWAYML_API_SECRET=your-runway-secret
```

**Required for web UI:**

```env
SECRET_KEY=dev-secret-not-for-production
VIDEO_ACCESS_CODE=local
```

**Optional for presentation-only (no Runway cost):**

```env
PIPELINE_OUTPUT=presentation
```

When `PIPELINE_OUTPUT=presentation`, the web and Celery task run the HTML presentation pipeline instead of video. You do **not** need `RUNWAYML_API_SECRET` in that case.

**Example `.env` for presentation-only testing:**

```env
DEBUG=True
SECRET_KEY=dev-secret-not-for-production
VIDEO_ACCESS_CODE=local
GEMINI_API_KEY=your-gemini-key
PIPELINE_OUTPUT=presentation
SIMULATION_MODE=False
CELERY_BROKER_URL=redis://localhost:6379/0
CELERY_RESULT_BACKEND=redis://localhost:6379/0
```

Do **not** commit `.env` (it is in `.gitignore`). Keys stay only on your machine.

---

## 2. Do I need to rerun the Celery worker?

**Yes, after changing `.env`.** The worker reads the environment when it starts. So:

1. Edit `.env` (e.g. add `PIPELINE_OUTPUT=presentation` or fix a key).
2. Stop the Celery worker (Ctrl+C in its terminal).
3. Start it again: `celery -A config worker --loglevel=info`.

Same idea for Django if you rely on env at startup: restart `runserver` after changing `.env` if something doesn’t pick up the new value.

---

## 3. Option A – CLI only (presentation pipeline, no web)

Good for testing the HTML presentation pipeline without Redis/Celery/Django. Only Gemini is used (script + TTS).

### 3.1 Prerequisites

- Python 3.12, venv with project dependencies (e.g. `pip install -r requirements.txt` or `uv sync` in pipeline).
- `.env` in project root with at least `GEMINI_API_KEY`.

### 3.2 Run from project root

```bash
cd /Users/Noralroub/Video_Generator_Final
source venv/bin/activate   # or: uv run (if using uv from pipeline dir)
```

Use a **PMCID** (e.g. `PMC10979640`) so the pipeline can fetch full text from PubMed Central.

**Full pipeline (fetch → script → audio → presentation):**

```bash
python pipeline/main.py generate-video PMC10979640 ./media/PMC10979640 --output-format presentation
```

Output:

- `./media/PMC10979640/paper.json`
- `./media/PMC10979640/script.json`
- `./media/PMC10979640/audio.wav`
- `./media/PMC10979640/audio_metadata.json`
- `./media/PMC10979640/presentation.html`

Open `./media/PMC10979640/presentation.html` in a browser (and keep `audio.wav` in the same folder so the embedded audio works).

**Only regenerate the HTML from existing script/audio:**

```bash
python pipeline/main.py generate-presentation ./media/PMC10979640
```

---

## 4. Option B – Full web UI (end-to-end with Celery)

Use this to test the full flow from the browser: upload → status page → result (video or presentation).

### 4.1 Prerequisites

- Redis installed and running.
- FFmpeg installed (only needed for **video** mode; not required for **presentation** mode).
- `.env` configured (see section 1).

### 4.2 Install and start Redis (macOS)

```bash
brew install redis
redis-server
```

Leave this running in a terminal (or run in background: `redis-server &`).

### 4.3 Three terminals

**Terminal 1 – Redis** (if not already running):

```bash
redis-server
```

**Terminal 2 – Celery worker** (from project root, venv activated):

```bash
cd /Users/Noralroub/Video_Generator_Final
source venv/bin/activate
celery -A config worker --loglevel=info
```

Wait until you see something like `celery@... ready`.  
If you change `.env`, stop (Ctrl+C) and start this again.

**Terminal 3 – Django** (from project root):

```bash
cd /Users/Noralroub/Video_Generator_Final
source venv/bin/activate
python manage.py runserver 8080
```

### 4.4 Run a test from the UI

1. Open **http://127.0.0.1:8080/**
2. Go to **Upload** (or the link that starts a generation).
3. Enter:
   - **PubMed ID or PMCID:** e.g. `PMC10979640` (must be PubMed Central).
   - **Access code:** `local` (must match `VIDEO_ACCESS_CODE` in `.env`).
4. Submit.
5. You’ll be redirected to the status page. It will update as the pipeline runs.
6. When it finishes:
   - **Video mode:** “Play/Download Video” and result page with video.
   - **Presentation mode:** “View presentation” and result page with link to `presentation.html`.

### 4.5 Presentation mode via web

In `.env` set:

```env
PIPELINE_OUTPUT=presentation
```

Restart the **Celery worker** (and optionally Django). Then run a job as above; the worker will run the presentation pipeline (no Runway). Output will be under `media/<pmid>/presentation.html` and the status/result pages will show “View presentation”.

---

## 5. Quick checklist

**API keys**

- [ ] `.env` in project root (copy from `.env.example`).
- [ ] `GEMINI_API_KEY` set (required for script + TTS).
- [ ] `RUNWAYML_API_SECRET` set only if testing **video** mode.
- [ ] `VIDEO_ACCESS_CODE=local` (and use `local` in the UI).

**Presentation-only (no Runway)**

- [ ] `PIPELINE_OUTPUT=presentation` in `.env` (optional; for web/Celery).
- [ ] Restart Celery after changing `.env`.

**CLI test**

- [ ] `python pipeline/main.py generate-video PMC10979640 ./media/PMC10979640 --output-format presentation`
- [ ] Open `./media/PMC10979640/presentation.html` in a browser.

**Web test**

- [ ] Redis running (`redis-server`).
- [ ] Celery running (`celery -A config worker --loglevel=info`).
- [ ] Django running (`python manage.py runserver 8080`).
- [ ] Upload with a PMCID and access code `local`.

---

## 6. Troubleshooting

- **“Invalid access code”** – Use `local` and set `VIDEO_ACCESS_CODE=local` in `.env`.
- **Job stays “pending”** – Start Redis, then Celery; check Celery terminal for errors.
- **Missing GEMINI_API_KEY / RUNWAYML** – Add keys to `.env` and **restart the Celery worker**.
- **Presentation not showing in web** – Ensure `media/<pmid>/presentation.html` exists (e.g. run with `PIPELINE_OUTPUT=presentation` or run CLI with `--output-format presentation` and output to `media/<pmid>/`).
- **Paper not found** – Use a PMCID (e.g. `PMC10979640`) or a PMID that has full text on PubMed Central.

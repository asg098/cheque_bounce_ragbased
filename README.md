# JUDIQ v5.0 — Render Deployment Guide

## What's in this folder

| File | Purpose |
|---|---|
| `judiq_render.py` | Main API server (FastAPI) |
| `requirements.txt` | Python dependencies |
| `render.yaml` | Render platform config |

---

## Step 1 — Prepare your Google Drive KB file

1. Open **Google Drive**
2. Upload your `cheque_bounce_kb.csv` file
3. Right-click the file → **Share** → **"Anyone with the link"** → **Viewer**
4. Copy the **File ID** from the URL:
   ```
   https://drive.google.com/file/d/1aBcDeFgHiJkLmNoPqRsTuV/view
                                    ↑ this is your File ID
   ```
5. Save this File ID — you'll need it in Step 3

---

## Step 2 — Push to GitHub

1. Create a new GitHub repository (public or private)
2. Add these 3 files to the repo root:
   - `judiq_render.py`
   - `requirements.txt`
   - `render.yaml`
3. Push to GitHub

---

## Step 3 — Deploy on Render

1. Go to [render.com](https://render.com) and sign in
2. Click **"New"** → **"Web Service"**
3. Connect your GitHub account and select your repository
4. Render will auto-detect `render.yaml` — just click **"Apply"**
5. Before deploying, set the environment variable:
   - Key: `GDRIVE_FILE_ID`
   - Value: *(paste your File ID from Step 1)*
6. Click **"Create Web Service"**
7. Wait ~3 minutes for the first build

---

## Step 4 — Verify it's working

Once deployed, Render gives you a URL like:
```
https://judiq-api.onrender.com
```

Open these in your browser to verify:

| URL | What you should see |
|---|---|
| `/health` | `{"status": "healthy"}` |
| `/docs` | Full Swagger UI with all endpoints |
| `/kb/status` | KB status + whether Drive is connected |

---

## Step 5 — Load your Knowledge Base

After deployment:
1. Open `https://your-render-url.onrender.com/kb/status`
   - Check if `gdrive_configured` is `true`
   - If not, double-check the `GDRIVE_FILE_ID` env var on Render
2. Call `POST /kb/reload` to download and activate the KB
3. Check `/kb/status` again — `kb_loaded` should be `true` and `kb_rows` > 1

---

## Updating the KB later

Whenever you have a new version of `cheque_bounce_kb.csv`:
1. Upload the new file to Google Drive (same file ID if you replace it, or new ID if new file)
2. If new file ID → update `GDRIVE_FILE_ID` env var on Render → Redeploy
3. If same file → just call `POST /kb/reload` — no redeploy needed

---

## API Endpoints

| Method | Endpoint | Purpose |
|---|---|---|
| `POST` | `/analyze-case` | Full Section 138 case analysis |
| `POST` | `/generate-cross-examination` | Cross-examination questions |
| `GET` | `/kb/status` | KB load status |
| `POST` | `/kb/reload` | Re-download KB from Drive |
| `GET` | `/health` | Health check |
| `GET` | `/docs` | Swagger UI (all endpoints + examples) |

---

## Connecting your Netlify Frontend

In your frontend JS, point all API calls to your Render URL:

```javascript
const API_BASE = "https://your-judiq-app.onrender.com";

const response = await fetch(`${API_BASE}/analyze-case`, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify(caseData)
});
const result = await response.json();
```

CORS is already open — no extra config needed.

---

## Important Notes

- **Free tier spin-down**: Render free tier sleeps after 15 min of inactivity.
  First request after sleep takes ~30 seconds. Upgrade to Starter ($7/mo) for always-on.
- **KB cache**: KB is cached locally for 24 hours. Call `/kb/reload` to force refresh.
- **Database**: SQLite DB is stored in `data_judiq/case_analysis/judiq.db` on the Render disk.
  Note: Render free tier disk is ephemeral (resets on redeploy). Upgrade to use persistent disk.

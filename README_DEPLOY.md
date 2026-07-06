# HIT RADAR — Railway Deploy Guide

## Files in this folder
- `server.py` — backend (Flask). Hardcoded API keys removed, now reads from env vars only.
- `requirements.txt` — Python dependencies
- `Procfile` / `railway.json` — tells Railway how to start the app
- `users.json` — default login accounts (seeded automatically into the volume on first boot)
- `.env.example` — list of environment variables to set in Railway
- `hit_radar_dashboard.html` — frontend, upload/host this separately on Netlify

## Step 1 — Push backend to Railway
1. Create a new GitHub repo, upload everything in this folder **except** `hit_radar_dashboard.html`
   (that file goes to Netlify separately).
2. Go to https://railway.app → New Project → Deploy from GitHub repo → select your repo.
3. Railway auto-detects Python via `railway.json` / `Procfile` and installs `requirements.txt`.

## Step 2 — Add a persistent Volume (important)
Without this, `users.json` edits and uploaded dengue-burden files reset on every redeploy.
1. In your Railway service → **Settings → Volumes → New Volume**.
2. Mount path: `/data`
3. In **Variables**, add `DATA_DIR=/data`

## Step 3 — Set environment variables
In Railway → your service → **Variables**, add (see `.env.example`):
- `GROQ_API_KEY`
- `GEMINI_API_KEY`
- `MISTRAL_API_KEY`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`
- `NEWSDATA_API_KEY`
- `OWM_API_KEY` (optional)
- `WAQI_API_KEY` (optional)
- `DATA_DIR=/data` (from Step 2)

Get fresh keys from each provider's dashboard — regenerate them if they were ever shared/exposed
anywhere before, since the old ones baked into the original file should be treated as compromised.

Railway sets `PORT` automatically — no action needed, `server.py` already reads it.

## Step 4 — Get your backend URL
After deploy finishes, Railway gives you a public URL like:
`https://hit-radar-production.up.railway.app`

Test it: open `https://your-url.up.railway.app/api/health` — should return a JSON status block.

## Step 5 — Point the frontend at it
Open `hit_radar_dashboard.html`, find this line near the top of the `<script>`:
```js
const API_BASE = 'https://REPLACE-WITH-YOUR-RAILWAY-URL.up.railway.app';
```
Replace with your actual Railway URL (no trailing slash), save, then upload/deploy this HTML file
on Netlify (drag-and-drop the file into Netlify's "Deploys" tab, or connect it via Git).

## Step 6 — Login
Default accounts are in `users.json`:
- admin@hitfik.com / HIT@2024 (Admin)
- analyst@hitfik.com / Analyst@2024 (Analyst)
- manager@hitfik.com / Manager@2024 (Campaign Manager)

**Change these passwords** before going live — edit `users.json` directly in the Railway volume
(or redeploy with updated values) since it's publicly guessable otherwise.

## Notes
- Managing users: edit `/data/users.json` (via Railway's shell or a redeploy that overwrites the seed file) — changes take effect immediately, no restart needed.
- The scheduler (weather/news refresh) only runs correctly on an always-on host like Railway — this would NOT work on Netlify Functions.
- CORS is already set to allow all origins (`*`) in `server.py`, so no extra config is needed for Netlify → Railway calls.

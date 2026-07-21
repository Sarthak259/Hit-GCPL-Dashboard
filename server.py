"""
╔═════════════════════════════════════════════════════════════════════════════╗
║          HIT RADAR — Complete Backend Server (Single File)                  ║
║      Realtime Trigger-Based Media Intelligence — v3.0.0                     ║
╠═════════════════════════════════════════════════════════════════════════════╣
║  APIs Used:                                                                 ║
║   • Open-Meteo         — Weather + Air Quality (free, no key)               ║
║   • Google News RSS    — 4 keyword feeds (free, no key)                     ║
║   • NewsData.io API    — Structured news JSON (free key, Gmail ok)          ║
║   • Groq API           — AI briefs, fast (free key)                         ║
║   • Gemini API         — AI daily summary, quality (free key)               ║
║   • Mistral API        — AI fallback (free key)                             ║
║   • Telegram Bot API   — Push alerts (free key)                             ║
║   • WAQI API           — Air Quality Index (free key)                       ║
╠═════════════════════════════════════════════════════════════════════════════╣
║  KEY CHANGE: Contextual Risk Score is now the PRIMARY risk model            ║
║  All views (Map, Triggers, Tables) use Contextual Risk Score                ║
║  Thresholds: LOW 0-2.5 | MONITOR 2.5-5.0 | PREPARE 5.0-7.0 | BOOST 7.0-10.0 ║
╚═════════════════════════════════════════════════════════════════════════════╝

Run:
    pip install flask flask-cors requests apscheduler feedparser groq google-generativeai pdfplumber openpyxl
    python server.py
"""

# ══════════════════════════════════════════════════════════════════════
# IMPORTS
# ══════════════════════════════════════════════════════════════════════
import os, time, math, logging, threading, hashlib
from datetime import datetime, timezone, timedelta
import email.utils
from urllib.parse import quote

import json
import re
import csv
import io
import time as time_module

import requests
from requests.adapters import HTTPAdapter
try:
    from urllib3.util.retry import Retry
except ImportError:
    from requests.packages.urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
import feedparser
try:
    import pdfplumber
    PDF_SUPPORT = True
except ImportError:
    PDF_SUPPORT = False
try:
    import openpyxl
    XLSX_SUPPORT = True
except ImportError:
    XLSX_SUPPORT = False
from flask import Flask, jsonify, request
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler

# ══════════════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════════════
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("hit_radar")

# ══════════════════════════════════════════════════════════════════════
# SHARED HTTP SESSION — connection pooling + transport-level retry.
# Fixes intermittent Open-Meteo read timeouts: a single ad-hoc
# requests.get() per call opens a fresh TCP/TLS connection every time and
# gives up after one slow response. This session reuses connections and
# auto-retries on connection resets / 5xx / 429 before our own app-level
# retry loop even kicks in.
# ══════════════════════════════════════════════════════════════════════
def _build_http_session():
    session = requests.Session()
    retry = Retry(
        total=2,
        connect=2,
        read=2,
        backoff_factor=0.6,          # 0.6s, 1.2s between transport-level retries
        # NOTE: 429 (rate limit) deliberately excluded here. Retrying a 429
        # at the transport layer AND then again in our own app-level retry
        # loop below used to multiply every rate-limited request by up to
        # 3x-9x, which is exactly the wrong response to a rate limit and is
        # what caused the sustained 429 storms. Only true transient errors
        # get an automatic transport-level retry; 429s are handled once,
        # deliberately, with a real cooldown, by the app-level retry loops.
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session

HTTP = _build_http_session()
# (connect_timeout, read_timeout) — Open-Meteo occasionally takes >15s to
# respond under load; give reads more room while still failing fast on
# dead connections.
HTTP_TIMEOUT = (8, 25)


def _retry_sleep(exc, attempt, base=2):
    """How long to wait before the next attempt. A 429 gets a real cooldown
    (respecting Retry-After if Open-Meteo sends one) instead of the short
    2s/4s backoff used for ordinary transient errors — hammering a rate
    limit with quick retries just keeps the limit triggered for longer."""
    resp = getattr(exc, "response", None)
    if resp is not None and resp.status_code == 429:
        retry_after = resp.headers.get("Retry-After")
        if retry_after:
            try:
                return max(float(retry_after), 5)
            except ValueError:
                pass
        return min(10 * attempt, 30)
    return base * attempt

# ══════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════
CONFIG = {
    "GROQ_API_KEY":          os.getenv("GROQ_API_KEY",    ""),
    "GEMINI_API_KEY":        os.getenv("GEMINI_API_KEY",  ""),
    "MISTRAL_API_KEY":       os.getenv("MISTRAL_API_KEY", ""),
    "UNSPLASH_ACCESS_KEY":   os.getenv("UNSPLASH_ACCESS_KEY", ""),
    "TELEGRAM_BOT_TOKEN":    os.getenv("TELEGRAM_BOT_TOKEN", ""),
    "TELEGRAM_CHAT_ID":      os.getenv("TELEGRAM_CHAT_ID",   ""),
    "NEWSDATA_API_KEY":      os.getenv("NEWSDATA_API_KEY", ""),
    "OWM_API_KEY":           os.getenv("OWM_API_KEY",  ""),
    "WAQI_API_KEY":          os.getenv("WAQI_API_KEY", ""),

    "WEATHER_REFRESH_SEC":   1800,  # was 600 (10min) — bumped to 30min to cut Open-Meteo call volume
    "NEWS_REFRESH_SEC":      300,
    "NEWSDATA_REFRESH_SEC":  3600,
    "AQ_REFRESH_SEC":        3600,
    "THRESH_RAIN_MM":        10.0,
    "THRESH_HUMIDITY_PCT":   80.0,
    "THRESH_TEMP_C":         40.0,
    "THRESH_MONITOR_TEMP":   38.0,
}

# ══════════════════════════════════════════════════════════════════════
# COLOR MAPPING FOR MAP CIRCLES - Matches UI colors
# ══════════════════════════════════════════════════════════════════════
TRIGGER_COLORS = {
    "BOOST": {
        "hex": "#ff2d55",      # Red - matches UI
        "rgb": "255, 45, 85",
        "fill_opacity": 0.45,
        "radius_multiplier": 3.0
    },
    "PREPARE": {
        "hex": "#ff6b35",      # Orange - matches UI
        "rgb": "255, 107, 53",
        "fill_opacity": 0.40,
        "radius_multiplier": 2.5
    },
    "MONITOR": {
        "hex": "#00d4ff",      # Blue - matches UI
        "rgb": "0, 212, 255",
        "fill_opacity": 0.35,
        "radius_multiplier": 2.0
    },
    "LOW": {
        "hex": "#94a3b8",      # Gray - matches UI
        "rgb": "148, 163, 184",
        "fill_opacity": 0.30,
        "radius_multiplier": 1.5
    }
}

# ══════════════════════════════════════════════════════════════════════
# DISTRICTS — North, South, East, West India (64+ districts)
# ══════════════════════════════════════════════════════════════════════
DISTRICTS = [
    # ── North India ────────────────────────────────────────────────
    {"name": "New Delhi",   "state": "Delhi",   "lat": 28.6139, "lon": 77.2090},
    {"name": "North Delhi", "state": "Delhi",   "lat": 28.7041, "lon": 77.1025},
    {"name": "South Delhi", "state": "Delhi",   "lat": 28.5355, "lon": 77.2510},
    {"name": "East Delhi",  "state": "Delhi",   "lat": 28.6600, "lon": 77.3010},
    {"name": "Gurugram",    "state": "Haryana", "lat": 28.4595, "lon": 77.0266},
    {"name": "Faridabad",   "state": "Haryana", "lat": 28.4089, "lon": 77.3178},
    {"name": "Rohtak",      "state": "Haryana", "lat": 28.8955, "lon": 76.6066},
    {"name": "Ambala",      "state": "Haryana", "lat": 30.3752, "lon": 76.7821},
    {"name": "Ludhiana",    "state": "Punjab",  "lat": 30.9010, "lon": 75.8573},
    {"name": "Amritsar",    "state": "Punjab",  "lat": 31.6340, "lon": 74.8723},
    {"name": "Jalandhar",   "state": "Punjab",  "lat": 31.3260, "lon": 75.5762},
    {"name": "Patiala",     "state": "Punjab",  "lat": 30.3398, "lon": 76.3869},
    {"name": "Lucknow",     "state": "UP",      "lat": 26.8467, "lon": 80.9462},
    {"name": "Kanpur",      "state": "UP",      "lat": 26.4499, "lon": 80.3319},
    {"name": "Agra",        "state": "UP",      "lat": 27.1767, "lon": 78.0081},
    {"name": "Varanasi",    "state": "UP",      "lat": 25.3176, "lon": 82.9739},
    # ── South India ────────────────────────────────────────────────
    {"name": "Bengaluru",   "state": "Karnataka", "lat": 12.9716, "lon": 77.5946},
    {"name": "Chennai",     "state": "Tamil Nadu", "lat": 13.0827, "lon": 80.2707},
    {"name": "Hyderabad",   "state": "Telangana", "lat": 17.3850, "lon": 78.4867},
    {"name": "Coimbatore",  "state": "Tamil Nadu", "lat": 11.0168, "lon": 76.9558},
    {"name": "Kochi",       "state": "Kerala",    "lat": 9.9312,  "lon": 76.2673},
    {"name": "Vijayawada",  "state": "AP",       "lat": 16.5062, "lon": 80.6480},
    {"name": "Visakhapatnam", "state": "AP",     "lat": 17.6868, "lon": 83.2185},
    {"name": "Mysuru",      "state": "Karnataka", "lat": 12.2958, "lon": 76.6394},
    {"name": "Thiruvananthapuram", "state": "Kerala", "lat": 8.5241, "lon": 76.9366},
    {"name": "Kozhikode",   "state": "Kerala",    "lat": 11.2588, "lon": 75.7804},
    {"name": "Madurai",     "state": "Tamil Nadu", "lat": 9.9252,  "lon": 78.1198},
    {"name": "Mangaluru",   "state": "Karnataka", "lat": 12.9141, "lon": 74.8560},
    # ── East India ──────────────────────────────────────────────────
    {"name": "Kolkata",     "state": "West Bengal", "lat": 22.5726, "lon": 88.3639},
    {"name": "Bhubaneswar", "state": "Odisha",     "lat": 20.2961, "lon": 85.8245},
    {"name": "Guwahati",    "state": "Assam",      "lat": 26.1445, "lon": 91.7362},
    {"name": "Ranchi",      "state": "Jharkhand",  "lat": 23.3441, "lon": 85.3096},
    {"name": "Patna",       "state": "Bihar",      "lat": 25.5941, "lon": 85.1376},
    {"name": "Siliguri",    "state": "West Bengal", "lat": 26.7208, "lon": 88.4286},
    {"name": "Howrah",      "state": "West Bengal", "lat": 22.5958, "lon": 88.2636},
    {"name": "Dhanbad",     "state": "Jharkhand",  "lat": 23.7928, "lon": 86.4329},
    {"name": "Jamshedpur",  "state": "Jharkhand",  "lat": 22.8046, "lon": 86.2029},
    {"name": "Cuttack",     "state": "Odisha",     "lat": 20.4625, "lon": 85.8830},
    {"name": "Dibrugarh",   "state": "Assam",      "lat": 27.4724, "lon": 94.9120},
    {"name": "Gangtok",     "state": "Sikkim",     "lat": 27.3389, "lon": 88.6065},
    {"name": "Imphal",      "state": "Manipur",    "lat": 24.8170, "lon": 93.9368},
    {"name": "Agartala",    "state": "Tripura",    "lat": 23.8315, "lon": 91.2868},
    {"name": "Aizawl",      "state": "Mizoram",    "lat": 23.7271, "lon": 92.7176},
    {"name": "Shillong",    "state": "Meghalaya",  "lat": 25.5788, "lon": 91.8933},
    # ── West India ──────────────────────────────────────────────────
    {"name": "Mumbai",      "state": "Maharashtra", "lat": 19.0760, "lon": 72.8777},
    {"name": "Pune",        "state": "Maharashtra", "lat": 18.5204, "lon": 73.8567},
    {"name": "Ahmedabad",   "state": "Gujarat",    "lat": 23.0225, "lon": 72.5714},
    {"name": "Surat",       "state": "Gujarat",    "lat": 21.1702, "lon": 72.8311},
    {"name": "Vadodara",    "state": "Gujarat",    "lat": 22.3072, "lon": 73.1812},
    {"name": "Nagpur",      "state": "Maharashtra", "lat": 21.1458, "lon": 79.0882},
    {"name": "Nashik",      "state": "Maharashtra", "lat": 19.9975, "lon": 73.7898},
    {"name": "Rajkot",      "state": "Gujarat",    "lat": 22.3039, "lon": 70.8022},
    {"name": "Indore",      "state": "MP",         "lat": 22.7196, "lon": 75.8577},
    {"name": "Bhopal",      "state": "MP",         "lat": 23.2599, "lon": 77.4126},
    {"name": "Jaipur",      "state": "Rajasthan",  "lat": 26.9124, "lon": 75.7873},
    {"name": "Jodhpur",     "state": "Rajasthan",  "lat": 26.2389, "lon": 73.0243},
    {"name": "Udaipur",     "state": "Rajasthan",  "lat": 24.5854, "lon": 73.7125},
    {"name": "Kota",        "state": "Rajasthan",  "lat": 25.2138, "lon": 75.8648},
    {"name": "Ajmer",       "state": "Rajasthan",  "lat": 26.4499, "lon": 74.6399},
    {"name": "Bikaner",     "state": "Rajasthan",  "lat": 28.0170, "lon": 73.3167},
]

# Dengue burden (loaded from uploaded PDF; persisted to dengue_burden.json)
DENGUE_BURDEN = {}
# DATA_DIR points at a persistent Railway Volume when mounted (set DATA_DIR
# env var to the mount path, e.g. /data). Falls back to the app folder for
# local runs where no volume is attached.
DATA_DIR = os.getenv("DATA_DIR", os.path.dirname(__file__))
os.makedirs(DATA_DIR, exist_ok=True)
DENGUE_BURDEN_FILE = os.path.join(DATA_DIR, "dengue_burden.json")
DENGUE_BURDEN_META_FILE = os.path.join(DATA_DIR, "dengue_burden_meta.json")

DENGUE_BURDEN_YEARS = ["2019", "2020", "2021", "2022", "2023", "2024", "2025"]
DENGUE_BURDEN_SOURCE = {"filename": None, "file_type": None, "uploaded_at": None}


def _load_dengue_burden_from_disk():
    global DENGUE_BURDEN, DENGUE_BURDEN_SOURCE
    try:
        if os.path.exists(DENGUE_BURDEN_FILE):
            with open(DENGUE_BURDEN_FILE, "r", encoding="utf-8") as f:
                DENGUE_BURDEN = json.load(f)
            log.info(f"✅ Loaded dengue burden data for {len(DENGUE_BURDEN)} states from disk")
    except Exception as e:
        log.error(f"❌ Failed to load dengue_burden.json: {e}")
    try:
        if os.path.exists(DENGUE_BURDEN_META_FILE):
            with open(DENGUE_BURDEN_META_FILE, "r", encoding="utf-8") as f:
                DENGUE_BURDEN_SOURCE = json.load(f)
    except Exception as e:
        log.error(f"❌ Failed to load dengue_burden_meta.json: {e}")


def _save_dengue_burden_to_disk():
    try:
        with open(DENGUE_BURDEN_FILE, "w", encoding="utf-8") as f:
            json.dump(DENGUE_BURDEN, f, indent=2)
    except Exception as e:
        log.error(f"❌ Failed to save dengue_burden.json: {e}")


def _save_dengue_burden_meta_to_disk():
    try:
        with open(DENGUE_BURDEN_META_FILE, "w", encoding="utf-8") as f:
            json.dump(DENGUE_BURDEN_SOURCE, f, indent=2)
    except Exception as e:
        log.error(f"❌ Failed to save dengue_burden_meta.json: {e}")


def _to_num(token):
    """Convert a table cell (str, int, float, or None) to int, or None if
    it's NR / blank / unparsable."""
    if token is None:
        return None
    if isinstance(token, (int, float)):
        return int(round(token))
    token = str(token).strip()
    if token.upper() in ("NR", "-", "—", ""):
        return None
    try:
        return int(round(float(token.replace(",", ""))))
    except ValueError:
        return None


DENGUE_SKIP_WORDS = {"sl", "no", "slno", "total", "affected", "states", "state", "uts", "ut", "c", "d",
                      "dengue", "cases", "deaths", "country", "since", "year"}


def _build_dengue_state_entry(sl_no, state_name, value_tokens):
    """Builds the per-state record from a Sl.No, State name, and the 14
    year/C,D value cells (2019..2025). Returns None if the row doesn't
    look like real data (header/footer/total row)."""
    state_name = (state_name or "").strip(" *")
    normalized = re.sub(r"[^a-z]", "", state_name.lower())
    if not state_name or normalized in DENGUE_SKIP_WORDS:
        return None
    if len(value_tokens) < 14:
        return None

    values = [_to_num(t) for t in value_tokens[:14]]
    if all(v is None for v in values):
        return None

    years = {}
    total_cases, total_deaths = 0, 0
    for i, year in enumerate(DENGUE_BURDEN_YEARS):
        cases = values[i * 2]
        deaths = values[i * 2 + 1]
        years[year] = {"cases": cases, "deaths": deaths}
        total_cases += cases or 0
        total_deaths += deaths or 0

    peak_year, peak_cases = None, -1
    for year, v in years.items():
        if (v["cases"] or 0) > peak_cases:
            peak_cases = v["cases"] or 0
            peak_year = year

    try:
        sl_no_int = int(sl_no) if sl_no not in (None, "") else None
    except (ValueError, TypeError):
        sl_no_int = None

    return {
        "name": state_name,
        "sl_no": sl_no_int,
        "years": years,
        "total_cases": total_cases,
        "total_deaths": total_deaths,
        "peak_year": int(peak_year) if peak_year else None,
        "peak_cases": peak_cases if peak_cases >= 0 else 0,
        "y2025_cases": years["2025"]["cases"] or 0,
        "y2025_deaths": years["2025"]["deaths"] or 0,
    }


def parse_dengue_burden_pdf(file_stream):
    """
    Parses the GOI/NVBDCP 'Dengue Cases and Deaths in the Country since 2019'
    table from a PDF. Expected row shape:
        <Sl.No> <State/UT name (can be multi-word)> <C D> x7   (2019..2025)
    i.e. 2 + 14 numeric tokens per state row (NR allowed in place of a number).
    Returns a dict keyed by state name.
    """
    if not PDF_SUPPORT:
        raise RuntimeError("pdfplumber is not installed — run: pip install pdfplumber")

    states = {}

    with pdfplumber.open(file_stream) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ""
            for raw_line in text.split("\n"):
                line = raw_line.strip()
                if not line:
                    continue

                m = re.match(r"^(\d{1,2})\s+(.+)$", line)
                if not m:
                    continue
                sl_no, rest = m.group(1), m.group(2)

                tokens = rest.split()
                if len(tokens) < 15:
                    # not a full data row (e.g. wrapped header/footer line)
                    continue

                value_tokens = tokens[-14:]
                name_tokens = tokens[:-14]
                state_name = " ".join(name_tokens)

                entry = _build_dengue_state_entry(sl_no, state_name, value_tokens)
                if entry:
                    states[entry["name"]] = entry

    if not states:
        raise ValueError(
            "No state rows could be parsed. Make sure the PDF matches the "
            "GOI/NVBDCP 'Dengue Cases and Deaths' table layout."
        )

    _classify_dengue_burden(states)
    return states


def _parse_dengue_burden_rows(rows):
    """
    Shared parser for already-tokenized tabular rows (from CSV or Excel).
    Each row is a list of cell values (str/int/float/None). Auto-detects
    whether a leading Sl.No column is present:
        - With Sl.No:    [Sl.No, State, C19, D19, C20, D20, ..., C25, D25]  (16 cols)
        - Without Sl.No: [State, C19, D19, C20, D20, ..., C25, D25]        (15 cols)
    Returns a dict keyed by state name.
    """
    states = {}

    for raw_row in rows:
        # Strip trailing empty cells, normalize to strings for inspection
        row = list(raw_row)
        while row and (row[-1] is None or str(row[-1]).strip() == ""):
            row.pop()
        if len(row) < 15:
            continue

        first_cell = row[0]
        first_is_int = False
        try:
            int(str(first_cell).strip())
            first_is_int = True
        except (ValueError, TypeError):
            pass

        second_cell_is_text = len(row) > 1 and isinstance(row[1], str) and not row[1].strip().replace(",", "").replace(".", "").replace("-", "").isdigit()

        if first_is_int and second_cell_is_text and len(row) >= 16:
            # Sl.No, State, <14 values>
            sl_no = row[0]
            state_name = str(row[1])
            value_tokens = row[2:16]
        elif isinstance(first_cell, str) and len(row) >= 15:
            # State, <14 values>  (no Sl.No column)
            sl_no = None
            state_name = str(first_cell)
            value_tokens = row[1:15]
        else:
            continue

        entry = _build_dengue_state_entry(sl_no, state_name, value_tokens)
        if entry:
            states[entry["name"]] = entry

    return states


def parse_dengue_burden_csv(file_stream):
    """Parses a CSV export of the GOI/NVBDCP dengue burden table."""
    raw_bytes = file_stream.read()
    try:
        text = raw_bytes.decode("utf-8-sig")
    except UnicodeDecodeError:
        text = raw_bytes.decode("latin-1")

    reader = csv.reader(io.StringIO(text))
    rows = list(reader)

    states = _parse_dengue_burden_rows(rows)
    if not states:
        raise ValueError(
            "No state rows could be parsed from the CSV. Expected columns: "
            "[Sl.No,] State, then Cases/Deaths pairs for 2019–2025 (14 value columns)."
        )

    _classify_dengue_burden(states)
    return states


def parse_dengue_burden_xlsx(file_stream):
    """Parses an Excel (.xlsx/.xls) export of the GOI/NVBDCP dengue burden table."""
    if not XLSX_SUPPORT:
        raise RuntimeError("openpyxl is not installed — run: pip install openpyxl")

    wb = openpyxl.load_workbook(file_stream, data_only=True, read_only=True)
    ws = wb.active

    rows = []
    for row_cells in ws.iter_rows(values_only=True):
        rows.append(list(row_cells))

    states = _parse_dengue_burden_rows(rows)
    if not states:
        raise ValueError(
            "No state rows could be parsed from the Excel file. Expected columns: "
            "[Sl.No,] State, then Cases/Deaths pairs for 2019–2025 (14 value columns)."
        )

    _classify_dengue_burden(states)
    return states


def _classify_dengue_burden(states):
    """Assigns LOW/MODERATE/HIGH/VERY HIGH + campaign boost % by quartile
    of total 7-year case burden across the uploaded states."""
    totals = sorted(s["total_cases"] for s in states.values())
    n = len(totals)
    if n == 0:
        return

    def pct(p):
        idx = min(int(n * p), n - 1)
        return totals[idx]

    q1, q2, q3 = pct(0.25), pct(0.50), pct(0.75)

    for s in states.values():
        t = s["total_cases"]
        if t >= q3:
            s["level"], s["boost"] = "VERY HIGH", "+30%"
        elif t >= q2:
            s["level"], s["boost"] = "HIGH", "+20%"
        elif t >= q1:
            s["level"], s["boost"] = "MODERATE", "+10%"
        else:
            s["level"], s["boost"] = "LOW", "+5%"


# News keywords
NEWS_KEYWORDS = {
    "weather_escalation": {
        "rainfall": 2.0, "heavy rain": 2.0, "yellow alert": 1.5,
        "red alert": 2.5, "monsoon": 1.8, "waterlogging": 2.0,
        "waterlogged": 1.8, "flood": 2.5, "cloudburst": 2.5,
        "thunderstorm": 1.5, "imd alert": 2.0,
    },
    "disease_risk": {
        "dengue": 3.0, "malaria": 2.5, "chikungunya": 2.0,
        "viral fever": 1.5, "outbreak": 3.0, "epidemic": 3.5,
        "cases rising": 2.5, "health alert": 2.0,
    },
    "mosquito_breeding": {
        "mosquito": 2.0, "mosquitoes": 2.0, "stagnant water": 2.5,
        "breeding": 2.0, "larvae": 2.0, "vector": 1.5,
        "fumigation": 1.5,
    },
}

GEO_KEYWORDS = [
    "delhi", "ncr", "gurugram", "gurgaon", "faridabad", "noida",
    "new delhi", "north delhi", "south delhi", "east delhi",
    "haryana", "punjab", "ludhiana", "amritsar", "jalandhar", "patiala",
    "rohtak", "ambala", "up", "uttar pradesh", "lucknow", "kanpur",
    "agra", "varanasi",
    "bengaluru", "bangalore", "chennai", "hyderabad", "coimbatore", "kochi",
    "vijayawada", "visakhapatnam", "mysuru", "mysore", "thiruvananthapuram",
    "trivandrum", "kozhikode", "calicut", "madurai", "mangaluru", "mangalore",
    "karnataka", "tamil nadu", "kerala", "andhra pradesh", "telangana",
    "kolkata", "calcutta", "bhubaneswar", "guwahati", "ranchi", "patna",
    "siliguri", "howrah", "dhanbad", "jamshedpur", "cuttack", "dibrugarh",
    "gangtok", "imphal", "agartala", "aizawl", "shillong",
    "west bengal", "odisha", "assam", "jharkhand", "bihar", "sikkim",
    "manipur", "tripura", "mizoram", "meghalaya",
    "mumbai", "bombay", "pune", "ahmedabad", "surat", "vadodara", "baroda",
    "nagpur", "nashik", "rajkot", "indore", "bhopal", "jaipur", "jodhpur",
    "udaipur", "kota", "ajmer", "bikaner", "maharashtra", "gujarat",
    "madhya pradesh", "rajasthan",
]

RSS_FEEDS = [
    "https://news.google.com/rss/search?q=dengue+mosquito+India&hl=en-IN&gl=IN&ceid=IN:en",
    "https://news.google.com/rss/search?q=rainfall+flood+waterlogging+India&hl=en-IN&gl=IN&ceid=IN:en",
    "https://news.google.com/rss/search?q=malaria+outbreak+India&hl=en-IN&gl=IN&ceid=IN:en",
    "https://news.google.com/rss/search?q=dengue+rainfall+Delhi+NCR+UP+Punjab+Haryana&hl=en-IN&gl=IN&ceid=IN:en",
    "https://news.google.com/rss/search?q=dengue+rainfall+Bengaluru+Chennai+Hyderabad&hl=en-IN&gl=IN&ceid=IN:en",
    "https://news.google.com/rss/search?q=dengue+rainfall+Kolkata+Bhubaneswar+Guwahati&hl=en-IN&gl=IN&ceid=IN:en",
    "https://news.google.com/rss/search?q=dengue+rainfall+Mumbai+Pune+Ahmedabad&hl=en-IN&gl=IN&ceid=IN:en",
]

# ══════════════════════════════════════════════════════════════════════
# CACHE
# ══════════════════════════════════════════════════════════════════════
class Cache:
    def __init__(self):
        self._store = {}
        self._lock = threading.RLock()

    def get(self, key):
        with self._lock:
            entry = self._store.get(key)
            if not entry:
                return None
            if time.time() > entry["expires"]:
                del self._store[key]
                return None
            return entry["value"]

    def set(self, key, value, ttl=600):
        with self._lock:
            self._store[key] = {
                "value": value,
                "expires": time.time() + ttl,
                "set_at": time.time(),
            }

    def delete(self, key):
        with self._lock:
            self._store.pop(key, None)

    def clear(self):
        with self._lock:
            self._store.clear()
            log.info("🧹 Cache cleared")

    def stats(self):
        with self._lock:
            now = time.time()
            active = [k for k, v in self._store.items() if v["expires"] > now]
            return {"total": len(self._store), "active": len(active), "keys": active}

cache = Cache()

# ── Single-flight fetch: prevents concurrent requests from each kicking off
# their own duplicate expensive fetch (e.g. weather/risk/contextual/chat
# routes all read weather_data — if they race while the cache is cold, that
# used to mean 3-4 SIMULTANEOUS full 60-district Open-Meteo fetches instead
# of 1, which is a big part of what triggered the 429 rate-limit storms. ──
_fetch_locks = {}
_fetch_locks_guard = threading.Lock()

def get_or_fetch(key, fetch_fn):
    cached = cache.get(key)
    if cached is not None:
        return cached
    with _fetch_locks_guard:
        lock = _fetch_locks.setdefault(key, threading.Lock())
    with lock:
        cached = cache.get(key)  # another thread may have just finished fetching
        if cached is not None:
            return cached
        return fetch_fn()


# ══════════════════════════════════════════════════════════════════════
# WEATHER SERVICE
# ══════════════════════════════════════════════════════════════════════
def weather_code_to_desc(code):
    table = {
        0: "Clear sky", 1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
        45: "Foggy", 48: "Icy fog",
        51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
        61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
        71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow",
        80: "Slight showers", 81: "Moderate showers", 82: "Violent showers",
        95: "Thunderstorm", 96: "Thunderstorm with hail", 99: "Thunderstorm heavy hail",
    }
    return table.get(code, f"Code {code}")


def _parse_district_weather(d, item):
    cur = item.get("current", {})
    daily = item.get("daily", {})
    hourly = item.get("hourly", {})

    temp = cur.get("temperature_2m", 0)
    feels = cur.get("apparent_temperature", temp)
    humidity = cur.get("relative_humidity_2m", 0)
    rainfall = cur.get("precipitation", 0)
    rain = cur.get("rain", 0)
    wind_spd = cur.get("wind_speed_10m", 0)
    wind_dir = cur.get("wind_direction_10m", 0)
    pressure = cur.get("surface_pressure", 0)
    wcode = cur.get("weather_code", 0)

    hourly_precip = hourly.get("precipitation", [])
    forecast_24h = round(sum(hourly_precip[:24]), 2) if hourly_precip else 0

    forecast_3d = []
    d_dates = daily.get("time", [])
    d_maxT = daily.get("temperature_2m_max", [])
    d_minT = daily.get("temperature_2m_min", [])
    d_rain = daily.get("precipitation_sum", [])
    d_precip = daily.get("precipitation_probability_max", [])
    for j in range(min(3, len(d_dates))):
        forecast_3d.append({
            "date": d_dates[j] if j < len(d_dates) else "",
            "temp_max": round(d_maxT[j], 1) if j < len(d_maxT) else 0,
            "temp_min": round(d_minT[j], 1) if j < len(d_minT) else 0,
            "rain_mm": round(d_rain[j], 2) if j < len(d_rain) else 0,
            "rain_prob": d_precip[j] if j < len(d_precip) else 0,
        })

    rain_score = min(rainfall / 2.0, 3.0)
    humidity_score = min(max((humidity - 60) / 10.0, 0), 2.5)
    temp_score = min(max((temp - 35) / 5.0, 0), 2.0)
    forecast_rain_score = min(forecast_24h / 10.0, 1.5)
    post_rain_score = 1.5 if (rainfall < 2 and forecast_24h > 10) else 0

    base_score = round(min(rain_score + humidity_score + temp_score + forecast_rain_score + post_rain_score, 10.0), 2)

    scores_map = {
        "Rainfall": rain_score,
        "Humidity": humidity_score,
        "Temperature": temp_score,
        "Forecast": forecast_rain_score,
    }
    driver = max(scores_map, key=scores_map.get)

    return {
        "name": d["name"],
        "state": d["state"],
        "lat": d["lat"],
        "lon": d["lon"],
        "temp": round(temp, 1),
        "feels_like": round(feels, 1),
        "humidity": round(humidity, 1),
        "rainfall": round(rainfall, 2),
        "rain": round(rain, 2),
        "wind_speed": round(wind_spd, 1),
        "wind_dir": round(wind_dir, 0),
        "pressure": round(pressure, 1),
        "weather_code": wcode,
        "weather_desc": weather_code_to_desc(wcode),
        "forecast_24h_rain": forecast_24h,
        "forecast_3d": forecast_3d,
        "risk_score": base_score,
        "driver": driver,
        "triggers": {
            "rain": rainfall >= CONFIG["THRESH_RAIN_MM"],
            "humidity": humidity >= CONFIG["THRESH_HUMIDITY_PCT"],
            "temp": temp >= CONFIG["THRESH_TEMP_C"],
        },
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


def _fetch_single_district(d, retries=3, backoff=2):
    """Weather for ONE district — used only as a fallback when that
    district's whole batch request fails."""
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={d['lat']}&longitude={d['lon']}"
        "&current=temperature_2m,relative_humidity_2m,precipitation,"
        "rain,weather_code,wind_speed_10m,wind_direction_10m,"
        "surface_pressure,apparent_temperature"
        "&hourly=precipitation_probability,precipitation"
        "&daily=precipitation_sum,temperature_2m_max,temperature_2m_min,"
        "precipitation_probability_max"
        "&timezone=Asia/Kolkata"
        "&forecast_days=3"
    )
    for attempt in range(1, retries + 1):
        try:
            resp = HTTP.get(url, timeout=HTTP_TIMEOUT)
            resp.raise_for_status()
            raw = resp.json()
            if isinstance(raw, list):
                raw = raw[0]
            return _parse_district_weather(d, raw)
        except Exception as e:
            log.warning(f"⚠️ {d['name']} attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                time.sleep(_retry_sleep(e, attempt))
    log.error(f"❌ Weather failed for {d['name']} after {retries} attempts")
    return None


def _fetch_weather_chunk(chunk, retries=2, backoff=2):
    """Fetch current weather for a WHOLE CHUNK of districts in a single HTTP
    call, using Open-Meteo's comma-separated multi-location support. Returns
    a list of raw per-location dicts (same order as `chunk`), or None if the
    whole chunk failed."""
    lats = ",".join(str(d["lat"]) for d in chunk)
    lons = ",".join(str(d["lon"]) for d in chunk)
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={lats}&longitude={lons}"
        "&current=temperature_2m,relative_humidity_2m,precipitation,"
        "rain,weather_code,wind_speed_10m,wind_direction_10m,"
        "surface_pressure,apparent_temperature"
        "&hourly=precipitation_probability,precipitation"
        "&daily=precipitation_sum,temperature_2m_max,temperature_2m_min,"
        "precipitation_probability_max"
        "&timezone=Asia/Kolkata"
        "&forecast_days=3"
    )
    for attempt in range(1, retries + 1):
        try:
            resp = HTTP.get(url, timeout=HTTP_TIMEOUT)
            resp.raise_for_status()
            raw = resp.json()
            if isinstance(raw, dict):
                raw = [raw]
            if len(raw) == len(chunk):
                return raw
            log.warning(f"⚠️ Weather chunk size mismatch: got {len(raw)}, expected {len(chunk)}")
            return None
        except Exception as e:
            log.warning(f"⚠️ Weather chunk attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                time.sleep(_retry_sleep(e, attempt))
    return None


def fetch_weather():
    """Current weather for all districts — Open-Meteo.

    Uses BATCHED multi-location requests (chunks of ~15 districts per HTTP
    call) instead of one request per district — for 60 districts that's ~4
    requests fired in parallel instead of 60, which is what actually cuts
    the wait time down. If a whole chunk fails, falls back to fetching just
    that chunk's districts individually so one bad batch doesn't take out
    15 districts at once, and any district that still fails falls back to
    its last-known-good cached reading.
    """
    prev = cache.get("weather_all") or {}
    prev_by_name = {x["name"]: x for x in prev.get("districts", [])}

    results = []
    failed = []
    stale_used = []

    CHUNK_SIZE = 56
    chunks = list(_chunked(DISTRICTS, CHUNK_SIZE))

    # Throttled: only a couple of chunk requests in flight at once, with a
    # short stagger between launches, instead of firing every chunk in the
    # same instant. Open-Meteo's 429s were driven by burst concurrency, not
    # total volume — this keeps the burst small.
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_to_chunk = {}
        for i, c in enumerate(chunks):
            if i > 0:
                time.sleep(1.5)
            future_to_chunk[executor.submit(_fetch_weather_chunk, c)] = c
        for future in as_completed(future_to_chunk):
            chunk = future_to_chunk[future]
            try:
                raw_list = future.result()
            except Exception as e:
                log.error(f"❌ Weather chunk worker crashed: {e}")
                raw_list = None

            if raw_list is None:
                # Don't hammer individually anymore — if the batch got 429'd,
                # the whole IP is rate-limited right now, so per-district
                # retries just add more requests into the same wall. Fall
                # straight back to last-known-good cache for this chunk and
                # let the next scheduled cycle try again.
                log.warning(f"⚠️ Weather chunk failed (rate limited) — using stale cache for {len(chunk)} districts")
                for d in chunk:
                    if d["name"] in prev_by_name:
                        results.append(prev_by_name[d["name"]])
                        stale_used.append(d["name"])
                    else:
                        failed.append(d["name"])
                continue

            for d, raw in zip(chunk, raw_list):
                parsed = None
                try:
                    parsed = _parse_district_weather(d, raw)
                except Exception as e:
                    log.warning(f"⚠️ Failed to parse weather for {d['name']}: {e}")
                if parsed:
                    results.append(parsed)
                elif d["name"] in prev_by_name:
                    results.append(prev_by_name[d["name"]])
                    stale_used.append(d["name"])
                else:
                    failed.append(d["name"])

    if not results:
        log.error("❌ Weather fetch failed: all districts returned empty")
        if prev:
            log.info("⚠️ Serving fully stale weather cache")
            return prev
        return {"error": "All district fetches failed", "districts": []}

    if failed:
        log.warning(f"⚠️ Weather missing for: {failed}")
    if stale_used:
        log.warning(f"⚠️ Weather using last-known-good (stale) data for: {stale_used}")

    payload = {
        "districts": results,
        "total": len(results),
        "failed": failed,
        "stale": stale_used,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "source": "Open-Meteo",
    }
    cache.set("weather_all", payload, ttl=CONFIG["WEATHER_REFRESH_SEC"])
    log.info(f"✅ Weather fetched for {len(results)}/{len(DISTRICTS)} districts" +
             (f" | live-failed: {failed}" if failed else "") +
             (f" | stale: {stale_used}" if stale_used else ""))
    return payload


def _imd_rain_category(mm):
    """IMD daily rainfall classification (mm/24h)."""
    if mm < 2.5:
        return "No Rain"
    if mm < 15.6:
        return "Light"
    if mm < 64.5:
        return "Moderate"
    if mm < 115.6:
        return "Heavy"
    if mm < 204.5:
        return "Very Heavy"
    return "Extremely Heavy"


def _forecast_confidence(day_index):
    """Open-Meteo's model skill drops off sharply for convective/monsoon
    rainfall beyond ~day 3 — flag later days as lower confidence."""
    if day_index <= 2:
        return "High"
    if day_index <= 4:
        return "Moderate"
    return "Low"


def _chunked(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def _parse_rainfall_forecast_item(d, raw):
    """Turn one location's raw Open-Meteo /v1/forecast response into our
    7-day rainfall forecast shape. Shared by both the batched multi-location
    fetch and the single-district fallback fetch."""
    daily = raw.get("daily", {})
    dates = daily.get("time", [])
    rain_mm = daily.get("precipitation_sum", [])
    rain_prob = daily.get("precipitation_probability_max", [])
    tmax = daily.get("temperature_2m_max", [])
    tmin = daily.get("temperature_2m_min", [])

    days = []
    for j in range(len(dates)):
        mm = round(rain_mm[j], 1) if j < len(rain_mm) and rain_mm[j] is not None else 0.0
        confidence = _forecast_confidence(j)
        category = _imd_rain_category(mm)
        # Flag as needing verification when a far-out day predicts very heavy+ rain —
        # convective/monsoon totals that far ahead carry real model uncertainty.
        extreme_flag = confidence != "High" and category in ("Very Heavy", "Extremely Heavy")
        days.append({
            "date": dates[j],
            "rain_mm": mm,
            "rain_probability": rain_prob[j] if j < len(rain_prob) else 0,
            "temp_max": round(tmax[j], 1) if j < len(tmax) else None,
            "temp_min": round(tmin[j], 1) if j < len(tmin) else None,
            "category": category,
            "confidence": confidence,
            "extreme_flag": extreme_flag,
        })
    if not days:
        return None

    total_7d = round(sum(x["rain_mm"] for x in days), 1)
    heaviest = max(days, key=lambda x: x["rain_mm"]) if days else None
    has_extreme_flag = any(x["extreme_flag"] for x in days)

    # "Reliable" heaviest day — the single biggest predicted rainfall day,
    # restricted to High-confidence days only (the near-term window where
    # Open-Meteo's model skill is solid). This is what feeds the Contextual
    # Risk Score, since a Day-6/7 low-confidence model spike shouldn't drive
    # a campaign trigger the way a confirmed near-term heavy-rain day should.
    high_conf_days = [x for x in days if x["confidence"] == "High"]
    heaviest_reliable = max(high_conf_days, key=lambda x: x["rain_mm"]) if high_conf_days else heaviest

    return {
        "name": d["name"],
        "state": d["state"],
        "lat": d["lat"],
        "lon": d["lon"],
        "days": days,
        "total_7d_mm": total_7d,
        "heaviest_day": heaviest["date"] if heaviest else None,
        "heaviest_day_mm": heaviest["rain_mm"] if heaviest else 0,
        "has_extreme_flag": has_extreme_flag,
        "heaviest_reliable_day": heaviest_reliable["date"] if heaviest_reliable else None,
        "heaviest_reliable_mm": heaviest_reliable["rain_mm"] if heaviest_reliable else 0,
        "heaviest_reliable_confidence": heaviest_reliable["confidence"] if heaviest_reliable else None,
        "heaviest_reliable_probability": heaviest_reliable["rain_probability"] if heaviest_reliable else 0,
    }


def _fetch_single_rainfall_forecast(d, retries=3, backoff=2):
    """7-day daily rainfall (mm) forecast for ONE district — used only as a
    fallback when that district's whole batch request fails."""
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={d['lat']}&longitude={d['lon']}"
        "&daily=precipitation_sum,precipitation_probability_max,temperature_2m_max,temperature_2m_min"
        "&timezone=Asia/Kolkata"
        "&forecast_days=7"
    )
    for attempt in range(1, retries + 1):
        exc = None
        try:
            resp = HTTP.get(url, timeout=HTTP_TIMEOUT)
            resp.raise_for_status()
            raw = resp.json()
            if isinstance(raw, list):
                raw = raw[0]
            parsed = _parse_rainfall_forecast_item(d, raw)
            if parsed:
                return parsed
        except Exception as e:
            exc = e
            log.warning(f"⚠️ Rainfall forecast {d['name']} attempt {attempt}/{retries} failed: {e}")
        if attempt < retries:
            time.sleep(_retry_sleep(exc, attempt) if exc else backoff * attempt)
    log.error(f"❌ Rainfall forecast failed for {d['name']} after {retries} attempts")
    return None


def _fetch_rainfall_forecast_chunk(chunk, retries=2, backoff=2):
    """Fetch 7-day rainfall for a WHOLE CHUNK of districts in a single HTTP
    call — Open-Meteo supports comma-separated multi-location queries, so
    e.g. 15 districts = 1 request instead of 15. Returns a list of raw
    per-location dicts (same order as `chunk`), or None if the whole chunk
    failed."""
    lats = ",".join(str(d["lat"]) for d in chunk)
    lons = ",".join(str(d["lon"]) for d in chunk)
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={lats}&longitude={lons}"
        "&daily=precipitation_sum,precipitation_probability_max,temperature_2m_max,temperature_2m_min"
        "&timezone=Asia/Kolkata"
        "&forecast_days=7"
    )
    for attempt in range(1, retries + 1):
        try:
            resp = HTTP.get(url, timeout=HTTP_TIMEOUT)
            resp.raise_for_status()
            raw = resp.json()
            if isinstance(raw, dict):
                raw = [raw]
            if len(raw) == len(chunk):
                return raw
            log.warning(f"⚠️ Rainfall forecast chunk size mismatch: got {len(raw)}, expected {len(chunk)}")
            return None
        except Exception as e:
            log.warning(f"⚠️ Rainfall forecast chunk attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                time.sleep(backoff * attempt)
    return None


def fetch_rainfall_forecast():
    """7-day rainfall forecast (mm) for all districts — Open-Meteo.

    Uses BATCHED multi-location requests (chunks of ~15 districts per HTTP
    call) instead of one request per district. For 60 districts that's ~4
    requests fired in parallel instead of 60 — this is what actually cuts
    the wait time down, not just retrying/parallelizing individual calls.
    If a whole chunk fails, we fall back to fetching just that chunk's
    districts individually so a single bad batch doesn't take out 15
    districts at once.
    """
    prev = cache.get("rainfall_forecast_all") or {}
    prev_by_name = {x["name"]: x for x in prev.get("districts", [])}

    results = []
    failed = []
    stale_used = []

    CHUNK_SIZE = 56
    chunks = list(_chunked(DISTRICTS, CHUNK_SIZE))

    # Throttled: only a couple of chunk requests in flight at once, with a
    # short stagger between launches, instead of firing every chunk in the
    # same instant. Open-Meteo's 429s were driven by burst concurrency, not
    # total volume — this keeps the burst small.
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_to_chunk = {}
        for i, c in enumerate(chunks):
            if i > 0:
                time.sleep(1.5)
            future_to_chunk[executor.submit(_fetch_rainfall_forecast_chunk, c)] = c
        for future in as_completed(future_to_chunk):
            chunk = future_to_chunk[future]
            try:
                raw_list = future.result()
            except Exception as e:
                log.error(f"❌ Rainfall forecast chunk worker crashed: {e}")
                raw_list = None

            if raw_list is None:
                # Don't hammer individually anymore — if the batch got 429'd,
                # the whole IP is rate-limited right now, so per-district
                # retries just add more requests into the same wall. Fall
                # straight back to last-known-good cache for this chunk and
                # let the next scheduled cycle try again.
                log.warning(f"⚠️ Rainfall forecast chunk failed (rate limited) — using stale cache for {len(chunk)} districts")
                for d in chunk:
                    if d["name"] in prev_by_name:
                        results.append(prev_by_name[d["name"]])
                        stale_used.append(d["name"])
                    else:
                        failed.append(d["name"])
                continue

            for d, raw in zip(chunk, raw_list):
                parsed = _parse_rainfall_forecast_item(d, raw)
                if parsed:
                    results.append(parsed)
                elif d["name"] in prev_by_name:
                    results.append(prev_by_name[d["name"]])
                    stale_used.append(d["name"])
                else:
                    failed.append(d["name"])

    if not results:
        log.error("❌ Rainfall forecast fetch failed: all districts returned empty")
        if prev:
            log.info("⚠️ Serving fully stale rainfall forecast cache")
            return prev
        return {"error": "All district fetches failed", "districts": []}

    if failed:
        log.warning(f"⚠️ Rainfall forecast missing for: {failed}")
    if stale_used:
        log.warning(f"⚠️ Rainfall forecast using last-known-good (stale) data for: {stale_used}")

    payload = {
        "districts": results,
        "total": len(results),
        "failed": failed,
        "stale": stale_used,
        "forecast_days": 7,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "source": "Open-Meteo",
    }
    cache.set("rainfall_forecast_all", payload, ttl=CONFIG["WEATHER_REFRESH_SEC"])
    log.info(f"✅ 7-day rainfall forecast fetched for {len(results)}/{len(DISTRICTS)} districts" +
             (f" | live-failed: {failed}" if failed else "") +
             (f" | stale: {stale_used}" if stale_used else ""))
    return payload


# ══════════════════════════════════════════════════════════════════════
# FORECAST VALIDATION ENGINE — historical backtest (past N weeks)
#
# Compares what Open-Meteo's model FORECASTED for a past date against what
# ACTUALLY happened (ERA5 reanalysis), for both rainfall and our contextual
# risk formula. This proves out the scoring model using real historical
# data — no fabricated numbers.
#
# NOTE ON THE NEWS COMPONENT: the live Contextual Risk Score is 40% news
# signal, but historical news articles were never stored, so they cannot be
# reconstructed for past dates. This backtest therefore scores a
# "Weather-Only Contextual Score" — Rainfall, Rainy Days, and Temperature
# only, with their weights redistributed proportionally (25/25/50 instead
# of 15/15/30). This is disclosed in the API response and must be labeled
# as such on the dashboard — do not present it as identical to the live
# 4-factor score.
# ══════════════════════════════════════════════════════════════════════
HIST_FORECAST_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"
HIST_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Weather-only weight redistribution (news excluded — see note above)
WO_RAIN_WEIGHT = 0.25
WO_PRECIP_WEIGHT = 0.25
WO_TEMP_WEIGHT = 0.50


def _weather_only_contextual_score(heaviest_day_mm, rainy_days, temp_c, flood_alert=False):
    rain_raw = _rainfall_raw(heaviest_day_mm)
    precip_raw = _rainy_days_raw(rainy_days, flood_alert=flood_alert)
    temp_raw = _temperature_raw(temp_c)
    score = round(
        rain_raw * WO_RAIN_WEIGHT + precip_raw * WO_PRECIP_WEIGHT + temp_raw * WO_TEMP_WEIGHT,
        2
    )
    return score, _contextual_trigger_state(score)


def _fetch_hist_chunk(url, chunk, start_date, end_date, extra_params=""):
    lats = ",".join(str(d["lat"]) for d in chunk)
    lons = ",".join(str(d["lon"]) for d in chunk)
    full_url = (
        f"{url}?latitude={lats}&longitude={lons}"
        f"&start_date={start_date}&end_date={end_date}"
        "&daily=precipitation_sum,temperature_2m_max,temperature_2m_min"
        f"&timezone=Asia/Kolkata{extra_params}"
    )
    try:
        resp = HTTP.get(full_url, timeout=HTTP_TIMEOUT)
        resp.raise_for_status()
        raw = resp.json()
        if isinstance(raw, dict):
            raw = [raw]
        if len(raw) == len(chunk):
            return raw
        log.warning(f"⚠️ Backtest chunk size mismatch for {url}: got {len(raw)}, expected {len(chunk)}")
        return None
    except Exception as e:
        log.warning(f"⚠️ Backtest fetch failed ({url}): {e}")
        return None


def _weekly_ranges(num_weeks=4, reference_date=None):
    """Returns a list of (label, start_date, end_date) tuples for the past
    N full weeks, oldest first, each week Mon-Sun, ending before today
    (today's data is incomplete so we don't include the current week)."""
    ref = reference_date or datetime.now(timezone.utc).date()
    this_monday = ref - timedelta(days=ref.weekday())
    weeks = []
    for i in range(num_weeks, 0, -1):
        week_start = this_monday - timedelta(weeks=i)
        week_end = week_start + timedelta(days=6)
        label = f"{week_start.strftime('%d %b')} – {week_end.strftime('%d %b')}"
        weeks.append((label, week_start.isoformat(), week_end.isoformat()))
    return weeks


def compute_forecast_accuracy(num_weeks=4, sample_size=20):
    """
    PRIMARY BACKTEST — pulls real historical forecast data (what the model
    predicted) and real historical actual/reanalysis data (what happened)
    for the past `num_weeks` full weeks, for a representative sample of
    districts, and computes:
      - Rainfall forecast accuracy (predicted mm vs actual mm)
      - Weather-only Contextual Risk Score accuracy (predicted trigger
        state vs actual trigger state)
    Sampling a subset of districts (default 20 of 60+) keeps this fast and
    within free-tier API rate limits; increase sample_size for a fuller run.
    """
    sample_districts = DISTRICTS[:sample_size] if sample_size < len(DISTRICTS) else DISTRICTS
    weeks = _weekly_ranges(num_weeks)
    CHUNK_SIZE = 15
    chunks = list(_chunked(sample_districts, CHUNK_SIZE))

    weekly_results = []

    for label, start_date, end_date in weeks:
        # Fetch FORECAST (what the model said would happen) and ACTUAL
        # (ERA5 reanalysis — what really happened) for every district chunk.
        forecast_by_name = {}
        actual_by_name = {}

        for chunk in chunks:
            fc_raw = _fetch_hist_chunk(HIST_FORECAST_URL, chunk, start_date, end_date)
            ac_raw = _fetch_hist_chunk(HIST_ARCHIVE_URL, chunk, start_date, end_date)
            if fc_raw:
                for d, raw in zip(chunk, fc_raw):
                    forecast_by_name[d["name"]] = raw
            if ac_raw:
                for d, raw in zip(chunk, ac_raw):
                    actual_by_name[d["name"]] = raw

        district_rows = []
        rainfall_errors_pct = []
        trigger_matches = 0
        trigger_total = 0

        for d in sample_districts:
            fc = forecast_by_name.get(d["name"])
            ac = actual_by_name.get(d["name"])
            if not fc or not ac:
                continue

            fc_daily = fc.get("daily", {})
            ac_daily = ac.get("daily", {})
            fc_rain_days = fc_daily.get("precipitation_sum", []) or []
            ac_rain_days = ac_daily.get("precipitation_sum", []) or []
            fc_tmax_days = fc_daily.get("temperature_2m_max", []) or []
            ac_tmax_days = ac_daily.get("temperature_2m_max", []) or []

            if not fc_rain_days or not ac_rain_days:
                continue

            fc_total_rain = round(sum(x for x in fc_rain_days if x is not None), 1)
            ac_total_rain = round(sum(x for x in ac_rain_days if x is not None), 1)
            fc_heaviest = round(max((x for x in fc_rain_days if x is not None), default=0), 1)
            ac_heaviest = round(max((x for x in ac_rain_days if x is not None), default=0), 1)
            fc_rainy_days = sum(1 for x in fc_rain_days if x and x > 0)
            ac_rainy_days = sum(1 for x in ac_rain_days if x and x > 0)
            fc_avg_temp = round(sum(x for x in fc_tmax_days if x is not None) / max(len(fc_tmax_days), 1), 1)
            ac_avg_temp = round(sum(x for x in ac_tmax_days if x is not None) / max(len(ac_tmax_days), 1), 1)

            # Rainfall accuracy: error as % of actual (capped so a near-zero
            # actual with a near-zero forecast doesn't blow up the %).
            denom = max(ac_total_rain, 5.0)
            rain_error_pct = min(abs(fc_total_rain - ac_total_rain) / denom * 100, 100)
            rainfall_errors_pct.append(rain_error_pct)

            fc_score, fc_trigger = _weather_only_contextual_score(fc_heaviest, fc_rainy_days, fc_avg_temp)
            ac_score, ac_trigger = _weather_only_contextual_score(ac_heaviest, ac_rainy_days, ac_avg_temp)

            trigger_total += 1
            if fc_trigger == ac_trigger:
                trigger_matches += 1

            district_rows.append({
                "name": d["name"],
                "state": d["state"],
                "predicted_rainfall_mm": fc_total_rain,
                "actual_rainfall_mm": ac_total_rain,
                "rainfall_error_pct": round(rain_error_pct, 1),
                "predicted_score": fc_score,
                "actual_score": ac_score,
                "predicted_trigger": fc_trigger,
                "actual_trigger": ac_trigger,
                "trigger_match": fc_trigger == ac_trigger,
            })

        avg_rain_accuracy = round(100 - (sum(rainfall_errors_pct) / len(rainfall_errors_pct)), 1) if rainfall_errors_pct else None
        trigger_accuracy = round((trigger_matches / trigger_total) * 100, 1) if trigger_total else None

        weekly_results.append({
            "week_label": label,
            "start_date": start_date,
            "end_date": end_date,
            "districts_evaluated": len(district_rows),
            "rainfall_forecast_accuracy_pct": avg_rain_accuracy,
            "contextual_score_accuracy_pct": trigger_accuracy,
            "districts": district_rows,
        })

    overall_rain = [w["rainfall_forecast_accuracy_pct"] for w in weekly_results if w["rainfall_forecast_accuracy_pct"] is not None]
    overall_trigger = [w["contextual_score_accuracy_pct"] for w in weekly_results if w["contextual_score_accuracy_pct"] is not None]

    return {
        "weeks": weekly_results,
        "overall_rainfall_accuracy_pct": round(sum(overall_rain) / len(overall_rain), 1) if overall_rain else None,
        "overall_contextual_accuracy_pct": round(sum(overall_trigger) / len(overall_trigger), 1) if overall_trigger else None,
        "sample_size": len(sample_districts),
        "total_districts": len(DISTRICTS),
        "methodology": (
            "Rainfall accuracy compares Open-Meteo's archived forecast for each "
            "past week against ERA5 reanalysis actuals for the same week. "
            "Contextual Score accuracy compares predicted vs actual trigger "
            "state (BOOST/PREPARE/MONITOR/LOW) using a Weather-Only version of "
            "the scoring formula (Rainfall 25% + Rainy Days 25% + Temperature "
            "50%) — the live News component (40% weight) is excluded here "
            "because historical news signal data was not being stored yet, "
            "so it cannot be reconstructed for past weeks."
        ),
        "computed_at": datetime.now(timezone.utc).isoformat(),
    }


def fetch_air_quality():
    lats = ",".join(str(d["lat"]) for d in DISTRICTS)
    lons = ",".join(str(d["lon"]) for d in DISTRICTS)
    url = (
        "https://air-quality-api.open-meteo.com/v1/air-quality"
        f"?latitude={lats}&longitude={lons}"
        "&current=pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone,us_aqi"
        "&timezone=Asia/Kolkata"
    )
    for attempt in range(1, 3):
        try:
            resp = HTTP.get(url, timeout=HTTP_TIMEOUT)
            resp.raise_for_status()
            break
        except Exception as e:
            log.warning(f"⚠️ Air quality attempt {attempt}/2 failed: {e}")
            if attempt < 2:
                time.sleep(2)
            else:
                log.error(f"❌ Air quality fetch failed: {e}")
                cached = cache.get("aq_all")
                return cached if cached else {"error": str(e), "districts": []}
    try:
        raw = resp.json()
        if isinstance(raw, dict):
            raw = [raw]
        results = []
        for i, item in enumerate(raw):
            d = DISTRICTS[i]
            cur = item.get("current", {})
            aqi = cur.get("us_aqi", 0)
            if aqi <= 50:
                aqi_level = "Good"
            elif aqi <= 100:
                aqi_level = "Moderate"
            elif aqi <= 150:
                aqi_level = "Unhealthy for Sensitive"
            elif aqi <= 200:
                aqi_level = "Unhealthy"
            elif aqi <= 300:
                aqi_level = "Very Unhealthy"
            else:
                aqi_level = "Hazardous"
            results.append({
                "name": d["name"],
                "state": d["state"],
                "pm2_5": round(cur.get("pm2_5", 0), 1),
                "pm10": round(cur.get("pm10", 0), 1),
                "co": round(cur.get("carbon_monoxide", 0), 1),
                "no2": round(cur.get("nitrogen_dioxide", 0), 1),
                "ozone": round(cur.get("ozone", 0), 1),
                "us_aqi": aqi,
                "aqi_level": aqi_level,
            })
        payload = {
            "districts": results,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "source": "Open-Meteo Air Quality API",
        }
        cache.set("aq_all", payload, ttl=CONFIG["AQ_REFRESH_SEC"])
        log.info(f"✅ Air quality fetched for {len(results)} districts")
        return payload
    except Exception as e:
        log.error(f"❌ Air quality fetch failed: {e}")
        cached = cache.get("aq_all")
        return cached if cached else {"error": str(e), "districts": []}


# ══════════════════════════════════════════════════════════════════════
# CONTEXTUAL RISK SCORING — PRIMARY RISK MODEL
# Thresholds: LOW 0-2.5 | MONITOR 2.5-5.0 | PREPARE 5.0-7.0 | BOOST 7.0-10.0
# ══════════════════════════════════════════════════════════════════════
def _rainfall_raw(mm):
    if mm < 10:
        return 0.0
    if mm < 25:
        return 2.5
    if mm < 50:
        return 5.0
    if mm <= 100:
        return 7.5
    return 10.0


def _rainy_days_raw(days, flood_alert=False):
    if flood_alert:
        return 10.0
    if days <= 1:
        return 0.0
    if days == 2:
        return 3.0
    if days <= 4:
        return 6.0
    if days == 5:
        return 8.0
    return 10.0


def _temperature_raw(temp_c):
    if temp_c < 20 or temp_c > 36:
        return 0.0
    if temp_c < 24:
        return 3.0
    if temp_c < 28:
        return 7.0
    if temp_c <= 32:
        return 10.0
    if temp_c <= 34:
        return 7.0
    return 3.0


def _news_raw(article_count):
    if article_count == 0:
        return 0.0
    if article_count == 1:
        return 2.5
    if article_count == 2:
        return 5.0
    if article_count <= 5:
        return 7.5
    return 10.0


def _contextual_trigger_state(score):
    if score < 2.5:
        return "LOW"
    if score < 5.0:
        return "MONITOR"
    if score < 7.0:
        return "PREPARE"
    return "BOOST"


def compute_contextual_risk(weather_payload, news_total_articles, news_articles=None, rainfall_forecast_payload=None):
    """
    PRIMARY RISK MODEL — Contextual Risk Score
    Weights: Rainfall (Heaviest Day) 15% | Rainy Days 15% | Temperature 30% | News 40%
    Returns per-district contextual risk score and trigger state.
    """
    districts = weather_payload.get("districts", [])

    # Lookup: district name -> heaviest forecast day (mm) across the full
    # 7-day window — simply the single day with the max predicted rainfall,
    # no confidence filtering. Falls back to the 3-day current+forecast
    # sum below if the 7-day rainfall forecast hasn't loaded yet.
    heaviest_day_lookup = {}
    if rainfall_forecast_payload:
        for rd in rainfall_forecast_payload.get("districts", []):
            heaviest_day_lookup[rd["name"]] = {
                "mm": rd.get("heaviest_day_mm", 0),
                "date": rd.get("heaviest_day"),
            }

    district_article_counts = {}
    if news_articles:
        for d in districts:
            name_lower = d["name"].lower()
            state_lower = d.get("state", "").lower()
            count = sum(
                1 for a in news_articles
                if name_lower in (a.get("title", "") + " ".join(a.get("geo_zones", []))).lower()
                or state_lower in " ".join(a.get("geo_zones", [])).lower()
            )
            district_article_counts[d["name"]] = count
    else:
        for d in districts:
            district_article_counts[d["name"]] = news_total_articles

    results = []
    for d in districts:
        district_articles = district_article_counts.get(d["name"], 0)

        heaviest_info = heaviest_day_lookup.get(d["name"])
        if heaviest_info is not None:
            heaviest_day_mm = round(heaviest_info["mm"], 2)
            heaviest_day_date = heaviest_info["date"]
        else:
            # Fallback: 7-day rainfall forecast not loaded yet — use current
            # conditions + 3-day weather forecast as a rough proxy.
            heaviest_day_mm = round(
                max(
                    [d.get("rainfall", 0)] +
                    [f.get("rain_mm", 0) for f in d.get("forecast_3d", [])]
                ),
                2
            )
            heaviest_day_date = None

        rainy_days = sum(1 for f in d.get("forecast_3d", []) if f.get("rain_mm", 0) > 0)
        if d.get("rainfall", 0) > 0:
            rainy_days += 1
        temp = d.get("temp", 0)
        flood = d.get("triggers", {}).get("rain", False)

        rain_raw = _rainfall_raw(heaviest_day_mm)
        precip_raw = _rainy_days_raw(rainy_days, flood_alert=flood)
        temp_raw = _temperature_raw(temp)
        news_raw = _news_raw(district_articles)

        rain_w = round(rain_raw * 0.15, 2)
        precip_w = round(precip_raw * 0.15, 2)
        temp_w = round(temp_raw * 0.30, 2)
        news_w = round(news_raw * 0.40, 2)

        ctx_score = round(rain_w + precip_w + temp_w + news_w, 2)
        trigger = _contextual_trigger_state(ctx_score)

        # Get color for map based on trigger state
        color_info = TRIGGER_COLORS.get(trigger, TRIGGER_COLORS["LOW"])

        results.append({
            "name": d["name"],
            "state": d["state"],
            "lat": d.get("lat"),
            "lon": d.get("lon"),
            "rainfall_heaviest_day_mm": heaviest_day_mm,
            "rainfall_heaviest_day_date": heaviest_day_date,
            "rainy_days_7d": rainy_days,
            "temp_c": temp,
            "news_articles": district_articles,
            "flood_alert": flood,
            "rainfall_raw": rain_raw,
            "rainfall_weighted": rain_w,
            "precip_raw": precip_raw,
            "precip_weighted": precip_w,
            "temp_raw": temp_raw,
            "temp_weighted": temp_w,
            "news_raw": news_raw,
            "news_weighted": news_w,
            "contextual_score": ctx_score,
            "trigger_state": trigger,
            # Map color information
            "map_color": color_info["hex"],
            "map_color_rgb": color_info["rgb"],
            "map_fill_opacity": color_info["fill_opacity"],
            "map_radius_multiplier": color_info["radius_multiplier"],
            # Additional fields for backward compatibility
            "score": ctx_score,
            "status": trigger,
            "risk": trigger,
            "driver": d.get("driver", "Weather"),
            "temp": d.get("temp", 0),
            "humidity": d.get("humidity", 0),
            "rainfall": d.get("rainfall", 0),
        })

    results.sort(key=lambda x: x["contextual_score"], reverse=True)

    counts = {"BOOST": 0, "PREPARE": 0, "MONITOR": 0, "LOW": 0}
    for r in results:
        counts[r["trigger_state"]] = counts.get(r["trigger_state"], 0) + 1

    return {
        "districts": results,
        "total": len(results),
        "counts": counts,
        "news_articles_used": news_total_articles,
        "computed_at": datetime.now(timezone.utc).isoformat(),
        "scoring_weights": {
            "rainfall_pct": 15,
            "rainy_days_pct": 15,
            "temperature_pct": 30,
            "news_pct": 40,
        },
        "trigger_thresholds": {
            "LOW": "0.0 – 2.5 · Passive only",
            "MONITOR": "2.5 – 5.0 · Internal alert",
            "PREPARE": "5.0 – 7.0 · Brief brand team",
            "BOOST": "7.0 – 10.0 · Deploy now",
        },
        "trigger_colors": {
            "LOW": {"hex": "#94a3b8", "label": "Gray"},
            "MONITOR": {"hex": "#00d4ff", "label": "Blue"},
            "PREPARE": {"hex": "#ff6b35", "label": "Orange"},
            "BOOST": {"hex": "#ff2d55", "label": "Red"},
        }
    }


# ══════════════════════════════════════════════════════════════════════
# NEWS SERVICE
# ══════════════════════════════════════════════════════════════════════
def _score_article(title, text, pub_raw, pub_fmt, link, source, feed_name, cutoff_48h, seen_hashes):
    if not title:
        return None
    title_hash = hashlib.md5(title.lower().encode()).hexdigest()
    if title_hash in seen_hashes:
        return None
    seen_hashes.add(title_hash)

    full_text = (title + " " + text).lower()
    kw_hits = {}
    total_score = 0.0
    for category, keywords in NEWS_KEYWORDS.items():
        for kw, weight in keywords.items():
            if kw in full_text:
                kw_hits[kw] = weight
                total_score += weight

    if total_score == 0:
        return None

    geo_found = [g for g in GEO_KEYWORDS if g in full_text]

    if total_score >= 7.0:
        severity = "CRITICAL"
    elif total_score >= 4.0:
        severity = "HIGH"
    elif total_score >= 2.0:
        severity = "MODERATE"
    else:
        severity = "LOW"

    return {
        "title": title,
        "link": link,
        "source": source,
        "published": pub_fmt,
        "keywords": list(kw_hits.keys()),
        "geo_zones": geo_found,
        "score": round(total_score, 2),
        "severity": severity,
        "feed": feed_name,
        "_pub_raw": pub_raw,
    }


def _build_news_payload(articles, newsdata_key):
    articles.sort(key=lambda x: x["score"], reverse=True)
    for a in articles:
        a.pop("_pub_raw", None)

    if articles:
        top_scores = [a["score"] for a in articles[:20]]
        signal_score = round(min(sum(top_scores) / (len(top_scores) * 5.0) * 10, 10.0), 2)
    else:
        signal_score = 0.0

    if signal_score >= 7.0:
        signal_level = "CRITICAL"
    elif signal_score >= 5.0:
        signal_level = "HIGH"
    elif signal_score >= 3.0:
        signal_level = "MODERATE"
    elif signal_score >= 1.0:
        signal_level = "LOW"
    else:
        signal_level = "NONE"

    all_keywords = {}
    for art in articles:
        for kw in art["keywords"]:
            all_keywords[kw] = all_keywords.get(kw, 0) + 1

    all_geo = {}
    for art in articles:
        for g in art["geo_zones"]:
            all_geo[g] = all_geo.get(g, 0) + 1

    severity_counts = {"CRITICAL": 0, "HIGH": 0, "MODERATE": 0, "LOW": 0}
    for art in articles:
        severity_counts[art["severity"]] = severity_counts.get(art["severity"], 0) + 1

    return {
        "articles": articles,
        "total": len(articles),
        "signal_score": signal_score,
        "signal_level": signal_level,
        "top_keywords": dict(sorted(all_keywords.items(), key=lambda x: x[1], reverse=True)[:15]),
        "geo_zones": dict(sorted(all_geo.items(), key=lambda x: x[1], reverse=True)),
        "severity_counts": severity_counts,
        "trigger_recommendation": f"{'ACTIVATE' if signal_level in ['HIGH','CRITICAL'] else 'MONITOR'} — {signal_level} news signal",
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "sources": ["Google News RSS", "NewsData.io"] if newsdata_key else ["Google News RSS"],
    }


def _parse_pub_date(pub_str):
    if not pub_str:
        return None
    pub_str = pub_str.strip()
    try:
        parsed = email.utils.parsedate_to_datetime(pub_str)
        return parsed.astimezone(timezone.utc)
    except Exception:
        pass
    try:
        return datetime.fromisoformat(pub_str).astimezone(timezone.utc)
    except Exception:
        pass
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(pub_str, fmt).replace(tzinfo=timezone.utc)
        except Exception:
            pass
    try:
        t = email.utils.parsedate(pub_str)
        if t:
            return datetime(*t[:6], tzinfo=timezone.utc)
    except Exception:
        pass
    return None


def _fetch_feed_safe(feed_url, timeout=None):
    """Fetch an RSS feed's raw bytes ourselves (via the resilient HTTP
    session, which has a real timeout) and hand them to feedparser to parse.

    feedparser.parse(url) does its OWN networking under the hood with NO
    timeout at all — if the remote server hangs, this call can block
    forever, which is what was freezing server startup at 'Fetching initial
    news feed...'. Fetching the bytes ourselves first fixes that."""
    try:
        resp = HTTP.get(feed_url, timeout=timeout or HTTP_TIMEOUT)
        resp.raise_for_status()
        return feedparser.parse(resp.content)
    except Exception as e:
        log.warning(f"⚠️ RSS feed fetch error ({feed_url[:60]}...): {e}")
        return feedparser.FeedParserDict(entries=[])


def fetch_rss_news():
    cutoff_48h = datetime.now(timezone.utc) - timedelta(hours=48)
    newsdata_key = CONFIG.get("NEWSDATA_API_KEY", "")
    cached = cache.get("news_feed") or {}
    existing = [a for a in cached.get("articles", []) if a.get("feed") == "NewsData.io"]
    seen_hashes = set(hashlib.md5(a["title"].lower().encode()).hexdigest() for a in existing)
    articles = list(existing)

    for feed_url in RSS_FEEDS:
        try:
            feed = _fetch_feed_safe(feed_url)
            for entry in feed.entries[:40]:
                title = entry.get("title", "").strip()
                link = entry.get("link", "")
                source = entry.get("source", {}).get("title", "Unknown")
                pub = entry.get("published", "")
                summary = entry.get("summary", "")
                pub_dt = _parse_pub_date(pub)
                if pub_dt is not None and pub_dt < cutoff_48h:
                    continue
                art = _score_article(title, summary, pub, pub[:10] if pub else "", link, source, "Google News RSS", cutoff_48h, seen_hashes)
                if art:
                    articles.append(art)
        except Exception as e:
            log.warning(f"⚠️ RSS feed error ({feed_url[:60]}...): {e}")

    payload = _build_news_payload(articles, newsdata_key)
    cache.set("news_feed", payload, ttl=CONFIG["NEWS_REFRESH_SEC"])
    log.info(f"✅ RSS news fetched: {len(articles)} total articles, signal={payload['signal_level']} ({payload['signal_score']}/10)")
    return payload


def fetch_newsdata():
    newsdata_key = CONFIG.get("NEWSDATA_API_KEY", "")
    if not newsdata_key:
        log.info("ℹ️ NewsData.io key not set — skipping")
        return cache.get("news_feed") or {}

    cutoff_48h = datetime.now(timezone.utc) - timedelta(hours=48)
    cached = cache.get("news_feed") or {}
    existing = [a for a in cached.get("articles", []) if a.get("feed") == "Google News RSS"]
    seen_hashes = set(hashlib.md5(a["title"].lower().encode()).hexdigest() for a in existing)
    articles = list(existing)

    newsdata_queries = [
        {"q": "dengue mosquito", "category": "health"},
        {"q": "rainfall flood monsoon", "category": "environment"},
        {"q": "malaria outbreak", "category": "health"},
        {"q": "dengue Bengaluru Chennai Hyderabad", "category": "health"},
        {"q": "dengue Kolkata Bhubaneswar Guwahati", "category": "health"},
        {"q": "dengue Mumbai Pune Ahmedabad", "category": "health"},
    ]
    for qobj in newsdata_queries:
        try:
            url = (
                f"https://newsdata.io/api/1/news"
                f"?apikey={newsdata_key}"
                f"&q={quote(qobj['q'])}"
                f"&country=in"
                f"&language=en"
                f"&category={qobj['category']}"
            )
            resp = requests.get(url, timeout=12)
            resp.raise_for_status()
            data = resp.json()
            for item in data.get("results", []):
                title = (item.get("title") or "").strip()
                link = item.get("link") or item.get("source_url") or ""
                source = (item.get("source_id") or item.get("source_name") or "NewsData").replace("-", " ").title()
                desc = item.get("description") or item.get("content") or ""
                raw_pub = item.get("pubDate") or ""
                pub_fmt = raw_pub[:10] if raw_pub else ""
                pub_dt = _parse_pub_date(raw_pub)
                if pub_dt is not None and pub_dt < cutoff_48h:
                    continue
                art = _score_article(title, desc, raw_pub, pub_fmt, link, source, "NewsData.io", cutoff_48h, seen_hashes)
                if art:
                    articles.append(art)
        except Exception as e:
            log.warning(f"⚠️ NewsData error ({qobj['q']}): {e}")

    payload = _build_news_payload(articles, newsdata_key)
    cache.set("news_feed", payload, ttl=CONFIG["NEWSDATA_REFRESH_SEC"])
    log.info(f"✅ NewsData fetched: {len(articles)} total articles, signal={payload['signal_level']} ({payload['signal_score']}/10)")
    return payload


def fetch_news():
    cutoff_48h = datetime.now(timezone.utc) - timedelta(hours=48)
    newsdata_key = CONFIG.get("NEWSDATA_API_KEY", "")
    seen_hashes = set()
    articles = []

    for feed_url in RSS_FEEDS:
        try:
            feed = _fetch_feed_safe(feed_url)
            for entry in feed.entries[:40]:
                title = entry.get("title", "").strip()
                link = entry.get("link", "")
                source = entry.get("source", {}).get("title", "Unknown")
                pub = entry.get("published", "")
                summary = entry.get("summary", "")
                pub_dt = _parse_pub_date(pub)
                if pub_dt is not None and pub_dt < cutoff_48h:
                    continue
                art = _score_article(title, summary, pub, pub[:10] if pub else "", link, source, "Google News RSS", cutoff_48h, seen_hashes)
                if art:
                    articles.append(art)
        except Exception as e:
            log.warning(f"⚠️ RSS feed error ({feed_url[:60]}...): {e}")

    if newsdata_key:
        newsdata_queries = [
            {"q": "dengue mosquito", "category": "health"},
            {"q": "rainfall flood monsoon", "category": "environment"},
            {"q": "malaria outbreak", "category": "health"},
            {"q": "dengue Bengaluru Chennai Hyderabad", "category": "health"},
            {"q": "dengue Kolkata Bhubaneswar Guwahati", "category": "health"},
            {"q": "dengue Mumbai Pune Ahmedabad", "category": "health"},
        ]
        for qobj in newsdata_queries:
            try:
                url = (
                    f"https://newsdata.io/api/1/news"
                    f"?apikey={newsdata_key}"
                    f"&q={quote(qobj['q'])}"
                    f"&country=in"
                    f"&language=en"
                    f"&category={qobj['category']}"
                )
                resp = requests.get(url, timeout=12)
                resp.raise_for_status()
                data = resp.json()
                for item in data.get("results", []):
                    title = (item.get("title") or "").strip()
                    link = item.get("link") or item.get("source_url") or ""
                    source = (item.get("source_id") or item.get("source_name") or "NewsData").replace("-", " ").title()
                    desc = item.get("description") or item.get("content") or ""
                    raw_pub = item.get("pubDate") or ""
                    pub_fmt = raw_pub[:10] if raw_pub else ""
                    pub_dt = _parse_pub_date(raw_pub)
                    if pub_dt is not None and pub_dt < cutoff_48h:
                        continue
                    art = _score_article(title, desc, raw_pub, pub_fmt, link, source, "NewsData.io", cutoff_48h, seen_hashes)
                    if art:
                        articles.append(art)
            except Exception as e:
                log.warning(f"⚠️ NewsData error ({qobj['q']}): {e}")

    payload = _build_news_payload(articles, newsdata_key)
    cache.set("news_feed", payload, ttl=CONFIG["NEWS_REFRESH_SEC"])
    log.info(f"✅ News fetched: {len(articles)} articles, signal={payload['signal_level']} ({payload['signal_score']}/10)")
    return payload


def get_news_signal_score():
    cached = cache.get("news_feed")
    if cached:
        return cached.get("signal_score", 0.0)
    return 0.0


# ══════════════════════════════════════════════════════════════════════
# AI SERVICE
# ══════════════════════════════════════════════════════════════════════
def call_groq(system_prompt, user_prompt, temperature=0.7, max_tokens=800):
    key = CONFIG.get("GROQ_API_KEY", "")
    if not key:
        raise ValueError("GROQ_API_KEY not set")
    resp = requests.post(
        "https://api.groq.com/openai/v1/chat/completions",
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        json={
            "model": "llama-3.3-70b-versatile",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"].strip()


def call_gemini(system_prompt, user_prompt, temperature=0.7, max_tokens=1000):
    key = CONFIG.get("GEMINI_API_KEY", "")
    if not key:
        raise ValueError("GEMINI_API_KEY not set")
    resp = requests.post(
        f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={key}",
        headers={"Content-Type": "application/json"},
        json={
            "system_instruction": {"parts": [{"text": system_prompt}]},
            "contents": [{"parts": [{"text": user_prompt}]}],
            "generationConfig": {"temperature": temperature, "maxOutputTokens": max_tokens},
        },
        timeout=35,
    )
    resp.raise_for_status()
    return resp.json()["candidates"][0]["content"]["parts"][0]["text"].strip()


def call_gemini_image(prompt, aspect_ratio="16:9"):
    """
    Generates an image via Gemini's native image model (gemini-3.1-flash-image-preview,
    aka "Nano Banana 2") using the SAME GEMINI_API_KEY already configured for text chat —
    same generateContent endpoint, different model, response contains inline base64 image
    data instead of text. Returns a data: URI string, or None on failure.

    Retries on 429 (rate limit) with backoff — this model has a tight per-minute
    quota, so a single retry after a short wait meaningfully improves success rate
    when the frontend is also spacing out its own calls.
    """
    key = CONFIG.get("GEMINI_API_KEY", "")
    if not key:
        raise ValueError("GEMINI_API_KEY not set")
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-3.1-flash-image-preview:generateContent?key={key}"
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "responseModalities": ["IMAGE"],
            "imageConfig": {"aspectRatio": aspect_ratio},
        },
    }
    last_error = None
    for attempt in range(1, 4):  # 1 initial try + 2 retries
        try:
            resp = requests.post(url, headers={"Content-Type": "application/json"}, json=body, timeout=60)
            if resp.status_code == 429 and attempt < 3:
                time.sleep(attempt * 12)  # 12s, then 24s — free-tier image quota needs real cooldown
                continue
            resp.raise_for_status()
            parts = resp.json()["candidates"][0]["content"]["parts"]
            for part in parts:
                inline = part.get("inlineData") or part.get("inline_data")
                if inline and inline.get("data"):
                    mime = inline.get("mimeType", "image/png")
                    return f"data:{mime};base64,{inline['data']}"
            return None
        except Exception as e:
            last_error = e
            if attempt >= 3:
                raise
    raise last_error


def fetch_unsplash_image(query, orientation="landscape"):
    """
    Fetches a real, licensed royalty-free photo from Unsplash for a given
    context (e.g. "Mumbai monsoon flooding street"). orientation='landscape'
    guarantees a rectangular (wide) image — never portrait/square — matching
    the report layout. Returns {'url', 'credit_name', 'credit_link'} or None.
    """
    key = CONFIG.get("UNSPLASH_ACCESS_KEY", "")
    if not key:
        return None
    try:
        resp = requests.get(
            "https://api.unsplash.com/search/photos",
            params={"query": query, "orientation": orientation, "per_page": 1, "content_filter": "high"},
            headers={"Authorization": f"Client-ID {key}"},
            timeout=15,
        )
        resp.raise_for_status()
        results = resp.json().get("results", [])
        if not results:
            return None
        photo = results[0]
        return {
            "url": photo["urls"]["regular"],  # already rectangular per orientation filter
            "credit_name": photo["user"]["name"],
            "credit_link": photo["user"]["links"]["html"] + "?utm_source=hit_radar&utm_medium=referral",
        }
    except Exception as e:
        log.warning(f"⚠️ Unsplash fetch failed for '{query}': {e}")
        return None


def call_mistral(system_prompt, user_prompt, temperature=0.7, max_tokens=800):
    key = CONFIG.get("MISTRAL_API_KEY", "")
    if not key:
        raise ValueError("MISTRAL_API_KEY not set")
    resp = requests.post(
        "https://api.mistral.ai/v1/chat/completions",
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        json={
            "model": "mistral-small-latest",
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"].strip()


def call_ai(system_prompt, user_prompt, prefer="groq", temperature=0.7, max_tokens=800):
    chain = (
        [call_groq, call_gemini, call_mistral] if prefer == "groq"
        else [call_gemini, call_groq, call_mistral]
    )
    last_error = None
    for fn in chain:
        try:
            return {"text": fn(system_prompt, user_prompt, temperature, max_tokens), "model": fn.__name__.replace("call_", "")}
        except Exception as e:
            log.warning(f"⚠️ AI call {fn.__name__} failed: {e}")
            last_error = e
            continue
    return {"text": f"All AI services unavailable: {last_error}", "model": "none", "error": True}


def generate_daily_summary(weather_data, news_data, date_str=None, contextual_data=None):
    system = (
        "You are Laren, HIT RADAR's senior intelligence analyst for India. "
        "Write a sharp, executive-level daily intelligence summary for media planners. "
        "Structure: 1) Situation Overview 2) Top District Recommendations "
        "3) News Signal Assessment 4) Next 24h Actions. "
        "Use natural, flowing language. Max 250 words. Professional tone."
    )
    districts = weather_data.get("districts", [])
    active = [d for d in districts if d.get("status") in ["ACTIVE", "BOOST", "CRITICAL"]]
    top_risk = sorted(districts, key=lambda x: x.get("risk_score", 0), reverse=True)[:5]
    news_articles = news_data.get("articles", [])[:5]
    top_headlines = [a["title"] for a in news_articles]

    ctx_info = ""
    if contextual_data:
        ctx_info = (
            f"Contextual Risk Score breakdown:\n"
            f"BOOST districts: {contextual_data.get('total_boost', 0)} — {', '.join(contextual_data.get('boost', [])[:5])}\n"
            f"PREPARE districts: {contextual_data.get('total_prepare', 0)} — {', '.join(contextual_data.get('prepare', [])[:5])}\n"
            f"MONITOR districts: {contextual_data.get('total_monitor', 0)} — {', '.join(contextual_data.get('monitor', [])[:5])}\n"
        )

    user = (
        f"Date: {date_str or datetime.now().strftime('%A, %d %B %Y')}\n"
        f"Total districts monitored: {len(districts)}\n"
        f"Active trigger districts: {len(active)} — {[d['name'] for d in active[:8]]}\n"
        f"Top risk districts: {[(d['name'], d.get('risk_score',0)) for d in top_risk]}\n"
        f"News signal: {news_data.get('signal_level')} ({news_data.get('signal_score')}/10)\n"
        f"Top news: {top_headlines}\n"
        f"Top keywords trending: {list(news_data.get('top_keywords', {}).keys())[:8]}\n"
        f"Geo zones active in news: {list(news_data.get('geo_zones', {}).keys())[:6]}\n"
        f"{ctx_info}"
        "Write the daily intelligence summary in natural, human language."
    )
    return call_ai(system, user, prefer="gemini")


def generate_news_narrative(news_data):
    system = (
        "You are HIT RADAR's news intelligence engine. "
        "Summarize the current news signal in exactly 2 sentences for media planners. "
        "Be specific about which keywords and geo zones are trending and what it means for campaign activation."
    )
    articles = news_data.get("articles", [])[:8]
    headlines = [a["title"] for a in articles]
    keywords = list(news_data.get("top_keywords", {}).keys())[:10]
    geo_zones = list(news_data.get("geo_zones", {}).keys())[:6]
    user = (
        f"Signal level: {news_data.get('signal_level')} ({news_data.get('signal_score')}/10)\n"
        f"Headlines: {headlines}\n"
        f"Keywords: {keywords}\n"
        f"Geo zones: {geo_zones}"
    )
    return call_ai(system, user, prefer="groq")


PLAN_REFERENCE_DISCLAIMER = (
    "⚠️ Reference only — this AI-generated output (including budget %, channels, "
    "timeline, and KPIs) is meant to help the Planning team quickly gauge current "
    "market/risk conditions. It is NOT a final plan. Do not execute as-is — final "
    "campaign decisions must be reviewed and approved by the Planning team."
)

DETAIL_INTENT_RE = re.compile(
    r'\b(detail(?:ed)?|in-?depth|elaborate|comprehensive|full\s+plan|complete\s+plan|'
    r'breakdown|thorough|deep\s?dive|extensive|full\s+report|detailed\s+report|'
    r'detailed\s+plan|step[- ]by[- ]step|campaign\s+plan)\b',
    re.IGNORECASE
)

CHART_INTENT_RE = re.compile(
    r'\b(chart|graph|plot|visuali[sz]e|visuali[sz]ation|pie\s*chart|bar\s*chart|'
    r'line\s*chart|trend\s*graph|diagram|draw.*chart|show.*trend)\b',
    re.IGNORECASE
)

# ── Explicit, code-level intent classification (NOT model-decided) ─────
# The model was previously asked to self-select MODE A/B/C/D purely from
# a persona-heavy, campaign-saturated system prompt. In practice that
# biases it toward campaign/media-strategy framing even for a plain
# "hi" or "what's the status today". These two regexes catch the two
# most common everyday intents in code, so we can tell the model exactly
# which mode to use instead of asking it to guess. This does NOT touch
# is_detailed / wants_chart / plan-generation logic — those stay exactly
# as they were.
CASUAL_INTENT_RE = re.compile(
    r'^\s*(hi|hii+|hello|hey|yo|sup|good\s?morning|good\s?afternoon|good\s?evening|'
    r'gm|gn|namaste|kaise\s?ho|kya\s?haal|how\s+are\s+you|thanks?|thank\s?you|'
    r'thnx|ty|ok(ay)?|cool|nice|great|bye|see\s?you|who\s+are\s+you|what\s+can\s+you\s+do)'
    r'\s*[!.?]*\s*$',
    re.IGNORECASE
)

STATUS_INTENT_RE = re.compile(
    r'\b(current\s+status|status\s+update|what.?s\s+the\s+status|live\s+status|'
    r'today.?s\s+risk|current\s+risk|risk\s+score|trigger\s+state|weather\s+(today|now|update)|'
    r'rainfall|humidity|temperature|dengue\s+(cases|burden|numbers)|news\s+signal|'
    r'top\s+keywords|geo\s+zones|dashboard\s+(state|data|numbers)|which\s+district(s)?\s+(is|are)|'
    r'how\s+many\s+district|show\s+me\s+the\s+data|whats?\s+happening|overview)\b',
    re.IGNORECASE
)


def ai_chat(message, history, live_context, geo_context=None, dashboard_context=None):
    weather = live_context.get("weather", {})
    news = live_context.get("news", {})

    # ── Pull live contextual risk scores (PRIMARY model) ──────────────
    risk_data = live_context.get("risk", {})
    risk_districts = risk_data.get("districts", [])

    # Sort districts by contextual score descending
    sorted_districts = sorted(
        risk_districts,
        key=lambda d: d.get("contextual_score", d.get("score", 0)),
        reverse=True
    )

    boost_districts   = [d for d in sorted_districts if d.get("trigger_state") == "BOOST"]
    prepare_districts = [d for d in sorted_districts if d.get("trigger_state") == "PREPARE"]
    monitor_districts = [d for d in sorted_districts if d.get("trigger_state") == "MONITOR"]

    def fmt_district(d):
        name  = d.get("name", "?")
        score = d.get("contextual_score", d.get("score", 0))
        temp  = d.get("temp_c", d.get("temp", "?"))
        hum   = d.get("humidity", "?")
        rain  = d.get("rainfall", d.get("rain", 0))
        driver = d.get("driver", "")
        return f"{name} [score={score}/10, temp={temp}°C, hum={hum}%, rain={rain}mm, driver={driver}]"

    boost_str   = "; ".join(fmt_district(d) for d in boost_districts[:6])   or "None"
    prepare_str = "; ".join(fmt_district(d) for d in prepare_districts[:6]) or "None"
    monitor_str = "; ".join(fmt_district(d) for d in monitor_districts[:4]) or "None"

    # ── Pull live news articles (top 10 headlines) ────────────────────
    articles = news.get("articles", [])[:10]
    news_headlines = "\n".join(
        f"  • [{a.get('severity','?').upper()}] {a.get('title','?')} ({a.get('source','?')})"
        for a in articles
    ) or "  No articles available"

    geo_zones   = list(news.get("geo_zones",   {}).keys())[:6]
    top_keywords = list(news.get("top_keywords", {}).keys())[:8]

    # ── Real channel/audience profiles (sent from the dashboard's GEO_PROFILES) ──
    geo_context = geo_context or {}
    geo_lines = []
    for name, info in list(geo_context.items())[:14]:
        channels = ", ".join(info.get("channels", []) or [])
        profile = info.get("profile", "")
        geo_lines.append(f"  • {name}: channels=[{channels}], audience=\"{profile}\"")
    geo_str = "\n".join(geo_lines) or "  No channel/audience profile data supplied for this turn"

    # ── Dengue burden data (real, uploaded PDF/CSV/Excel) ──────────────
    burden_lines = []
    for state, entry in list(DENGUE_BURDEN.items())[:20]:
        y_cases  = entry.get("y2025_cases", 0)
        y_deaths = entry.get("y2025_deaths", 0)
        peak_yr  = entry.get("peak_year", "?")
        peak_c   = entry.get("peak_cases", 0)
        burden_lines.append(
            f"  • {state}: 2025={y_cases} cases/{y_deaths} deaths | "
            f"peak year {peak_yr} ({peak_c} cases) | total {entry.get('total_cases', 0)} cases since 2019"
        )
    burden_str = "\n".join(burden_lines) or "  No dengue burden data uploaded yet"

    # ── Arbitrary current on-screen dashboard state, sent by the frontend ──
    # (whatever the user is currently looking at — filtered districts, rainfall
    # forecast, media recs, KPI cards etc.) so Laren can answer questions about
    # ANY data visible on the dashboard, not just weather/news/risk.
    dashboard_context = dashboard_context or {}
    try:
        dashboard_str = json.dumps(dashboard_context, default=str)[:4000]
    except Exception:
        dashboard_str = "{}"

    is_detailed = bool(DETAIL_INTENT_RE.search(message))
    wants_chart = bool(CHART_INTENT_RE.search(message))
    has_history = len(history) > 0

    # ── Code-level classification (only relevant when NOT detailed — the
    # detailed/plan-generation branch below is untouched) ─────────────
    is_casual = bool(CASUAL_INTENT_RE.search(message.strip()))
    is_status = bool(STATUS_INTENT_RE.search(message))
    if is_casual and not is_status:
        classified_mode = "MODE C — CASUAL CHAT"
    elif is_status and not is_casual:
        classified_mode = "MODE A — LIVE STATUS"
    elif wants_chart:
        classified_mode = "MODE D — CHART REQUEST"
    else:
        classified_mode = None  # ambiguous — let the model pick B/A/C itself

    if is_detailed:
        system = (
            "You are Laren, HIT RADAR's senior media strategy AI for India. "
            "You have REAL-TIME access to live contextual risk scores, weather, news, and REAL channel/audience profiles. "
            "The user asked for a DETAILED plan or report. Do not invent any number, channel, or audience segment "
            "that isn't grounded in the DATA sections below.\n\n"

            "RESPOND IN EXACTLY TWO PARTS, IN THIS ORDER:\n\n"

            "PART 1 — human-readable response. Use plain-text section headers relevant to the ask "
            "(e.g. District Summary, News Signal, Objective, Channel & Audience Plan, Budget Allocation, "
            "Creative Strategy, Timeline, KPIs, Action). Trigger states in CAPS (BOOST/PREPARE/MONITOR/LOW). "
            "Scores as X.X/10. Bullet lines start with →. Bold district names/numbers with **. "
            "Budget allocation must be expressed as PERCENTAGES grounded in relative risk scores — only state an "
            "absolute currency figure if the user gave a total budget in their message. "
            "Channels and audience descriptions must come only from the CHANNEL & AUDIENCE PROFILES section below — "
            "never invent a channel or audience segment for a district that isn't listed there. "
            "Keep this part under 500 words, no filler phrases.\n\n"

            "PART 2 — after the text, on its own line write exactly: ===PLAN_JSON===\n"
            "then a single raw JSON object (no markdown fence, no commentary before/after) with this schema:\n"
            '{"title": "string", "objective": "string", '
            '"executive_summary": {"business_objective": "string", "media_objective": "string", "jtbd": "string"}, '
            '"budget_allocation": [{"district": "string", "pct": number, "rationale": "string"}], '
            '"audience_architecture": [{"district": "string", "buyer_definition": "string", "barriers_motivators": "string", "touchpoints": ["string"]}], '
            '"channels": [{"district": "string", "channels": ["string"], "audience": "string"}], '
            '"funnel_strategy": {"awareness": ["string"], "consideration": ["string"], "conversion": ["string"]}, '
            '"creative_themes": ["string"], '
            '"creative_localization": [{"district": "string", "language": "string", "notes": "string"}], '
            '"timeline": [{"phase": "string", "activities": "string"}], '
            '"kpis": [{"metric": "string", "target": "string"}], '
            '"data_gaps": ["string"]}\n'
            "Only include districts in budget_allocation that are BOOST/PREPARE or explicitly named by the user. "
            "pct values across budget_allocation should sum to 100. If there's nothing meaningful for a field, use an empty array or empty object. "
            "executive_summary.business_objective and media_objective must be kept clearly separate (business outcome vs. media metric) — "
            "do not state a precise numeric business target (e.g. '+1.5% volume share') unless the user explicitly gave that number; "
            "otherwise phrase it directionally (e.g. 'grow volume share in high-risk districts'). "
            "audience_architecture.buyer_definition must be derived only from the CHANNEL & AUDIENCE PROFILES data below — "
            "never invent a persona that isn't grounded in that data. touchpoints must be real channels from that same data "
            "(OOH, radio, retail, digital), not invented ones. "
            "funnel_strategy channels must be a subset of the real channels already listed per district in CHANNEL & AUDIENCE PROFILES — "
            "group them into awareness/consideration/conversion by channel type (e.g. CTV/programmatic video = awareness, "
            "Meta/radio = consideration, retail media/search = conversion) — do not invent channels not present in the data. "
            "creative_localization.language should be the standard regional language for that district's state (e.g. Assamese for "
            "Guwahati/Dibrugarh, Hindi for Lucknow/Varanasi) — this is general knowledge, not live data, so keep it to language name only. "
            "data_gaps MUST list, in plain language, any standard media-planning inputs this plan could NOT include because the live "
            "data doesn't have them — always include at least: Category Development Index (CDI), competitor Share of Voice (SOV), "
            "channel CPM/rate-card benchmarks, and MMM/geo-lift attribution modeling, since none of these exist in the data sections "
            "below. Never fabricate numbers for these — only ever list them here as gaps requiring Planning team input. "
            "IMPORTANT: JSON string values must be PLAIN TEXT ONLY — never include markdown like ** or * or # inside any JSON field.\n\n"
        )
    else:
        system = (
            "You are Laren, HIT RADAR's media strategy AI for India. "
            "You are designed for WPP Media, HIT RADAR is their Project. "
            "If anyone is asking For any Doubt or enquiry or problem regarding this dashboard tell them contact sarthak.gunjal@wppmedia.com. "
            "You have REAL-TIME access to live contextual risk scores, weather, news, dengue burden data, and the "
            "current dashboard state. You are also a capable, knowledgeable general marketing/advertising assistant "
            "and a normal conversational AI — you are not limited to a rigid report format.\n\n"

            + (
                f"MESSAGE HAS ALREADY BEEN CLASSIFIED AS: {classified_mode}. "
                "Use that mode. Do NOT use campaign/report formatting (headers, →, budget, plan language) "
                "unless that classification is MODE A or MODE D and the live data genuinely calls for it. "
                "If classified CASUAL CHAT, reply in 1-2 short natural sentences ONLY — no headers, no bullet "
                "points, no district data, no campaign talk, even if district/news data is available below.\n\n"
                if classified_mode else ""
            ) +

            "FIRST, decide which MODE this message needs "
            + ("(classification above is a strong prior — only override it if the message clearly contradicts it):\n"
               if classified_mode else ":\n") +
            "MODE A — LIVE STATUS: the user is asking about current district risk, trigger states, weather, news, "
            "or dengue burden. Use ONLY the live data below, never fabricate a number, district, or headline. "
            "Follow the STRICT FORMAT RULES section.\n"
            "MODE B — GENERAL MARKETING / RECOMMENDATIONS: the user is asking for marketing/advertising strategy, "
            "campaign ideas, creative recommendations, media planning advice, or any question not tied to the live "
            "data above. Answer naturally and helpfully using your general marketing knowledge, like a sharp senior "
            "strategist would — no forced headers, no refusing for 'lack of data'. You may still reference live data "
            "if it's relevant to strengthen the recommendation.\n"
            "MODE C — CASUAL CHAT: greetings, small talk, or anything conversational. Just reply naturally and briefly, "
            "like a helpful colleague — no data dump, no forced format.\n"
            "MODE D — CHART REQUEST: the user explicitly asks for a chart/graph/plot/visualization. Give a short "
            "1-3 line answer, then append the CHART_JSON block described below, using only real numbers grounded in "
            "the data sections (live risk scores, dengue burden, rainfall, or dashboard snapshot).\n\n"

            "STRICT FORMAT RULES (MODE A only):\n"
            "1. If this is the first message in the conversation, or the user is asking for a general status/overview, "
            "structure your response with section headers: District Summary, News Signal, Action.\n"
            "2. If this is a FOLLOW-UP question in an ongoing conversation and the user is asking something specific "
            "(e.g. targeting, a single district, a narrow question) — do NOT repeat the full District Summary/News Signal "
            "block again. Answer the specific question directly in 2-4 lines, still using CAPS trigger states, **bold**, "
            "and X.X/10 score format inline where relevant.\n"
            "3. Always include the score as X.X/10 format\n"
            "4. Start action/recommendation lines with → symbol\n"
            "5. Use **bold** only for district names and key numbers\n"
            "6. Keep total response under 160 words — concise, professional, no filler phrases\n"
            "7. Never say 'based on the data' or 'as of now' — just state the facts directly\n\n"

            "EXAMPLE FORMAT (MODE A, first message / general overview only):\n"
            "District Summary\n"
            "**Lucknow** — BOOST 8.4/10 | 42°C, high humidity, active dengue news\n"
            "**Agra** — PREPARE 6.1/10 | 41°C, rainfall spike\n"
            "**Ambala** — MONITOR 3.8/10 | Pre-threshold\n\n"
            "News Signal\n"
            "Signal score: 5.7/10 | 24 articles | Keywords: dengue, mosquito, UP health\n\n"
            "Action\n"
            "→ Activate BOOST districts immediately. Pre-load creatives for PREPARE districts.\n"
            "---\n\n"

            "CHART_JSON BLOCK (MODE D only — omit entirely for MODE A/B/C):\n"
            "After your short text answer, on its own line write exactly: ===CHART_JSON===\n"
            "then a single raw JSON object (no markdown fence, no commentary before/after) with this schema:\n"
            '{"chart_type": "bar", "title": "string", "labels": ["string"], '
            '"datasets": [{"label": "string", "data": [number]}]}\n'
            'chart_type must be one of: "bar", "line", "pie", "doughnut", "radar". '
            "Only include numbers that come from the live data sections below or the dashboard snapshot — never invent values.\n\n"
        )

    system += (
        f"CONVERSATION STATE: {'follow-up message (history present)' if has_history else 'first message in this session'}\n\n"
        f"CHANNEL & AUDIENCE PROFILES (real, from dashboard — use only these, never invent):\n{geo_str}\n\n"
        f"LIVE CONTEXTUAL RISK SCORES ({datetime.now().strftime('%H:%M IST')}):\n"
        f"BOOST (activate now): {boost_str}\n"
        f"PREPARE (ready): {prepare_str}\n"
        f"MONITOR (watch): {monitor_str}\n\n"

        f"LIVE NEWS SIGNAL:\n"
        f"Score: {news.get('signal_score','—')}/10 | Level: {news.get('signal_level','—')} | Articles: {news.get('total', 0)}\n"
        f"Keywords: {top_keywords}\n"
        f"Geo zones: {geo_zones}\n"
        f"Headlines:\n{news_headlines}\n\n"

        f"DENGUE BURDEN DATA (real, uploaded by the user — historical cases/deaths by state):\n{burden_str}\n\n"

        f"DASHBOARD SNAPSHOT (raw JSON of whatever is currently on-screen for the user — use this to answer "
        f"questions about any dashboard data not already covered above, e.g. rainfall forecast, media recs, KPI cards):\n"
        f"{dashboard_str}\n\n"

        "STRICT RULE: When answering about live district status, risk scores, news, or dengue burden (MODE A), "
        "answer only from the live data above — never fabricate a district status, score, news, channel, or budget "
        "figure. For general marketing advice, recommendations, or casual conversation (MODE B/C), you may use your "
        "own knowledge, but stay honest and don't present a general opinion as if it were live dashboard data."
    )

    messages = []
    for h in history[-10:]:
        messages.append({"role": h.get("role"), "content": h.get("content")})
    messages.append({"role": "user", "content": message})

    full_prompt = "\n".join([
        f"{m['role'].upper()}: {m['content']}" for m in messages
    ])

    # The detailed plan schema now includes 9+ sections (exec summary, budget,
    # audience architecture, funnel, localization, timeline, KPIs, data gaps,
    # plus the human-readable PART 1 text) — 1800 tokens was enough for the
    # old, smaller schema but truncates mid-JSON on this one, producing
    # invalid/unparseable JSON ("Unterminated string..."). Bumped to give the
    # model real headroom to finish the JSON object cleanly.
    max_tok = 4000 if is_detailed else (1200 if wants_chart else 800)
    temp = 0.5 if is_detailed else 0.6
    result = call_ai(system, full_prompt, prefer="groq", temperature=temp, max_tokens=max_tok)

    # ── Split off the structured plan JSON (detailed mode only) ──────
    plan = None
    text = result.get("text", "")
    if "===PLAN_JSON===" in text:
        human_part, _, json_part = text.partition("===PLAN_JSON===")
        text = human_part.strip()
        json_part = json_part.strip()
        # Strip accidental markdown fences the model might still add
        json_part = re.sub(r'^```(?:json)?\s*|\s*```$', '', json_part.strip())
        try:
            plan = json.loads(json_part)
        except Exception as e:
            log.warning(f"⚠️ Failed to parse Laren plan JSON (attempt 1): {e}")
            plan = None
            # Safety net: the model may have produced an unusually long plan
            # (many districts) that still overran the token budget. Retry once
            # with significantly more headroom before giving up.
            try:
                retry_result = call_ai(system, full_prompt, prefer="groq", temperature=temp, max_tokens=6500)
                retry_text = retry_result.get("text", "")
                if "===PLAN_JSON===" in retry_text:
                    retry_human, _, retry_json = retry_text.partition("===PLAN_JSON===")
                    retry_json = re.sub(r'^```(?:json)?\s*|\s*```$', '', retry_json.strip())
                    plan = json.loads(retry_json)
                    text = retry_human.strip()
                    result = retry_result
            except Exception as e2:
                log.warning(f"⚠️ Failed to parse Laren plan JSON (retry): {e2}")
                plan = None

        # Force the reference-only disclaimer in code — never rely on the
        # model to remember to include it, so it can't be dropped/hallucinated
        # away on any turn. Applied to BOTH the human-readable text and the
        # structured plan JSON (the latter is what PDF/docx export reads from).
        if PLAN_REFERENCE_DISCLAIMER not in text:
            text = f"{text}\n\n{PLAN_REFERENCE_DISCLAIMER}"
        if isinstance(plan, dict):
            plan["disclaimer"] = PLAN_REFERENCE_DISCLAIMER
            if not plan.get("data_gaps"):
                plan["data_gaps"] = [
                    "Category Development Index (CDI) — not available in live data",
                    "Competitor Share of Voice (SOV) — not available in live data",
                    "Channel CPM / rate-card benchmarks — not available in live data",
                    "MMM / geo-lift attribution modeling — requires dedicated statistical analysis, not generated here",
                ]

    # ── Split off the structured chart JSON (any mode, chart-intent) ─
    chart = None
    if "===CHART_JSON===" in text:
        human_part, _, json_part = text.partition("===CHART_JSON===")
        text = human_part.strip()
        json_part = json_part.strip()
        json_part = re.sub(r'^```(?:json)?\s*|\s*```$', '', json_part.strip())
        try:
            chart = json.loads(json_part)
            valid_types = {"bar", "line", "pie", "doughnut", "radar"}
            if not isinstance(chart, dict) or chart.get("chart_type") not in valid_types \
               or not chart.get("labels") or not chart.get("datasets"):
                log.warning("⚠️ Laren chart JSON failed shape validation")
                chart = None
        except Exception as e:
            log.warning(f"⚠️ Failed to parse Laren chart JSON: {e}")
            chart = None

    result["text"] = text
    result["plan"] = plan
    result["chart"] = chart
    result["is_detailed"] = is_detailed
    result["wants_chart"] = wants_chart
    return result


# ══════════════════════════════════════════════════════════════════════
# TELEGRAM SERVICE
# ══════════════════════════════════════════════════════════════════════
def send_telegram(text):
    token = CONFIG.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = CONFIG.get("TELEGRAM_CHAT_ID", "")
    if not token or not chat_id:
        return {"success": False, "error": "Telegram not configured"}

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    body = {"chat_id": chat_id, "text": text, "parse_mode": "HTML"}

    for attempt in range(1, 4):
        try:
            resp = requests.post(url, json=body, timeout=30)
            resp.raise_for_status()
            return {"success": True, "message_id": resp.json().get("result", {}).get("message_id")}
        except requests.exceptions.ConnectTimeout:
            log.warning(f"⚠️ Telegram connect timeout (attempt {attempt}/3)")
            if attempt < 3:
                time.sleep(5 * attempt)
        except requests.exceptions.ReadTimeout:
            log.warning(f"⚠️ Telegram read timeout (attempt {attempt}/3)")
            if attempt < 3:
                time.sleep(5 * attempt)
        except Exception as e:
            log.error(f"❌ Telegram send failed: {e}")
            return {"success": False, "error": str(e)}

    return {"success": False, "error": "Telegram unreachable after 3 attempts"}


def send_trigger_alert(district_data):
    d = district_data
    name = d.get("name", "Unknown")
    state = d.get("state", "")
    status = d.get("trigger_state", d.get("status", "LOW"))
    score = d.get("contextual_score", d.get("score", 0))
    temp = d.get("temp_c", d.get("temp", 0))
    hum = d.get("humidity", 0)
    rain = d.get("rainfall", 0)

    status_emoji = {
        "BOOST": "🔴", "PREPARE": "🟡", "MONITOR": "🔵", "LOW": "🟢"
    }.get(status, "⚪")

    now = datetime.now().strftime("%d %b %Y, %H:%M IST")
    text = (
        f"{status_emoji} <b>HIT RADAR TRIGGER ALERT</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📍 <b>{name}</b>, {state}\n"
        f"🕐 {now}\n\n"
        f"<b>Status:</b> {status}\n"
        f"📊 <b>Contextual Risk Score:</b> {score}/10\n\n"
        f"<b>Live Weather:</b>\n"
        f"🌡 Temp: {temp}°C\n"
        f"💧 Humidity: {hum}%\n"
        f"🌧 Rainfall: {rain}mm\n\n"
        f"<i>HIT RADAR v3.0.0 — HIT FIK Operations</i>"
    )
    return send_telegram(text)


# ══════════════════════════════════════════════════════════════════════
# SCHEDULED REFRESH
# ══════════════════════════════════════════════════════════════════════
def scheduled_weather_refresh():
    log.info("⏱ Scheduler: refreshing weather...")
    fetch_weather()


def scheduled_rainfall_forecast_refresh():
    log.info("⏱ Scheduler: refreshing 7-day rainfall forecast...")
    fetch_rainfall_forecast()


def scheduled_news_refresh():
    log.info("⏱ Scheduler: refreshing RSS news...")
    fetch_rss_news()


def scheduled_newsdata_refresh():
    log.info("⏱ Scheduler: refreshing NewsData.io...")
    fetch_newsdata()


def scheduled_aq_refresh():
    log.info("⏱ Scheduler: refreshing air quality...")
    fetch_air_quality()


# ══════════════════════════════════════════════════════════════════════
# FLASK APP & ROUTES
# ══════════════════════════════════════════════════════════════════════
app = Flask(__name__)
CORS(app, origins=["*"])

# ══════════════════════════════════════════════════════════════════════
# USER AUTH — backed by editable users.json (add/remove users by hand)
# ══════════════════════════════════════════════════════════════════════
USERS_FILE = os.path.join(DATA_DIR, "users.json")
_SEED_USERS_FILE = os.path.join(os.path.dirname(__file__), "users.json")
# Always sync from the repo's users.json on every deploy/restart, so editing
# users.json in GitHub and pushing is enough to update logins — no need to
# touch the Railway volume/console directly.
if os.path.exists(_SEED_USERS_FILE):
    try:
        import shutil
        shutil.copy(_SEED_USERS_FILE, USERS_FILE)
        log.info(f"Synced {USERS_FILE} from repo users.json")
    except Exception as e:
        log.error(f"Failed to sync users.json into DATA_DIR: {e}")


def load_users():
    """Reload users.json fresh on every call so manual edits take effect
    immediately without restarting the server."""
    try:
        with open(USERS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        users = data.get("users", [])
        # basic shape validation, skip malformed entries instead of crashing
        clean = []
        for u in users:
            if isinstance(u, dict) and u.get("login_id") and u.get("password"):
                clean.append({
                    "name": u.get("name", "User"),
                    "login_id": u["login_id"].strip().lower(),
                    "password": u["password"],
                    "role": u.get("role", "Analyst"),
                })
        return clean
    except FileNotFoundError:
        log.warning(f"⚠️ {USERS_FILE} not found — no users can log in until it is created")
        return []
    except Exception as e:
        log.error(f"❌ Failed to read users.json: {e}")
        return []


@app.route("/api/auth/login", methods=["POST"])
def api_auth_login():
    payload = request.json or {}
    login_id = str(payload.get("login_id") or payload.get("email") or "").strip().lower()
    password = str(payload.get("password") or "")

    if not login_id or not password:
        return jsonify({"success": False, "error": "Login ID and password are required"}), 400

    users = load_users()
    match = next((u for u in users if u["login_id"] == login_id), None)

    if not match or match["password"] != password:
        return jsonify({"success": False, "error": "Invalid login ID or password"}), 401

    return jsonify({
        "success": True,
        "user": {
            "name": match["name"],
            "login_id": match["login_id"],
            "role": match["role"],
        },
    })


@app.route("/api/auth/users", methods=["GET"])
def api_auth_users():
    """Lists users for admin reference — passwords are never returned."""
    users = load_users()
    return jsonify({
        "users": [{"name": u["name"], "login_id": u["login_id"], "role": u["role"]} for u in users],
        "total": len(users),
        "source": USERS_FILE,
    })


# ── Weather ──────────────────────────────────────────────────────────
@app.route("/api/weather/all", methods=["GET"])
def api_weather_all():
    force = request.args.get("force", "false").lower() == "true"
    if force:
        cache.delete("weather_all")
    data = get_or_fetch("weather_all", fetch_weather)
    return jsonify(data)


@app.route("/api/weather/district/<name>", methods=["GET"])
def api_weather_district(name):
    data = get_or_fetch("weather_all", fetch_weather)
    match = next((d for d in data.get("districts", []) if d["name"].lower() == name.lower()), None)
    if not match:
        return jsonify({"error": "District not found"}), 404
    return jsonify(match)


@app.route("/api/weather/rainfall-forecast", methods=["GET"])
def api_rainfall_forecast():
    """7-day rainfall forecast (mm) for all districts — Open-Meteo."""
    force = request.args.get("force", "false").lower() == "true"
    if force:
        cache.delete("rainfall_forecast_all")
    data = get_or_fetch("rainfall_forecast_all", fetch_rainfall_forecast)
    return jsonify(data)


# ── Air Quality ──────────────────────────────────────────────────────
@app.route("/api/airquality/all", methods=["GET"])
def api_aq_all():
    data = get_or_fetch("aq_all", fetch_air_quality)
    return jsonify(data)


# ── News ─────────────────────────────────────────────────────────────
@app.route("/api/news/feed", methods=["GET"])
def api_news_feed():
    force = request.args.get("force", "false").lower() == "true"
    if force:
        cache.delete("news_feed")
    data = get_or_fetch("news_feed", fetch_news)
    return jsonify(data)


@app.route("/api/news/signal", methods=["GET"])
def api_news_signal():
    data = get_or_fetch("news_feed", fetch_news)
    return jsonify({
        "signal_score": data.get("signal_score"),
        "signal_level": data.get("signal_level"),
        "total_articles": data.get("total"),
        "top_keywords": data.get("top_keywords"),
        "geo_zones": data.get("geo_zones"),
        "severity_counts": data.get("severity_counts"),
        "trigger_recommendation": data.get("trigger_recommendation"),
        "fetched_at": data.get("fetched_at"),
    })


# ── PRIMARY RISK API — Contextual Risk Score ────────────────────────
@app.route("/api/risk/compute", methods=["GET"])
def api_risk_compute():
    """
    PRIMARY RISK API — Uses Contextual Risk Score as the main model.
    Returns map_color and map_color_rgb for each district.
    This drives Live Risk Map, Trigger States, and all risk displays.
    """
    force = request.args.get("force", "false").lower() == "true"
    weather_data = get_or_fetch("weather_all", fetch_weather)
    news_data = get_or_fetch("news_feed", fetch_news)
    news_total = news_data.get("total", 0)
    news_articles = news_data.get("articles", [])
    rainfall_forecast_data = get_or_fetch("rainfall_forecast_all", fetch_rainfall_forecast)
    
    # Compute contextual risk (primary model)
    contextual_result = compute_contextual_risk(weather_data, news_total, news_articles, rainfall_forecast_data)
    
    # Get districts with map colors
    districts = contextual_result.get("districts", [])
    
    # Add campaign signal based on contextual trigger state
    for d in districts:
        trigger = d.get("trigger_state", "LOW")
        if trigger == "BOOST":
            d["campaign_signal"] = "VERY HIGH"
            d["signal"] = "VERY HIGH"
        elif trigger == "PREPARE":
            d["campaign_signal"] = "HIGH"
            d["signal"] = "HIGH"
        elif trigger == "MONITOR":
            d["campaign_signal"] = "MODERATE"
            d["signal"] = "MODERATE"
        else:
            d["campaign_signal"] = "LOW"
            d["signal"] = "LOW"
        
        # Ensure fields match expected format
        d["score"] = d.get("contextual_score", 0)
        d["risk_score"] = d.get("contextual_score", 0)
        d["status"] = d.get("trigger_state", "LOW")
        d["final_status"] = d.get("trigger_state", "LOW")
        d["final_risk_score"] = d.get("contextual_score", 0)
        d["risk"] = d.get("trigger_state", "LOW")
        d["driver"] = d.get("driver", "Contextual Risk")
    
    return jsonify({
        "districts": districts,
        "active_count": contextual_result.get("counts", {}).get("BOOST", 0) + contextual_result.get("counts", {}).get("PREPARE", 0),
        "monitor_count": contextual_result.get("counts", {}).get("MONITOR", 0),
        "critical_count": contextual_result.get("counts", {}).get("BOOST", 0),
        "total": len(districts),
        "computed_at": contextual_result.get("computed_at"),
        "scoring_weights": contextual_result.get("scoring_weights"),
        "trigger_thresholds": contextual_result.get("trigger_thresholds"),
        "trigger_colors": contextual_result.get("trigger_colors"),
        "counts": contextual_result.get("counts"),
    })


@app.route("/api/risk/contextual", methods=["GET"])
def api_risk_contextual():
    """Direct access to contextual risk score data with map colors."""
    force = request.args.get("force", "false").lower() == "true"
    weather_data = get_or_fetch("weather_all", fetch_weather)
    news_data = get_or_fetch("news_feed", fetch_news)
    news_total = news_data.get("total", 0)
    news_articles = news_data.get("articles", [])
    rainfall_forecast_data = get_or_fetch("rainfall_forecast_all", fetch_rainfall_forecast)
    result = compute_contextual_risk(weather_data, news_total, news_articles, rainfall_forecast_data)
    return jsonify(result)


# ── Forecast Validation Engine (historical backtest) ────────────────
@app.route("/api/analytics/forecast-accuracy", methods=["GET"])
def api_forecast_accuracy():
    """
    Backtests the rainfall forecast and Weather-Only Contextual Score against
    real historical data (Open-Meteo archived forecast vs ERA5 reanalysis)
    for the past N weeks. Cached for 6 hours since this pulls a fair amount
    of historical data per district and doesn't change intra-day.
    """
    num_weeks = int(request.args.get("weeks", 4))
    sample_size = int(request.args.get("sample", 20))
    force = request.args.get("force", "false").lower() == "true"

    cache_key = f"forecast_accuracy_{num_weeks}_{sample_size}"
    if force:
        cache.delete(cache_key)

    cached = cache.get(cache_key)
    if cached:
        return jsonify(cached)

    result = compute_forecast_accuracy(num_weeks=num_weeks, sample_size=sample_size)
    cache.set(cache_key, result, ttl=21600)  # 6 hours
    return jsonify(result)


# ── AI Endpoints ─────────────────────────────────────────────────────
@app.route("/api/ai/brief", methods=["POST"])
def api_ai_brief():
    payload = request.json
    if not payload or not payload.get("name"):
        return jsonify({"error": "District data with 'name' required"}), 400
    result = call_ai(
        "You are Laren, HIT RADAR's AI media strategist. Generate a campaign brief.",
        f"Generate campaign brief for {payload.get('name')} with contextual risk score {payload.get('contextual_score', 0)}",
        prefer="groq"
    )
    return jsonify(result)


@app.route("/api/ai/daily-summary", methods=["POST"])
def api_ai_daily_summary():
    weather_data = get_or_fetch("weather_all", fetch_weather)
    news_data = get_or_fetch("news_feed", fetch_news)
    date_str = (request.json or {}).get("date")
    contextual_data = (request.json or {}).get("contextual_data")
    result = generate_daily_summary(weather_data, news_data, date_str, contextual_data)
    return jsonify(result)


@app.route("/api/ai/chat", methods=["POST"])
def api_ai_chat():
    payload = request.json
    if not payload or not payload.get("message"):
        return jsonify({"error": "message required"}), 400
    weather_data = get_or_fetch("weather_all", fetch_weather)
    news_data    = cache.get("news_feed")    or fetch_news()

    # Feed the PRIMARY model (contextual risk) into the chatbot context
    news_total    = news_data.get("total", 0)
    news_articles = news_data.get("articles", [])
    rainfall_forecast_data = get_or_fetch("rainfall_forecast_all", fetch_rainfall_forecast)
    risk_data     = compute_contextual_risk(weather_data, news_total, news_articles, rainfall_forecast_data)

    result = ai_chat(
        message=payload["message"],
        history=payload.get("history", []),
        live_context={"weather": weather_data, "news": news_data, "risk": risk_data},
        geo_context=payload.get("geo_context", {}),
        dashboard_context=payload.get("dashboard_context", {}),
    )
    return jsonify(result)


@app.route("/api/ai/generate-creative-image", methods=["POST"])
def api_generate_creative_image():
    """
    Generates an ORIGINAL AI creative-concept image (poster/illustration style)
    for a plan's creative theme — via Gemini's native image model. This is
    always clearly an AI-generated concept, never presented as a real photo.
    """
    payload = request.json or {}
    theme = (payload.get("theme") or "").strip()
    if not theme:
        return jsonify({"error": "theme required"}), 400
    try:
        prompt = (
            f"Flat vector illustration poster for a public health awareness campaign. "
            f"Theme: \"{theme}\". Clean modern flat-design style, bold simple shapes, "
            f"limited color palette (teal/cyan/orange), friendly approachable tone, "
            f"NO photorealism, NO readable body text/paragraphs — at most a short 2-4 word "
            f"headline in clean sans-serif type. Wide rectangular composition, professional "
            f"marketing-creative quality, no watermarks, no logos."
        )
        image_data_uri = call_gemini_image(prompt, aspect_ratio="16:9")
        if not image_data_uri:
            return jsonify({"error": "Image generation returned no image"}), 502
        return jsonify({"image": image_data_uri, "type": "ai_generated_concept"})
    except Exception as e:
        log.warning(f"⚠️ Creative image generation failed: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/ai/generate-funnel-image", methods=["POST"])
def api_generate_funnel_image():
    """
    Generates a full-funnel visual (Awareness → Consideration → Conversion)
    as an AI illustration — labels/stage names only, NO numbers or stats,
    since image models render digits unreliably. Real numeric funnel data
    stays in the Chart.js bar chart, not this image.
    """
    payload = request.json or {}
    stages = payload.get("stages") or ["Awareness", "Consideration", "Conversion"]
    try:
        stage_list = " → ".join(stages)
        prompt = (
            f"Flat vector infographic illustration of a marketing funnel with exactly these "
            f"stages left to right: {stage_list}. Each stage as a labeled geometric segment "
            f"(funnel/arrow shape) with a short 1-2 word text label matching the stage name — "
            f"NO numbers, NO statistics, NO percentages, NO data figures anywhere in the image, "
            f"purely conceptual and visual. Clean modern flat-design style, teal/cyan/purple "
            f"gradient palette, wide rectangular composition, professional quality, no watermarks."
        )
        image_data_uri = call_gemini_image(prompt, aspect_ratio="16:9")
        if not image_data_uri:
            return jsonify({"error": "Image generation returned no image"}), 502
        return jsonify({"image": image_data_uri, "type": "ai_generated_concept"})
    except Exception as e:
        log.warning(f"⚠️ Funnel image generation failed: {e}")
        return jsonify({"error": str(e)}), 500


@app.route("/api/ai/context-photo", methods=["POST"])
def api_context_photo():
    """
    Fetches a REAL, licensed, rectangular (landscape) stock photo from Unsplash
    for a district — never AI-generated, never scraped from copyrighted news
    sources. Tries flood/waterlogging-specific queries for the city first
    (most relevant to a dengue/monsoon risk plan), then falls back to
    progressively broader monsoon queries if that specific city has no
    flood-tagged photos on Unsplash. Always returns proper credit.
    """
    payload = request.json or {}
    city = (payload.get("city") or payload.get("query") or "").strip()
    if not city:
        return jsonify({"error": "city required"}), 400

    query_chain = [
        f"{city} flood India",
        f"{city} waterlogging monsoon",
        f"{city} India monsoon rain street",
        "India monsoon flood street",
    ]
    for q in query_chain:
        photo = fetch_unsplash_image(q, orientation="landscape")
        if photo:
            photo["query_used"] = q
            return jsonify(photo)

    return jsonify({"error": "No Unsplash result / UNSPLASH_ACCESS_KEY not configured"}), 404


@app.route("/api/ai/news-narrative", methods=["GET"])
def api_ai_news_narrative():
    news_data = get_or_fetch("news_feed", fetch_news)
    return jsonify(generate_news_narrative(news_data))


# ── Telegram Alerts ──────────────────────────────────────────────────
@app.route("/api/alerts/send", methods=["POST"])
def api_alerts_send():
    payload = request.json
    if not payload:
        return jsonify({"error": "District data required"}), 400
    return jsonify(send_trigger_alert(payload))


@app.route("/api/alerts/bulk", methods=["POST"])
def api_alerts_bulk():
    weather_data = get_or_fetch("weather_all", fetch_weather)
    news_data = get_or_fetch("news_feed", fetch_news)
    news_total = news_data.get("total", 0)
    news_articles = news_data.get("articles", [])
    rainfall_forecast_data = get_or_fetch("rainfall_forecast_all", fetch_rainfall_forecast)
    contextual_result = compute_contextual_risk(weather_data, news_total, news_articles, rainfall_forecast_data)
    
    active = [
        d for d in contextual_result.get("districts", [])
        if d.get("trigger_state") in ["BOOST", "PREPARE"]
    ]
    results = []
    for d in active:
        r = send_trigger_alert(d)
        results.append({"district": d.get("name"), "sent": r.get("success", False)})
    return jsonify({"total_sent": len(results), "results": results})


@app.route("/api/alerts/daily-brief", methods=["POST"])
def api_alerts_daily_brief():
    weather_data = get_or_fetch("weather_all", fetch_weather)
    news_data = get_or_fetch("news_feed", fetch_news)
    return jsonify(send_telegram("Daily brief sent via API"))


@app.route("/api/alerts/test", methods=["POST"])
def api_alerts_test():
    return jsonify(send_telegram("✅ HIT RADAR — Connection Test Successful"))


# ── Config ───────────────────────────────────────────────────────────
@app.route("/api/config/keys", methods=["POST"])
def api_config_keys():
    payload = request.json or {}
    key_map = {
        "groq_key": "GROQ_API_KEY",
        "gemini_key": "GEMINI_API_KEY",
        "mistral_key": "MISTRAL_API_KEY",
        "telegram_bot_token": "TELEGRAM_BOT_TOKEN",
        "telegram_chat_id": "TELEGRAM_CHAT_ID",
        "newsdata_key": "NEWSDATA_API_KEY",
        "owm_key": "OWM_API_KEY",
        "waqi_key": "WAQI_API_KEY",
    }
    updated = []
    for field, env_name in key_map.items():
        val = payload.get(field, "").strip()
        if val:
            CONFIG[env_name] = val
            os.environ[env_name] = val
            updated.append(field)

    env_path = os.path.join(DATA_DIR, ".env")
    try:
        existing = {}
        if os.path.exists(env_path):
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if "=" in line and not line.startswith("#"):
                        k, v = line.split("=", 1)
                        existing[k.strip()] = v.strip()
        for field, env_name in key_map.items():
            val = payload.get(field, "").strip()
            if val:
                existing[env_name] = val
        with open(env_path, "w") as f:
            for k, v in existing.items():
                f.write(f"{k}={v}\n")
        log.info(f"✅ API keys saved: {updated}")
    except Exception as e:
        log.error(f"❌ Failed to write .env: {e}")

    return jsonify({"success": True, "updated": updated})


@app.route("/api/config/test-key", methods=["POST"])
def api_config_test_key():
    payload = request.json or {}
    service = payload.get("service", "")
    key = payload.get("key", "").strip()

    if service == "telegram":
        token = key or CONFIG.get("TELEGRAM_BOT_TOKEN", "")
        if not token:
            return jsonify({"success": False, "error": "No token provided"})
        try:
            r = requests.get(f"https://api.telegram.org/bot{token}/getMe", timeout=8)
            r.raise_for_status()
            return jsonify({"success": True, "bot": r.json().get("result", {}).get("username")})
        except Exception as e:
            return jsonify({"success": False, "error": str(e)})

    elif service == "groq":
        api_key = key or CONFIG.get("GROQ_API_KEY", "")
        if not api_key:
            return jsonify({"success": False, "error": "No key provided"})
        try:
            r = requests.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json={"model": "llama-3.3-70b-versatile", "messages": [{"role": "user", "content": "ping"}], "max_tokens": 5},
                timeout=10,
            )
            r.raise_for_status()
            return jsonify({"success": True})
        except Exception as e:
            return jsonify({"success": False, "error": str(e)})

    elif service == "gemini":
        api_key = key or CONFIG.get("GEMINI_API_KEY", "")
        if not api_key:
            return jsonify({"success": False, "error": "No key provided"})
        try:
            r = requests.post(
                f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={api_key}",
                json={"contents": [{"parts": [{"text": "ping"}]}]},
                timeout=10,
            )
            r.raise_for_status()
            return jsonify({"success": True})
        except Exception as e:
            return jsonify({"success": False, "error": str(e)})

    return jsonify({"success": False, "error": "Unknown service"})


# ── Health ───────────────────────────────────────────────────────────
@app.route("/api/health", methods=["GET"])
def api_health():
    return jsonify({
        "status": "ok",
        "version": "3.0.0",
        "time": datetime.now(timezone.utc).isoformat(),
        "config": {
            "groq": bool(CONFIG.get("GROQ_API_KEY")),
            "gemini": bool(CONFIG.get("GEMINI_API_KEY")),
            "mistral": bool(CONFIG.get("MISTRAL_API_KEY")),
            "telegram": bool(CONFIG.get("TELEGRAM_BOT_TOKEN")),
            "newsdata": bool(CONFIG.get("NEWSDATA_API_KEY")),
            "waqi": bool(CONFIG.get("WAQI_API_KEY")),
            "owm": bool(CONFIG.get("OWM_API_KEY")),
        },
        "cache": cache.stats(),
        "scheduler": {
            "weather_refresh_sec": CONFIG["WEATHER_REFRESH_SEC"],
            "news_refresh_sec": CONFIG["NEWS_REFRESH_SEC"],
            "aq_refresh_sec": CONFIG["AQ_REFRESH_SEC"],
        },
        "districts": len(DISTRICTS),
    })


@app.route("/api/dengue-burden", methods=["GET"])
def api_dengue_burden_get():
    return jsonify({
        "burden": DENGUE_BURDEN,
        "states": list(DENGUE_BURDEN.keys()),
        "loaded": len(DENGUE_BURDEN) > 0,
        "years": DENGUE_BURDEN_YEARS,
        "source_file": DENGUE_BURDEN_SOURCE.get("filename"),
        "source_file_type": DENGUE_BURDEN_SOURCE.get("file_type"),
        "uploaded_at": DENGUE_BURDEN_SOURCE.get("uploaded_at"),
        "message": "Upload a PDF, CSV, or Excel file via POST /api/dengue-burden/upload to populate this data" if not DENGUE_BURDEN else f"{len(DENGUE_BURDEN)} states loaded from {DENGUE_BURDEN_SOURCE.get('filename') or 'unknown source'}",
    })


@app.route("/api/dengue-burden/upload", methods=["POST"])
def api_dengue_burden_upload():
    global DENGUE_BURDEN, DENGUE_BURDEN_SOURCE

    if "file" not in request.files:
        return jsonify({"success": False, "error": "No file uploaded (expected form field 'file')"}), 400

    f = request.files["file"]
    if not f.filename:
        return jsonify({"success": False, "error": "Empty filename"}), 400

    filename_lower = f.filename.lower()

    if filename_lower.endswith(".pdf"):
        if not PDF_SUPPORT:
            return jsonify({
                "success": False,
                "error": "pdfplumber is not installed on the server. Run: pip install pdfplumber",
            }), 500
        parser_fn = parse_dengue_burden_pdf
        file_type = "PDF"
    elif filename_lower.endswith(".csv"):
        parser_fn = parse_dengue_burden_csv
        file_type = "CSV"
    elif filename_lower.endswith((".xlsx", ".xls")):
        if not XLSX_SUPPORT:
            return jsonify({
                "success": False,
                "error": "openpyxl is not installed on the server. Run: pip install openpyxl",
            }), 500
        parser_fn = parse_dengue_burden_xlsx
        file_type = "Excel"
    else:
        return jsonify({
            "success": False,
            "error": "Unsupported file type. Upload a PDF, CSV, or Excel (.xlsx/.xls) file.",
        }), 400

    try:
        parsed = parser_fn(f.stream)
    except Exception as e:
        log.error(f"❌ Dengue burden file parse failed ({f.filename}): {e}")
        return jsonify({"success": False, "error": str(e)}), 422

    DENGUE_BURDEN = parsed
    DENGUE_BURDEN_SOURCE = {
        "filename": f.filename,
        "file_type": file_type,
        "uploaded_at": datetime.now().isoformat(),
    }
    _save_dengue_burden_to_disk()
    _save_dengue_burden_meta_to_disk()
    log.info(f"✅ Dengue burden data updated from {f.filename}: {len(DENGUE_BURDEN)} states")

    return jsonify({
        "success": True,
        "states_loaded": len(DENGUE_BURDEN),
        "states": list(DENGUE_BURDEN.keys()),
        "years": DENGUE_BURDEN_YEARS,
        "burden": DENGUE_BURDEN,
        "source_file": f.filename,
        "source_file_type": file_type,
        "uploaded_at": DENGUE_BURDEN_SOURCE["uploaded_at"],
    })


@app.route("/api/dengue-burden/reset", methods=["POST"])
def api_dengue_burden_reset():
    global DENGUE_BURDEN, DENGUE_BURDEN_SOURCE
    DENGUE_BURDEN = {}
    DENGUE_BURDEN_SOURCE = {"filename": None, "file_type": None, "uploaded_at": None}
    try:
        if os.path.exists(DENGUE_BURDEN_FILE):
            os.remove(DENGUE_BURDEN_FILE)
        if os.path.exists(DENGUE_BURDEN_META_FILE):
            os.remove(DENGUE_BURDEN_META_FILE)
    except Exception as e:
        log.error(f"❌ Failed to remove dengue_burden.json: {e}")
    return jsonify({"success": True, "message": "Dengue burden data cleared"})


@app.route("/api/cache/clear", methods=["POST"])
def api_cache_clear():
    cache.clear()
    return jsonify({"success": True, "message": "Cache cleared"})


# ══════════════════════════════════════════════════════════════════════
# STARTUP
# ══════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    log.info("╔══════════════════════════════════════════╗")
    log.info("║      HIT RADAR Backend v3.0.0            ║")
    log.info("║      PRIMARY MODEL: Contextual Risk      ║")
    log.info("║      MAP COLORS:                         ║")
    log.info("║      BOOST 🔴  | PREPARE 🟠 | MONITOR 🔵 | LOW 🔘  ║")
    log.info("║      Thresholds: LOW 0-2.5 | MONITOR 2.5-5.0 | PREPARE 5.0-7.0 | BOOST 7.0-10.0 ║")
    log.info("╚══════════════════════════════════════════╝")

    env_path = os.path.join(DATA_DIR, ".env")
    if os.path.exists(env_path):
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    k, v = k.strip(), v.strip()
                    os.environ[k] = v
                    CONFIG[k] = v
        log.info(f"✅ Loaded .env from {env_path}")

    # ── COLD START SEQUENCING: fetch ONE at a time with cooldowns ────
    # Open-Meteo's free tier rate-limits by IP — firing weather, rainfall
    # forecast, AND air quality at the same instant guarantees 429s.
    # Instead, we sequence them with deliberate cooldowns, and defer the
    # secondary fetches to background threads so the server starts fast.
    
    log.info("📡 [1/3] Fetching initial weather data...")
    fetch_weather()
    time_module.sleep(8)  # cooldown before next Open-Meteo call

    log.info("🦟 Loading saved dengue burden data (if any)...")
    _load_dengue_burden_from_disk()

    # Defer rainfall forecast to background — the dashboard can still load
    # without it, and it uses the same Open-Meteo endpoint, so running it
    # immediately after weather guarantees a 429.
    log.info("🌧 [2/3] Fetching initial 7-day rainfall forecast (background — delayed 15s)...")
    def _deferred_rainfall():
        time_module.sleep(15)
        fetch_rainfall_forecast()
    threading.Thread(target=_deferred_rainfall, daemon=True).start()

    # News and Air Quality use DIFFERENT APIs, so they can run immediately
    # without affecting Open-Meteo's rate limit.
    log.info("📰 Fetching initial news feed (background)...")
    threading.Thread(target=fetch_news, daemon=True).start()

    log.info("💨 [3/3] Fetching initial air quality data (background — delayed 5s)...")
    def _deferred_aq():
        time_module.sleep(5)
        fetch_air_quality()
    threading.Thread(target=_deferred_aq, daemon=True).start()

    # Scheduler: stagger job start times so weather and rainfall forecast
    # never fire in the same instant (that was doubling the burst).
    scheduler = BackgroundScheduler(daemon=True)
    
    base_time = datetime.now(timezone.utc)
    weather_interval = CONFIG["WEATHER_REFRESH_SEC"]  # 1800s = 30min
    
    scheduler.add_job(
        scheduled_weather_refresh, "interval", seconds=weather_interval,
        id="weather",
        next_run_time=base_time + timedelta(seconds=weather_interval),
    )
    # Offset rainfall by 120s so it NEVER fires at the same second as weather
    scheduler.add_job(
        scheduled_rainfall_forecast_refresh, "interval", seconds=weather_interval,
        id="rainfall_forecast",
        next_run_time=base_time + timedelta(seconds=weather_interval + 120),
    )
    scheduler.add_job(
        scheduled_news_refresh, "interval", seconds=CONFIG["NEWS_REFRESH_SEC"],
        id="news_rss",
    )
    scheduler.add_job(
        scheduled_newsdata_refresh, "interval", seconds=CONFIG["NEWSDATA_REFRESH_SEC"],
        id="news_structured",
    )
    scheduler.add_job(
        scheduled_aq_refresh, "interval", seconds=CONFIG["AQ_REFRESH_SEC"],
        id="aq",
    )
    scheduler.start()
    log.info(f"⏱ Scheduler started — weather every {weather_interval//60}min (staggered)")

    log.info(f"🚀 HIT RADAR Backend ready → http://localhost:5000")
    log.info(f"📋 Monitoring {len(DISTRICTS)} districts across India")
    log.info("📋 /api/risk/compute → PRIMARY risk model with map colors")

    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False, threaded=True)

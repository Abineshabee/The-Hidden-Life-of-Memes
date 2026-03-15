"""
=============================================================================
  MEME LIFECYCLE DATASET COLLECTOR  v3.0
  Research: "The Hidden Life of Memes - Survival of the Funniest"
=============================================================================

  INSTALL DEPENDENCIES FIRST:
      pip install requests beautifulsoup4 pandas

  RUN:
      python meme_collector.py

  OUTPUTS (saved to ./meme_dataset_output/):
      meme_lifecycle_dataset.csv   - 1 row per meme, ~50 columns
      meme_weekly_trends.csv       - weekly Google Trends (long format)
      meme_reddit_posts.csv        - individual Reddit post records
      meme_collection_report.txt   - summary + errors

  DATA SOURCES:
      1. Google Trends   - weekly interest timeline (direct HTTP, no pytrends)
      2. Know Your Meme  - origin, status, platforms, description
      3. Reddit          - engagement scores, timestamps, community spread
      4. Wayback Machine - archive snapshot count per year (dead meme recovery)
      5. Wikipedia       - checks if meme has Wikipedia article (longevity signal)

=============================================================================
"""

import csv
import json
import os
import random
import re
import sys
import time
import urllib.parse
from collections import Counter, defaultdict
from datetime import datetime, timezone

try:
    import requests
    from bs4 import BeautifulSoup
    import pandas as pd
except ImportError as e:
    sys.exit(f"\n  Missing library: {e}\n  Run:  pip install requests beautifulsoup4 pandas\n")


# =============================================================================
#  MEME LIST - edit freely
#  Format: ("Google Trends search term", "KnowYourMeme slug", "Reddit keywords")
# =============================================================================

MEMES = [
    ("distracted boyfriend meme",        "distracted-boyfriend",        "distracted boyfriend"),
    ("doge meme",                         "doge",                        "doge dogecoin shibe"),
    ("this is fine dog meme",             "this-is-fine",                "this is fine fire"),
    ("wojak meme",                        "wojak",                       "wojak feels guy"),
    ("pepe the frog meme",                "pepe-the-frog",               "pepe frog rare pepe"),
    ("drake hotline bling meme",          "drakeposting",                "drake hotline bling meme"),
    ("galaxy brain meme",                 "galaxy-brain",                "galaxy brain expanding"),
    ("stonks meme",                       "stonks",                      "stonks stocks meme"),
    ("coffin dance meme",                 "coffin-dance",                "coffin dance ghana"),
    ("chad vs virgin meme",               "virgin-vs-chad",              "chad virgin walk"),
    ("trollface meme",                    "trollface",                   "trollface problem"),
    ("nyan cat meme",                     "nyan-cat",                    "nyan cat rainbow"),
    ("rickroll meme",                     "rickrolling",                 "rickroll never gonna give"),
    ("surprised pikachu meme",            "surprised-pikachu",           "surprised pikachu face"),
    ("among us meme",                     "among-us",                    "among us sus impostor"),
    ("hide the pain harold meme",         "hide-the-pain-harold",        "harold hide pain"),
    ("mocking spongebob meme",            "mocking-spongebob",           "mocking spongebob mimicking"),
    ("gru plan meme",                     "grus-plan",                   "gru plan step"),
    ("woman yelling at cat meme",         "woman-yelling-at-a-cat",      "woman yelling cat"),
    ("two buttons meme",                  "two-buttons",                 "two buttons sweating"),
]

# =============================================================================
#  CONFIG
# =============================================================================

TRENDS_START  = "2010-01-01"
TRENDS_END    = "2024-12-31"
OUTPUT_DIR    = "meme_dataset_output"
REDDIT_LIMIT  = 25
REDDIT_SUBS   = "memes+dankmemes+AdviceAnimals+me_irl+funny"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# =============================================================================
#  LOGGING
# =============================================================================

def log(msg, level="INFO"):
    icons = {"INFO": "●", "OK": "✓", "WARN": "⚠", "ERR": "✗", "HEAD": "═"}
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"  [{ts}] {icons.get(level, '●')} {msg}")


# =============================================================================
#  HTTP HELPERS
# =============================================================================

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

REDDIT_HEADERS = {
    "User-Agent": "MemeResearchBot/1.0 (academic research)",
    "Accept": "application/json",
}


def get(url, params=None, headers=None, retries=3, wait=3, timeout=15):
    """GET with retry + polite wait. Returns Response or None."""
    h = headers or BROWSER_HEADERS
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, params=params, headers=h,
                             timeout=timeout, allow_redirects=True)
            if r.status_code == 200:
                return r
            if r.status_code == 429:
                backoff = wait * (3 ** attempt) + random.uniform(5, 15)
                log(f"429 on {url[:55]} — sleeping {backoff:.0f}s (attempt {attempt})", "WARN")
                time.sleep(backoff)
                continue
            if r.status_code in (403, 404):
                log(f"HTTP {r.status_code} for {url[:60]}", "WARN")
                return None
            log(f"HTTP {r.status_code} attempt {attempt}", "WARN")
        except requests.exceptions.ConnectionError as e:
            log(f"Connection error attempt {attempt}: {str(e)[:60]}", "WARN")
        except requests.exceptions.Timeout:
            log(f"Timeout attempt {attempt}", "WARN")
        except Exception as e:
            log(f"Error attempt {attempt}: {str(e)[:60]}", "WARN")
        if attempt < retries:
            time.sleep(wait * attempt)
    return None


# =============================================================================
#  MODULE 1 - GOOGLE TRENDS (direct HTTP, no pytrends)
# =============================================================================

_trends_session = None


def _get_trends_session():
    global _trends_session
    if _trends_session is not None:
        return _trends_session
    s = requests.Session()
    s.headers.update(BROWSER_HEADERS)
    try:
        r = s.get("https://trends.google.com/trends/explore", timeout=12)
        cookie_names = list(s.cookies.get_dict().keys())
        log(f"Trends session ready (cookies: {cookie_names})", "OK")
    except Exception as e:
        log(f"Trends session init warning: {e}", "WARN")
    _trends_session = s
    return s


def fetch_trends(term):
    """
    Fetch weekly Google Trends interest-over-time.
    Uses the same undocumented API the Trends website uses.
    Returns dict with dates, values, and lifecycle metrics.
    """
    log(f"Google Trends  → {term}")
    s = _get_trends_session()

    explore_url = "https://trends.google.com/trends/api/explore"
    req_payload = {
        "comparisonItem": [{
            "keyword": term,
            "time": f"{TRENDS_START} {TRENDS_END}",
            "geo": ""
        }],
        "category": 0,
        "property": ""
    }
    explore_params = {
        "hl": "en-US",
        "tz": "-330",
        "req": json.dumps(req_payload, separators=(",", ":")),
    }

    # Retry with escalating backoff on 429
    wait_seq = [0, 20, 45, 90, 180]
    r = None
    for attempt, backoff in enumerate(wait_seq, 1):
        if backoff:
            log(f"  Trends 429 - waiting {backoff}s before retry {attempt}/5", "WARN")
            time.sleep(backoff)
        try:
            r = s.get(explore_url, params=explore_params, timeout=15)
            if r.status_code == 429 or (r.url and "sorry" in r.url.lower()):
                r = None
                continue
            if r.status_code != 200:
                log(f"  Trends explore returned HTTP {r.status_code}", "WARN")
                return {}
            break
        except Exception as e:
            log(f"  Trends explore error: {e}", "WARN")
            r = None
            if attempt >= len(wait_seq):
                return {}

    if r is None:
        log(f"  Trends: all retries exhausted for '{term}'", "ERR")
        return {}

    # Parse XSSI-protected JSON (starts with )]}',\n)
    try:
        raw = r.text
        if raw.startswith(")]}'"):
            raw = raw.split("\n", 1)[1]
        data = json.loads(raw)
        widgets = data.get("widgets", [])
        ts_widget = next((w for w in widgets if w.get("id") == "TIMESERIES"), None)
        if not ts_widget:
            log(f"  No TIMESERIES widget for '{term}'", "WARN")
            return {}
        token   = ts_widget["token"]
        req_obj = ts_widget["request"]
    except Exception as e:
        log(f"  Trends explore parse error: {e}", "WARN")
        return {}

    time.sleep(random.uniform(2, 4))

    # Step 2: get actual data from multiline endpoint
    ml_url = "https://trends.google.com/trends/api/widgetdata/multiline"
    ml_params = {
        "hl":    "en-US",
        "tz":    "-330",
        "req":   json.dumps(req_obj, separators=(",", ":")),
        "token": token,
        "geo":   "",
    }

    r2 = None
    for attempt, backoff in enumerate([0, 20, 45, 90], 1):
        if backoff:
            time.sleep(backoff)
        try:
            r2 = s.get(ml_url, params=ml_params, timeout=15)
            if r2.status_code == 429 or (r2.url and "sorry" in r2.url.lower()):
                r2 = None
                continue
            if r2.status_code != 200:
                return {}
            break
        except Exception:
            r2 = None
            if attempt >= 4:
                return {}

    if r2 is None:
        return {}

    try:
        raw2 = r2.text
        if raw2.startswith(")]}'"):
            raw2 = raw2.split("\n", 1)[1]
        ml_data  = json.loads(raw2)
        timeline = ml_data["default"]["timelineData"]
    except Exception as e:
        log(f"  Trends multiline parse error: {e}", "WARN")
        return {}

    if not timeline:
        log(f"  Empty timeline for '{term}'", "WARN")
        return {}

    dates, values = [], []
    for point in timeline:
        try:
            ts_sec = int(point["time"])
            date   = datetime.fromtimestamp(ts_sec, tz=timezone.utc).strftime("%Y-%m-%d")
            val    = int(point["value"][0])
            dates.append(date)
            values.append(val)
        except Exception:
            continue

    if not values:
        return {}

    lc = _lifecycle_metrics(values, dates)
    log(f"  {len(dates)} weeks | peak={lc['peak_value']} | {lc['lifecycle_label']}", "OK")
    time.sleep(random.uniform(8, 14))

    return {
        "trends_dates":           dates,
        "trends_values":          values,
        "trends_birth_date":      lc["birth_date"],
        "trends_peak_date":       lc["peak_date"],
        "trends_death_date":      lc["death_date"],
        "trends_reborn_date":     lc["reborn_date"],
        "trends_lifespan_weeks":  lc["lifespan_weeks"],
        "trends_peak_value":      lc["peak_value"],
        "trends_rise_speed":      lc["rise_speed"],
        "trends_decay_speed":     lc["decay_speed"],
        "trends_was_reborn":      lc["was_reborn"],
        "trends_lifecycle_label": lc["lifecycle_label"],
        "trends_phases":          lc["phases"],
        "trends_normalized":      lc["normalized"],
    }


def _lifecycle_metrics(values, dates):
    n = len(values)
    if n == 0 or max(values) == 0:
        empty = {k: None for k in [
            "birth_date","peak_date","death_date","reborn_date",
            "lifespan_weeks","peak_value","rise_speed","decay_speed",
            "was_reborn","lifecycle_label","phases","normalized"
        ]}
        return empty

    peak_val = max(values)
    peak_i   = values.index(peak_val)
    norm     = [round(v / peak_val, 4) for v in values]

    birth_i  = next((i for i, v in enumerate(norm) if v > 0.05), 0)
    decay_i  = next((i for i in range(peak_i, n) if norm[i] < 0.5), n - 1)
    death_i  = next((i for i in range(decay_i, n) if norm[i] < 0.10), None)
    reborn_i = None
    if death_i:
        reborn_i = next((i for i in range(death_i + 4, n) if norm[i] > 0.30), None)

    lifespan  = (death_i or n - 1) - birth_i
    rise_spd  = round(peak_val / max(peak_i - birth_i, 1), 2)
    decay_spd = round(peak_val / max((death_i or n - 1) - peak_i, 1), 2)

    if death_i and reborn_i:
        label = "reborn"
    elif death_i:
        label = "dead"
    elif lifespan > 260:
        label = "evergreen"
    elif lifespan > 104:
        label = "long_lived"
    elif lifespan > 26:
        label = "moderate"
    else:
        label = "short_lived"

    phases = []
    for i in range(n):
        if i < birth_i:
            phases.append("pre_birth")
        elif birth_i <= i < peak_i:
            phases.append("growth")
        elif i == peak_i:
            phases.append("peak")
        elif death_i and i >= death_i:
            phases.append("reborn" if (reborn_i and i >= reborn_i) else "dead")
        elif i > peak_i and i < decay_i:
            phases.append("post_peak")
        else:
            phases.append("decay")

    def _d(i):
        return dates[i] if (i is not None and 0 <= i < len(dates)) else None

    return {
        "birth_date":      _d(birth_i),
        "peak_date":       _d(peak_i),
        "death_date":      _d(death_i),
        "reborn_date":     _d(reborn_i),
        "lifespan_weeks":  lifespan,
        "peak_value":      peak_val,
        "rise_speed":      rise_spd,
        "decay_speed":     decay_spd,
        "was_reborn":      reborn_i is not None,
        "lifecycle_label": label,
        "phases":          phases,
        "normalized":      norm,
    }


# =============================================================================
#  MODULE 2 - KNOW YOUR MEME
# =============================================================================

def fetch_kym(slug):
    log(f"Know Your Meme → {slug}")
    url = f"https://knowyourmeme.com/memes/{slug}"
    r   = get(url)
    if not r:
        return {"kym_url": url, "kym_status": "fetch_failed"}

    soup = BeautifulSoup(r.text, "html.parser")
    out  = {"kym_url": url}

    meta = {}
    for dt in soup.find_all("dt"):
        dd = dt.find_next_sibling("dd")
        if dd:
            key = dt.get_text(strip=True).lower().replace(" ", "_").rstrip(":")
            val = dd.get_text(" ", strip=True)
            meta[key] = val

    out["kym_year"]            = meta.get("year", meta.get("added", ""))
    out["kym_origin_platform"] = meta.get("origin", meta.get("type", ""))
    out["kym_added"]           = meta.get("added", "")
    out["kym_updated"]         = meta.get("updated", "")
    out["kym_tags"]            = meta.get("tags", "")

    body = soup.get_text(" ").lower()
    status_el = soup.find(class_="status") or soup.find(attrs={"data-entry-status": True})
    if status_el:
        out["kym_status"] = status_el.get_text(strip=True).lower()
    elif any(p in body for p in ["submission is dead", "no longer in use", "considered dead"]):
        out["kym_status"] = "dead"
    elif any(p in body for p in ["forced meme", "considered overdone", "overdone"]):
        out["kym_status"] = "overdone"
    else:
        out["kym_status"] = "active"

    spread = soup.find("section", id="spread")
    spread_text = spread.get_text(" ").lower() if spread else body
    platforms_found = [p for p in
        ["reddit","twitter","facebook","instagram","tiktok","tumblr",
         "youtube","4chan","imgur","vine","ifunny","discord","twitch"]
        if p in spread_text]
    out["kym_spread_platforms"] = "; ".join(platforms_found)
    out["kym_platform_count"]   = len(platforms_found)

    about = soup.find("section", id="about")
    if about:
        paras = [p.get_text(" ", strip=True) for p in about.find_all("p")]
        out["kym_description"] = " ".join(paras)[:400]
    else:
        out["kym_description"] = ""

    examples = soup.find("section", id="examples")
    out["kym_example_count"] = len(examples.find_all("img")) if examples else 0

    for cls in ["views-count", "likes-count", "entry-views", "entry-likes"]:
        el = soup.find(class_=cls)
        if el:
            key = "kym_views" if "view" in cls else "kym_likes"
            out[key] = el.get_text(strip=True)
    out.setdefault("kym_views", "n/a")
    out.setdefault("kym_likes", "n/a")

    log(f"  status={out['kym_status']} | platforms={out['kym_platform_count']} | year={out['kym_year']}", "OK")
    time.sleep(random.uniform(3, 6))
    return out


# =============================================================================
#  MODULE 3 - REDDIT (public JSON, no auth)
# =============================================================================

def fetch_reddit(keywords, meme_name):
    log(f"Reddit         → {keywords}")
    url = f"https://www.reddit.com/r/{REDDIT_SUBS}/search.json"
    params = {"q": keywords, "sort": "top", "t": "all", "limit": REDDIT_LIMIT}
    r = get(url, params=params, headers=REDDIT_HEADERS)
    if not r:
        return {}, []

    try:
        children = r.json()["data"]["children"]
    except Exception as e:
        log(f"  Reddit parse error: {e}", "WARN")
        return {}, []

    posts, scores, comments, dates, subs_seen = [], [], [], [], set()
    awards_tot = 0

    for child in children:
        p      = child.get("data", {})
        score  = p.get("score", 0)
        ncoms  = p.get("num_comments", 0)
        ts     = p.get("created_utc", 0)
        sub    = p.get("subreddit", "")
        awards = p.get("total_awards_received", 0)
        title  = p.get("title", "")[:150]
        link   = "https://reddit.com" + p.get("permalink", "")
        date   = (datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d") if ts else "")

        scores.append(score)
        comments.append(ncoms)
        if date:
            dates.append(date)
        subs_seen.add(sub)
        awards_tot += awards
        posts.append({
            "meme_name":    meme_name,
            "post_title":   title,
            "post_date":    date,
            "subreddit":    sub,
            "score":        score,
            "num_comments": ncoms,
            "awards":       awards,
            "url":          link,
        })

    if not scores:
        return {}, []

    sorted_dates = sorted(dates)
    agg = {
        "reddit_posts_found":      len(posts),
        "reddit_avg_score":        round(sum(scores) / len(scores), 1),
        "reddit_max_score":        max(scores),
        "reddit_total_score":      sum(scores),
        "reddit_avg_comments":     round(sum(comments) / len(comments), 1),
        "reddit_max_comments":     max(comments),
        "reddit_total_awards":     awards_tot,
        "reddit_earliest_post":    sorted_dates[0]  if sorted_dates else "n/a",
        "reddit_latest_post":      sorted_dates[-1] if sorted_dates else "n/a",
        "reddit_subreddits":       "; ".join(sorted(subs_seen)),
        "reddit_community_count":  len(subs_seen),
    }
    log(f"  {len(posts)} posts | max_score={max(scores):,} | communities={len(subs_seen)}", "OK")
    time.sleep(random.uniform(2, 4))
    return agg, posts


# =============================================================================
#  MODULE 4 - WAYBACK MACHINE (CDX API)
# =============================================================================

def fetch_wayback(slug):
    log(f"Wayback Machine→ {slug}")
    params = {
        "url":      f"knowyourmeme.com/memes/{slug}",
        "output":   "json",
        "fl":       "timestamp,statuscode",
        "limit":    "1000",
        "collapse": "timestamp:6",
        "filter":   "statuscode:200",
    }
    r = get("http://web.archive.org/cdx/search/cdx", params=params)
    if not r:
        return {}

    try:
        rows = r.json()
    except Exception:
        return {}

    if not rows or len(rows) < 2:
        return {"wayback_total_snapshots": 0}

    rows = rows[1:]
    timestamps  = [row[0] for row in rows if row and len(row[0]) >= 4]
    year_counts = Counter(ts[:4] for ts in timestamps)
    peak_year   = max(year_counts, key=year_counts.get) if year_counts else "n/a"
    years_active = sorted(year_counts.keys())

    out = {
        "wayback_total_snapshots": len(timestamps),
        "wayback_first_snapshot":  timestamps[0][:8]  if timestamps else "n/a",
        "wayback_last_snapshot":   timestamps[-1][:8] if timestamps else "n/a",
        "wayback_peak_year":       peak_year,
        "wayback_years_active":    len(years_active),
        "wayback_year_counts":     json.dumps(dict(sorted(year_counts.items()))),
    }
    log(f"  {len(timestamps)} snapshots | peak={peak_year} | {len(years_active)} active years", "OK")
    time.sleep(random.uniform(1, 3))
    return out


# =============================================================================
#  MODULE 5 - WIKIPEDIA
# =============================================================================

def fetch_wikipedia(term):
    log(f"Wikipedia      → {term}")
    search_term = term.replace(" meme", "").strip()
    params = {
        "action":    "query",
        "titles":    search_term,
        "prop":      "info|categories",
        "inprop":    "url",
        "format":    "json",
        "redirects": 1,
    }
    r = get("https://en.wikipedia.org/w/api.php", params=params)
    if not r:
        return {"wiki_has_article": False}

    try:
        pages   = r.json()["query"]["pages"]
        page    = next(iter(pages.values()))
        missing = "missing" in page
        if missing:
            log("  No Wikipedia article", "OK")
            return {"wiki_has_article": False, "wiki_title": "", "wiki_url": ""}
        cats = [c["title"] for c in page.get("categories", [])]
        out  = {
            "wiki_has_article":  True,
            "wiki_title":        page.get("title", ""),
            "wiki_url":          page.get("fullurl", ""),
            "wiki_categories":   "; ".join(cats[:8]),
        }
        log(f"  Found: {page.get('title','')}", "OK")
        time.sleep(random.uniform(1, 2))
        return out
    except Exception as e:
        log(f"  Wikipedia parse error: {e}", "WARN")
        return {"wiki_has_article": False}


# =============================================================================
#  DERIVED RESEARCH FEATURES
# =============================================================================

def compute_features(rec):
    f = {}

    peak      = rec.get("trends_peak_value") or 0
    max_score = rec.get("reddit_max_score")  or 0
    snaps     = rec.get("wayback_total_snapshots") or 0
    f["virality_score"] = min(100, round(
        (peak * 0.40) +
        (min(max_score, 200_000) / 2000 * 0.40) +
        (min(snaps, 500) / 5 * 0.20),
    1))

    plat_count    = rec.get("kym_platform_count", 0) or 0
    example_count = rec.get("kym_example_count",  0) or 0
    f["adaptability_score"] = min(100, plat_count * 12 + example_count * 3)

    rise  = rec.get("trends_rise_speed")  or 0
    decay = rec.get("trends_decay_speed") or 0
    f["rise_decay_ratio"] = round(rise / decay, 3) if decay else None

    wb_peak = str(rec.get("wayback_peak_year", ""))
    tr_peak = str(rec.get("trends_peak_date",  "") or "")[:4]
    f["mainstream_lag_years"] = (
        int(wb_peak) - int(tr_peak)
        if wb_peak.isdigit() and tr_peak.isdigit() else None
    )

    has_wiki = rec.get("wiki_has_article", False)
    lifespan = rec.get("trends_lifespan_weeks") or 0
    f["cultural_permanence_score"] = min(100, round(
        (50 if has_wiki else 0) + (min(lifespan, 500) / 5), 1
    ))

    lc         = rec.get("trends_lifecycle_label", "")
    kym_status = (rec.get("kym_status") or "").lower()
    was_reborn = rec.get("trends_was_reborn", False)
    if was_reborn or lc == "reborn":
        f["survival_tier"] = "phoenix"
    elif lc == "evergreen" or lifespan > 260:
        f["survival_tier"] = "immortal"
    elif "dead" in kym_status or lc == "dead":
        f["survival_tier"] = "extinct"
    elif lc == "long_lived":
        f["survival_tier"] = "survivor"
    elif lc == "moderate":
        f["survival_tier"] = "fading"
    else:
        f["survival_tier"] = "flash"

    f["mutation_score"] = min(100, round(example_count * 4 + plat_count * 8, 0))
    return f


# =============================================================================
#  MAIN COLLECTION LOOP
# =============================================================================

def run():
    print("\n")
    print("  ╔══════════════════════════════════════════════════════════╗")
    print("  ║   MEME LIFECYCLE DATASET COLLECTOR  v3.0                ║")
    print("  ║   The Hidden Life of Memes - Survival of the Funniest   ║")
    print("  ╚══════════════════════════════════════════════════════════╝\n")
    log(f"Studying {len(MEMES)} memes  |  Output → ./{OUTPUT_DIR}/", "HEAD")

    main_rows    = []
    weekly_rows  = []
    reddit_posts = []
    errors       = []

    for idx, (trend_term, kym_slug, reddit_kw) in enumerate(MEMES, 1):
        meme_name = trend_term.replace(" meme", "").title()
        print(f"\n  {'─'*62}")
        log(f"[{idx:02}/{len(MEMES)}]  {meme_name}", "HEAD")
        print(f"  {'─'*62}")

        row = {
            "meme_name":       meme_name,
            "trend_term":      trend_term,
            "kym_slug":        kym_slug,
            "reddit_keywords": reddit_kw,
            "collected_at":    datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        }

        # Google Trends
        try:
            t      = fetch_trends(trend_term)
            dates  = t.pop("trends_dates",      [])
            values = t.pop("trends_values",     [])
            phases = t.pop("trends_phases",     [])
            norms  = t.pop("trends_normalized", [])
            row.update(t)
            for d, v, ph, nv in zip(dates, values, phases, norms):
                weekly_rows.append({
                    "meme_name":        meme_name,
                    "week_date":        d,
                    "interest_value":   v,
                    "normalized_value": nv,
                    "lifecycle_phase":  ph,
                })
        except Exception as e:
            errors.append(f"{meme_name} | Trends | {e}")
            log(f"Trends failed: {e}", "ERR")

        # Know Your Meme
        try:
            row.update(fetch_kym(kym_slug))
        except Exception as e:
            errors.append(f"{meme_name} | KYM | {e}")
            log(f"KYM failed: {e}", "ERR")

        # Reddit
        try:
            agg, posts = fetch_reddit(reddit_kw, meme_name)
            row.update(agg)
            reddit_posts.extend(posts)
        except Exception as e:
            errors.append(f"{meme_name} | Reddit | {e}")
            log(f"Reddit failed: {e}", "ERR")

        # Wayback Machine
        try:
            row.update(fetch_wayback(kym_slug))
        except Exception as e:
            errors.append(f"{meme_name} | Wayback | {e}")
            log(f"Wayback failed: {e}", "ERR")

        # Wikipedia
        try:
            row.update(fetch_wikipedia(trend_term))
        except Exception as e:
            errors.append(f"{meme_name} | Wikipedia | {e}")
            log(f"Wikipedia failed: {e}", "ERR")

        # Derived features
        try:
            row.update(compute_features(row))
        except Exception as e:
            errors.append(f"{meme_name} | Features | {e}")

        main_rows.append(row)
        log(
            f"Done  tier={row.get('survival_tier','?')}  "
            f"virality={row.get('virality_score','?')}  "
            f"lifecycle={row.get('trends_lifecycle_label','?')}",
            "OK"
        )

    # ── Write CSVs ───────────────────────────────────────────────────────────
    print(f"\n  {'═'*62}")
    log("Writing output files ...", "HEAD")

    # Main dataset
    if main_rows:
        all_cols = list(dict.fromkeys(col for row in main_rows for col in row.keys()))
        fpath = os.path.join(OUTPUT_DIR, "meme_lifecycle_dataset.csv")
        with open(fpath, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=all_cols, extrasaction="ignore")
            w.writeheader()
            w.writerows(main_rows)
        log(f"meme_lifecycle_dataset.csv  {len(main_rows)} rows x {len(all_cols)} columns", "OK")

    # Weekly trends
    if weekly_rows:
        fpath = os.path.join(OUTPUT_DIR, "meme_weekly_trends.csv")
        with open(fpath, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=[
                "meme_name","week_date","interest_value","normalized_value","lifecycle_phase"
            ])
            w.writeheader()
            w.writerows(weekly_rows)
        log(f"meme_weekly_trends.csv      {len(weekly_rows)} rows", "OK")

    # Reddit posts
    if reddit_posts:
        fpath = os.path.join(OUTPUT_DIR, "meme_reddit_posts.csv")
        with open(fpath, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=[
                "meme_name","post_title","post_date","subreddit",
                "score","num_comments","awards","url"
            ])
            w.writeheader()
            w.writerows(reddit_posts)
        log(f"meme_reddit_posts.csv       {len(reddit_posts)} rows", "OK")

    # Report
    tier_counts = Counter(r.get("survival_tier", "?") for r in main_rows)
    lc_counts   = Counter(r.get("trends_lifecycle_label", "?") for r in main_rows)
    sorted_vir  = sorted(main_rows, key=lambda x: x.get("virality_score") or 0, reverse=True)

    lines = [
        "=" * 68,
        "  MEME LIFECYCLE DATASET — COLLECTION REPORT",
        f"  Generated : {datetime.now(tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "=" * 68,
        f"  Memes studied        : {len(main_rows)}",
        f"  Weekly trend rows    : {len(weekly_rows)}",
        f"  Reddit posts         : {len(reddit_posts)}",
        f"  Errors               : {len(errors)}",
        "",
        "  SURVIVAL TIER",
        *[f"    {t:<18} {c}" for t, c in tier_counts.most_common()],
        "",
        "  LIFECYCLE LABELS",
        *[f"    {l:<18} {c}" for l, c in lc_counts.most_common()],
        "",
        "  VIRALITY RANKING",
        *[f"    {r['meme_name']:<35} {str(r.get('virality_score','n/a')):>6}" for r in sorted_vir],
        "",
        "  ERRORS",
        *(errors if errors else ["    None"]),
        "",
        "  FILES",
        f"    {OUTPUT_DIR}/meme_lifecycle_dataset.csv",
        f"    {OUTPUT_DIR}/meme_weekly_trends.csv",
        f"    {OUTPUT_DIR}/meme_reddit_posts.csv",
        "=" * 68,
    ]

    report = "\n".join(lines)
    print("\n" + report)
    fpath = os.path.join(OUTPUT_DIR, "meme_collection_report.txt")
    with open(fpath, "w", encoding="utf-8") as f:
        f.write(report)
    log("meme_collection_report.txt  saved", "OK")
    print(f"\n  All done. Open ./{OUTPUT_DIR}/ to find your dataset.\n")


if __name__ == "__main__":
    run()

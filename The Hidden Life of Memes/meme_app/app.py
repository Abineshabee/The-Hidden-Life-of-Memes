"""
Meme Lifecycle Explorer — Flask App v2
========================================
SETUP:
    pip install flask pandas

CSV PATHS — set all three below:
    WEEKLY_CSV    = meme_weekly_trends.csv
    LIFECYCLE_CSV = meme_lifecycle_dataset.csv
    REDDIT_CSV    = meme_reddit_posts.csv

Run:   python app.py
Open:  http://localhost:5000
"""

from flask import Flask, render_template, jsonify, abort
import pandas as pd
import json, os, math

app = Flask(__name__)

# ── SET YOUR CSV PATHS HERE ───────────────────────────────────────────────────
WEEKLY_CSV    = "meme_weekly_trends.csv"
LIFECYCLE_CSV = "meme_lifecycle_dataset.csv"
REDDIT_CSV    = "meme_reddit_posts.csv"
# e.g. WEEKLY_CSV = r"C:\Users\You\Downloads\meme_weekly_trends.csv"
# ─────────────────────────────────────────────────────────────────────────────

PHASE_COLORS = {
    "pre_birth": "#444654", "growth": "#1DD1A1", "peak": "#FECA57",
    "post_peak": "#FF9F43", "decay": "#FF6B6B", "dead": "#636E72",
    "reborn":    "#A29BFE", "unknown": "#74B9FF",
}

TIER_COLORS = {
    "phoenix": "#A29BFE", "immortal": "#FECA57", "survivor": "#1DD1A1",
    "extinct": "#FF6B6B", "fading":   "#FF9F43", "flash":    "#48DBFB",
}

MEME_PALETTE = [
    "#FF6B6B","#FF9F43","#FECA57","#1DD1A1","#48DBFB",
    "#A29BFE","#FF78C4","#FD79A8","#00CEC9","#6C5CE7",
    "#E17055","#74B9FF","#55EFC4","#FDCB6E","#E84393",
    "#0984E3","#00B894","#D63031","#F368E0","#B2BEC3",
]

# ── DATA LOADERS ──────────────────────────────────────────────────────────────
_cache = {}

def load_weekly():
    if "weekly" not in _cache:
        df = pd.read_csv(WEEKLY_CSV, parse_dates=["week_date"])
        df["week_date"] = pd.to_datetime(df["week_date"])
        df = df.sort_values(["meme_name","week_date"])
        _cache["weekly"] = df
    return _cache["weekly"]

def load_lifecycle():
    if "lifecycle" not in _cache:
        df = pd.read_csv(LIFECYCLE_CSV)
        _cache["lifecycle"] = df
    return _cache["lifecycle"]

def load_reddit():
    if "reddit" not in _cache:
        df = pd.read_csv(REDDIT_CSV, parse_dates=["post_date"])
        df["post_date"] = pd.to_datetime(df["post_date"], errors="coerce")
        _cache["reddit"] = df
    return _cache["reddit"]

def make_slug(name):
    return name.lower().replace(" ", "-")

def get_color(memes_list, name):
    idx = sorted(memes_list).index(name) if name in memes_list else 0
    return MEME_PALETTE[idx % len(MEME_PALETTE)]

def safe(val, default="N/A"):
    if val is None: return default
    try:
        if isinstance(val, float) and math.isnan(val): return default
    except: pass
    return val

def fmt_num(val):
    try:
        v = float(val)
        if math.isnan(v): return "N/A"
        if v >= 1_000_000: return f"{v/1_000_000:.1f}M"
        if v >= 1_000:     return f"{v/1_000:.1f}K"
        return str(int(v)) if v == int(v) else f"{v:.1f}"
    except:
        return "N/A"

# ── HOME ──────────────────────────────────────────────────────────────────────
@app.route("/")
def home():
    wdf   = load_weekly()
    ldf   = load_lifecycle()
    # Use lifecycle CSV as master list — it has all computed labels
    memes = sorted(ldf["meme_name"].unique().tolist())
    cards = []
    for i, name in enumerate(memes):
        # Weekly data only for sparkline
        sub   = wdf[wdf["meme_name"] == name]
        spark = sub.tail(52)["normalized_value"].round(3).tolist() if len(sub) else []
        # All labels and stats from lifecycle CSV only
        row   = ldf[ldf["meme_name"] == name]
        lrow  = row.iloc[0] if len(row) else None
        tier      = safe(lrow["survival_tier"]           if lrow is not None else None, "unknown")
        lifecycle = safe(lrow["trends_lifecycle_label"]  if lrow is not None else None, "unknown")
        peak      = int(safe(lrow["trends_peak_value"]   if lrow is not None else None, 0))
        virality  = safe(lrow["virality_score"]          if lrow is not None else None, 0)
        peak_date = safe(lrow["trends_peak_date"]        if lrow is not None else None, "")
        img = name.lower().replace(" ", "_") + ".jpg"
        cards.append({
            "name": name, "slug": make_slug(name),
            "color": MEME_PALETTE[i % len(MEME_PALETTE)],
            "peak": peak, "lifecycle": lifecycle, "tier": tier,
            "virality": virality, "peak_date": str(peak_date)[:7],
            "spark": spark, "img": img,
        })
    return render_template("index.html",
        view="home", cards=cards,
        total_memes=len(memes),
        total_weeks=len(wdf),
    )

# ── MEME DETAIL ───────────────────────────────────────────────────────────────
@app.route("/meme/<slug>")
def meme_detail(slug):
    wdf   = load_weekly()
    ldf   = load_lifecycle()
    memes = sorted(ldf["meme_name"].unique().tolist())  # master list from lifecycle CSV
    smap  = {make_slug(m): m for m in memes}
    name  = smap.get(slug)
    if not name: abort(404)
    idx   = memes.index(name)
    color = MEME_PALETTE[idx % len(MEME_PALETTE)]
    df_m  = wdf[wdf["meme_name"] == name]  # weekly only for charts
    row   = ldf[ldf["meme_name"] == name]
    lrow  = row.iloc[0].to_dict() if len(row) else {}
    # clean NaN
    lrow  = {k: (None if isinstance(v, float) and math.isnan(v) else v) for k,v in lrow.items()}
    phases_dist = df_m["lifecycle_phase"].value_counts().to_dict()
    prev_name = memes[idx-1] if idx > 0 else memes[-1]
    next_name = memes[idx+1] if idx < len(memes)-1 else memes[0]
    return render_template("index.html",
        view="detail", name=name, slug=slug, color=color,
        lrow=lrow, phases_dist=phases_dist,
        phase_colors=PHASE_COLORS,
        img=name.lower().replace(" ", "_") + ".jpg",
        prev_slug=make_slug(prev_name), prev_name=prev_name,
        next_slug=make_slug(next_name), next_name=next_name,
        all_memes=[{"name":m,"slug":make_slug(m)} for m in memes],
        safe=safe, fmt=fmt_num,
    )

# ── EXPLORE PAGE ──────────────────────────────────────────────────────────────
@app.route("/explore")
def explore():
    ldf  = load_lifecycle()
    rdf  = load_reddit()
    memes = sorted(ldf["meme_name"].unique().tolist())

    # Survival tier distribution
    tier_dist = ldf["survival_tier"].value_counts().to_dict()

    # Lifecycle label distribution
    lc_dist = ldf["trends_lifecycle_label"].value_counts().to_dict()

    # Top memes by virality
    top_virality = (ldf[["meme_name","virality_score","survival_tier"]]
        .dropna(subset=["virality_score"])
        .sort_values("virality_score", ascending=False)
        .head(20)
        .to_dict("records"))

    # Origin platforms
    origin_counts = ldf["kym_origin_platform"].value_counts().to_dict()

    # Lifespan data
    lifespan_data = (ldf[["meme_name","trends_lifespan_weeks"]]
        .dropna()
        .sort_values("trends_lifespan_weeks", ascending=False)
        .to_dict("records"))

    # Reddit top posts overall
    top_reddit = (rdf.sort_values("score", ascending=False)
        .head(15)[["meme_name","post_title","post_date","subreddit","score","num_comments","awards","url"]]
        .fillna("")
        .to_dict("records"))

    # Reddit activity by meme
    reddit_by_meme = (rdf.groupby("meme_name")
        .agg(total_score=("score","sum"), max_score=("score","max"),
             posts=("score","count"), avg_comments=("num_comments","mean"))
        .reset_index()
        .sort_values("total_score", ascending=False)
        .to_dict("records"))

    # Reddit posts over time (monthly)
    rdf2 = rdf.copy()
    rdf2["month"] = rdf2["post_date"].dt.to_period("M").astype(str)
    monthly_reddit = (rdf2.groupby("month")["score"].sum()
        .reset_index().sort_values("month").to_dict("records"))

    # Wayback years active vs lifespan scatter
    scatter_data = (ldf[["meme_name","wayback_years_active","trends_lifespan_weeks","virality_score","survival_tier"]]
        .dropna().to_dict("records"))

    # Rise vs decay speed
    rise_decay = (ldf[["meme_name","trends_rise_speed","trends_decay_speed","survival_tier"]]
        .dropna().to_dict("records"))

    # Platform spread
    plat_data = (ldf[["meme_name","kym_platform_count","virality_score"]]
        .dropna().to_dict("records"))

    # Full lifecycle table
    cols = ["meme_name","kym_year","kym_origin_platform","trends_peak_date",
            "trends_lifespan_weeks","trends_peak_value","virality_score",
            "adaptability_score","cultural_permanence_score","survival_tier",
            "trends_was_reborn","wiki_has_article"]
    table_df = ldf[cols].copy()
    table_df = table_df.fillna("N/A")
    table_rows = table_df.to_dict("records")

    colors_map = {m: MEME_PALETTE[i % len(MEME_PALETTE)] for i,m in enumerate(memes)}

    return render_template("explore.html",
        tier_dist=tier_dist, lc_dist=lc_dist,
        top_virality=top_virality, origin_counts=origin_counts,
        lifespan_data=lifespan_data, top_reddit=top_reddit,
        reddit_by_meme=reddit_by_meme, monthly_reddit=monthly_reddit,
        scatter_data=scatter_data, rise_decay=rise_decay,
        plat_data=plat_data, table_rows=table_rows,
        colors_map=colors_map, tier_colors=TIER_COLORS,
        meme_palette=MEME_PALETTE, total_memes=len(memes),
        total_reddit=len(rdf),
    )

# ── JSON APIs ─────────────────────────────────────────────────────────────────
@app.route("/api/meme/<slug>")
def api_meme(slug):
    wdf  = load_weekly()
    memes = wdf["meme_name"].unique().tolist()
    smap  = {make_slug(m): m for m in memes}
    name  = smap.get(slug)
    if not name: abort(404)
    df_m = wdf[wdf["meme_name"] == name].sort_values("week_date").copy()
    phases = df_m["lifecycle_phase"].tolist()
    timeline = {
        "dates":      df_m["week_date"].dt.strftime("%Y-%m-%d").tolist(),
        "values":     df_m["interest_value"].tolist(),
        "normalized": df_m["normalized_value"].round(4).tolist(),
        "phases":     phases,
    }
    phase_dist = df_m["lifecycle_phase"].value_counts().to_dict()
    df_m["year"] = df_m["week_date"].dt.year
    yearly = df_m.groupby("year")["interest_value"].mean().reset_index()
    yearly_data = {"labels": yearly["year"].tolist(),
                   "values": [round(float(v),1) for v in yearly["interest_value"]]}
    segs, cur_phase, cur_start = [], None, None
    for _, row in df_m.iterrows():
        if row["lifecycle_phase"] != cur_phase:
            if cur_phase:
                segs.append({"phase":cur_phase,"start":cur_start,"end":row["week_date"].strftime("%Y-%m-%d")})
            cur_phase  = row["lifecycle_phase"]
            cur_start  = row["week_date"].strftime("%Y-%m-%d")
    if cur_phase:
        segs.append({"phase":cur_phase,"start":cur_start,"end":df_m["week_date"].iloc[-1].strftime("%Y-%m-%d")})
    return jsonify({"name":name,"timeline":timeline,"phase_dist":phase_dist,
                    "yearly":yearly_data,"phase_segments":segs,"phase_colors":PHASE_COLORS})

@app.route("/api/compare")
def api_compare():
    wdf   = load_weekly()
    ldf   = load_lifecycle()
    memes = sorted(ldf["meme_name"].unique().tolist())  # from lifecycle CSV
    out   = []
    for i, name in enumerate(memes):
        sub  = wdf[wdf["meme_name"] == name]
        lrow = ldf[ldf["meme_name"] == name]
        v = lrow.iloc[0]["virality_score"] if len(lrow) else 0
        t = lrow.iloc[0]["survival_tier"]  if len(lrow) else "unknown"
        out.append({"name":name,"peak":int(sub["interest_value"].max()),
                    "avg":round(float(sub["interest_value"].mean()),1),
                    "virality":float(v) if v and not (isinstance(v,float) and math.isnan(v)) else 0,
                    "tier":str(t),"color":MEME_PALETTE[i % len(MEME_PALETTE)]})
    out.sort(key=lambda x: x["virality"], reverse=True)
    return jsonify(out)

@app.route("/lifecycle")
def lifecycle():
    return render_template("lifecycle_algorithm_explorer.html")


if __name__ == "__main__":
    print("\n  Meme Lifecycle Explorer v2")
    print(f"  Weekly CSV    : {WEEKLY_CSV}")
    print(f"  Lifecycle CSV : {LIFECYCLE_CSV}")
    print(f"  Reddit CSV    : {REDDIT_CSV}")
    print("  Open: http://localhost:5000\n")
    app.run(debug=True, port=5000)

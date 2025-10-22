import os, re, json, glob
from datetime import date
from typing import List, Dict, Any, Tuple
import pandas as pd
import math

# ---------------------------
# Configuration
# ---------------------------
BASE_DIR   = os.path.dirname(__file__) if "__file__" in globals() else "."
FILES_DIR  = os.path.join(BASE_DIR, "aiPerformanceFiles")
OUTPUT_DIR = os.path.join(BASE_DIR, "aiPerformanceText")
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(FILES_DIR,  exist_ok=True)

VALID_MARKETS = ["ALL","AE","QA","SA","JO","BH","NZ","KW","EG","GCC","BET","Others","OTHERS"]
DRILLDOWN = {
    "ALL": ["AE","QA","SA","JO","BH","NZ","KW","EG","Others"],
    "GCC": ["AE","QA","SA","JO","BH","KW"],
    "BET": ["NZ"],
}

# ---------------------------
# Utils
# ---------------------------
def _fmt_num(x, decimals=0):
    try:
        v = float(x)
        if abs(v) >= 1000:
            return f"{v:,.0f}"
        return f"{v:,.{decimals}f}" if decimals>0 else f"{v:,.0f}"
    except Exception:
        return ""

def _pretty_metric(s: str) -> str:
    u = str(s or "").strip().upper()
    if u in {"DEPS","DEPOSITS","DEPOSIT"}: return "Deposits"
    if u == "FTDS": return "FTDs"
    if u == "GGR": return "GGR"
    if u == "NGR": return "NGR"
    if u == "MARGIN": return "Margin"
    if u == "RETENTION": return "Retention"
    return str(s).strip()

def _cleanup_old_ai_text(dir_path: str, today: str):
    patt_a1 = re.compile(r'^anomalies_(\d{4}-\d{2}-\d{2})\.json$', re.I)
    patt_js = re.compile(r'^(\d{4}-\d{2}-\d{2})_main-insights\.(?:js|json)$', re.I)
    for fp in glob.glob(os.path.join(dir_path, '*')):
        base = os.path.basename(fp)
        m = patt_a1.match(base) or patt_js.match(base)
        if not m:
            continue
        file_date = m.group(1)
        if file_date < today:
            try:
                os.remove(fp)
            except Exception:
                pass

# ---------------------------
# Loaders (DIFFERENCES as the main source)
# ---------------------------
def read_csvs_differences() -> pd.DataFrame:
    def normalize(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty: return pd.DataFrame()
        rename_map = {}
        for c in df.columns:
            lc = str(c).lower().strip()
            if lc in ["market","country"]: rename_map[c] = "Market"
            elif lc == "period": rename_map[c] = "Period"
            elif lc in ["metric","kpi"]: rename_map[c] = "METRIC"
            elif lc in ["value","current","current_value","amount"]: rename_map[c] = "VALUE"
            elif lc in ["previous","prev","prior"]: rename_map[c] = "Previous"
            elif lc in ["diff_absolute","diff_abs","delta"]: rename_map[c] = "DIFF_ABSOLUTE"
            elif lc in ["diff_percentage","diffpct","diff%","diff percent","diff_%"]: rename_map[c] = "DIFF_PERCENTAGE"
        out = df.rename(columns=rename_map)
        for col in ["Market","Period","METRIC","VALUE","Previous","DIFF_ABSOLUTE","DIFF_PERCENTAGE"]:
            if col not in out.columns: out[col] = None
        out["Market"]          = out["Market"].astype(str)
        out["Period"]          = out["Period"].astype(str)
        out["METRIC"]          = out["METRIC"].astype(str)
        out["VALUE"]           = pd.to_numeric(out["VALUE"], errors="coerce")
        out["Previous"]        = pd.to_numeric(out["Previous"], errors="coerce")
        out["DIFF_ABSOLUTE"]   = pd.to_numeric(out["DIFF_ABSOLUTE"], errors="coerce")
        out["DIFF_PERCENTAGE"] = pd.to_numeric(out["DIFF_PERCENTAGE"], errors="coerce")
        return out

    dfs = []
    for f in glob.glob(os.path.join(FILES_DIR, "*_ai-summary-differences_*.csv")):
        try:
            df = pd.read_csv(f)
        except Exception:
            try:
                df = pd.read_csv(f, encoding="latin-1")
            except Exception:
                df = pd.DataFrame()
        if not df.empty:
            dfs.append(normalize(df))
    if not dfs:
        return pd.DataFrame(columns=["Market","Period","METRIC","VALUE","Previous","DIFF_ABSOLUTE","DIFF_PERCENTAGE"])
    return pd.concat(dfs, ignore_index=True)

def read_groups_csv(market_code: str) -> pd.DataFrame:
    candidates = glob.glob(os.path.join(FILES_DIR, f"*ai-summary-groups_{market_code}.csv"))
    if not candidates:
        return pd.DataFrame(columns=["Period","Market","METRIC","FTD_Group","VALUE","PREVIOUS","DIFF_ABSOLUTE","DIFF_PERCENTAGE"])
    latest = sorted(candidates)[-1]
    try:
        df = pd.read_csv(latest)
    except Exception:
        df = pd.read_csv(latest, encoding="latin-1")

    rename_map = {
        "market":"Market","country":"Market","period":"Period","metric":"METRIC",
        "ftd_group":"FTD_Group","previous":"PREVIOUS","diff_absolute":"DIFF_ABSOLUTE",
        "diff_percentage":"DIFF_PERCENTAGE"
    }
    out = df.rename(columns={c: rename_map.get(c.lower(), c) for c in df.columns})
    for col in ["VALUE","PREVIOUS","DIFF_ABSOLUTE","DIFF_PERCENTAGE"]:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")
    return out

def markets_from_files(df: pd.DataFrame) -> List[str]:
    if df.empty:
        return [m for m in VALID_MARKETS if m != "OTHERS"]
    ms = sorted(df["Market"].dropna().astype(str).str.upper().unique().tolist())
    # Normalize "OTHERS" to "Others" in list
    ms = [("Others" if m == "OTHERS" else m) for m in ms]
    order = [m for m in ["ALL","GCC","BET","AE","QA","SA","JO","BH","KW","EG","NZ","Others"] if m in ms]
    extras = [m for m in ms if m not in order]
    return order + extras

def periods_from_df(df: pd.DataFrame) -> List[str]:
    if df.empty:
        return ["Yesterday","7 days","30 days","MTD"]
    return sorted(df["Period"].dropna().astype(str).unique().tolist())

# ---------------------------
# Anomaly detection helpers
# ---------------------------
def _select_anomalous_rows(df_sub: pd.DataFrame, top_n: int = 5, min_abs_pct: float = 25.0) -> pd.DataFrame:
    d = df_sub.copy()
    d = d[pd.notna(d["DIFF_PERCENTAGE"])]
    d["ABS__"] = d["DIFF_PERCENTAGE"].abs()
    d = d.sort_values(["ABS__","DIFF_ABSOLUTE"], ascending=[False, False])
    d = d[d["ABS__"] >= min_abs_pct]
    return d.head(top_n)

def _drilldown_ftd_group(metric: str, period: str, parent_market: str, sign: int) -> Tuple[str,str,Dict[str,float]]:
    sub_markets = DRILLDOWN.get(parent_market.upper(), [parent_market.upper()])
    best_score, best_row, best_market = None, None, None
    for m in sub_markets:
        gdf = read_groups_csv(m)
        if gdf.empty:
            continue
        block = gdf[(gdf["Period"].astype(str)==period) & (gdf["METRIC"].astype(str).str.upper()==metric.upper())].copy()
        if block.empty:
            continue
        block = block.sort_values("DIFF_PERCENTAGE", ascending=(sign<0))
        row   = block.iloc[0]
        score = abs(float(row.get("DIFF_PERCENTAGE") or 0))
        if (best_score is None) or (score > best_score):
            best_score, best_row, best_market = score, row, m
    if best_row is None:
        return (parent_market, None, {})
    fig = {
        "value":      float(best_row.get("VALUE", 0) or 0),
        "previous":   float(best_row.get("PREVIOUS", 0) or 0),
        "diff_abs":   float(best_row.get("DIFF_ABSOLUTE", 0) or 0),
        "diff_pct":   float(best_row.get("DIFF_PERCENTAGE", 0) or 0),
    }
    return (best_market, str(best_row.get("FTD_Group")), fig)

def _is_currency_metric(metric: str) -> bool:
    u = str(metric or "").upper().strip()
    return u in {"DEPS","DEPOSITS","DEPOSIT","STAKE","NGR","GGR"}

def _is_percent_metric(metric: str) -> bool:
    u = str(metric or "").upper().strip()
    return u in {"MARGIN","RETENTION"}

_SEGMENT_LONG = {
    "[FTDs]": "new players",
    "[1-3]": "the segment of users that have made their first deposit between one and three months ago",
    "[4-12]": "the segment of users that have made their first deposit between four and twelve months ago",
    "[13-24]": "the segment of users that have made their first deposit between one and two years ago",
    "[> 25]": "the segment of users that have made their first deposit more than two years ago",
}

def _normalize_segment_tag(tag: str) -> str:
    if not tag: return ""
    t = str(tag).strip()
    t = t.replace("[ >25]", "[> 25]").replace("[>25]", "[> 25]")
    if not t.startswith("["): t = f"[{t}]"
    return t

def _segment_phrase(ftd_group: str) -> str:
    tg = _normalize_segment_tag(ftd_group)
    if tg in _SEGMENT_LONG:
        return _SEGMENT_LONG[tg]
    return "the overall mix of segments"

def _fmt_with_unit(metric: str, value) -> str:
    if value is None or (isinstance(value, float) and (pd.isna(value) or not math.isfinite(value))):
        return ""
    if _is_currency_metric(metric):
        return f"${_fmt_num(value)}"
    if _is_percent_metric(metric):
        return f"{_fmt_num(value)}%"
    return _fmt_num(value)

def _location_prefix(metric: str, area_market: str) -> str:
    mname = _pretty_metric(metric)
    am = str(area_market or "").strip()
    if am.lower() == "general":
        return f"General {mname}"
    if am.upper() in {"OTHERS","OTHERS*"} or am == "Others":
        return f"In the other countries, the {mname}"
    return f"In {am}, the {mname}"

def _pct_to_text(prev, val, diff_abs, raw_pct) -> Tuple[str, bool]:
    try:
        pct = float(raw_pct) if raw_pct is not None else None
    except Exception:
        pct = None
    if pct is None or not math.isfinite(pct):
        if (prev is not None) and float(prev) == 0 and (val is not None) and float(val) != 0:
            return "∞%", (diff_abs or 0) > 0
        return "0%", (diff_abs or 0) > 0
    return f"{int(round(abs(pct)))}%", pct >= 0

def _compose_change_phrase(change_word: str, diff_abs_txt: str, pct_txt: str) -> str:
    diff_abs_txt = (diff_abs_txt or "").strip()
    if diff_abs_txt:
        return f"{change_word} {diff_abs_txt} ({pct_txt})"
    # no absolute value -> speak only in percentages
    return f"{change_word} {pct_txt}"

def _build_bullet(market: str,
                  period: str,
                  metric: str,
                  row: Dict[str, Any],
                  area_market: str,
                  ftd_group: str,
                  figs: Dict[str, float]) -> Dict[str, Any]:
    # Prefer drilled figures when available
    val      = figs.get("value",        row.get("VALUE"))
    prev     = figs.get("previous",     row.get("Previous"))
    diff_abs = figs.get("diff_abs",     row.get("DIFF_ABSOLUTE"))
    raw_pct  = figs.get("diff_pct",     row.get("DIFF_PERCENTAGE"))

    pct_txt, increased = _pct_to_text(prev, val, diff_abs, raw_pct)
    change_word = "increased" if increased else "decreased"

    title = f"{_pretty_metric(metric)} anomaly"
    diff_abs_txt = _fmt_with_unit(metric, diff_abs)
    total_txt    = _fmt_with_unit(metric, val)
    seg_phrase   = _segment_phrase(ftd_group) if ftd_group else "the overall mix of segments"
    start        = _location_prefix(metric, area_market)

    change_phrase = _compose_change_phrase(change_word, diff_abs_txt, pct_txt)

    # Build sentence avoiding empty ", to a total of ,"
    parts = [f"{start} {change_phrase}"]
    if total_txt:
        parts.append(f"to a total of {total_txt}")
    parts.append(f"mainly driven by {seg_phrase}.")
    text = ", ".join(parts[:-1]) + ", " + parts[-1]

    return {"metric": _pretty_metric(metric), "title": title, "text": text}

def _build_filler_bullet(row: pd.Series, market: str, period: str, mode: str) -> Dict[str, Any]:
    metric = str(row["METRIC"])
    val    = row.get("VALUE")
    prev   = row.get("Previous")
    diff_a = row.get("DIFF_ABSOLUTE")
    rawpct = row.get("DIFF_PERCENTAGE")
    pct_txt, increased = _pct_to_text(prev, val, diff_a, rawpct)
    mname  = _pretty_metric(metric)
    total_txt = _fmt_with_unit(metric, val)
    diff_txt  = _fmt_with_unit(metric, diff_a)
    area = "General" if market.upper() in DRILLDOWN else market
    start = _location_prefix(metric, area)
    if mode == "stability":
        title = f"{mname} stability"
        base  = f"{start} remained relatively stable"
        if total_txt:
            base += f" at {total_txt}"
        text  = f"{base} ({pct_txt} vs previous), with no major segment shifts."
    else:
        title = f"{mname} trend"
        trend_word = "increase" if increased else "decrease"
        base  = f"{start} showed a mild {trend_word}"
        if diff_txt:
            base += f" of {diff_txt}"
        text  = f"{base} ({pct_txt})"
        if total_txt:
            text += f" to {total_txt}"
        text += ", with the overall mix of segments."
    return {"metric": mname, "title": title, "text": text}

def _ensure_four_bullets(df_diff: pd.DataFrame,
                         bullets: List[Dict[str, Any]],
                         market: str, period: str) -> List[Dict[str, Any]]:
    if len(bullets) >= 4:
        return bullets[:4]

    sub = df_diff[(df_diff["Market"].str.upper()==market.upper()) & (df_diff["Period"].astype(str)==period)].copy()
    if sub.empty:
        while len(bullets) < 4:
            bullets.append({"metric":"Retention","title":"Retention stability","text":"General Retention remained relatively stable, with no major segment shifts."})
        return bullets[:4]

    used_metrics = {b.get("metric") for b in bullets}

    # Prefer adding a Retention note if missing
    ret = sub[sub["METRIC"].astype(str).str.upper()=="RETENTION"]
    if not ret.empty and "Retention" not in used_metrics and len(bullets) < 4:
        r = ret.iloc[0]
        mode = "stability" if abs(float(r.get("DIFF_PERCENTAGE") or 0)) < 10 else "trend"
        bullets.append(_build_filler_bullet(r, market, period, mode))
        used_metrics.add("Retention")

    # Fill remaining with smallest |%| first (stability), then larger as trend
    sub["ABS_"] = sub["DIFF_PERCENTAGE"].abs()
    for _, r in sub.sort_values("ABS_").iterrows():
        if len(bullets) >= 4: break
        mname = _pretty_metric(r["METRIC"])
        if mname in used_metrics: 
            continue
        abs_pct = abs(float(r.get("DIFF_PERCENTAGE") or 0))
        mode = "stability" if abs_pct < 10 else "trend"
        b = _build_filler_bullet(r, market, period, mode)
        bullets.append(b)
        used_metrics.add(mname)

    while len(bullets) < 4:
        bullets.append({"metric":"Retention","title":"Retention stability","text":"General Retention remained relatively stable, with no major segment shifts."})
    return bullets[:4]

def detect_anomalies(df_diff: pd.DataFrame,
                     market: str,
                     period: str,
                     top_n: int = 5,
                     min_abs_pct: float = 25.0) -> List[Dict[str, Any]]:
    sub = df_diff[(df_diff["Market"].str.upper()==market.upper()) & (df_diff["Period"].astype(str)==period)].copy()
    if sub.empty:
        return []
    picks = _select_anomalous_rows(sub, top_n=top_n, min_abs_pct=min_abs_pct)
    bullets: List[Dict[str, Any]] = []
    for _, r in picks.iterrows():
        metric = str(r["METRIC"])
        sign   = 1 if float(r["DIFF_PERCENTAGE"]) >= 0 else -1
        area_market, ftd_group, figs = _drilldown_ftd_group(metric, period, market, sign)

        # Composite and no group row -> use "General ..." with parent figures
        if market.upper() in DRILLDOWN and area_market.upper() == market.upper():
            area_market = "General"
            ftd_group = None
            figs = {}
        use_market = area_market if market.upper() in DRILLDOWN else market
        bullets.append(_build_bullet(market, period, metric, r, use_market, ftd_group, figs))

    return _ensure_four_bullets(df_diff, bullets, market, period)

# ---------------------------
# Runner
# ---------------------------
def run():
    df = read_csvs_differences()
    # Normalize "OTHERS" to "Others" in data for consistency
    if not df.empty:
        df["Market"] = df["Market"].str.replace("^OTHERS$", "Others", regex=True)
    mkts    = markets_from_files(df)
    periods = periods_from_df(df)
    today   = date.today().strftime("%Y-%m-%d")
    _cleanup_old_ai_text(OUTPUT_DIR, today)

    insights_map: Dict[str, Dict[str, Any]] = {}

    for market in mkts:
        insights_map.setdefault(market, {})
        for period in periods:
            print(f"→ {market} / {period}")
            bullets = detect_anomalies(df, market, period, top_n=5, min_abs_pct=25.0)
            insights_map[market][period] = {"bullets": bullets}

    # outputs
    out_json = os.path.join(OUTPUT_DIR, f"anomalies_{today}.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(insights_map, f, ensure_ascii=False, indent=2)

    dated_js = os.path.join(OUTPUT_DIR, f"{today}_main-insights.js")
    with open(dated_js, "w", encoding="utf-8") as f2:
        f2.write("window.AI_INSIGHTS = ")
        json.dump(insights_map, f2, ensure_ascii=False)
        f2.write(";")

if __name__ == "__main__":
    run()
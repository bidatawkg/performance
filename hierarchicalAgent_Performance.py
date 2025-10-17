
#!/usr/bin/env python3
"""
AI Insights Agent (DeepSeek) — Adapter for Performance Monitor dashboard

What it does
------------
Reads your aiPerformanceFiles/*_ai-summary-targets_*.csv files and asks DeepSeek
to draft 4 short executive insights for each (Market, Period):

- critical_alert
- trend_analysis
- retention_issue
- opportunity

It then writes (all into aiPerformanceText/):
  1) anomalies_YYYY-MM-DD.json  -> full structured list
  2) YYYY-MM-DD_main-insights.js -> window.AI_INSIGHTS = {..} for the dashboard.

Integration (minimal)
---------------------
In your build_dashboard_Final.py template HTML:
1) Include the dated file (today):
   <script src="aiPerformanceText/YYYY-MM-DD_main-insights.js"></script>

2) Give ids to the four insight boxes:
   <div class="insight-box" id="insCritical">...</div>
   <div class="insight-box" id="insTrend">...</div>
   <div class="insight-box" id="insRetention">...</div>
   <div class="insight-box" id="insOpportunity">...</div>

3) In the <script> of the HTML, add a function and hook into refreshAll():
   function renderInsights(){
     const p = document.getElementById('periodSelect').value;
     const m = document.getElementById('marketSelect').value;
     const data = (window.AI_INSIGHTS && window.AI_INSIGHTS[m] && window.AI_INSIGHTS[m][p]) || null;
     const setText = (id, txt) => { const el = document.getElementById(id); if (el) el.innerHTML = txt || '[No insight]'; };
     setText('insCritical', data ? (data.critical_alert || '[No insight]') : '[No insight]');
     setText('insTrend', data ? (data.trend_analysis || '[No insight]') : '[No insight]');
     setText('insRetention', data ? (data.retention_issue || '[No insight]') : '[No insight]');
     setText('insOpportunity', data ? (data.opportunity || '[No insight]') : '[No insight]');
   }
   // call it when filters change and on init:
   function refreshAll(){ renderCards(); renderStacked(); renderGauges(); renderInsights(); }

Notes
-----
- If DEEPSEEK_API_KEY is missing or the API fails, the script falls back to
  deterministic heuristics so you still get useful insights.
- The prompt is concise and asks DeepSeek to return strict JSON. We additionally
  sanitize/repair the JSON if the model returns code fences or extra prose.

Author: IA Project
"""

import os, re, json, glob, time
from io import StringIO
from datetime import date
from typing import List, Dict, Any, Tuple
import pandas as pd

def _cleanup_old_ai_text(dir_path: str, today: str):
    """Delete generated files older than `today` in aiPerformanceText/.
    Matches BOTH naming schemes:
      1) anomalies_YYYY-MM-DD.json
      2) YYYY-MM-DD_anomalies.json
      3) YYYY-MM-DD_main-insights.js/json
    """
    try:
        import re, os, glob
        patt_a1 = re.compile(r'^anomalies_(\d{4}-\d{2}-\d{2})\.json$', re.I)
        patt_a2 = re.compile(r'^(\d{4}-\d{2}-\d{2})_anomalies\.json$', re.I)
        patt_js = re.compile(r'^(\d{4}-\d{2}-\d{2})_main-insights\.(?:js|json)$', re.I)
        for fp in glob.glob(os.path.join(dir_path, '*')):
            base = os.path.basename(fp)
            m = patt_a1.match(base) or patt_a2.match(base) or patt_js.match(base)
            if not m:
                continue
            file_date = m.group(1)
            if file_date < today:
                try:
                    os.remove(fp)
                    print('🧹 Deleted old file:', base)
                except Exception as e:
                    print('⚠️ Could not delete', base, '->', e)
    except Exception as e:
        print('⚠️ Cleanup failed:', e)
    """
    Delete any generated files with a date prefix older than today, e.g.:
      YYYY-MM-DD_main-insights.js
      anomalies_YYYY-MM-DD.json
    """
    try:
        import re, os, glob
        patt_js = re.compile(r"^(\d{4}-\d{2}-\d{2})_main-insights\.js$")
        patt_json = re.compile(r"^anomalies_(\d{4}-\d{2}-\d{2})\.json$")
        for fp in glob.glob(os.path.join(dir_path, "*")):
            base = os.path.basename(fp)
            m = patt_js.match(base) or patt_json.match(base)
            if m and m.group(1) < today:
                try:
                    os.remove(fp)
                    print("🧹 Deleted old file:", fp)
                except Exception as e:
                    print("⚠️ Could not delete", fp, "->", e)
    except Exception as e:
        print("⚠️ Cleanup failed:", e)

# ---------------------------
# Configuration
# ---------------------------
BASE_DIR = os.path.dirname(__file__) if '__file__' in globals() else "."
FILES_DIR = os.path.join(BASE_DIR, "aiPerformanceFiles")
OUTPUT_DIR = os.path.join(BASE_DIR, "aiPerformanceText")
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(FILES_DIR, exist_ok=True)

# You can tailor the markets you care about here. We'll auto-detect from filenames too.
VALID_MARKETS = ["ALL","AE","QA","SA","JO","BH","NZ","KW","EG","GCC","BET","Others"]

# ---------------------------
# Optional DeepSeek client
# ---------------------------
def _get_deepseek_client():
    """
    Lazily import OpenAI with a DeepSeek base_url if the API key is present.
    Returns (client, is_available: bool).
    """
    api = os.getenv("DEEPSEEK_API_KEY")
    if not api:
        return None, False
    try:
        from openai import OpenAI
        client = OpenAI(api_key=api, base_url="https://api.deepseek.com/v1")
        return client, True
    except Exception as e:
        print("⚠️ Failed to initialize DeepSeek client:", e)
        return None, False

def _safe_chat_complete(client, messages, retries=2, delay=8):
    """
    Call DeepSeek with exponential backoff.
    """
    last_err = None
    for i in range(retries+1):
        try:
            resp = client.chat.completions.create(
                model="deepseek-chat",
                messages=messages,
                temperature=0.2,
            )
            return resp
        except Exception as e:
            last_err = e
            print(f"⚠️ DeepSeek error (attempt {i+1}/{retries+1}): {e}")
            time.sleep(delay * (i+1))
    raise last_err

# ---------------------------
# CSV utils
# ---------------------------

# ---------- Metric aliases & pretty names ----------
DEPS_ALIASES = {"DEPS","DEPOSITS","DEPOSIT"}
NGR_ALIASES = {"NGR"}
MARGIN_ALIASES = {"MARGIN"}
RETENTION_ALIASES = {"RETENTION"}

def _metric_match(s, aliases):
    return str(s).strip().upper() in aliases

def _pretty_metric(s):
    u = str(s).strip().upper()
    if u in DEPS_ALIASES: return "Deposits"
    if u in MARGIN_ALIASES: return "Margin"
    if u in RETENTION_ALIASES: return "Retention"
    if u in NGR_ALIASES: return "NGR"
    return str(s).strip().title()


def read_csvs_summary() -> pd.DataFrame:
    """
    Load *summary-targets* CSVs and normalize column names.
    Expected columns (best-effort): Period, Country, METRIC, VALUE, TARGET, DIFF_PERCENTAGE, DIFF_ABSOLUTE, Date
    """
    def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        rename_map = {}
        for c in df.columns:
            lc = str(c).lower().strip()
            if lc in ["metric","kpi"]:
                rename_map[c] = "METRIC"
            elif lc in ["country","market"]:
                rename_map[c] = "Country"
            elif lc == "period":
                rename_map[c] = "Period"
            elif lc in ["value","amount","current_value","current"]:
                rename_map[c] = "VALUE"
            elif lc == "target":
                rename_map[c] = "TARGET"
            elif lc in ["diff_percentage","diff %","diffpct","diffpercent","diff_percentage_%"]:
                rename_map[c] = "DIFF_PERCENTAGE"
            elif lc == "date":
                rename_map[c] = "Date"
            elif lc in ["diff_abs","diff_absolute","difference_absolute","delta"]:
                rename_map[c] = "DIFF_ABSOLUTE"
        out = df.rename(columns=rename_map)
        out.columns = [str(c).strip() for c in out.columns]
        return out

    dfs = []
    for f in glob.glob(os.path.join(FILES_DIR, "*_ai-summary-targets_*.csv")):
        try:
            df = pd.read_csv(f)
        except Exception:
            try:
                df = pd.read_csv(f, encoding="latin-1")
            except Exception:
                df = pd.DataFrame()
        if df.empty:
            continue
        df = normalize_columns(df)
        # Ensure required columns exist
        for col in ["Period","Country","METRIC","VALUE","TARGET","DIFF_PERCENTAGE","DIFF_ABSOLUTE","Date"]:
            if col not in df.columns:
                df[col] = None
        # Coerce
        df["Period"] = df["Period"].fillna("Yesterday").astype(str)
        df["Country"] = df["Country"].fillna("ALL").astype(str)
        df["METRIC"] = df["METRIC"].fillna("UNKNOWN").astype(str)
        dfs.append(df)
    if not dfs:
        return pd.DataFrame(columns=["Period","Country","METRIC","VALUE","TARGET","DIFF_PERCENTAGE","DIFF_ABSOLUTE","Date"])
    out = pd.concat(dfs, ignore_index=True)
    return out

def markets_from_files(df: pd.DataFrame) -> List[str]:
    if df.empty: 
        return VALID_MARKETS
    ms = sorted(df["Country"].dropna().astype(str).str.upper().unique().tolist())
    # keep known order with extras appended
    order = [m for m in VALID_MARKETS if m in ms]
    extras = [m for m in ms if m not in order]
    return order + extras

def periods_from_df(df: pd.DataFrame) -> List[str]:
    if df.empty:
        return ["Yesterday","7 days","30 days","MTD"]
    return sorted(df["Period"].dropna().astype(str).unique().tolist())

# ---------------------------
# LLM prompt + parse
# ---------------------------
LLM_SYSTEM = "You are a Chief Analytics Officer reviewing casino affiliates acquisition KPIs."
LLM_USER_TEMPLATE = """Analyze the affiliate acquisition performance for MARKET = {market} and PERIOD = {period}.
Use the CSV below (subset of ai-summary-targets) containing columns like Period, Country, METRIC, VALUE, TARGET, DIFF_PERCENTAGE, DIFF_ABSOLUTE, Date.

Return a STRICT JSON array with at most 4 objects (one per category). Follow these preferences hard: critical_alert should preferably be on DEPS/Deposits (if present), trend_analysis should preferably be on NGR, opportunity should preferably be on MARGIN and must be unique (do not repeat another comment), and avoid phrases like "is a small base". 
[
  {{
    "type": "critical_alert" | "trend_analysis" | "retention_issue" | "opportunity",
    "metric": "<metric>",
    "market": "{market}",
    "period": "{period}",
    "finding": "<1–2 sentences with concrete figures and %>",
    "impact": "<High|Medium|Low + short impact description>",
    "confidence": <0.0–1.0>,
    "suggested_action": "<short actionable step>"
  }}
]

Rules:
- Prioritize metrics with the largest negative DIFF_PERCENTAGE for critical_alert.
- Use RETENTION for retention_issue when possible; otherwise pick a related drop-off metric.
- For opportunity, pick a smaller-volume metric/segment that is growing or above target.
- Be concise, keep numbers readable (use % and $ when suitable). 
- Respond with JSON ONLY — no backticks, no prose, no explanations.
CSV:
{csv_text}
"""

def _extract_json(text: str) -> List[Dict[str, Any]]:
    if text is None:
        return []
    t = text.strip()
    # Remove code fences
    if t.startswith("```"):
        t = re.sub(r"^```(?:json)?", "", t, flags=re.I).strip()
        if t.endswith("```"):
            t = t[:-3].strip()
    # Try direct JSON
    try:
        data = json.loads(t)
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return [data]
    except Exception:
        pass
    # Bracket extract fallback
    m = re.search(r"\[.*\]", t, flags=re.S)
    if m:
        try:
            data = json.loads(m.group(0))
            if isinstance(data, list):
                return data
        except Exception:
            pass
    return []

# ---------------------------
# Heuristic fallbacks
# ---------------------------
def _best_row_for_metric(df: pd.DataFrame, market: str, period: str, metric_aliases: List[str]) -> Dict[str, Any]:
    if df.empty:
        return {}
    subset = df[(df["Country"].str.upper()==market.upper()) & (df["Period"].astype(str)==period)]
    if subset.empty:
        return {}
    # match metric aliases case-insensitively
    al = [a.lower() for a in metric_aliases]
    s = subset[subset["METRIC"].str.lower().isin(al)]
    if s.empty:
        return {}
    # choose the row with the largest absolute DIFF_PERCENTAGE magnitude
    s = s.copy()
    s["__abs"] = s["DIFF_PERCENTAGE"].astype(float).abs()
    s = s.sort_values("__abs", ascending=False)
    return s.iloc[0].to_dict()


def _pick_best(df, market, period, aliases, want='abs', sign=None):
    #Pick a row for market/period among metric aliases.
    #want: 'abs' (largest |diff|), 'neg' (most negative diff), 'pos' (most positive diff)
    #sign: optional override ('neg' or 'pos').
    #Returns dict(row) or {}.
    subset = df[(df["Country"].str.upper()==market.upper()) & (df["Period"].astype(str)==period)].copy()
    if subset.empty: return {}
    subset["DIFF_PERCENTAGE"] = pd.to_numeric(subset["DIFF_PERCENTAGE"], errors="coerce")
    subset["VALUE"] = pd.to_numeric(subset["VALUE"], errors="coerce")
    subset["TARGET"] = pd.to_numeric(subset["TARGET"], errors="coerce")
    block = subset[subset["METRIC"].str.upper().isin([a.upper() for a in aliases])]
    if block.empty: 
        return {}
    b = block.copy()
    if (sign or want) == 'neg':
        b = b.sort_values("DIFF_PERCENTAGE", ascending=True)
    elif (sign or want) == 'pos':
        b = b.sort_values("DIFF_PERCENTAGE", ascending=False)
    else:  # abs
        b["__abs"] = b["DIFF_PERCENTAGE"].abs()
        b = b.sort_values("__abs", ascending=False)
    return b.iloc[0].to_dict() if not b.empty else {}

def _fmt_line(metric, dp, val, tgt, template='generic', direction=None):
    name = _pretty_metric(metric)
    # dp may be NaN; guard
    dp_num = float(dp) if dp is not None else 0.0
    val_num = float(val) if val is not None else 0.0
    tgt_num = float(tgt) if tgt is not None else 0.0
    if template=='critical':
        return f"{name}: {dp_num:.1f}% vs target ({val_num:,.0f} vs {tgt_num:,.0f}). Immediate attention required."
    if template=='trend':
        dir_word = 'growth' if (direction=='up' or dp_num>0) else 'decline'
        return f"{name} shows {dir_word} of {abs(dp_num):.1f}% vs target. Monitor sustainability."
    if template=='retention':
        return f"Retention at {val_num:.1f}% ({dp_num:.1f}% vs target {tgt_num:.1f}%). Review CRM cycles, reactivation and bonus policy."
    if template=='oppty_margin':
        # speak in % points feel using explicit values
        return f"Margin at {val_num:.1f}% ({dp_num:.1f}% vs target {tgt_num:.1f}%). Scale profitable channels."
    if template=='oppty_generic':
        sign = '+' if dp_num>=0 else ''
        return f"{name} outperforming {sign}{dp_num:.1f}% vs target. Consider budget shift."
    # fallback
    return f"{name}: {dp_num:.1f}% vs target ({val_num:,.0f} vs {tgt_num:,.0f})."

def _heuristics_four(df: pd.DataFrame, market: str, period: str) -> Dict[str, str]:
    res = {"critical_alert": None, "trend_analysis": None, "retention_issue": None, "opportunity": None}
    subset = df[(df["Country"].str.upper()==market.upper()) & (df["Period"].astype(str)==period)].copy()
    if subset.empty:
        return {k: "[No data]" for k in res}

    subset["DIFF_PERCENTAGE"] = pd.to_numeric(subset["DIFF_PERCENTAGE"], errors="coerce")
    subset["VALUE"] = pd.to_numeric(subset["VALUE"], errors="coerce")
    subset["TARGET"] = pd.to_numeric(subset["TARGET"], errors="coerce")

    # 1) Critical alert — prefer DEPS most negative; else global most negative
    pick = _pick_best(subset, market, period, DEPS_ALIASES, want='neg')
    if pick:
        res["critical_alert"] = _fmt_line(pick["METRIC"], pick["DIFF_PERCENTAGE"], pick["VALUE"], pick["TARGET"], template='critical')
    else:
        worst = subset.sort_values("DIFF_PERCENTAGE", ascending=True).head(1)
        if not worst.empty:
            r = worst.iloc[0]
            res["critical_alert"] = _fmt_line(r["METRIC"], r["DIFF_PERCENTAGE"], r["VALUE"], r["TARGET"], template='critical')

    # 2) Retention issue — prefer explicit RETENTION (below target)
    ret = subset[subset["METRIC"].str.upper().isin(list(RETENTION_ALIASES))].sort_values("DIFF_PERCENTAGE", ascending=True)
    if not ret.empty:
        rr = ret.iloc[0]
        res["retention_issue"] = _fmt_line(rr["METRIC"], rr["DIFF_PERCENTAGE"], rr["VALUE"], rr["TARGET"], template='retention')
    else:
        # fallback: second worst metric
        second = subset.sort_values("DIFF_PERCENTAGE", ascending=True).head(2).tail(1)
        if not second.empty:
            r2 = second.iloc[0]
            res["retention_issue"] = f"{_pretty_metric(r2['METRIC'])} weakness: {float(r2['DIFF_PERCENTAGE']):.1f}% vs target. Investigate cohort drop-offs."

    # 3) Trend analysis — prefer NGR (largest absolute move)
    trow = _pick_best(subset, market, period, NGR_ALIASES, want='abs')
    if trow:
        res["trend_analysis"] = _fmt_line(trow["METRIC"], trow["DIFF_PERCENTAGE"], trow["VALUE"], trow["TARGET"], template='trend')
    else:
        trend = subset.copy(); trend["ABS"] = trend["DIFF_PERCENTAGE"].abs()
        trend = trend.sort_values("ABS", ascending=False).head(1)
        if not trend.empty:
            t = trend.iloc[0]
            res["trend_analysis"] = _fmt_line(t["METRIC"], t["DIFF_PERCENTAGE"], t["VALUE"], t["TARGET"], template='trend')

    # 4) Opportunity — prefer Margin with positive diff; ensure it's not a duplicate and avoid 'small base'
    mpos = subset[(subset["METRIC"].str.upper().isin(list(MARGIN_ALIASES))) & (subset["DIFF_PERCENTAGE"] > 0)].sort_values("DIFF_PERCENTAGE", ascending=False)
    if not mpos.empty:
        m = mpos.iloc[0]
        res["opportunity"] = _fmt_line(m["METRIC"], m["DIFF_PERCENTAGE"], m["VALUE"], m["TARGET"], template='oppty_margin')
    else:
        # fallback: best positive DIFF% on a metric not already used above
        used_metrics = set()
        for k in ["critical_alert","trend_analysis","retention_issue"]:
            txt = res.get(k) or ""
            # add the first token as metric heuristic
            mm = re.match(r"([A-Za-z]+)", txt)
            if mm: used_metrics.add(mm.group(1).upper())
        pos = subset[subset["DIFF_PERCENTAGE"] > 0].copy()
        pos["RANK"] = pos["DIFF_PERCENTAGE"]
        pos = pos.sort_values("RANK", ascending=False)
        for _, row in pos.iterrows():
            mname = str(row["METRIC"]).upper()
            if mname not in used_metrics:
                res["opportunity"] = _fmt_line(row["METRIC"], row["DIFF_PERCENTAGE"], row["VALUE"], row["TARGET"], template='oppty_generic')
                break
        if not res["opportunity"]:
            res["opportunity"] = "No clear upside detected this period."

    # Final cleanups
    for k in list(res.keys()):
        if not res[k]:
            res[k] = "[No insight]"
        # avoid "small base" phrasing per requirement
        res[k] = re.sub(r'small base', 'niche segment', res[k], flags=re.I)

    return res

# ---------------------------
# Orchestration
# ---------------------------
def _reduce_for_prompt(df: pd.DataFrame, market: str, period: str, max_rows: int = 40) -> str:
    subset = df[(df["Country"].str.upper()==market.upper()) & (df["Period"].astype(str)==period)].copy()
    if subset.empty:
        return "Country,Period,METRIC,VALUE,TARGET,DIFF_PERCENTAGE,DIFF_ABSOLUTE,Date\n"
    cols = [c for c in ["Country","Period","METRIC","VALUE","TARGET","DIFF_PERCENTAGE","DIFF_ABSOLUTE","Date"] if c in subset.columns]
    subset = subset[cols]
    # Keep top 12 by absolute diff %, plus RETENTION, plus GLOBAL if present
    subset["__abs"] = pd.to_numeric(subset["DIFF_PERCENTAGE"], errors="coerce").abs()
    important = pd.concat([
        subset.sort_values("__abs", ascending=False).head(12),
        subset[subset["METRIC"].str.upper()=="RETENTION"]
    ]).drop_duplicates()
    # cap max rows
    reduced = important.head(max_rows).drop(columns=["__abs"], errors="ignore")
    return reduced.to_csv(index=False)

def _ensure_four_categories(items: List[Dict[str, Any]], heur: Dict[str, str], market: str, period: str) -> Dict[str, str]:
    # Convert LLM items into our 4-box dict (priority: use LLM, then heuristics)
    mapping = {"critical_alert": None, "trend_analysis": None, "retention_issue": None, "opportunity": None}
    for it in items or []:
        t = str(it.get("type","")).lower().strip()
        finding = it.get("finding") or it.get("insight") or ""
        # compact one-liner with action hint
        if not finding:
            finding = f"{it.get('metric','').upper()} – {it.get('impact','').strip()}."
        if t in mapping and not mapping[t]:
            mapping[t] = finding
    # fill missing with heuristics
    for k,v in list(mapping.items()):
        if not v:
            mapping[k] = heur.get(k, "[No insight]")
    return mapping

def _enforce_preferences(mapping: Dict[str,str], df: pd.DataFrame, market: str, period: str) -> Dict[str,str]:
    #Ensure preferences are met even if LLM didn't follow them.
    # Prepare subset
    subset = df[(df["Country"].str.upper()==market.upper()) & (df["Period"].astype(str)==period)].copy()
    subset["DIFF_PERCENTAGE"] = pd.to_numeric(subset["DIFF_PERCENTAGE"], errors="coerce")
    subset["VALUE"] = pd.to_numeric(subset["VALUE"], errors="coerce")
    subset["TARGET"] = pd.to_numeric(subset["TARGET"], errors="coerce")

    # Critical on DEPS if available and negative
    deps_row = subset[(subset["METRIC"].str.upper().isin(list(DEPS_ALIASES)))].sort_values("DIFF_PERCENTAGE", ascending=True).head(1)
    if not deps_row.empty and pd.notna(deps_row.iloc[0]["DIFF_PERCENTAGE"]) and float(deps_row.iloc[0]["DIFF_PERCENTAGE"]) < 0:
        mapping["critical_alert"] = _fmt_line(deps_row.iloc[0]["METRIC"], deps_row.iloc[0]["DIFF_PERCENTAGE"], deps_row.iloc[0]["VALUE"], deps_row.iloc[0]["TARGET"], template='critical')

    # Trend prefer NGR
    ngr = subset[(subset["METRIC"].str.upper().isin(list(NGR_ALIASES)))].copy()
    if not ngr.empty:
        ngr["ABS"] = ngr["DIFF_PERCENTAGE"].abs()
        r = ngr.sort_values("ABS", ascending=False).iloc[0]
        mapping["trend_analysis"] = _fmt_line(r["METRIC"], r["DIFF_PERCENTAGE"], r["VALUE"], r["TARGET"], template='trend')

    # Opportunity prefer Margin positive; ensure uniqueness and avoid 'small base'
    marg = subset[(subset["METRIC"].str.upper().isin(list(MARGIN_ALIASES))) & (subset["DIFF_PERCENTAGE"]>0)].copy()
    if not marg.empty:
        m = marg.sort_values("DIFF_PERCENTAGE", ascending=False).iloc[0]
        mapping["opportunity"] = _fmt_line(m["METRIC"], m["DIFF_PERCENTAGE"], m["VALUE"], m["TARGET"], template='oppty_margin')
    # De-duplicate if same as another box
    texts = set()
    for k in ["critical_alert","trend_analysis","retention_issue"]:
        if mapping.get(k): texts.add(mapping[k].strip())
    if mapping.get("opportunity") in texts or not mapping.get("opportunity"):
        # pick next best positive metric not used by others
        used = set()
        for k in ["critical_alert","trend_analysis","retention_issue"]:
            t = mapping.get(k) or ""
            mm = re.match(r"([A-Za-z]+)", t)
            if mm: used.add(mm.group(1).upper())
        pos = subset[subset["DIFF_PERCENTAGE"]>0].sort_values("DIFF_PERCENTAGE", ascending=False)
        for _, row in pos.iterrows():
            if str(row["METRIC"]).upper() not in used:
                mapping["opportunity"] = _fmt_line(row["METRIC"], row["DIFF_PERCENTAGE"], row["VALUE"], row["TARGET"], template='oppty_generic')
                break
        if not mapping.get("opportunity"):
            mapping["opportunity"] = "No clear upside detected this period."

    # remove banned phrase
    mapping["opportunity"] = re.sub(r'\bsmall base\b', 'niche segment', mapping["opportunity"], flags=re.I)
    return mapping

    # Convert LLM items into our 4-box dict (priority: use LLM, then heuristics)
    mapping = {"critical_alert": None, "trend_analysis": None, "retention_issue": None, "opportunity": None}
    for it in items or []:
        t = str(it.get("type","")).lower().strip()
        finding = it.get("finding") or it.get("insight") or ""
        # compact one-liner with action hint
        if not finding:
            finding = f"{it.get('metric','').upper()} – {it.get('impact','').strip()}."
        if t in mapping and not mapping[t]:
            mapping[t] = finding
    # fill missing with heuristics
    for k,v in list(mapping.items()):
        if not v:
            mapping[k] = heur.get(k, "[No insight]")
    # Enforce preferences & uniqueness
    mapping = _enforce_preferences(mapping, df_global_cache, market, period) if 'df_global_cache' in globals() else mapping
    return mapping

def run():
    global df_global_cache
    df = read_csvs_summary()
    df_global_cache = df
    mkts = markets_from_files(df)
    periods = periods_from_df(df)
    today = date.today().strftime("%Y-%m-%d")
    # Clean previous-dated files from aiPerformanceText
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    _cleanup_old_ai_text(OUTPUT_DIR, today)

    client, has_llm = _get_deepseek_client()
    all_rows = []
    insights_map = {}  # Market -> Period -> { four boxes }

    for market in mkts:
        insights_map.setdefault(market, {})
        for period in periods:
            print(f"→ {market} / {period}")
            csv_text = _reduce_for_prompt(df, market, period)
            heur = _heuristics_four(df, market, period)

            llm_items = []
            if has_llm and csv_text.strip():
                try:
                    messages = [
                        {"role": "system", "content": LLM_SYSTEM},
                        {"role": "user", "content": LLM_USER_TEMPLATE.format(market=market, period=period, csv_text=csv_text)}
                    ]
                    resp = _safe_chat_complete(client, messages)
                    raw = resp.choices[0].message.content
                    llm_items = _extract_json(raw)
                except Exception as e:
                    print("⚠️ LLM failure; using heuristics", e)

            four = _ensure_four_categories(llm_items, heur, market, period)
            # collect full record rows for JSON dump
            for k, txt in four.items():
                all_rows.append({
                    "market": market,
                    "period": period,
                    "type": k,
                    "text": txt
                })
            insights_map[market][period] = {
                "critical_alert": four["critical_alert"],
                "trend_analysis": four["trend_analysis"],
                "retention_issue": four["retention_issue"],
                "opportunity": four["opportunity"],
            }

    # Outputs
    out_json = os.path.join(OUTPUT_DIR, f"anomalies_{today}.json")
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(all_rows, f, ensure_ascii=False, indent=2)
    print("✔ Wrote", out_json)

    # Dated JS for the dashboard
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    dated_js = os.path.join(OUTPUT_DIR, f"{today}_main-insights.js")
    with open(dated_js, "w", encoding="utf-8") as f2:
        f2.write("window.AI_INSIGHTS = ")
        json.dump(insights_map, f2, ensure_ascii=False)
        f2.write(";")
    print("✔ Wrote", dated_js)

    

if __name__ == "__main__":
    run()

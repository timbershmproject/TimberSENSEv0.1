# -*- coding: utf-8 -*-
"""
TIMBER-SENSE • SMT Query Dashboard (Premium Plotly)
- White, clean UI + meaningful colors
- Zoom/pan + hover (Plotly)
- Plot Converted and/or Raw (Value) with toggles
- Dynamic smoothing (optional): EMA / rolling mean / rolling median
- Threshold overlays (MC + Battery) with color coding
- Query box (regex-based) for quick “find input 5 below 22” style filters
- Robust outlier detection (Rolling MAD z-score + IQR)
- MC recomputation section (from RAW resistance) for Inputs 18/20 using your Excel formula
"""

# ------------------------------------------------------------
# MUST BE FIRST STREAMLIT CALL + ONLY ONCE
# ------------------------------------------------------------
import streamlit as st
st.set_page_config(
    page_title="TIMBER-SENSE • SMT Query Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ------------------------------------------------------------
# Imports
# ------------------------------------------------------------
import re
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.io as pio
from plotly.colors import qualitative

# ------------------------------------------------------------
# Premium UI styling (white theme, subtle timber accents)
# ------------------------------------------------------------
st.markdown(
    """
<style>
:root{
  --bg: #ffffff;
  --card: #ffffff;
  --text: #0f172a;
  --muted: #64748b;
  --border: rgba(15, 23, 42, 0.10);
  --shadow: 0 10px 25px rgba(15,23,42,0.06);
  --accent: #166534;     /* timber green */
  --accent2: #b45309;    /* amber */
  --accent3: #b91c1c;    /* red */
  --accent4: #0ea5e9;    /* sky (IAQ vibe) */
}

html, body, [class*="css"] { background: var(--bg) !important; color: var(--text) !important; }

.block-container { padding-top: 1.0rem; max-width: 1400px; }

h1,h2,h3 { letter-spacing: -0.02em; }

.tcard {
  background: var(--card);
  border: 1px solid var(--border);
  border-radius: 16px;
  padding: 14px 16px;
  box-shadow: var(--shadow);
}

.small-muted { color: var(--muted); font-size: 0.92rem; }

.badge {
  display:inline-block; padding: 4px 10px; border-radius: 999px;
  border: 1px solid var(--border); background: rgba(22,101,52,0.06);
  color: var(--accent); font-weight: 600; font-size: 0.85rem;
}

hr { border: none; height: 1px; background: rgba(15, 23, 42, 0.08); margin: 16px 0; }

div[data-testid="stMetricValue"] { font-size: 1.65rem; }
div[data-testid="stMetricLabel"] { color: var(--muted); }
</style>
""",
    unsafe_allow_html=True
)

# ------------------------------------------------------------
# Plotly template (white, crisp)
# ------------------------------------------------------------
pio.templates["timbersense"] = go.layout.Template(
    layout=dict(
        paper_bgcolor="#ffffff",
        plot_bgcolor="#ffffff",
        font=dict(family="Inter, Segoe UI, Arial", size=13, color="#0f172a"),
        xaxis=dict(
            showgrid=True,
            gridcolor="rgba(15,23,42,0.06)",
            zeroline=False,
            linecolor="rgba(15,23,42,0.15)",
            ticks="outside",
            tickcolor="rgba(15,23,42,0.20)",
        ),
        yaxis=dict(
            showgrid=True,
            gridcolor="rgba(15,23,42,0.06)",
            zeroline=False,
            linecolor="rgba(15,23,42,0.15)",
            ticks="outside",
            tickcolor="rgba(15,23,42,0.20)",
        ),
        legend=dict(
            bgcolor="rgba(255,255,255,0.7)",
            bordercolor="rgba(15,23,42,0.08)",
            borderwidth=1,
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="left",
            x=0,
        ),
        margin=dict(l=60, r=30, t=60, b=60),
        hovermode="x unified",
    )
)
pio.templates.default = "timbersense"

# ------------------------------------------------------------
# Sensor mapping (edit once, used everywhere)
# ------------------------------------------------------------
SENSOR_MAP = {
    5:  {"name": "Internal Temperature",        "unit": "°C",   "group": "TEMP"},
    6:  {"name": "Integrated RH",               "unit": "%RH",  "group": "RH"},
    7:  {"name": "Battery",                     "unit": "V",    "group": "BAT"},
    17: {"name": "PMM1 Temperature",            "unit": "°C",   "group": "TEMP_PMM"},
    18: {"name": "Moisture Content (PMM1)",     "unit": "%",    "group": "MC"},
    19: {"name": "PMM2 Temperature",            "unit": "°C",   "group": "TEMP_PMM"},
    20: {"name": "Moisture Content (PMM2)",     "unit": "%",    "group": "MC"},
    21: {"name": "Aux Channel 21",              "unit": "",     "group": "AUX"},
    22: {"name": "Aux Channel 22",              "unit": "",     "group": "AUX"},
    23: {"name": "Aux Channel 23",              "unit": "",     "group": "AUX"},
    24: {"name": "Aux Channel 24",              "unit": "",     "group": "AUX"},
}

# Meaningful colors (engineering semantics)
COLOR = {
    "TEMP": "#0ea5e9",      # sky
    "RH":   "#2563eb",      # blue
    "BAT":  "#7c3aed",      # violet
    "MC":   "#166534",      # timber green
    "AUX":  "#334155",      # slate
    "UNK":  "#0f172a",
}

LINE_DASHES = ["solid", "dash", "dot", "dashdot", "longdash", "longdashdot"]

SENSOR_OPTIONS = [
    "MF52 Temperature",
    "PMM on-board temp",
    "PMM MC",
    "RH",
    "CO2 2000",
    "CO2 5000",
    "SPOT",
    "Battery",
    "Aux",
    "Custom",
]

STANDARD_COLOR_CYCLE = qualitative.Safe + qualitative.Dark24

# Threshold colors (your request)
THR_COL = {
    "mc_baseline": "rgba(22,101,52,0.80)",  # green
    "mc_mould":    "rgba(180,83,9,0.90)",   # orange
    "mc_sub":      "rgba(185,28,28,0.95)",  # red
    "bat_dead":    "rgba(185,28,28,0.95)",  # red
}

# Defaults
DEFAULT_THRESHOLDS = {
    "BAT_DEAD_V": 3.0,
    "MC_BASELINE_LO": 11.0,
    "MC_BASELINE_HI": 16.0,
    "MC_MOULD": 20.0,
    "MC_SUBMERGED": 28.0,
}

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def label_for_input(inp: int):
    channel_cfg = st.session_state.get("channel_config", {})
    if int(inp) in channel_cfg:
        cfg = channel_cfg[int(inp)]
        return cfg.get("name", f"Input {int(inp)}"), cfg.get("unit", ""), cfg.get("group", "AUX")
    meta = SENSOR_MAP.get(int(inp), {"name": f"Input {int(inp)}", "unit": "", "group": "UNK"})
    return meta["name"], meta["unit"], meta.get("group", "UNK")

def color_for_input(inp: int, group: str = "UNK"):
    if int(inp) in SENSOR_MAP and SENSOR_MAP[int(inp)].get("group") in COLOR:
        return COLOR[SENSOR_MAP[int(inp)]["group"]]
    return STANDARD_COLOR_CYCLE[int(inp) % len(STANDARD_COLOR_CYCLE)]

def apply_days_axis(fig: go.Figure, times: pd.Series):
    t = pd.to_datetime(times.dropna())
    if t.empty:
        return fig
    t0 = t.min()
    step = max(1, int(len(t) / 8))
    tickvals = t[::step]
    # Conflict resolution choice: keep primary timestamp axis at bottom and derived elapsed-days axis on top.
    fig.update_layout(
        xaxis=dict(title="Time", side="bottom"),
        xaxis2=dict(
            overlaying="x",
            side="top",
            tickmode="array",
            tickvals=tickvals,
            ticktext=[f"{((x - t0).total_seconds() / 86400):.1f}" for x in tickvals],
            title="Days since start",
            showgrid=False,
        ),
    )
    fig.update_xaxes(matches="x", selector=dict(anchor="y"))
    return fig

def parse_time_robust(series: pd.Series) -> pd.Series:
    # Try ISO-like first (dayfirst=False), then dayfirst=True fallback
    t = pd.to_datetime(series, errors="coerce", dayfirst=False)
    mask = t.isna()
    if mask.any():
        t2 = pd.to_datetime(series[mask], errors="coerce", dayfirst=True)
        t.loc[mask] = t2
    return t

def ensure_numeric(x):
    return pd.to_numeric(x, errors="coerce")

def apply_time_window(df, start_str, end_str):
    def parse_user_dt(x):
        x = str(x).strip()
        if not x:
            return None
        t = pd.to_datetime(x, errors="coerce", dayfirst=False)
        if pd.isna(t):
            t = pd.to_datetime(x, errors="coerce", dayfirst=True)
        if pd.isna(t):
            raise ValueError(f"Could not parse date: {x}")
        return t

    st_ts = parse_user_dt(start_str) if str(start_str).strip() else None
    en_ts = parse_user_dt(end_str) if str(end_str).strip() else None

    out = df
    if st_ts is not None:
        out = out[out["Time"] >= st_ts]
    if en_ts is not None:
        out = out[out["Time"] <= en_ts]
    return out, st_ts, en_ts

def smooth_series(y: pd.Series, method: str, param: int) -> pd.Series:
    y = y.copy()
    if method == "None":
        return y
    if method == "EMA":
        span = max(2, int(param))
        return y.ewm(span=span, adjust=False).mean()
    if method == "Rolling mean":
        w = max(2, int(param))
        return y.rolling(w, min_periods=max(2, w//3)).mean()
    if method == "Rolling median":
        w = max(2, int(param))
        return y.rolling(w, min_periods=max(2, w//3)).median()
    return y

# -------------------------
# Query parsing (simple, reliable)
# Supports:
#   "find input 5 below 22"
#   "input 6 between 50 and 70"
# -------------------------
COND_RE = re.compile(
    r"(?:find\s+)?(?:input|channel|ch)\s*(\d+)\s*"
    r"(<=|>=|<|>|=|below|under|above|over|between)\s*"
    r"(-?\d+(?:\.\d+)?)"
    r"(?:\s*(?:and|to)\s*(-?\d+(?:\.\d+)?))?"
)

def parse_conditions(text: str):
    q = (text or "").lower().strip()
    q = re.sub(r"\s+", " ", q)

    out = []
    for m in COND_RE.finditer(q):
        inp = int(m.group(1))
        op_raw = m.group(2)
        a = float(m.group(3))
        b = float(m.group(4)) if m.group(4) is not None else None

        if op_raw in ["below", "under"]:
            op = "<"
        elif op_raw in ["above", "over"]:
            op = ">"
        elif op_raw == "between":
            op = "between"
            if b is None:
                continue
        else:
            op = op_raw

        out.append({"input": inp, "op": op, "a": a, "b": b})
    return out

def apply_condition(df, cond, col="ConvertedNum"):
    inp = int(cond["input"])
    op = cond["op"]
    a = float(cond["a"])
    b = cond.get("b", None)

    sub = df[df["Input"] == inp].copy()
    v = sub[col]

    if op == "<":  return sub[v < a]
    if op == ">":  return sub[v > a]
    if op == "<=": return sub[v <= a]
    if op == ">=": return sub[v >= a]
    if op == "=":  return sub[np.isclose(v, a, atol=1e-12)]
    if op == "between":
        lo, hi = (a, float(b)) if a <= float(b) else (float(b), a)
        return sub[(v >= lo) & (v <= hi)]
    return sub.iloc[0:0]

# -------------------------
# Robust outliers
# -------------------------
def rolling_mad_outliers(sub, value_col="ConvertedNum", window="60min", z=4.0):
    # sub must have Time + value_col
    tmp = sub[["Time", value_col]].dropna().sort_values("Time").copy()
    if tmp.empty:
        return tmp.assign(roll_med=np.nan, roll_mad=np.nan, robust_z=np.nan, is_outlier=False)

    s = tmp.set_index("Time")[value_col].astype(float)
    med = s.rolling(window, min_periods=10).median()
    mad = (s - med).abs().rolling(window, min_periods=10).median()

    denom = mad.replace(0, np.nan)
    rz = 0.6745 * (s - med) / denom

    out = tmp.copy()
    out["roll_med"] = med.values
    out["roll_mad"] = mad.values
    out["robust_z"] = rz.values
    out["is_outlier"] = out["robust_z"].abs() >= float(z)
    return out

def iqr_outliers(sub, value_col="ConvertedNum", k=1.5):
    tmp = sub[["Time", value_col]].dropna().sort_values("Time").copy()
    if tmp.empty:
        return tmp.assign(is_outlier=False, lo=np.nan, hi=np.nan)
    q1 = tmp[value_col].quantile(0.25)
    q3 = tmp[value_col].quantile(0.75)
    iqr = q3 - q1
    lo = q1 - float(k) * iqr
    hi = q3 + float(k) * iqr
    tmp["is_outlier"] = (tmp[value_col] < lo) | (tmp[value_col] > hi)
    tmp["lo"] = lo
    tmp["hi"] = hi
    return tmp

# ------------------------------------------------------------
# MC recomputation (your Excel formula)
# ------------------------------------------------------------
def compute_rs_from_raw(raw_value: pd.Series) -> pd.Series:
    """
    From your sheet:
      R = Raw/1000
      Rs = exp(4.095417 - 0.14006*ln(R))
    """
    R = ensure_numeric(raw_value) / 1000.0
    R = R.replace([np.inf, -np.inf], np.nan)
    R = R.where(R > 0, np.nan)
    return np.exp(4.095417 - (0.14006 * np.log(R)))

def mc_from_rs_temp(Rs: pd.Series, temp_c: pd.Series, a: float, b: float) -> pd.Series:
    """
    Excel:
    MC = ( ((Rs + (0.567 - 0.026*T + 0.000051*T^2)) / (0.881*(1.0056^T))) - b ) / a
    """
    T = ensure_numeric(temp_c)
    poly = (0.567 - 0.026*T + 0.000051*(T**2))
    denom = (0.881 * (1.0056 ** T))
    inner = (Rs + poly) / denom
    return (inner - float(b)) / float(a)

def merge_asof_time(left, right, tol="2min"):
    # merge on nearest time
    tol_td = pd.Timedelta(tol)
    L = left.sort_values("Time").copy()
    R = right.sort_values("Time").copy()
    return pd.merge_asof(L, R, on="Time", direction="nearest", tolerance=tol_td)

# ------------------------------------------------------------
# Header
# ------------------------------------------------------------
st.markdown(
    """
<div class="tcard">
  <div style="display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;">
    <div>
      <div class="badge">TIMBER-SENSE</div>
      <h2 style="margin:6px 0 2px 0;">SMT Query Dashboard</h2>
      <div class="small-muted">Zoom • Pan • Thresholds • Smoothing • Outliers • MC recompute (from RAW)</div>
    </div>
    <div class="small-muted">White theme • Engineering semantics • Premium Plotly</div>
  </div>
</div>
""",
    unsafe_allow_html=True
)

left_logo_bytes = st.session_state.get("left_logo_bytes")
right_logo_bytes = st.session_state.get("right_logo_bytes")
if left_logo_bytes is not None or right_logo_bytes is not None:
    lg1, lg_mid, lg2 = st.columns([1, 6, 1])
    with lg1:
        if left_logo_bytes is not None:
            st.image(left_logo_bytes, use_container_width=True)
    with lg2:
        if right_logo_bytes is not None:
            st.image(right_logo_bytes, use_container_width=True)

st.caption(f"Active DAQ: {st.session_state.get('daq_name', 'N/A')} • DAQ Serial: {st.session_state.get('daq_serial', 'N/A')}")

# ------------------------------------------------------------
# Login gate (basic auth for pilot stage)
# ------------------------------------------------------------
if "is_authenticated" not in st.session_state:
    st.session_state.is_authenticated = False

if not st.session_state.is_authenticated:
    st.subheader("Login")
    with st.form("login_form", clear_on_submit=False):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        login_btn = st.form_submit_button("Sign in")

    if login_btn:
        if username == "admin" and password == "timbershmproject123":
            st.session_state.is_authenticated = True
            st.success("Login successful.")
            st.rerun()
        else:
            st.error("Invalid username or password.")
    st.stop()

# ------------------------------------------------------------
# Sidebar: load + filters + plot toggles
# ------------------------------------------------------------
st.sidebar.header("0) Pre-flight checklist")
daq_name = st.sidebar.text_input("DAQ model / ID", value=st.session_state.get("daq_name", "A3 Wireless 9753"))
daq_serial = st.sidebar.text_input("DAQ serial number", value=st.session_state.get("daq_serial", ""))

left_logo = st.sidebar.file_uploader("Header logo (left)", type=["png", "jpg", "jpeg", "svg"], key="left_logo_upload")
right_logo = st.sidebar.file_uploader("Header logo (right)", type=["png", "jpg", "jpeg", "svg"], key="right_logo_upload")

if left_logo is not None:
    st.session_state["left_logo_bytes"] = left_logo.getvalue()
if right_logo is not None:
    st.session_state["right_logo_bytes"] = right_logo.getvalue()

channel_config = {}
for ch in range(17, 25):
    default_name = SENSOR_MAP.get(ch, {"name": f"Input {ch}"})["name"]
    with st.sidebar.expander(f"Input {ch} configuration", expanded=False):
        sensor_type = st.selectbox(
            f"Input {ch} sensor type",
            SENSOR_OPTIONS,
            index=SENSOR_OPTIONS.index("PMM on-board temp") if ch in [17, 19] else (SENSOR_OPTIONS.index("PMM MC") if ch in [18, 20] else SENSOR_OPTIONS.index("Aux")),
            key=f"sensor_type_{ch}",
        )
        custom_label = st.text_input(f"Input {ch} display label", value=st.session_state.get(f"sensor_label_{ch}", default_name), key=f"sensor_label_{ch}")
        sensor_serial = st.text_input(f"Input {ch} sensor serial", value=st.session_state.get(f"sensor_serial_{ch}", ""), key=f"sensor_serial_{ch}")

        group_guess = "MC" if "mc" in sensor_type.lower() else ("TEMP" if "temp" in sensor_type.lower() else ("RH" if sensor_type == "RH" else "AUX"))
        channel_config[ch] = {
            "type": sensor_type,
            "name": custom_label,
            "sensor_serial": sensor_serial,
            "group": group_guess,
            "unit": SENSOR_MAP.get(ch, {}).get("unit", "%" if sensor_type == "PMM MC" else ""),
        }

st.session_state["channel_config"] = channel_config
st.session_state["daq_name"] = daq_name
st.session_state["daq_serial"] = daq_serial

st.sidebar.header("1) Load data")
uploaded = st.sidebar.file_uploader("Upload SMT CSV", type=["csv"])

if not uploaded:
    st.info("Upload an SMT CSV to begin.")
    st.stop()

df = pd.read_csv(uploaded)

date_col = "LocalDate" if "LocalDate" in df.columns else ("UTCDate" if "UTCDate" in df.columns else None)
if date_col is None:
    st.error("CSV must contain LocalDate or UTCDate.")
    st.stop()

df["Time"] = parse_time_robust(df[date_col])
df = df.dropna(subset=["Time"]).sort_values("Time").reset_index(drop=True)

if "Input" not in df.columns:
    st.error("CSV must contain column: Input")
    st.stop()

df["Input"] = df["Input"].astype(int)

df["ConvertedNum"] = ensure_numeric(df["Converted"]) if "Converted" in df.columns else np.nan
df["RawNum"] = ensure_numeric(df["Value"]) if "Value" in df.columns else np.nan

# Node filter
st.sidebar.header("2) Baseline filters")
node_sel = None
if "Node" in df.columns:
    nodes = sorted(df["Node"].dropna().unique().tolist())
    node_sel = st.sidebar.selectbox("Node", nodes, index=0)
    df = df[df["Node"] == node_sel].copy()

inputs_all = sorted(df["Input"].unique().tolist())
default_inputs = [x for x in [5, 6, 7, 17, 18, 19, 20, 21, 22] if x in inputs_all] or inputs_all[:10]
inputs_sel = st.sidebar.multiselect("Inputs to include", inputs_all, default=default_inputs)
df = df[df["Input"].isin(inputs_sel)].copy()

# Time window
st.sidebar.header("3) Time window")
tmin, tmax = df["Time"].min(), df["Time"].max()
st.sidebar.caption(f"Data span: {tmin} → {tmax}")

start_str = st.sidebar.text_input("Start (optional)", value="")
end_str = st.sidebar.text_input("End (optional)", value="")

try:
    dfw, st_ts, en_ts = apply_time_window(df, start_str, end_str)
except Exception as e:
    st.sidebar.error(str(e))
    st.stop()

if dfw.empty:
    st.warning("No rows after baseline filters/time window.")
    st.stop()

# Threshold controls
st.sidebar.header("4) Threshold overlays")
BAT_DEAD_V = st.sidebar.number_input("Battery dead (V)", value=float(DEFAULT_THRESHOLDS["BAT_DEAD_V"]), step=0.1)
MC_BASELINE_LO = st.sidebar.number_input("MC baseline low (%)", value=float(DEFAULT_THRESHOLDS["MC_BASELINE_LO"]), step=0.5)
MC_BASELINE_HI = st.sidebar.number_input("MC baseline high (%)", value=float(DEFAULT_THRESHOLDS["MC_BASELINE_HI"]), step=0.5)
MC_MOULD = st.sidebar.number_input("MC mould risk (%)", value=float(DEFAULT_THRESHOLDS["MC_MOULD"]), step=0.5)
MC_SUBMERGED = st.sidebar.number_input("MC submerged (%)", value=float(DEFAULT_THRESHOLDS["MC_SUBMERGED"]), step=0.5)

THR = dict(
    BAT_DEAD_V=BAT_DEAD_V,
    MC_BASELINE_LO=MC_BASELINE_LO,
    MC_BASELINE_HI=MC_BASELINE_HI,
    MC_MOULD=MC_MOULD,
    MC_SUBMERGED=MC_SUBMERGED,
)

# Plot controls
st.sidebar.header("5) Plot controls")
show_converted = st.sidebar.checkbox("Show Converted plots", value=True)
show_raw = st.sidebar.checkbox("Show RAW (Value) plots", value=False)

plot_mode = st.sidebar.radio("Plot mode", ["Per input (separate)", "Overlay selected inputs"], index=0)
overlay_inputs = []
if plot_mode == "Overlay selected inputs":
    overlay_inputs = st.sidebar.multiselect(
        "Inputs to overlay",
        sorted(dfw["Input"].unique().tolist()),
        default=[x for x in [5, 6, 7] if x in dfw["Input"].unique().tolist()] or sorted(dfw["Input"].unique().tolist())[:3]
    )

smooth_on = st.sidebar.checkbox("Enable smoothing (premium curve)", value=False)
smooth_method = st.sidebar.selectbox("Smoothing method", ["EMA", "Rolling mean", "Rolling median", "None"], index=0)
smooth_param = st.sidebar.slider("Smoothing strength", 3, 101, 21, 2)  # span/window

line_width = st.sidebar.slider("Line thickness", 2, 8, 4, 1)
marker_size = st.sidebar.slider("Marker size", 0, 8, 3, 1)

# ------------------------------------------------------------
# Main: quick status + explanation
# ------------------------------------------------------------
c1, c2, c3, c4 = st.columns(4)
with c1:
    st.metric("Rows (filtered)", f"{len(dfw):,}")
with c2:
    st.metric("Inputs", f"{len(sorted(dfw['Input'].unique()))}")
with c3:
    st.metric("Start", str(dfw["Time"].min()))
with c4:
    st.metric("End", str(dfw["Time"].max()))

with st.expander("What are these controls doing? (quick explanation)", expanded=False):
    st.markdown(
        """
- **Node**: filters to a single DAQ node (if present in the CSV).
- **Inputs**: selects which channels to include in analysis/plots.
- **Start/End**: optional time window. Leave blank to use the full CSV.
- **Converted vs RAW**: choose which domain to plot (engineering value vs device raw).
- **Smoothing**: optional signal conditioning to make plots readable (EMA is usually best for dashboards).
- **Threshold overlays**: adds your MC bands and battery-dead line with meaningful colors.
- **Plot mode**: either one plot per input, or multiple inputs overlaid in one chart.
"""
    )

st.markdown("<hr/>", unsafe_allow_html=True)

# ------------------------------------------------------------
# Query section (kept simple + reliable)
# ------------------------------------------------------------
st.subheader("Query (fast, reliable)")
q = st.text_input("Example: find input 5 below 22   |   input 6 between 50 and 70", value="")

query_hits = None
if q.strip():
    conds = parse_conditions(q)
    if len(conds) == 1:
        query_hits = apply_condition(dfw, conds[0], col="ConvertedNum")
        st.caption(f"Parsed: Input {conds[0]['input']}  {conds[0]['op']}  {conds[0]['a']}" + (f" and {conds[0]['b']}" if conds[0].get("b") is not None else ""))
        st.write(f"Matches: **{len(query_hits):,}**")
        st.dataframe(query_hits[["Time", "Node"] if "Node" in query_hits.columns else ["Time"]].join(query_hits[["Input","ConvertedNum","RawNum"]]), use_container_width=True, height=280)
    elif len(conds) == 0:
        st.warning("Could not parse query. Try: `find input 5 below 22` or `input 6 between 50 and 70`.")
    else:
        st.info("This lightweight query box supports 1 condition (by design). Use the plots + outlier tools for deeper exploration.")

st.markdown("<hr/>", unsafe_allow_html=True)

# ------------------------------------------------------------
# Plot building blocks
# ------------------------------------------------------------
def add_thresholds(fig: go.Figure, inp: int, is_mc: bool, is_bat: bool, yref="y"):
    x0 = dfw["Time"].min()
    x1 = dfw["Time"].max()

    if is_bat:
        fig.add_hline(
            y=THR["BAT_DEAD_V"],
            line_dash="dash",
            line_width=2,
            line_color=THR_COL["bat_dead"],
            annotation_text="Battery dead (3.0V)",
            annotation_position="top left",
        )

    if is_mc:
        # baseline band lines
        fig.add_hline(y=THR["MC_BASELINE_LO"], line_dash="dash", line_width=2, line_color=THR_COL["mc_baseline"],
                      annotation_text="Baseline ~11%", annotation_position="top left")
        fig.add_hline(y=THR["MC_BASELINE_HI"], line_dash="dash", line_width=2, line_color=THR_COL["mc_baseline"],
                      annotation_text="Baseline ~16%", annotation_position="top left")

        fig.add_hline(y=THR["MC_MOULD"], line_dash="dash", line_width=2, line_color=THR_COL["mc_mould"],
                      annotation_text="Mould risk ~20%", annotation_position="top left")
        fig.add_hline(y=THR["MC_SUBMERGED"], line_dash="dash", line_width=2, line_color=THR_COL["mc_sub"],
                      annotation_text="Submerged ~28%+", annotation_position="top left")

def make_series(df_inp: pd.DataFrame, col: str, smooth=False):
    s = df_inp[["Time", col]].dropna().sort_values("Time").copy()
    if s.empty:
        return s
    y = s[col].astype(float)
    if smooth and smooth_on and smooth_method != "None":
        y = smooth_series(y, smooth_method, smooth_param)
    s[col] = y
    return s

def plot_single_input(df_inp: pd.DataFrame, inp: int, show_converted=True, show_raw=False):
    name, unit, grp = label_for_input(inp)
    base_color = color_for_input(inp, grp)

    fig = go.Figure()

    if show_converted and df_inp["ConvertedNum"].notna().any():
        s = make_series(df_inp, "ConvertedNum", smooth=True)
        fig.add_trace(go.Scatter(
            x=s["Time"], y=s["ConvertedNum"],
            mode=("lines+markers" if marker_size > 0 else "lines"),
            line=dict(width=line_width, color=base_color),
            marker=dict(size=marker_size, color=base_color),
            name=f"Converted • {inp} {name}",
            hovertemplate="%{x|%d/%m/%Y %H:%M}<br>Converted=%{y:.4f}<extra></extra>",
        ))

    if show_raw and df_inp["RawNum"].notna().any():
        s2 = make_series(df_inp, "RawNum", smooth=False)
        fig.add_trace(go.Scatter(
            x=s2["Time"], y=s2["RawNum"],
            mode=("lines+markers" if marker_size > 0 else "lines"),
            line=dict(width=max(2, line_width-1), color="rgba(15,23,42,0.45)"),
            marker=dict(size=max(1, marker_size-1), color="rgba(15,23,42,0.45)"),
            name=f"RAW • {inp} {name}",
            hovertemplate="%{x|%d/%m/%Y %H:%M}<br>RAW=%{y:.0f}<extra></extra>",
        ))

    is_mc = (grp == "MC") or ("moist" in name.lower()) or (inp in [18, 20])
    is_bat = (grp == "BAT") or (inp == 7)

    # thresholds only make sense on Converted MC / Battery (still okay even if RAW plotted too)
    add_thresholds(fig, inp, is_mc=is_mc, is_bat=is_bat)

    ylab = f"{name} ({unit})" if unit else name
    fig.update_layout(
        title=f"Input {inp} • {name}",
        xaxis_title="Time",
        yaxis_title=ylab,
        height=420,
    )
    fig = apply_days_axis(fig, df_inp["Time"])
    return fig

def plot_overlay(df_all: pd.DataFrame, inputs: list, col="ConvertedNum"):
    fig = go.Figure()
    for idx, inp in enumerate(inputs):
        inp = int(inp)
        name, unit, grp = label_for_input(inp)
        base_color = color_for_input(inp, grp)
        sub = df_all[df_all["Input"] == inp].copy()
        if sub.empty:
            continue
        s = make_series(sub, col, smooth=True if col == "ConvertedNum" else False)
        if s.empty:
            continue
        fig.add_trace(go.Scatter(
            x=s["Time"], y=s[col],
            mode=("lines+markers" if marker_size > 0 else "lines"),
            line=dict(width=line_width, color=base_color, dash=LINE_DASHES[idx % len(LINE_DASHES)]),
            marker=dict(size=marker_size, color=base_color),
            name=f"{inp} • {name} ({'Conv' if col=='ConvertedNum' else 'RAW'})",
        ))

    fig.update_layout(
        title=f"Overlay • {col}",
        xaxis_title="Time",
        yaxis_title=col,
        height=520,
    )
    fig = apply_days_axis(fig, df_all[df_all["Input"].isin([int(x) for x in inputs])]["Time"])
    return fig

# ------------------------------------------------------------
# Plots section (zoom/pan is automatic in Plotly)
# ------------------------------------------------------------
st.subheader("Plots (zoom/pan/hover enabled)")

plot_tabs = st.tabs(["Converted", "RAW (Value)"])

with plot_tabs[0]:
    if not show_converted:
        st.info("Enable **Show Converted plots** in the sidebar to display Converted charts.")
    else:
        if plot_mode == "Per input (separate)":
            for inp in sorted(dfw["Input"].unique().tolist()):
                sub = dfw[dfw["Input"] == int(inp)].copy()
                fig = plot_single_input(sub, int(inp), show_converted=True, show_raw=False)
                st.plotly_chart(fig, use_container_width=True, config={"displaylogo": False})
        else:
            fig = plot_overlay(dfw, overlay_inputs, col="ConvertedNum")
            st.plotly_chart(fig, use_container_width=True, config={"displaylogo": False})

with plot_tabs[1]:
    if not show_raw:
        st.info("Enable **Show RAW (Value) plots** in the sidebar to display RAW charts.")
    else:
        if plot_mode == "Per input (separate)":
            for inp in sorted(dfw["Input"].unique().tolist()):
                sub = dfw[dfw["Input"] == int(inp)].copy()
                fig = plot_single_input(sub, int(inp), show_converted=False, show_raw=True)
                st.plotly_chart(fig, use_container_width=True, config={"displaylogo": False})
        else:
            fig = plot_overlay(dfw, overlay_inputs, col="RawNum")
            st.plotly_chart(fig, use_container_width=True, config={"displaylogo": False})

st.markdown("<hr/>", unsafe_allow_html=True)

# ------------------------------------------------------------
# Outlier analysis
# ------------------------------------------------------------
st.subheader("Outlier analysis")

oa1, oa2, oa3, oa4 = st.columns([1.2, 1.1, 1.1, 1.1])
with oa1:
    out_inp = st.selectbox("Input", sorted(dfw["Input"].unique().tolist()), index=0)
with oa2:
    out_col = st.selectbox("Value domain", ["ConvertedNum", "RawNum"], index=0)
with oa3:
    out_method = st.selectbox("Method", ["Rolling MAD (robust z)", "IQR"], index=0)
with oa4:
    out_param = st.slider("Sensitivity", 2.0, 10.0, 4.0, 0.5) if out_method.startswith("Rolling") else st.slider("IQR k", 0.5, 5.0, 1.5, 0.1)

sub_out = dfw[dfw["Input"] == int(out_inp)].copy()
if sub_out.empty or sub_out[out_col].dropna().empty:
    st.info("No data available for this input/value domain.")
else:
    if out_method.startswith("Rolling"):
        window = st.selectbox("Rolling window", ["30min", "60min", "120min", "240min"], index=1)
        out_df = rolling_mad_outliers(sub_out, value_col=out_col, window=window, z=float(out_param))
        hits = out_df[out_df["is_outlier"] == True].copy()
    else:
        out_df = iqr_outliers(sub_out, value_col=out_col, k=float(out_param))
        hits = out_df[out_df["is_outlier"] == True].copy()

    st.write(f"Outliers found: **{len(hits):,}**")

    if not hits.empty:
        st.dataframe(hits.head(300), use_container_width=True, height=260)
        st.download_button(
            "Download outliers CSV",
            data=hits.to_csv(index=False).encode("utf-8"),
            file_name=f"outliers_input_{int(out_inp)}_{out_col}.csv",
            mime="text/csv"
        )

    # Plot with outliers highlighted
    name, unit, grp = label_for_input(int(out_inp))
    base_color = color_for_input(int(out_inp), grp)
    fig = go.Figure()

    s = sub_out[["Time", out_col]].dropna().sort_values("Time").copy()
    if smooth_on and out_col == "ConvertedNum" and smooth_method != "None":
        s[out_col] = smooth_series(s[out_col].astype(float), smooth_method, smooth_param)

    fig.add_trace(go.Scatter(
        x=s["Time"], y=s[out_col],
        mode="lines",
        line=dict(width=line_width, color=base_color),
        name="signal",
    ))

    if not hits.empty:
        fig.add_trace(go.Scatter(
            x=hits["Time"], y=hits[out_col],
            mode="markers",
            marker=dict(size=9, color="rgba(185,28,28,0.90)", symbol="circle"),
            name="outliers",
        ))

    fig.update_layout(
        title=f"Outliers • Input {int(out_inp)} • {name} • {out_col}",
        xaxis_title="Time",
        yaxis_title=f"{name} ({unit})" if unit else out_col,
        height=420,
    )
    fig = apply_days_axis(fig, sub_out["Time"])
    st.plotly_chart(fig, use_container_width=True, config={"displaylogo": False})

st.markdown("<hr/>", unsafe_allow_html=True)

# ------------------------------------------------------------
# MC recomputation section (from RAW)
# ------------------------------------------------------------
st.subheader("Compute MC from RAW resistance (your Excel formula)")

st.markdown(
    """
<div class="tcard">
<b>What this does</b><br/>
- Uses <b>RAW</b> values from MC channels (18/20) as resistance input.<br/>
- Computes <b>Rs</b> and then <b>MC</b> using your Excel formula (Sitka spruce a,b).<br/>
- Uses PMM temperature channel (default 17 for PMM1, 19 for PMM2). If PMM temp is invalid (e.g. -104), it can fall back to channel 5 room temp.<br/>
- Outputs a CSV + comparison plot vs device Converted MC.
</div>
""",
    unsafe_allow_html=True
)

mc_col1, mc_col2, mc_col3, mc_col4 = st.columns([1.1, 1.1, 1.1, 1.2])

with mc_col1:
    mc_input = st.selectbox("MC channel", sorted(dfw["Input"].unique().tolist()), index=0)
with mc_col2:
    default_temp = 17 if int(mc_input) == 18 else 19
    temp_opts = sorted(dfw["Input"].unique().tolist())
    temp_input = st.selectbox("Temperature channel", temp_opts,
                              index=temp_opts.index(default_temp) if default_temp in temp_opts else 0)
with mc_col3:
    fallback_opts = ["Custom"] + [str(x) for x in sorted(dfw["Input"].unique().tolist())]
    fallback_choice = st.selectbox("Fallback temp (if invalid)", fallback_opts, index=1 if len(fallback_opts) > 1 else 0)
    fallback_temp_input = int(st.text_input("Fallback temp custom input", value="5")) if fallback_choice == "Custom" else int(fallback_choice)
with mc_col4:
    invalid_temp_if = st.number_input("Treat temp as invalid if ≤", value=-50.0, step=1.0)

ab1, ab2, ab3 = st.columns([1, 1, 2])
with ab1:
    a = st.number_input("a (Sitka spruce)", value=0.853, step=0.001, format="%.3f")
with ab2:
    b = st.number_input("b (Sitka spruce)", value=0.398, step=0.001, format="%.3f")
with ab3:
    tol = st.selectbox("Time align tolerance", ["30s", "1min", "2min", "5min"], index=2)

tw1, tw2 = st.columns([1.2, 1.2])
with tw1:
    mc_start = st.text_input("MC recompute start (optional)", value="06/01/2026 17:00")
with tw2:
    mc_end = st.text_input("MC recompute end (optional)", value="")

# Build filtered working df for MC compute
try:
    df_mcwin, _, _ = apply_time_window(dfw, mc_start, mc_end)
except Exception as e:
    st.error(str(e))
    df_mcwin = dfw.copy()

mc_rows = df_mcwin[df_mcwin["Input"] == int(mc_input)].copy()
t_rows = df_mcwin[df_mcwin["Input"] == int(temp_input)].copy()
fb_rows = df_mcwin[df_mcwin["Input"] == int(fallback_temp_input)].copy()

if mc_rows.empty:
    st.warning("No MC rows available in the selected window.")
elif mc_rows["RawNum"].dropna().empty:
    st.warning("MC channel has no RAW values (Value). Cannot recompute from resistance.")
else:
    # Prepare moisture raw (resistance input)
    mc_raw = mc_rows[["Time", "RawNum", "ConvertedNum"]].dropna(subset=["Time", "RawNum"]).copy()
    mc_raw = mc_raw.rename(columns={"RawNum": "MC_Raw", "ConvertedNum": "MC_DeviceConverted"})

    # Prepare main temp (use ConvertedNum)
    t_main = t_rows[["Time", "ConvertedNum"]].dropna().copy().rename(columns={"ConvertedNum": "T_main"})
    t_fb = fb_rows[["Time", "ConvertedNum"]].dropna().copy().rename(columns={"ConvertedNum": "T_fb"})

    # Merge nearest temp to MC timestamps
    merged = merge_asof_time(mc_raw, t_main, tol=tol)
    merged = merge_asof_time(merged, t_fb, tol=tol)

    # Choose temperature with fallback
    merged["TempUsed"] = merged["T_main"]
    bad = merged["TempUsed"].isna() | (merged["TempUsed"] <= float(invalid_temp_if))
    merged.loc[bad, "TempUsed"] = merged.loc[bad, "T_fb"]

    # Compute Rs + MC
    merged["Rs"] = compute_rs_from_raw(merged["MC_Raw"])
    merged["MC_Recomputed"] = mc_from_rs_temp(merged["Rs"], merged["TempUsed"], a=float(a), b=float(b))

    # Summary
    ok_rows = merged.dropna(subset=["MC_Recomputed"]).copy()
    st.write(f"Rows computed: **{len(ok_rows):,}** (after time-align + temperature selection)")

    # Download
    out = ok_rows[["Time", "MC_Raw", "MC_DeviceConverted", "T_main", "T_fb", "TempUsed", "Rs", "MC_Recomputed"]].copy()
    st.download_button(
        "Download MC recompute CSV",
        data=out.to_csv(index=False).encode("utf-8"),
        file_name=f"MC_recompute_input_{int(mc_input)}.csv",
        mime="text/csv"
    )

    # Plot: device converted vs recomputed
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=out["Time"], y=out["MC_DeviceConverted"],
        mode="lines",
        line=dict(width=line_width, color=COLOR["MC"]),
        name="Device Converted MC",
    ))
    fig.add_trace(go.Scatter(
        x=out["Time"], y=out["MC_Recomputed"],
        mode="lines",
        line=dict(width=line_width, color="rgba(180,83,9,0.95)"),
        name="Recomputed MC (from RAW)",
    ))

    # Add MC thresholds on this plot
    fig.add_hline(y=THR["MC_BASELINE_LO"], line_dash="dash", line_width=2, line_color=THR_COL["mc_baseline"], annotation_text="~11%")
    fig.add_hline(y=THR["MC_BASELINE_HI"], line_dash="dash", line_width=2, line_color=THR_COL["mc_baseline"], annotation_text="~16%")
    fig.add_hline(y=THR["MC_MOULD"], line_dash="dash", line_width=2, line_color=THR_COL["mc_mould"], annotation_text="~20% mould risk")
    fig.add_hline(y=THR["MC_SUBMERGED"], line_dash="dash", line_width=2, line_color=THR_COL["mc_sub"], annotation_text="~28% submerged")

    fig.update_layout(
        title=f"MC Comparison • Input {int(mc_input)}",
        xaxis_title="Time",
        yaxis_title="Moisture Content (%)",
        height=520,
    )
    fig = apply_days_axis(fig, out["Time"])
    st.plotly_chart(fig, use_container_width=True, config={"displaylogo": False})

    # Quick numeric check (how close?)
    if out["MC_DeviceConverted"].notna().any():
        diff = out.dropna(subset=["MC_DeviceConverted", "MC_Recomputed"]).copy()
        if not diff.empty:
            rmse = float(np.sqrt(np.mean((diff["MC_Recomputed"] - diff["MC_DeviceConverted"])**2)))
            mae = float(np.mean(np.abs(diff["MC_Recomputed"] - diff["MC_DeviceConverted"])))
            st.caption(f"Match metrics (where both exist): RMSE={rmse:.4f} | MAE={mae:.4f}")

st.markdown("<hr/>", unsafe_allow_html=True)

# ------------------------------------------------------------
# Export filtered dataset
# ------------------------------------------------------------
st.subheader("Export")
st.download_button(
    "Download filtered dataset (current baseline filters)",
    data=dfw.to_csv(index=False).encode("utf-8"),
    file_name="timbersense_filtered.csv",
    mime="text/csv"
)

st.caption("Tip: Plotly has built-in zoom/pan via the toolbar on each chart (box select, lasso, zoom, autoscale).")

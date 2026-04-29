"""
IMOVELA — Lead Intelligence Imobiliária
Dashboard de oportunidades em tempo real.

Run: streamlit run dashboard/app.py
     python main.py dashboard
"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from datetime import datetime

# ─── Page config ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Imovela · Lead Intelligence",
    page_icon="◆",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── CSS: IMOVELA premium dark theme ──────────────────────────────────────────
# Glassmorphism + animated gradients + micro-interactions. Designed for an
# upmarket SaaS feel: investors, executives, agencies. Drop-in safe — only
# Streamlit-DOM selectors, no app-level class restructuring.
#
# Brand palette
#   ink/00     #050810   page (deepest)
#   ink/10     #0b1020   surface (sidebar / cards background)
#   ink/20     #131a31   surface raised
#   ink/30     #1d2747   border / subtle
#   mint/0     #34d399   primary accent
#   mint/+     #10b981   primary deep
#   sky        #38bdf8   info
#   violet     #a78bfa   premium
#   rose       #fb7185   hot/danger
#   amber      #fbbf24   warm
#   ice        #f8fafc   text high
#   fog        #cbd5e1   text mid
#   smoke      #94a3b8   text muted
#   slate      #64748b   text dim

_CSS_BASE = """<style>
@import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=Inter:wght@400;500;600;700;800;900&display=swap');

/* ──── Root tokens ──────────────────────────────────────────────────────── */
:root {
  --ink-00: #050810;
  --ink-10: #0b1020;
  --ink-20: #131a31;
  --ink-30: #1d2747;
  --ink-40: #2a3760;

  --mint:    #34d399;
  --mint-d:  #10b981;
  --mint-l:  #6ee7b7;
  --sky:     #38bdf8;
  --violet:  #a78bfa;
  --rose:    #fb7185;
  --amber:   #fbbf24;

  --ice:   #f8fafc;
  --fog:   #cbd5e1;
  --smoke: #94a3b8;
  --slate: #64748b;
  --dust:  #475569;

  --grad-primary:  linear-gradient(135deg, #34d399 0%, #38bdf8 50%, #a78bfa 100%);
  --grad-hot:      linear-gradient(135deg, #fb7185 0%, #f43f5e 100%);
  --grad-warm:     linear-gradient(135deg, #fbbf24 0%, #f97316 100%);
  --grad-cold:     linear-gradient(135deg, #38bdf8 0%, #6366f1 100%);
  --grad-surface:  linear-gradient(180deg, rgba(19,26,49,.6) 0%, rgba(11,16,32,.4) 100%);

  --shadow-card:  0 1px 0 rgba(255,255,255,.04) inset, 0 8px 32px -16px rgba(0,0,0,.6);
  --shadow-glow:  0 0 0 1px rgba(52,211,153,.15), 0 8px 40px -12px rgba(52,211,153,.25);
  --shadow-glow-violet: 0 0 0 1px rgba(167,139,250,.18), 0 8px 40px -12px rgba(167,139,250,.3);
}

/* ──── Page chrome ─────────────────────────────────────────────────────── */
html, body, [class*="css"] {
    font-family: 'Inter', system-ui, sans-serif !important;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
}
.stApp {
    background:
        radial-gradient(900px 480px at 8% -10%, rgba(52,211,153,.06), transparent 60%),
        radial-gradient(800px 480px at 92% 110%, rgba(167,139,250,.08), transparent 60%),
        var(--ink-00) !important;
}
.main .block-container {
    padding: 0 2.2rem 5rem !important;
    max-width: 1480px !important;
}
section[data-testid="stSidebar"] {
    background:
        linear-gradient(180deg, var(--ink-10) 0%, #07091a 100%) !important;
    border-right: 1px solid rgba(255,255,255,.04) !important;
    box-shadow: 1px 0 0 rgba(255,255,255,.02);
}
section[data-testid="stSidebar"] > div { padding-top: 0 !important; }

h1, h2, h3, h4 {
    color: var(--ice) !important;
    font-family: 'Space Grotesk', 'Inter', sans-serif !important;
    letter-spacing: -.02em !important;
}
h1 { font-weight: 700 !important; }
h2 { font-weight: 600 !important; }

/* ──── Streamlit metrics → glass cards with hover lift ─────────────────── */
[data-testid="stMetric"] {
    background:
        linear-gradient(180deg, rgba(29,39,71,.45) 0%, rgba(11,16,32,.85) 100%) !important;
    backdrop-filter: blur(12px) saturate(140%);
    -webkit-backdrop-filter: blur(12px) saturate(140%);
    border: 1px solid rgba(255,255,255,.06) !important;
    border-radius: 14px !important;
    padding: 1.05rem 1.25rem !important;
    box-shadow: var(--shadow-card);
    transition: transform .25s cubic-bezier(.4,0,.2,1),
                border-color .25s, box-shadow .25s;
    position: relative; overflow: hidden;
}
[data-testid="stMetric"]::before {
    content: "";
    position: absolute; inset: 0;
    background: var(--grad-primary);
    opacity: 0;
    transition: opacity .3s;
    pointer-events: none;
    border-radius: 14px;
    -webkit-mask:
        linear-gradient(#fff 0 0) content-box,
        linear-gradient(#fff 0 0);
    -webkit-mask-composite: xor;
            mask-composite: exclude;
    padding: 1px;
}
[data-testid="stMetric"]:hover {
    transform: translateY(-2px);
    border-color: rgba(52,211,153,.3) !important;
    box-shadow: var(--shadow-glow);
}
[data-testid="stMetric"]:hover::before { opacity: .6; }

[data-testid="stMetricValue"] {
    font-family: 'Space Grotesk', sans-serif !important;
    font-size: 1.95rem !important;
    font-weight: 700 !important;
    color: var(--ice) !important;
    letter-spacing: -.025em !important;
    background: var(--grad-primary);
    -webkit-background-clip: text;
            background-clip: text;
    -webkit-text-fill-color: transparent;
    color: transparent !important;
}
[data-testid="stMetricLabel"] {
    font-size: .68rem !important;
    font-weight: 700 !important;
    color: var(--slate) !important;
    text-transform: uppercase !important;
    letter-spacing: 1.2px !important;
}
[data-testid="stMetricDelta"] svg { display: none; }

/* ──── Buttons ─────────────────────────────────────────────────────────── */
hr {
    border: none !important;
    border-top: 1px solid rgba(255,255,255,.05) !important;
    margin: 1.5rem 0 !important;
}
.stButton > button {
    background: rgba(29,39,71,.5) !important;
    color: var(--fog) !important;
    border: 1px solid rgba(255,255,255,.08) !important;
    border-radius: 10px !important;
    font-weight: 600 !important;
    font-family: 'Inter', sans-serif !important;
    letter-spacing: .01em !important;
    transition: all .2s cubic-bezier(.4,0,.2,1) !important;
    backdrop-filter: blur(8px);
    -webkit-backdrop-filter: blur(8px);
}
.stButton > button:hover {
    background: rgba(52,211,153,.1) !important;
    color: var(--mint) !important;
    border-color: rgba(52,211,153,.4) !important;
    transform: translateY(-1px);
    box-shadow: 0 6px 22px -10px rgba(52,211,153,.45) !important;
}
.stButton > button:active { transform: translateY(0); }

/* Primary-tinted button class — opt-in via key */
.stButton button[kind="primary"] {
    background: var(--grad-primary) !important;
    color: #052016 !important;
    border: none !important;
    font-weight: 700 !important;
    box-shadow: 0 6px 24px -8px rgba(52,211,153,.5) !important;
}
.stButton button[kind="primary"]:hover {
    color: #052016 !important;
    transform: translateY(-1px);
    box-shadow: 0 10px 32px -8px rgba(52,211,153,.7) !important;
}

/* ──── Form fields ─────────────────────────────────────────────────────── */
.stSelectbox [data-baseweb="select"] > div,
.stTextInput input,
.stTextArea textarea,
.stNumberInput input {
    background: rgba(11,16,32,.6) !important;
    border: 1px solid rgba(255,255,255,.08) !important;
    border-radius: 10px !important;
    color: var(--fog) !important;
    transition: border-color .2s, box-shadow .2s;
}
.stTextInput input:focus,
.stTextArea textarea:focus,
.stNumberInput input:focus {
    border-color: rgba(52,211,153,.5) !important;
    box-shadow: 0 0 0 3px rgba(52,211,153,.15) !important;
    outline: none !important;
}

/* ──── Data frames ─────────────────────────────────────────────────────── */
.stDataFrame {
    border: 1px solid rgba(255,255,255,.06) !important;
    border-radius: 12px !important;
    overflow: hidden !important;
    box-shadow: var(--shadow-card);
}

/* ──── Expanders ───────────────────────────────────────────────────────── */
.streamlit-expanderHeader {
    background: rgba(29,39,71,.4) !important;
    border: 1px solid rgba(255,255,255,.06) !important;
    border-radius: 10px !important;
    color: var(--fog) !important;
    transition: all .2s;
}
.streamlit-expanderHeader:hover {
    background: rgba(29,39,71,.6) !important;
    border-color: rgba(52,211,153,.2) !important;
}
.streamlit-expanderContent {
    background: rgba(5,8,16,.5) !important;
    border: 1px solid rgba(255,255,255,.04) !important;
    border-top: none !important;
    border-radius: 0 0 10px 10px !important;
    padding: 1.1rem !important;
}

/* ──── Tabs ────────────────────────────────────────────────────────────── */
.stTabs [data-baseweb="tab-list"] {
    gap: 4px;
    background: rgba(11,16,32,.5);
    padding: 4px;
    border-radius: 12px;
    border: 1px solid rgba(255,255,255,.05);
}
.stTabs [data-baseweb="tab"] {
    background: transparent !important;
    border-radius: 8px !important;
    color: var(--smoke) !important;
    font-weight: 600 !important;
    transition: all .2s !important;
}
.stTabs [aria-selected="true"] {
    background: rgba(52,211,153,.12) !important;
    color: var(--mint) !important;
    box-shadow: 0 0 0 1px rgba(52,211,153,.25);
}

/* ──── Scrollbar — slim premium ────────────────────────────────────────── */
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb {
    background: linear-gradient(180deg, var(--ink-30) 0%, var(--ink-40) 100%);
    border-radius: 3px;
}
::-webkit-scrollbar-thumb:hover {
    background: linear-gradient(180deg, var(--mint-d) 0%, var(--sky) 100%);
}

/* ──── Streamlit header: blend into bg ─────────────────────────────────── */
header[data-testid="stHeader"] {
    background: transparent !important;
    border-bottom: none !important;
    backdrop-filter: blur(8px);
}
[data-testid="stDecoration"] { display: none !important; }
[data-testid="stToolbar"] { background: transparent !important; }
[data-testid="stStatusWidget"] { color: var(--dust) !important; }
.stDeployButton { display: none !important; }
div[data-testid="stAppViewBlockContainer"] { padding-top: 1rem !important; }

/* ──── Page-level animations ───────────────────────────────────────────── */
@keyframes fadeUp {
    from { opacity: 0; transform: translateY(8px); }
    to   { opacity: 1; transform: translateY(0); }
}
@keyframes fadeIn {
    from { opacity: 0; }
    to   { opacity: 1; }
}
@keyframes shimmer {
    0%   { background-position: -200% 0; }
    100% { background-position: 200% 0; }
}
@keyframes glowPulse {
    0%, 100% { box-shadow: 0 0 0 0 rgba(52,211,153,.45); }
    50%      { box-shadow: 0 0 0 8px rgba(52,211,153,0); }
}
@keyframes float {
    0%, 100% { transform: translateY(0); }
    50%      { transform: translateY(-3px); }
}

[data-testid="stMetric"],
.card,
.kanban-card,
.intel-box,
.alert-card,
.pf-wrap {
    animation: fadeUp .45s cubic-bezier(.4,0,.2,1) both;
}
</style>"""

_CSS_CARDS = """<style>
/* ──── Lead cards — glass + animated borders ───────────────────────────── */
.card {
    background:
        linear-gradient(180deg, rgba(29,39,71,.55) 0%, rgba(11,16,32,.85) 100%);
    backdrop-filter: blur(14px) saturate(140%);
    -webkit-backdrop-filter: blur(14px) saturate(140%);
    border: 1px solid rgba(255,255,255,.06);
    border-radius: 14px;
    padding: 18px 20px;
    margin-bottom: 14px;
    position: relative;
    overflow: hidden;
    transition: transform .25s cubic-bezier(.4,0,.2,1),
                border-color .25s, box-shadow .25s;
    box-shadow: var(--shadow-card);
}
.card::after {
    content: "";
    position: absolute; inset: 0;
    border-radius: 14px;
    pointer-events: none;
    background: linear-gradient(135deg,
        rgba(52,211,153,0) 0%,
        rgba(52,211,153,.04) 30%,
        rgba(167,139,250,.04) 70%,
        rgba(56,189,248,0) 100%);
    opacity: 0;
    transition: opacity .3s;
}
.card:hover {
    transform: translateY(-2px);
    border-color: rgba(52,211,153,.25);
    box-shadow: 0 0 0 1px rgba(52,211,153,.1), 0 16px 48px -16px rgba(52,211,153,.18);
}
.card:hover::after { opacity: 1; }

.card-hot {
    border-color: rgba(251,113,133,.35);
    background:
        linear-gradient(135deg, rgba(251,113,133,.08) 0%, transparent 60%),
        linear-gradient(180deg, rgba(29,39,71,.55) 0%, rgba(11,16,32,.85) 100%);
    box-shadow: 0 0 0 1px rgba(251,113,133,.12), 0 12px 40px -16px rgba(251,113,133,.3);
}
.card-hot:hover {
    border-color: rgba(251,113,133,.6);
    box-shadow: 0 0 0 1px rgba(251,113,133,.25), 0 16px 56px -16px rgba(251,113,133,.5);
}
.card-warm {
    border-color: rgba(251,191,36,.3);
    background:
        linear-gradient(135deg, rgba(251,191,36,.05) 0%, transparent 60%),
        linear-gradient(180deg, rgba(29,39,71,.55) 0%, rgba(11,16,32,.85) 100%);
}

/* ──── Badges ──────────────────────────────────────────────────────────── */
.badge {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    padding: 3px 9px;
    border-radius: 999px;
    font-size: .62rem;
    font-weight: 800;
    letter-spacing: .8px;
    text-transform: uppercase;
    margin-right: 5px;
    backdrop-filter: blur(6px);
    -webkit-backdrop-filter: blur(6px);
    transition: transform .15s;
}
.badge:hover { transform: scale(1.04); }
.badge-hot {
    background: linear-gradient(135deg, rgba(251,113,133,.18), rgba(244,63,94,.12));
    color: #fb7185;
    border: 1px solid rgba(251,113,133,.35);
    box-shadow: 0 0 12px -2px rgba(251,113,133,.4);
    animation: glowPulse 2.4s ease-in-out infinite;
}
.badge-warm {
    background: linear-gradient(135deg, rgba(251,191,36,.18), rgba(245,158,11,.1));
    color: #fbbf24;
    border: 1px solid rgba(251,191,36,.3);
}
.badge-cold {
    background: linear-gradient(135deg, rgba(56,189,248,.14), rgba(99,102,241,.1));
    color: #38bdf8;
    border: 1px solid rgba(56,189,248,.25);
}
.badge-owner {
    background: linear-gradient(135deg, rgba(52,211,153,.16), rgba(16,185,129,.08));
    color: #34d399;
    border: 1px solid rgba(52,211,153,.3);
}
.badge-drop {
    background: linear-gradient(135deg, rgba(249,115,22,.18), rgba(234,88,12,.08));
    color: #fb923c;
    border: 1px solid rgba(249,115,22,.3);
    box-shadow: 0 0 14px -3px rgba(249,115,22,.45);
}
.badge-demo {
    background: rgba(148,163,184,.07);
    color: var(--dust);
    border: 1px solid rgba(148,163,184,.12);
    font-size: .56rem;
    letter-spacing: .4px;
}
.card-demo {
    opacity: .65;
    border-color: rgba(148,163,184,.1) !important;
}
.badge-phone {
    background: linear-gradient(135deg, rgba(52,211,153,.16), rgba(16,185,129,.08));
    color: #34d399;
    border: 1px solid rgba(52,211,153,.3);
}
.badge-email {
    background: linear-gradient(135deg, rgba(56,189,248,.16), rgba(14,165,233,.08));
    color: #38bdf8;
    border: 1px solid rgba(56,189,248,.3);
}
.badge-nocontact {
    background: rgba(239,68,68,.06);
    color: #f87171;
    border: 1px solid rgba(239,68,68,.18);
}

/* ──── Score orb ───────────────────────────────────────────────────────── */
.score-orb {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 56px; height: 56px;
    border-radius: 50%;
    font-family: 'Space Grotesk', sans-serif;
    font-size: 1.05rem;
    font-weight: 700;
    flex-shrink: 0;
    position: relative;
    transition: transform .25s, box-shadow .25s;
}
.score-orb::before {
    content: "";
    position: absolute; inset: -2px;
    border-radius: 50%;
    background: var(--grad-primary);
    z-index: -1;
    opacity: .4;
    filter: blur(8px);
    transition: opacity .3s;
}
.score-orb:hover { transform: scale(1.06); }
.score-orb:hover::before { opacity: .8; }
.orb-hot {
    background: rgba(251,113,133,.12);
    color: #fb7185;
    border: 2px solid rgba(251,113,133,.4);
    box-shadow: 0 4px 20px -4px rgba(251,113,133,.4);
}
.orb-hot::before { background: var(--grad-hot); }
.orb-warm {
    background: rgba(251,191,36,.12);
    color: #fbbf24;
    border: 2px solid rgba(251,191,36,.35);
    box-shadow: 0 4px 20px -4px rgba(251,191,36,.35);
}
.orb-warm::before { background: var(--grad-warm); }
.orb-cold {
    background: rgba(56,189,248,.1);
    color: #38bdf8;
    border: 2px solid rgba(56,189,248,.3);
    box-shadow: 0 4px 20px -4px rgba(56,189,248,.3);
}
.orb-cold::before { background: var(--grad-cold); }

/* ──── Chips ───────────────────────────────────────────────────────────── */
.chip {
    display: inline-block;
    background: rgba(29,39,71,.5);
    border: 1px solid rgba(255,255,255,.06);
    border-radius: 7px;
    padding: 3px 10px;
    font-size: .73rem;
    color: var(--fog);
    margin-right: 5px;
    backdrop-filter: blur(4px);
    -webkit-backdrop-filter: blur(4px);
    transition: all .15s;
}
.chip:hover {
    border-color: rgba(255,255,255,.15);
    color: var(--ice);
}

/* ──── Price typography ────────────────────────────────────────────────── */
.price {
    font-family: 'Space Grotesk', sans-serif;
    font-size: 1.22rem;
    font-weight: 700;
    color: var(--ice);
    letter-spacing: -.02em;
}
.delta-pos {
    font-size: .72rem;
    font-weight: 700;
    color: var(--mint);
    background: rgba(52,211,153,.08);
    padding: 2px 7px;
    border-radius: 5px;
}
.delta-neg {
    font-size: .72rem;
    font-weight: 700;
    color: var(--rose);
    background: rgba(251,113,133,.08);
    padding: 2px 7px;
    border-radius: 5px;
}

.lbl-section {
    font-family: 'Space Grotesk', sans-serif;
    font-size: .65rem;
    font-weight: 600;
    letter-spacing: 1.4px;
    text-transform: uppercase;
    color: var(--slate);
    margin-bottom: 10px;
}

/* ──── Kanban ──────────────────────────────────────────────────────────── */
.kanban-card {
    background:
        linear-gradient(180deg, rgba(29,39,71,.45) 0%, rgba(11,16,32,.7) 100%);
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
    border: 1px solid rgba(255,255,255,.06);
    border-radius: 11px;
    padding: 14px 16px;
    transition: transform .2s, border-color .2s;
    cursor: pointer;
}
.kanban-card:hover {
    transform: translateY(-2px);
    border-color: rgba(52,211,153,.25);
    box-shadow: 0 8px 24px -10px rgba(52,211,153,.25);
}

.activity-row {
    display: flex;
    gap: 12px;
    padding: 12px 0;
    border-bottom: 1px solid rgba(255,255,255,.04);
    font-size: .82rem;
    color: var(--fog);
    transition: background .2s, padding-left .2s;
}
.activity-row:hover {
    background: rgba(52,211,153,.04);
    padding-left: 8px;
}

/* ──── Intel boxes (KPI tiles) ─────────────────────────────────────────── */
.intel-box {
    background:
        linear-gradient(180deg, rgba(29,39,71,.5) 0%, rgba(11,16,32,.8) 100%);
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
    border: 1px solid rgba(255,255,255,.06);
    border-radius: 12px;
    padding: 14px 16px;
    margin-bottom: 10px;
    transition: all .25s;
    position: relative;
    overflow: hidden;
}
.intel-box::before {
    content: "";
    position: absolute;
    top: 0; left: 0; height: 2px; width: 100%;
    background: var(--grad-primary);
    opacity: 0;
    transition: opacity .3s;
}
.intel-box:hover {
    transform: translateY(-1px);
    border-color: rgba(52,211,153,.3);
}
.intel-box:hover::before { opacity: 1; }
.intel-lbl {
    font-family: 'Space Grotesk', sans-serif;
    font-size: .62rem;
    font-weight: 600;
    color: var(--slate);
    text-transform: uppercase;
    letter-spacing: 1px;
    margin-bottom: 6px;
}
.intel-val {
    font-family: 'Space Grotesk', sans-serif;
    font-size: 1.32rem;
    font-weight: 700;
    color: var(--ice);
    letter-spacing: -.025em;
}

/* ──── Alerts ──────────────────────────────────────────────────────────── */
.alert-card {
    display: flex;
    gap: 12px;
    background:
        linear-gradient(90deg, rgba(29,39,71,.55) 0%, rgba(11,16,32,.4) 100%);
    backdrop-filter: blur(8px);
    -webkit-backdrop-filter: blur(8px);
    border: 1px solid rgba(255,255,255,.06);
    border-left: 3px solid var(--sky);
    border-radius: 10px;
    padding: 12px 16px;
    margin-bottom: 10px;
    font-size: .82rem;
    color: var(--fog);
    transition: transform .2s, border-color .2s;
}
.alert-card:hover {
    transform: translateX(2px);
}
.alert-hot  {
    border-left-color: var(--rose);
    background: linear-gradient(90deg, rgba(251,113,133,.08) 0%, transparent 50%);
}
.alert-warm {
    border-left-color: var(--amber);
    background: linear-gradient(90deg, rgba(251,191,36,.06) 0%, transparent 50%);
}
.alert-grn  {
    border-left-color: var(--mint);
    background: linear-gradient(90deg, rgba(52,211,153,.06) 0%, transparent 50%);
}

/* ──── Pipeline / funnel steps ─────────────────────────────────────────── */
.pf-wrap {
    display: flex;
    gap: 0;
    background:
        linear-gradient(180deg, rgba(29,39,71,.4) 0%, rgba(11,16,32,.7) 100%);
    backdrop-filter: blur(10px);
    -webkit-backdrop-filter: blur(10px);
    border: 1px solid rgba(255,255,255,.06);
    border-radius: 16px;
    overflow: hidden;
    margin-bottom: 22px;
    box-shadow: var(--shadow-card);
}
.pf-step {
    flex: 1;
    padding: 18px 14px;
    font-size: .78rem;
    color: var(--smoke);
    border-right: 1px solid rgba(255,255,255,.04);
    transition: background .25s;
}
.pf-step:last-child { border-right: none; }
.pf-step:hover {
    background: rgba(52,211,153,.04);
}
.pf-step-active {
    background: linear-gradient(180deg, rgba(52,211,153,.08) 0%, rgba(56,189,248,.05) 100%);
    box-shadow: inset 0 -2px 0 var(--mint);
}
.pf-icon {
    font-size: 1.4rem;
    display: block;
    margin-bottom: 8px;
    filter: drop-shadow(0 0 8px currentColor);
}
.pf-n {
    font-family: 'Space Grotesk', sans-serif;
    font-size: .58rem;
    font-weight: 600;
    color: var(--slate);
    text-transform: uppercase;
    letter-spacing: 1px;
    margin-bottom: 3px;
}
.pf-name {
    font-size: .82rem;
    font-weight: 700;
    color: var(--fog);
    margin-bottom: 4px;
}
.pf-desc {
    font-size: .68rem;
    color: var(--dust);
    line-height: 1.45;
}

/* ──── Source bars ─────────────────────────────────────────────────────── */
.src-bar-row {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 7px 0;
}
.src-bar-name {
    font-family: 'Space Grotesk', sans-serif;
    font-size: .78rem;
    font-weight: 600;
    color: var(--fog);
    width: 90px;
    flex-shrink: 0;
}
.src-bar-track {
    flex: 1;
    background: rgba(11,16,32,.7);
    border-radius: 6px;
    height: 8px;
    overflow: hidden;
    border: 1px solid rgba(255,255,255,.04);
}
.src-bar-fill {
    height: 8px;
    border-radius: 5px;
    background: var(--grad-primary);
    box-shadow: 0 0 10px -2px rgba(52,211,153,.5);
    background-size: 200% 100%;
    animation: shimmer 4s linear infinite;
}
.src-bar-count {
    font-family: 'Space Grotesk', sans-serif;
    font-size: .8rem;
    font-weight: 700;
    color: var(--fog);
    width: 32px;
    text-align: right;
}

/* ──── Brand mark ──────────────────────────────────────────────────────── */
.imovela-mark {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 16px 0 12px;
    margin-bottom: 8px;
    border-bottom: 1px solid rgba(255,255,255,.05);
}
.imovela-logo {
    width: 38px; height: 38px;
    border-radius: 11px;
    background: var(--grad-primary);
    display: flex;
    align-items: center;
    justify-content: center;
    color: #052016;
    font-family: 'Space Grotesk', sans-serif;
    font-size: 1.1rem;
    font-weight: 800;
    box-shadow:
        0 6px 20px -6px rgba(52,211,153,.6),
        inset 0 -1px 2px rgba(0,0,0,.2);
    flex-shrink: 0;
    position: relative;
}
.imovela-logo::after {
    content: "";
    position: absolute; inset: 2px;
    border-radius: 9px;
    background: linear-gradient(135deg, rgba(255,255,255,.2), transparent 50%);
    pointer-events: none;
}
.imovela-name {
    font-family: 'Space Grotesk', sans-serif;
    font-size: 1.25rem;
    font-weight: 700;
    color: var(--ice);
    letter-spacing: -.01em;
    line-height: 1.1;
}
.imovela-tag {
    font-size: .68rem;
    color: var(--smoke);
    margin-top: 2px;
    letter-spacing: .4px;
}
.imovela-version {
    display: inline-block;
    margin-top: 6px;
    padding: 2px 8px;
    background: rgba(52,211,153,.1);
    border: 1px solid rgba(52,211,153,.25);
    border-radius: 999px;
    font-size: .58rem;
    font-weight: 700;
    color: var(--mint);
    letter-spacing: .8px;
    text-transform: uppercase;
}

/* ──── Hero header on main page ────────────────────────────────────────── */
.hero {
    position: relative;
    padding: 28px 32px;
    border-radius: 20px;
    background:
        radial-gradient(600px 320px at 0% 0%, rgba(52,211,153,.12), transparent 60%),
        radial-gradient(600px 320px at 100% 100%, rgba(167,139,250,.1), transparent 60%),
        linear-gradient(135deg, rgba(29,39,71,.7) 0%, rgba(11,16,32,.9) 100%);
    backdrop-filter: blur(16px) saturate(140%);
    -webkit-backdrop-filter: blur(16px) saturate(140%);
    border: 1px solid rgba(255,255,255,.07);
    box-shadow: var(--shadow-card);
    margin-bottom: 24px;
    overflow: hidden;
}
.hero::before {
    content: "";
    position: absolute;
    top: -2px; left: 0; right: 0; height: 2px;
    background: var(--grad-primary);
    background-size: 200% 100%;
    animation: shimmer 6s linear infinite;
}
.hero-title {
    font-family: 'Space Grotesk', sans-serif;
    font-size: 1.6rem;
    font-weight: 700;
    color: var(--ice);
    letter-spacing: -.025em;
    margin-bottom: 6px;
}
.hero-title-accent {
    background: var(--grad-primary);
    -webkit-background-clip: text;
            background-clip: text;
    -webkit-text-fill-color: transparent;
}
.hero-sub {
    font-size: .9rem;
    color: var(--smoke);
    line-height: 1.5;
}
</style>"""

st.markdown(_CSS_BASE, unsafe_allow_html=True)
st.markdown(_CSS_CARDS, unsafe_allow_html=True)


# ─── Helper functions ──────────────────────────────────────────────────────────

def label_emoji(label: str) -> str:
    """Plain-text emoji — safe for st.expander, st.radio, etc."""
    return {"HOT": "🔴", "WARM": "🟡", "COLD": "🔵"}.get(label, "⚪")

def badge_html(label: str) -> str:
    """HTML badge — only inside st.markdown(unsafe_allow_html=True)."""
    cls = {"HOT": "badge-hot", "WARM": "badge-warm", "COLD": "badge-cold"}.get(label, "badge-cold")
    return f'<span class="badge {cls}">{label}</span>'

def score_orb(score: int, label: str) -> str:
    cls = {"HOT": "orb-hot", "WARM": "orb-warm"}.get(label, "orb-cold")
    return f'<span class="score-orb {cls}">{score}</span>'

def fmt_price(p) -> str:
    if not p:
        return "—"
    return f"{float(p):,.0f} €".replace(",", ".")

def delta_html(delta) -> str:
    if delta is None:
        return ""
    if delta > 0:
        return f'<span class="delta-pos">&#9660; {delta:.1f}% abaixo mercado</span>'
    return f'<span class="delta-neg">&#9650; {abs(delta):.1f}% acima mercado</span>'

def owner_chip(is_owner, agency, owner_type: str = None) -> str:
    """Render owner type chip — uses owner_type when available, falls back to is_owner."""
    ot = owner_type or ("fsbo" if is_owner else "agency")
    if ot == "fsbo":
        return '<span class="chip" style="color:#10b981;border-color:rgba(16,185,129,.2);">&#128100; Particular</span>'
    if ot == "developer":
        return '<span class="chip" style="color:#f59e0b;border-color:rgba(245,158,11,.2);">&#127959; Promotor</span>'
    if ot == "unknown":
        return '<span class="chip" style="color:#94a3b8;border-color:rgba(148,163,184,.2);">&#10067; Desconhecido</span>'
    # agency (default)
    return f'<span class="chip">&#127970; {(agency or "Agência")[:20]}</span>'

def contact_chip(phone, email, contact_source: str = None) -> str:
    """
    Contact chip with optional source-type suffix.

    contact_source governs a small muted label appended to the chip:
      • "website:*"      → · Site    (agency homepage, generic office contact)
      • "cross_portal:*" → · Cross   (propagated from a matching listing on another portal)
      • anything else    → no suffix (direct listing contact — most trustworthy)
      • None             → no suffix (source unknown, treat as direct)
    """
    _src = contact_source or ""
    if _src.startswith("website:"):
        _suffix = (
            '<span style="font-size:.56rem;font-weight:700;opacity:.5;'
            'letter-spacing:.2px;margin-left:3px;">· Site</span>'
        )
    elif _src.startswith("cross_portal:"):
        _suffix = (
            '<span style="font-size:.56rem;font-weight:700;opacity:.5;'
            'letter-spacing:.2px;margin-left:3px;">· Cross</span>'
        )
    else:
        _suffix = ""

    if phone:
        return f'<span class="chip">&#128222; {phone}{_suffix}</span>'
    if email:
        return f'<span class="chip">&#9993; {email}{_suffix}</span>'
    return '<span class="chip" style="color:#33485e;">Sem contacto</span>'

def contact_badge(phone, email) -> str:
    """Distinctive badge showing contact availability — for card headers."""
    if phone:
        return '<span class="badge badge-phone">&#128222; TELEFONE</span>'
    if email:
        return '<span class="badge badge-email">&#9993; EMAIL</span>'
    return '<span class="badge badge-nocontact">SEM CONTACTO</span>'

def src_pill(source: str) -> str:
    return f'<span style="background:#16202f;border:1px solid #243450;border-radius:4px;font-size:.58rem;font-weight:800;padding:1px 5px;color:#56697e;text-transform:uppercase;">{source}</span>'

def confidence_chip(confidence) -> str:
    """Contact confidence chip: 100=Alta, 70+=Boa, 30+=Média, 0=Sem conf. Safe for None."""
    c = confidence or 0
    if c >= 100:
        return '<span class="chip" style="font-size:.65rem;color:#10b981;border-color:rgba(16,185,129,.2);">&#9679; Alta</span>'
    if c >= 70:
        return '<span class="chip" style="font-size:.65rem;color:#60a5fa;border-color:rgba(96,165,250,.2);">&#9679; Boa</span>'
    if c >= 30:
        return '<span class="chip" style="font-size:.65rem;color:#f59e0b;border-color:rgba(245,158,11,.2);">&#9679; Media</span>'
    return '<span class="chip" style="font-size:.65rem;color:#475569;border-color:rgba(148,163,184,.12);">&#9675; Sem conf.</span>'

def lead_type_chip(lead_type: str | None) -> str:
    """Visual chip for lead_type — fsbo / frbo / agency_listing / developer_listing / unknown."""
    lt = (lead_type or "unknown").lower()
    if lt == "fsbo":
        return '<span class="chip" style="color:#10b981;border-color:rgba(16,185,129,.2);font-size:.65rem;">&#128100; FSBO</span>'
    if lt == "frbo":
        return '<span class="chip" style="color:#60a5fa;border-color:rgba(96,165,250,.2);font-size:.65rem;">&#128273; FRBO</span>'
    if lt == "agency_listing":
        return '<span class="chip" style="color:#94a3b8;border-color:rgba(148,163,184,.15);font-size:.65rem;">&#127970; Agência</span>'
    if lt == "developer_listing":
        return '<span class="chip" style="color:#f59e0b;border-color:rgba(245,158,11,.2);font-size:.65rem;">&#127959; Promotor</span>'
    return '<span class="chip" style="color:#475569;border-color:rgba(148,163,184,.1);font-size:.65rem;">&#10067; —</span>'


def lead_quality_chip(lead_quality: str | None) -> str:
    """Visual chip for lead quality tier — high / medium / low."""
    lq = (lead_quality or "low").lower()
    if lq == "high":
        return '<span class="chip" style="color:#10b981;border-color:rgba(16,185,129,.25);font-size:.65rem;font-weight:700;">&#11088; Alta</span>'
    if lq == "medium":
        return '<span class="chip" style="color:#f59e0b;border-color:rgba(245,158,11,.2);font-size:.65rem;font-weight:700;">&#9679; Média</span>'
    return '<span class="chip" style="color:#475569;border-color:rgba(148,163,184,.1);font-size:.65rem;">&#9675; Baixa</span>'


def _gen_outreach_msg(typology, zone, price, owner_type, contact_name=None, first_name=None) -> str:
    """
    Generate a short outreach message using existing lead fields.
    Adapts tone to owner_type: fsbo (direct), agency, developer, unknown.
    """
    typ  = typology or "imóvel"
    z    = zone or "zona"
    prx  = f"{int(price):,}€".replace(",", " ") if price else "valor a confirmar"
    # Use first_name directly when available; fall back to splitting contact_name
    fname = first_name or ((contact_name or "").strip().split()[0] if contact_name else None)

    if owner_type == "fsbo":
        greet = f"Bom dia{f', {fname}' if fname else ''},"
        body  = (
            f"Vi o seu anúncio de {typ} em {z} pelo valor de {prx}.\n"
            f"Tenho interesse genuíno neste imóvel e gostaria de saber mais.\n"
            f"Poderia dar-me mais informações? Tenho disponibilidade para visita."
        )
    elif owner_type == "developer":
        greet = "Bom dia,"
        body  = (
            f"Venho manifestar interesse no {typ} disponível em {z} ({prx}).\n"
            f"Poderiam enviar-me informações adicionais ou documentação disponível?\n"
            f"Estou disponível para reunião ou visita conforme conveniência."
        )
    else:  # agency / unknown
        greet = "Bom dia,"
        body  = (
            f"Gostaria de obter mais informações sobre o {typ} em {z} anunciado por {prx}.\n"
            f"Tenho interesse e disponibilidade para visita breve.\n"
            f"Agradeço a atenção."
        )
    # Signature pulled from settings.contact_signature (configurable per
    # client — defaults to a neutral closing if unset).
    from config.settings import settings
    sig = (getattr(settings, "contact_signature", "") or "").strip()
    if not sig:
        sig = "Cumprimentos"
    return f"{greet}\n\n{body}\n\n{sig}"


def action_links(phone: str | None, email: str | None, sources: list | None = None) -> str:
    """
    Anchor-button links for immediate outreach — no JS, works in all browsers.
      tel:    → opens dialler on mobile / system phone app on desktop
      mailto: → opens default mail client
      source  → opens original listing in new tab (max 2)
    Returns empty string when nothing is available.
    """
    _BTN = (
        "display:inline-block;background:#111827;border:1px solid #243450;"
        "border-radius:6px;padding:3px 10px;font-size:.68rem;font-weight:700;"
        "text-decoration:none;"
    )
    parts = []
    if phone:
        tel = phone.replace(" ", "").replace("-", "")
        parts.append(f'<a href="tel:{tel}" style="{_BTN}color:#10b981;">&#128222; Ligar</a>')
    if email:
        parts.append(f'<a href="mailto:{email}" style="{_BTN}color:#60a5fa;">&#9993; Email</a>')
    if sources:
        for s in sources[:2]:
            url = (s.get("url") or "").strip()
            src = (s.get("source") or "").upper()
            if url:
                parts.append(
                    f'<a href="{url}" target="_blank" style="{_BTN}color:#56697e;">&#8599; {src}</a>'
                )
    if not parts:
        return ""
    return (
        '<div style="display:flex;gap:5px;flex-wrap:wrap;margin-top:6px;">'
        + "".join(parts)
        + "</div>"
    )

def source_label_pill(label: str, source: str) -> str:
    """Labeled source pill: 'LABEL [pill]'. Returns empty string if source is None/empty."""
    if not source:
        return ""
    return (
        f'<span style="font-size:.58rem;font-weight:700;color:#33485e;text-transform:uppercase;'
        f'letter-spacing:.4px;margin-right:2px;">{label} </span>'
        + src_pill(source)
    )

_MOTIV_RULES = [
    ("Heranca",   ["heranca", "herdeiro", "herdeiros", "herdar"],        "&#127968;"),
    ("Divorcio",  ["divorcio", "separacao", "separada", "separado"],     "&#128148;"),
    ("Emigracao", ["emigr", "mudar de pais", "partir para"],             "&#9992;"),
    ("Urgencia",  ["urgente", "urgencia", "venda rapida", "30 dias"],    "&#9889;"),
    ("Obras",     ["obra", "remodelar", "remodelacao", "restauro"],      "&#128296;"),
    ("Partilhas", ["partilha", "partilhas", "co-proprietario"],          "&#9878;"),
    ("Mudanca",   ["mudanca de cidade", "transferi", "relocac"],         "&#128230;"),
]

def detect_motivation(description: str) -> list[tuple[str, str]]:
    if not description:
        return []
    d = description.lower()
    # normalise accented chars for matching
    import unicodedata
    d_norm = ''.join(c for c in unicodedata.normalize('NFD', d) if unicodedata.category(c) != 'Mn')
    found = []
    for label, keywords, emoji in _MOTIV_RULES:
        if any(kw in d_norm for kw in keywords):
            found.append((label, emoji))
    return found

def generate_alerts(leads: list) -> list[dict]:
    alerts = []
    for lead in leads:
        lbl   = lead.get("label", "")
        score = lead.get("score", 0)
        zone  = lead.get("zone", "?")
        typo  = lead.get("typology", "?")
        price = fmt_price(lead.get("price"))
        delta = lead.get("price_delta_pct") or 0
        dom   = lead.get("days_on_market", 0)
        desc  = lead.get("description", "")

        if lbl == "HOT" and score >= 80:
            alerts.append({
                "type": "hot", "icon": "&#128308;",
                "title": f"HOT detectada — {score} pts",
                "body": f"{typo} em {zone} · {price} · {delta:.1f}% abaixo mercado",
                "meta": f"{dom} dias no mercado",
            })
        elif lead.get("is_owner") and delta > 12 and lbl in ("HOT", "WARM"):
            alerts.append({
                "type": "grn", "icon": "&#128100;",
                "title": f"Proprietario directo — {zone}",
                "body": f"{typo} · {price} · sem mediadora",
                "meta": f"Score {score} · {delta:.1f}% abaixo benchmark",
            })
        elif (lead.get("price_changes") or 0) > 0 and delta > 8:
            alerts.append({
                "type": "warm", "icon": "&#128201;",
                "title": f"Reducao de preco — {zone}",
                "body": f"{typo} · {price} · {lead.get('price_changes', 0)} reducao(oes)",
                "meta": f"Score {score} · {delta:.1f}% abaixo mercado",
            })

        motives = detect_motivation(desc)
        if motives and lbl in ("HOT", "WARM"):
            alerts.append({
                "type": "blue", "icon": motives[0][1],
                "title": f"Motivo de venda — {motives[0][0]}",
                "body": f"{typo} em {zone} · {price}",
                "meta": f"Score {score}",
            })

        if len(alerts) >= 8:
            break
    return alerts[:8]

def alert_card_html(a: dict) -> str:
    cls = {"hot": "alert-hot", "warm": "alert-warm", "grn": "alert-grn"}.get(a["type"], "")
    return (
        f'<div class="alert-card {cls}">'
        f'<div style="font-size:1rem;flex-shrink:0;">{a["icon"]}</div>'
        f'<div>'
        f'<div style="font-weight:700;color:#f1f5f9;font-size:.84rem;margin-bottom:2px;">{a["title"]}</div>'
        f'<div style="color:#94a3b8;font-size:.75rem;">{a["body"]}</div>'
        f'<div style="color:#56697e;font-size:.66rem;margin-top:3px;">{a["meta"]}</div>'
        f'</div></div>'
    )


# ─── Data loaders ──────────────────────────────────────────────────────────────

def _match_csource(src: str | None, csource_type: str) -> bool:
    """Return True when a lead's contact_source matches the requested category."""
    s = src or ""
    if csource_type == "website":
        return s.startswith("website:")
    if csource_type == "cross_portal":
        return s.startswith("cross_portal:")
    if csource_type == "direto":
        # Direct = has a non-empty source that is NOT website: or cross_portal:
        return bool(s) and not s.startswith(("website:", "cross_portal:"))
    return True  # unknown type — pass through


@st.cache_data(ttl=60)
def load_leads(zone=None, typology=None, score_min=0, stage=None, label=None, is_demo=None, contact=None, owner_type=None, csource_type=None):
    from storage.database import init_db, get_db
    from storage.repository import LeadRepo
    init_db()
    with get_db() as db:
        leads = LeadRepo(db).list_active(
            zone=zone, typology=typology,
            score_min=score_min, crm_stage=stage,
            label=label, is_demo=is_demo, contact=contact,
            owner_type=owner_type, limit=500,
        )
        rows = [{
            "id":               l.id,
            "is_demo":          l.is_demo,
            "score":            l.score,
            "label":            l.score_label,
            "title":            l.title,
            "typology":         l.typology,
            "zone":             l.zone,
            "price":            l.price,
            "area_m2":          l.area_m2,
            "price_per_m2":     l.price_per_m2,
            "price_benchmark":  l.price_benchmark,
            "price_delta_pct":  l.price_delta_pct,
            "is_owner":         l.is_owner,
            "owner_type":       l.owner_type,
            "contact_name":     l.contact_name,
            "first_name":       getattr(l, "first_name", None),
            "last_name":        getattr(l, "last_name", None),
            "birthday":         getattr(l, "birthday", None),
            "contact_phone":    l.contact_phone,
            "phone_type":       getattr(l, "phone_type", "unknown"),
            "contact_email":    l.contact_email,
            "has_phone":        bool(l.contact_phone),
            "has_email":        bool(l.contact_email),
            "has_contact":      bool(l.contact_phone or l.contact_email),
            "agency_name":      l.agency_name,
            "days_on_market":   l.days_on_market,
            "price_changes":    l.price_changes,
            "crm_stage":        l.crm_stage,
            "condition":        l.condition,
            "description":      (l.description or "")[:300],
            "address":          l.address,
            "first_seen_at":    l.first_seen_at.strftime("%d/%m/%Y") if l.first_seen_at else "—",
            "sources":          l.sources,
            "score_breakdown":  l.get_score_breakdown(),
            "discovery_source":   l.discovery_source,
            "contact_source":     getattr(l, "contact_source", None),
            "contact_confidence": l.contact_confidence,
            "lead_type":          getattr(l, "lead_type",    None),
            "lead_quality":       getattr(l, "lead_quality", None),
            "parish":             getattr(l, "parish",       None),
        } for l in leads]
        if csource_type:
            rows = [r for r in rows if _match_csource(r.get("contact_source"), csource_type)]
        return rows

@st.cache_data(ttl=60)
def load_stats():
    from storage.database import init_db
    from reports.generator import ReportGenerator
    init_db()
    return ReportGenerator().get_summary_stats()


# ─── Sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(
        '<div class="imovela-mark" style="padding:0 16px 14px;margin:14px 0 12px;">'
        '  <div class="imovela-logo">◆</div>'
        '  <div style="flex:1;">'
        '    <div class="imovela-name">Imovela</div>'
        '    <div class="imovela-tag">Lead intelligence imobiliária</div>'
        f'    <div class="imovela-version">{datetime.now().strftime("%d %b · %H:%M")}</div>'
        '  </div>'
        '</div>',
        unsafe_allow_html=True,
    )

    st.markdown('<div class="lbl-section">Navegacao</div>', unsafe_allow_html=True)
    page = st.radio("nav", [
        "&#128202;  Dashboard",
        "&#127919;  Oportunidades",
        "&#128203;  CRM",
        "&#128268;  Pre-Market",
        "&#9881;  Motor",
        "&#128228;  Exportar",
    ], label_visibility="collapsed")

    st.divider()
    st.markdown('<div class="lbl-section">Origem dos dados</div>', unsafe_allow_html=True)
    _DATA_MODES = ["Todos", "&#128994; Apenas reais", "&#128993; Apenas demo"]
    data_mode = st.radio("data_mode", _DATA_MODES, label_visibility="collapsed")

    st.divider()
    st.markdown('<div class="lbl-section">Contacto</div>', unsafe_allow_html=True)
    _CONTACT_MODES = ["Todos", "&#128222; Com telefone", "&#128241; Só telemóvel real", "&#9993; Com email", "&#9989; Qualquer contacto", "&#10060; Sem contacto"]
    contact_mode = st.radio("contact_mode", _CONTACT_MODES, label_visibility="collapsed")
    exclude_relay = st.checkbox("Excluir relay/OLX (6xx)", value=False, help="Remove números temporários OLX que expiram quando o anúncio sai")

    st.divider()
    st.markdown('<div class="lbl-section">Filtros</div>', unsafe_allow_html=True)
    ZONES = ["Todas as zonas", "Lisboa", "Cascais", "Sintra", "Almada", "Seixal", "Sesimbra"]
    sel_zone = st.selectbox("Zona", ZONES)
    TYPOS = ["Todas as tipologias", "T0", "T1", "T2", "T3", "T4+", "Moradia"]
    sel_typology = st.selectbox("Tipologia", TYPOS)
    _OWNER_MODES = ["Todos", "&#128100; Particular (FSBO)", "&#127970; Agência", "&#127959; Promotor", "&#10067; Desconhecido"]
    owner_mode = st.selectbox("Tipo vendedor", _OWNER_MODES)
    _LEAD_TYPE_MODES = ["Todos", "&#127968; FSBO (venda)", "&#128273; FRBO (arrendamento)", "&#128101; Active Owner", "&#127970; Agência", "&#128679; Promotor"]
    lead_type_mode = st.selectbox("Tipo de lead", _LEAD_TYPE_MODES)
    _CSOURCE_MODES = ["Todos", "Direto", "Agência / Site", "Cross-portal"]
    csource_mode = st.selectbox("Origem do contacto", _CSOURCE_MODES)
    score_floor = st.slider("Pontuacao minima", 0, 100, 0)
    st.divider()
    if st.button("Actualizar oportunidades", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    if st.button("Executar analise de mercado", use_container_width=True):
        with st.spinner("A analisar o mercado..."):
            try:
                from pipeline.runner import PipelineRunner
                from scoring.scorer import Scorer
                r = PipelineRunner().run_full()
                Scorer().score_all_pending()
                st.cache_data.clear()
                st.success(f"+{r.leads_created} novas · {r.leads_updated} actualizadas")
            except Exception as e:
                st.error(f"Erro: {e}")
    st.markdown(
        '<div style="font-size:.62rem;color:var(--dust);text-align:center;'
        'padding:14px 0 8px;letter-spacing:.6px;">'
        '<span style="color:var(--mint);">◆</span> Imovela &middot; v2.0'
        '</div>',
        unsafe_allow_html=True,
    )

zone_filter = None if sel_zone == "Todas as zonas" else sel_zone
typo_filter = None if sel_typology == "Todas as tipologias" else sel_typology
# Map radio selection → is_demo filter value passed to load_leads / LeadRepo
_demo_filter: bool | None = None
if data_mode == "&#128994; Apenas reais":
    _demo_filter = False
elif data_mode == "&#128993; Apenas demo":
    _demo_filter = True

# Map contact radio → contact filter string passed to LeadRepo
_contact_filter: str | None = None
_mobile_only: bool = False
if contact_mode == "&#128222; Com telefone":
    _contact_filter = "phone"
elif contact_mode == "&#128241; Só telemóvel real":
    _contact_filter = "phone"
    _mobile_only = True
elif contact_mode == "&#9993; Com email":
    _contact_filter = "email"
elif contact_mode == "&#9989; Qualquer contacto":
    _contact_filter = "any"
elif contact_mode == "&#10060; Sem contacto":
    _contact_filter = "none"

# Map owner_mode → owner_type filter string passed to LeadRepo
_owner_filter: str | None = None
if owner_mode == "&#128100; Particular (FSBO)":
    _owner_filter = "fsbo"
elif owner_mode == "&#127970; Agência":
    _owner_filter = "agency"
elif owner_mode == "&#127959; Promotor":
    _owner_filter = "developer"
elif owner_mode == "&#10067; Desconhecido":
    _owner_filter = "unknown"

# Map lead_type_mode → lead_type filter (applied post-load in-memory)
_lead_type_filter: str | None = None
if lead_type_mode == "&#127968; FSBO (venda)":
    _lead_type_filter = "fsbo"
elif lead_type_mode == "&#128273; FRBO (arrendamento)":
    _lead_type_filter = "frbo"
elif lead_type_mode == "&#128101; Active Owner":
    _lead_type_filter = "active_owner"
elif lead_type_mode == "&#127970; Agência":
    _lead_type_filter = "agency_listing"
elif lead_type_mode == "&#128679; Promotor":
    _lead_type_filter = "developer_listing"

# Map csource_mode → contact source category filter (applied post-load in load_leads)
_csource_filter: str | None = None
if csource_mode == "Direto":
    _csource_filter = "direto"
elif csource_mode == "Agência / Site":
    _csource_filter = "website"
elif csource_mode == "Cross-portal":
    _csource_filter = "cross_portal"


# ══════════════════════════════════════════════════════════════════════════════
#  PAGE: DASHBOARD
# ══════════════════════════════════════════════════════════════════════════════
if page == "&#128202;  Dashboard":

    stats = load_stats()
    leads = load_leads(zone=zone_filter, typology=typo_filter, score_min=score_floor, is_demo=_demo_filter, contact=_contact_filter, owner_type=_owner_filter, csource_type=_csource_filter)
    if _lead_type_filter:
        leads = [l for l in leads if l.get("lead_type") == _lead_type_filter]
    if _mobile_only:
        leads = [l for l in leads if l.get("phone_type") == "mobile"]
    elif exclude_relay:
        leads = [l for l in leads if l.get("phone_type") != "relay"]
    df    = pd.DataFrame(leads) if leads else pd.DataFrame()

    hot_n  = stats.get("hot_count", 0)
    warm_n = stats.get("warm_count", 0)
    total  = stats.get("total_active", 0)
    avg_s  = stats.get("avg_score", 0)

    # Hero banner — animated gradient + live KPIs
    st.markdown(
        f'<div class="hero">'
        f'  <div class="hero-title">'
        f'    <span class="hero-title-accent">Imovela</span> '
        f'    <span style="color:var(--smoke);font-weight:500;">·</span> '
        f'    Intelligence Dashboard'
        f'  </div>'
        f'  <div class="hero-sub">'
        f'    Lead intelligence em tempo real &middot; OLX, Imovirtual, Idealista, Sapo, Custojusto'
        f'    &middot; análise contínua &middot; alertas HOT automáticos'
        f'  </div>'
        f'  <div style="display:flex;gap:14px;margin-top:18px;flex-wrap:wrap;">'
        f'    <div class="intel-box" style="margin:0;min-width:120px;text-align:center;">'
        f'      <div class="intel-lbl">HOT agora</div>'
        f'      <div class="intel-val" style="color:var(--rose);">{hot_n}</div>'
        f'    </div>'
        f'    <div class="intel-box" style="margin:0;min-width:120px;text-align:center;">'
        f'      <div class="intel-lbl">WARM</div>'
        f'      <div class="intel-val" style="color:var(--amber);">{warm_n}</div>'
        f'    </div>'
        f'    <div class="intel-box" style="margin:0;min-width:120px;text-align:center;">'
        f'      <div class="intel-lbl">Total ativo</div>'
        f'      <div class="intel-val">{total}</div>'
        f'    </div>'
        f'    <div class="intel-box" style="margin:0;min-width:120px;text-align:center;">'
        f'      <div class="intel-lbl">Score médio</div>'
        f'      <div class="intel-val" style="color:var(--sky);">{avg_s}</div>'
        f'    </div>'
        f'  </div>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # KPI metrics
    k1, k2, k3, k4, k5 = st.columns(5)
    with k1: st.metric("🔴 HOT",         hot_n,  delta=f"+{stats.get('hot_today',0)} hoje",  help="Score >= 75")
    with k2: st.metric("🟡 WARM",        warm_n,                                              help="Score 50-74")
    with k3: st.metric("📥 Hoje",        stats.get("added_today", 0),                         help="Ultimas 24h")
    with k4:
        act = sum(v for k, v in stats.get("by_stage", {}).items() if k not in ("ganho", "perdido", "arquivado"))
        st.metric("🔄 Em Negociacao",   act,                                                  help="Leads activos no funil")
    with k5: st.metric("⭐ Score Medio", avg_s,                                               help="Media das oportunidades activas")

    # ── Contact availability row ───────────────────────────────────────────────
    ck1, ck2, ck3 = st.columns(3)
    with ck1: st.metric("📞 Com Telefone", stats.get("with_phone_count", 0), help="Leads com numero de telefone — contacto directo imediato (+15 pts)")
    with ck2: st.metric("✉️ Com Email",    stats.get("with_email_count", 0), help="Leads com email disponivel (+5 pts)")
    with ck3: st.metric("🚫 Sem Contacto", stats.get("no_contact_count", 0), help="Leads sem qualquer contacto — penalizacao -15 pts no score")

    # ── Contact confidence tier breakdown (from loaded leads, respects active filters) ──
    n_conf_high = sum(1 for l in leads if (l.get("contact_confidence") or 0) >= 70)
    n_conf_med  = sum(1 for l in leads if (l.get("contact_confidence") or 0) == 30)
    n_conf_zero = sum(1 for l in leads if (l.get("contact_confidence") or 0) == 0)
    ck4, ck5, ck6 = st.columns(3)
    with ck4: st.metric("🟢 Conf. Alta",    n_conf_high, help="contact_confidence >= 70 — telefone ou email confirmado")
    with ck5: st.metric("🟡 Conf. Media",   n_conf_med,  help="contact_confidence = 30 — nome disponivel, sem contacto directo")
    with ck6: st.metric("⚫ Sem Confianca",  n_conf_zero, help="contact_confidence = 0 — sem qualquer dado de contacto")

    # ── Contact source type breakdown (respects active filters) ──────────────
    n_src_direto  = sum(1 for l in leads if _match_csource(l.get("contact_source"), "direto"))
    n_src_website = sum(1 for l in leads if _match_csource(l.get("contact_source"), "website"))
    n_src_cross   = sum(1 for l in leads if _match_csource(l.get("contact_source"), "cross_portal"))
    ck7, ck8, ck9 = st.columns(3)
    with ck7: st.metric("📋 Direto",         n_src_direto,  help="Contacto do próprio anúncio — fonte mais fiável")
    with ck8: st.metric("🌐 Agência / Site",  n_src_website, help="Contacto obtido via website da agência (contact_source: website:*)")
    with ck9: st.metric("🔀 Cross-portal",    n_src_cross,   help="Contacto propagado de imóvel equivalente noutro portal (contact_source: cross_portal:*)")

    # ── Lead quality + type breakdown ─────────────────────────────────────────
    n_lq_high   = sum(1 for l in leads if l.get("lead_quality") == "high")
    n_lq_mid    = sum(1 for l in leads if l.get("lead_quality") == "medium")
    n_lt_fsbo   = sum(1 for l in leads if l.get("lead_type") == "fsbo")
    n_lt_frbo   = sum(1 for l in leads if l.get("lead_type") == "frbo")
    n_lt_agency = sum(1 for l in leads if l.get("lead_type") == "agency_listing")
    qk1, qk2, qk3, qk4, qk5 = st.columns(5)
    with qk1: st.metric("⭐ Alta qualidade", n_lq_high,   help="Telefone/WA + proprietário directo (FSBO/FRBO) — máxima accionabilidade")
    with qk2: st.metric("🟡 Média qualidade",n_lq_mid,    help="Email/site disponível, ou proprietário sem contacto directo")
    with qk3: st.metric("🏠 FSBO",          n_lt_fsbo,   help="For Sale By Owner — proprietário vende directamente")
    with qk4: st.metric("🔑 FRBO",          n_lt_frbo,   help="For Rent By Owner — proprietário arrenda directamente")
    with qk5: st.metric("🏢 Agências",       n_lt_agency, help="Anúncios de imobiliárias / mediadoras")

    st.divider()

    # ── Top Acionáveis ────────────────────────────────────────────────────────
    # Ranked by: contact_confidence → source quality → score → owner_type
    # Only leads with contact_confidence > 0 (i.e. at least one contact field)
    def _src_quality(src):
        s = src or ""
        if not s:                              return 0
        if s.startswith("website:"):           return 1
        if s.startswith("cross_portal:"):      return 2
        return 3   # direct portal source — most trustworthy

    _OT_QUALITY = {"fsbo": 2, "developer": 1, "unknown": 1, "agency": 0}

    _actionable = sorted(
        [l for l in leads if (l.get("contact_confidence") or 0) > 0],
        key=lambda l: (
            l.get("contact_confidence") or 0,
            _src_quality(l.get("contact_source")),
            l.get("score") or 0,
            _OT_QUALITY.get(l.get("owner_type") or "unknown", 1),
        ),
        reverse=True,
    )[:5]

    if _actionable:
        st.markdown('<div class="lbl-section">Top Accionaveis</div>', unsafe_allow_html=True)
        for rank, row in enumerate(_actionable, 1):
            lbl      = row.get("label", "COLD")
            area     = row.get("area_m2")
            area_txt = f' · {area:.0f} m²' if area else ''
            days     = row.get("days_on_market", 0)
            st.markdown(
                f'<div class="card" style="padding:9px 14px;margin-bottom:5px;">'
                f'<div style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;">'
                f'<span style="font-size:.65rem;font-weight:900;color:#33485e;min-width:16px;">#{rank}</span>'
                f'{score_orb(row["score"], lbl)}'
                f'<div style="flex:1;min-width:120px;">'
                f'<span style="font-size:.82rem;font-weight:600;color:#94a3b8;">{(row.get("title") or "—")[:55]}</span>'
                f'<span style="font-size:.68rem;color:#56697e;margin-left:6px;">'
                f'{row.get("typology","?")} · {row.get("zone","?")}{area_txt}</span>'
                f'</div>'
                f'<span class="price" style="font-size:.82rem;">{fmt_price(row.get("price"))}</span>'
                f'{contact_chip(row.get("contact_phone"), row.get("contact_email"), row.get("contact_source"))}'
                f'{confidence_chip(row.get("contact_confidence"))}'
                f'{owner_chip(row.get("is_owner"), row.get("agency_name"), row.get("owner_type"))}'
                f'{lead_type_chip(row.get("lead_type"))}'
                f'{lead_quality_chip(row.get("lead_quality"))}'
                f'<span class="chip" style="font-size:.62rem;color:#33485e;">⏱ {days}d</span>'
                f'</div>'
                f'{action_links(row.get("contact_phone"), row.get("contact_email"), row.get("sources"))}'
                f'</div>',
                unsafe_allow_html=True,
            )
            if row.get("crm_stage") == "novo":
                _, _btn_col = st.columns([4, 2])
                with _btn_col:
                    if st.button("📞 Marcar contactado", key=f"mk_{row['id']}", use_container_width=True):
                        from crm.manager import CRMManager
                        CRMManager().move_to_stage(row["id"], "contactado")
                        st.cache_data.clear()
                        st.rerun()
            _, _gm_col = st.columns([4, 2])
            with _gm_col:
                if st.button("💬 Mensagem", key=f"gb_{row['id']}", use_container_width=True):
                    _k = f"_gmsg_{row['id']}"
                    _was = st.session_state.get(_k, False)
                    st.session_state[_k] = not _was
                    if not _was:  # toggled ON → log once
                        from crm.manager import CRMManager
                        CRMManager().add_note(row["id"], "Mensagem de contacto sugerida gerada", "internal")
            if st.session_state.get(f"_gmsg_{row['id']}"):
                st.code(
                    _gen_outreach_msg(
                        row.get("typology"), row.get("zone"), row.get("price"),
                        row.get("owner_type"), row.get("contact_name"),
                        first_name=row.get("first_name"),
                    ),
                    language="",
                )
            with st.expander("📝 Nota rápida", expanded=False):
                _nk = st.session_state.get(f"_nk_{row['id']}", 0)
                _na, _nb = st.columns([5, 1])
                with _na:
                    _nota = st.text_input(
                        "nota", label_visibility="collapsed",
                        placeholder="Ex: Ligou, aceita visita. Pedir documentos...",
                        key=f"qn_{row['id']}_{_nk}",
                    )
                with _nb:
                    if st.button("💾", key=f"qnb_{row['id']}", use_container_width=True):
                        if _nota.strip():
                            from crm.manager import CRMManager
                            CRMManager().add_note(row["id"], _nota.strip())
                            st.session_state[f"_nk_{row['id']}"] = _nk + 1
                            st.toast("Nota guardada ✓")
                            st.rerun()
        st.divider()

    # Top HOT leads
    st.markdown('<div class="lbl-section">Oportunidades Prioritarias</div>', unsafe_allow_html=True)

    if df.empty:
        st.info("Sem oportunidades. Execute `python main.py seed-demo` para carregar dados de demonstracao.")
    else:
        # Leads with contact first (phone > email > none), then by score
        hot_with_contact    = [l for l in leads if l["label"] == "HOT" and l.get("has_contact")]
        hot_without_contact = [l for l in leads if l["label"] == "HOT" and not l.get("has_contact")]
        # Prioritise: HOT com contacto → HOT sem contacto → qualquer lead com contacto → fallback
        show = (hot_with_contact[:6] or hot_without_contact[:6] or
                [l for l in leads if l.get("has_contact")][:4] or leads[:4])
        ca, cb = st.columns(2, gap="medium")
        for i, row in enumerate(show):
            lbl  = row["label"]
            days = row.get("days_on_market", 0)
            area = row.get("area_m2")
            cond = f" · {row['condition']}" if row.get("condition") else ""
            sources_html = " ".join(src_pill(s["source"]) for s in (row.get("sources") or []))
            extras = ""
            if row.get("is_demo"):
                extras += '<span class="badge badge-demo">DEMO</span>'
            if (row.get("price_delta_pct") or 0) > 10:
                extras += '<span class="badge badge-drop">Reducao</span>'
            if row.get("is_owner"):
                extras += '<span class="badge badge-owner">Owner</span>'
            extras += contact_badge(row.get("contact_phone"), row.get("contact_email"))
            area_txt = f' · {area:.0f} m²' if area else ''
            demo_cls = " card-demo" if row.get("is_demo") else ""
            with (ca if i % 2 == 0 else cb):
                st.markdown(
                    f'<div class="card card-{lbl.lower()}{demo_cls}">'
                    f'<div style="display:flex;gap:12px;align-items:flex-start;">'
                    f'{score_orb(row["score"], lbl)}'
                    f'<div style="flex:1;min-width:0;">'
                    f'<div style="margin-bottom:6px;">{badge_html(lbl)}{extras} '
                    f'<span style="font-size:.72rem;color:#56697e;">{row.get("typology","?")} · {row.get("zone","?")}{cond}</span> {sources_html}</div>'
                    f'<div style="font-size:.86rem;font-weight:600;color:#94a3b8;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;margin-bottom:8px;">{(row.get("title") or "—")[:70]}</div>'
                    f'<div style="display:flex;align-items:baseline;gap:8px;margin-bottom:8px;">'
                    f'<span class="price">{fmt_price(row.get("price"))}</span>'
                    f'{delta_html(row.get("price_delta_pct"))}</div>'
                    f'<div>{owner_chip(row.get("is_owner"), row.get("agency_name"), row.get("owner_type"))} '
                    f'{lead_type_chip(row.get("lead_type"))} '
                    f'{lead_quality_chip(row.get("lead_quality"))} '
                    f'{contact_chip(row.get("contact_phone"), row.get("contact_email"), row.get("contact_source"))} '
                    f'{confidence_chip(row.get("contact_confidence"))} '
                    f'<span class="chip">&#9201; {days}d{area_txt}</span> '
                    f'{source_label_pill("via", row.get("discovery_source"))}</div>'
                    f'</div></div></div>',
                    unsafe_allow_html=True,
                )

    st.divider()

    if not df.empty:
        BG = "#0d1220"; GRD = "#1a2640"; TXT = "#4d6280"
        FNT = dict(family="Inter", color=TXT, size=11)

        ch1, ch2 = st.columns(2, gap="large")
        with ch1:
            st.markdown('<div class="lbl-section">Distribuicao de Pontuacoes</div>', unsafe_allow_html=True)
            fig = go.Figure()
            fig.add_trace(go.Histogram(x=df["score"], nbinsx=20, marker=dict(color="#1e3a6e", opacity=.9)))
            fig.add_vline(x=75, line_dash="dot", line_color="#f43f5e", line_width=1.5,
                          annotation_text="HOT", annotation_font_color="#f43f5e", annotation_font_size=10)
            fig.add_vline(x=50, line_dash="dot", line_color="#f59e0b", line_width=1.5,
                          annotation_text="WARM", annotation_font_color="#f59e0b", annotation_font_size=10)
            fig.update_layout(paper_bgcolor=BG, plot_bgcolor=BG, margin=dict(l=0, r=8, t=8, b=0),
                               height=200, font=FNT, showlegend=False,
                               xaxis=dict(gridcolor=GRD, linecolor=GRD),
                               yaxis=dict(gridcolor=GRD, linecolor=GRD))
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})

        with ch2:
            st.markdown('<div class="lbl-section">Oportunidades por Zona</div>', unsafe_allow_html=True)
            zd = stats.get("by_zone", {})
            if zd:
                sz  = sorted(zd.items(), key=lambda x: x[1])
                mx  = max(v for _, v in sz) if sz else 1
                clrs = ["#3b82f6" if v == mx else "#1a3258" for _, v in sz]
                fig2 = go.Figure(go.Bar(
                    x=[v for _, v in sz], y=[k for k, _ in sz], orientation="h",
                    marker=dict(color=clrs, line=dict(width=0)),
                    text=[str(v) for _, v in sz], textposition="outside",
                    textfont=dict(color=TXT, size=11),
                ))
                fig2.update_layout(paper_bgcolor=BG, plot_bgcolor=BG, margin=dict(l=0, r=40, t=8, b=0),
                                    height=200, font=FNT, showlegend=False,
                                    xaxis=dict(gridcolor=GRD, showticklabels=False),
                                    yaxis=dict(gridcolor="rgba(0,0,0,0)", tickfont=dict(color="#6a8aaa", size=11)))
                st.plotly_chart(fig2, use_container_width=True, config={"displayModeBar": False})

        # Alertas + Funil
        st.divider()
        al_col, fu_col = st.columns([3, 2], gap="large")
        with al_col:
            st.markdown('<div class="lbl-section">Alertas Comerciais</div>', unsafe_allow_html=True)
            alerts = generate_alerts(leads)
            if alerts:
                for a in alerts[:5]:
                    st.markdown(alert_card_html(a), unsafe_allow_html=True)
            else:
                st.caption("Sem alertas activos.")
        with fu_col:
            st.markdown('<div class="lbl-section">Funil de Negociacao</div>', unsafe_allow_html=True)
            stage_cfg = [
                ("novo",        "📥 Novo",        "#3b82f6"),
                ("contactado",  "📞 Contactado",  "#8b5cf6"),
                ("negociacao",  "🤝 Negociacao",  "#f59e0b"),
                ("ganho",       "✅ Ganho",        "#10b981"),
                ("perdido",     "❌ Perdido",      "#475569"),
            ]
            sd = stats.get("by_stage", {})
            for sk, sl, sc in stage_cfg:
                cnt = sd.get(sk, 0)
                st.markdown(
                    f'<div class="kanban-card" style="border-top:3px solid {sc};margin-bottom:8px;'
                    f'display:flex;justify-content:space-between;align-items:center;">'
                    f'<div style="font-size:.72rem;font-weight:700;color:#56697e;text-transform:uppercase;letter-spacing:.5px;">{sl}</div>'
                    f'<div style="font-size:1.8rem;font-weight:900;color:{sc};letter-spacing:-1.5px;">{cnt}</div>'
                    f'</div>',
                    unsafe_allow_html=True,
                )


# ══════════════════════════════════════════════════════════════════════════════
#  PAGE: OPORTUNIDADES
# ══════════════════════════════════════════════════════════════════════════════
elif page == "&#127919;  Oportunidades":

    st.markdown(
        '<div style="padding:1.5rem 0 1.2rem;border-bottom:1px solid #1a2640;margin-bottom:1.5rem;">'
        '<div style="font-size:.62rem;font-weight:700;color:#3b82f6;letter-spacing:1px;text-transform:uppercase;margin-bottom:4px;">Mercado</div>'
        '<div style="font-size:1.55rem;font-weight:900;color:#f1f5f9;letter-spacing:-.5px;">Ranking de Oportunidades</div>'
        '<div style="font-size:.82rem;color:#56697e;margin-top:4px;">Todas as propriedades detectadas, classificadas por pontuacao</div>'
        '</div>',
        unsafe_allow_html=True,
    )

    fa, fb, fc, fd = st.columns([1.2, 1.2, 1, 1])
    with fa: sel_label = st.selectbox("Classificacao", ["Todas", "🔴 HOT", "🟡 WARM", "🔵 COLD"])
    with fb: sel_stage = st.selectbox("Fase", ["Todas as fases", "novo", "contactado", "negociacao", "ganho", "perdido"])
    with fc: only_owner = st.checkbox("So proprietarios directos")
    with fd: only_drop  = st.checkbox("So com reducao de preco")

    lmap = {"Todas": None, "🔴 HOT": "HOT", "🟡 WARM": "WARM", "🔵 COLD": "COLD"}
    leads = load_leads(
        zone=zone_filter, typology=typo_filter, score_min=score_floor,
        stage=None if sel_stage == "Todas as fases" else sel_stage,
        label=lmap.get(sel_label),
        is_demo=_demo_filter,
        contact=_contact_filter,
        owner_type=_owner_filter,
        csource_type=_csource_filter,
    )
    if only_owner: leads = [l for l in leads if l.get("owner_type") in ("fsbo", None) or l.get("is_owner")]
    if only_drop:  leads = [l for l in leads if (l.get("price_delta_pct") or 0) > 0]
    if _lead_type_filter:
        leads = [l for l in leads if l.get("lead_type") == _lead_type_filter]
    if _mobile_only:
        leads = [l for l in leads if l.get("phone_type") == "mobile"]
    elif exclude_relay:
        leads = [l for l in leads if l.get("phone_type") != "relay"]

    hn = sum(1 for l in leads if l["label"] == "HOT")
    wn = sum(1 for l in leads if l["label"] == "WARM")
    cn = sum(1 for l in leads if l["label"] == "COLD")
    n_mobile = sum(1 for l in leads if l.get("phone_type") == "mobile")
    st.caption(f"{len(leads)} oportunidades · 🔴 {hn} HOT · 🟡 {wn} WARM · 🔵 {cn} COLD · 📱 {n_mobile} telemóvel real")

    if leads:
        df = pd.DataFrame(leads)
        dc = ["score", "label", "typology", "zone", "price", "price_delta_pct",
              "area_m2", "is_owner", "price_changes", "days_on_market", "crm_stage", "contact_phone"]
        dd = df[dc].copy()
        dd.columns = ["Pontuacao", "Class.", "Tipo", "Zona", "Preco (EUR)", "vs. Mercado %",
                      "Area m2", "Owner", "Reducoes", "Dias", "Fase", "Telefone"]
        st.dataframe(dd, use_container_width=True, height=420,
            column_config={
                "Pontuacao":     st.column_config.ProgressColumn("Pontuacao", min_value=0, max_value=100, format="%d pts"),
                "Class.":        st.column_config.TextColumn("Class.", width="small"),
                "Preco (EUR)":   st.column_config.NumberColumn("Preco", format="%.0f EUR"),
                "vs. Mercado %": st.column_config.NumberColumn("vs. Mercado", format="%.1f%%"),
                "Owner":         st.column_config.CheckboxColumn("Owner"),
                "Reducoes":      st.column_config.NumberColumn("Reducoes", format="%d"),
                "Dias":          st.column_config.NumberColumn("Dias", format="%d"),
            }, hide_index=True)

        # Intelligence detail
        st.divider()
        st.markdown('<div class="lbl-section">Relatorio de Inteligencia</div>', unsafe_allow_html=True)
        opts = [
            f"#{l['id']}  {l['score']}pts  [{l['label']}]  {l.get('typology','?')} {l.get('zone','?')}  —  {(l.get('title') or '')[:40]}"
            for l in leads[:60]
        ]
        sel = st.selectbox("Seleccionar oportunidade:", opts)

        if sel:
            sid = int(sel.split()[0].lstrip("#"))
            ld  = next((l for l in leads if l["id"] == sid), None)
            if ld:
                lbl   = ld["label"]
                area  = ld.get("area_m2")
                ppm2  = ld.get("price_per_m2")
                bench = ld.get("price_benchmark")
                sources_html = " ".join(src_pill(s["source"]) for s in (ld.get("sources") or []))

                motives = detect_motivation(ld.get("description", ""))
                motiv_html = " ".join(
                    f'<span style="background:rgba(139,92,246,.1);color:#a78bfa;border:1px solid rgba(139,92,246,.2);'
                    f'border-radius:20px;font-size:.64rem;font-weight:700;padding:2px 8px;">{e} {lb}</span>'
                    for lb, e in motives
                ) if motives else ""

                _demo_card_cls = " card-demo" if ld.get("is_demo") else ""
                _demo_badge    = '<span class="badge badge-demo">DEMO</span> ' if ld.get("is_demo") else ""
                _contact_bdg   = contact_badge(ld.get("contact_phone"), ld.get("contact_email"))
                # Confidence vars for intel-box
                _conf_c   = ld.get("contact_confidence") or 0
                _conf_lbl = "Alta" if _conf_c >= 100 else ("Boa" if _conf_c >= 70 else ("Media" if _conf_c >= 30 else "Sem conf."))
                _conf_clr = "#10b981" if _conf_c >= 100 else ("#60a5fa" if _conf_c >= 70 else ("#f59e0b" if _conf_c >= 30 else "#475569"))
                # Nome / Apelido / Aniversario row (shown when first_name exists)
                _name_row = ""
                if ld.get("first_name"):
                    _bday = ld.get("birthday") or "—"
                    _name_row = (
                        '<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:10px;">'
                        f'<div class="intel-box"><div class="intel-lbl">Nome</div><div class="intel-val" style="font-size:.85rem;">{ld["first_name"]}</div></div>'
                        f'<div class="intel-box"><div class="intel-lbl">Apelido</div><div class="intel-val" style="font-size:.85rem;">{ld.get("last_name") or "—"}</div></div>'
                        f'<div class="intel-box"><div class="intel-lbl">Aniversario</div><div class="intel-val" style="font-size:.85rem;">{_bday}</div></div>'
                        '</div>'
                    )
                st.markdown(
                    f'<div class="card{_demo_card_cls}" style="border-color:#243450;">'
                    f'<div style="display:flex;gap:14px;align-items:flex-start;margin-bottom:16px;padding-bottom:14px;border-bottom:1px solid #1a2640;">'
                    f'{score_orb(ld["score"], lbl)}'
                    f'<div style="flex:1;">'
                    f'<div style="margin-bottom:5px;">{_demo_badge}{badge_html(lbl)} {_contact_bdg} '
                    f'<span style="font-size:.72rem;color:#56697e;">{ld.get("typology","?")} · {ld.get("zone","?")}</span> {sources_html}</div>'
                    f'<div style="font-size:.92rem;font-weight:700;color:#f1f5f9;">{(ld.get("title") or "—")[:80]}</div>'
                    f'{"<div style=font-size:.72rem;color:#56697e;margin-top:3px;>📍 "+ld["address"]+"</div>" if ld.get("address") else ""}'
                    f'{"<div style=margin-top:6px;>"+motiv_html+"</div>" if motiv_html else ""}'
                    f'</div></div>'
                    f'<div style="display:grid;grid-template-columns:repeat(5,1fr);gap:8px;margin-bottom:12px;">'
                    f'<div class="intel-box"><div class="intel-lbl">Preco Pedido</div><div class="intel-val" style="font-size:.95rem;">{fmt_price(ld.get("price"))}</div></div>'
                    f'<div class="intel-box"><div class="intel-lbl">Preco/m2</div><div class="intel-val" style="font-size:.95rem;">{fmt_price(ppm2)}</div><div style="font-size:.6rem;color:#33485e;">bench {fmt_price(bench)}</div></div>'
                    f'<div class="intel-box"><div class="intel-lbl">Area</div><div class="intel-val" style="font-size:.95rem;">{f"{area:.0f} m2" if area else "—"}</div></div>'
                    f'<div class="intel-box"><div class="intel-lbl">Dias Mercado</div><div class="intel-val" style="font-size:.95rem;">{ld.get("days_on_market",0)}</div></div>'
                    f'<div class="intel-box"><div class="intel-lbl">Confianca</div>'
                    f'<div class="intel-val" style="font-size:.95rem;color:{_conf_clr};">{_conf_c}</div>'
                    f'<div style="font-size:.6rem;color:{_conf_clr};">{_conf_lbl}</div></div>'
                    f'</div>'
                    f'{_name_row}'
                    f'<div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;">'
                    f'{owner_chip(ld.get("is_owner"), ld.get("agency_name"), ld.get("owner_type"))}'
                    f'{lead_type_chip(ld.get("lead_type"))}'
                    f'{lead_quality_chip(ld.get("lead_quality"))}'
                    f'{contact_chip(ld.get("contact_phone"), ld.get("contact_email"), ld.get("contact_source"))}'
                    f'{source_label_pill("contacto", ld.get("contact_source"))}'
                    f'{source_label_pill("descoberta", ld.get("discovery_source"))}'
                    f'{"<span class=chip style=font-size:.62rem;color:#56697e;>📍 "+ld["parish"]+"</span>" if ld.get("parish") and ld["parish"] != ld.get("zone") else ""}'
                    f'</div>'
                    f'{action_links(ld.get("contact_phone"), ld.get("contact_email"))}'
                    f'{"<div style=margin-top:10px;background:#16202f;border:1px solid #1a2640;border-radius:8px;padding:10px 14px;font-size:.8rem;color:#94a3b8;line-height:1.6;>"+ld["description"]+"</div>" if ld.get("description") else ""}'
                    f'</div>',
                    unsafe_allow_html=True,
                )
                if ld.get("crm_stage") == "novo":
                    _, _btn_col = st.columns([4, 2])
                    with _btn_col:
                        if st.button("📞 Marcar contactado", key=f"mkd_{ld['id']}", use_container_width=True):
                            from crm.manager import CRMManager
                            CRMManager().move_to_stage(ld["id"], "contactado")
                            st.cache_data.clear()
                            st.rerun()

                _, _gmd_col = st.columns([4, 2])
                with _gmd_col:
                    if st.button("💬 Mensagem", key=f"gbd_{ld['id']}", use_container_width=True):
                        _dk = f"_gmsgd_{ld['id']}"
                        _dwas = st.session_state.get(_dk, False)
                        st.session_state[_dk] = not _dwas
                        if not _dwas:  # toggled ON → log once
                            from crm.manager import CRMManager
                            CRMManager().add_note(ld["id"], "Mensagem de contacto sugerida gerada", "internal")
                if st.session_state.get(f"_gmsgd_{ld['id']}"):
                    st.code(
                        _gen_outreach_msg(
                            ld.get("typology"), ld.get("zone"), ld.get("price"),
                            ld.get("owner_type"), ld.get("contact_name"),
                        ),
                        language="",
                    )

                st.markdown('<div style="font-size:.7rem;color:#56697e;margin:8px 0 4px;">📝 Nota rápida</div>', unsafe_allow_html=True)
                _dnk = st.session_state.get(f"_dnk_{ld['id']}", 0)
                _da, _db = st.columns([5, 1])
                with _da:
                    _dnota = st.text_input(
                        "nota_d", label_visibility="collapsed",
                        placeholder="Ex: Proprietário confirmado. Visita agendada...",
                        key=f"qnd_{ld['id']}_{_dnk}",
                    )
                with _db:
                    if st.button("💾", key=f"qnbd_{ld['id']}", use_container_width=True):
                        if _dnota.strip():
                            from crm.manager import CRMManager
                            CRMManager().add_note(ld["id"], _dnota.strip())
                            st.session_state[f"_dnk_{ld['id']}"] = _dnk + 1
                            st.toast("Nota guardada ✓")
                            st.rerun()

                bd = ld.get("score_breakdown") or {}
                if bd:
                    st.markdown('<div class="lbl-section" style="margin-top:14px;">Composicao da Pontuacao</div>', unsafe_allow_html=True)
                    dims = [
                        ("price_opportunity",        "Oportunidade de Preco",   30),
                        ("urgency_signals",          "Sinais de Urgencia",      25),
                        ("owner_direct",             "Proprietario Directo",    25),
                        ("days_on_market",           "Tempo no Mercado",        15),
                        ("data_quality",             "Qualidade da Ficha",       5),
                        ("zone_priority",            "Prioridade de Zona",       5),
                        ("contact_quality",          "Qualidade do Contacto",   20),
                        ("phone_type_bonus",         "Tipo de Telefone",         8),
                        ("contact_confidence_bonus", "Confianca no Contacto",    3),
                        ("agency_penalty",           "Penalizacao Agencia",     10),
                        ("repeated_phone_penalty",   "Telefone Repetido",       10),
                    ]
                    for key, dlbl, mx in dims:
                        v    = bd.get(key, 0)
                        # contact_quality can be negative — clamp bar to 0, show red
                        pct  = max(0, int(v / mx * 100)) if mx else 0
                        if v < 0:
                            clr = "#ef4444"   # penalty → red
                        elif lbl == "HOT" and pct >= 70:
                            clr = "#f43f5e"
                        elif pct >= 50:
                            clr = "#3b82f6"
                        else:
                            clr = "#2e4268"
                        # show sign (+/-) for contact_quality to make penalty explicit
                        score_txt = (f"{v:+d}/{mx}" if key == "contact_quality" else f"{v}/{mx}")
                        st.markdown(
                            f'<div style="display:flex;align-items:center;gap:10px;padding:4px 0;">'
                            f'<span style="font-size:.72rem;color:#94a3b8;width:180px;flex-shrink:0;">{dlbl}</span>'
                            f'<div style="flex:1;background:#16202f;border-radius:4px;height:6px;overflow:hidden;">'
                            f'<div style="width:{pct}%;height:6px;border-radius:4px;background:{clr};"></div></div>'
                            f'<span style="font-size:.7rem;font-weight:700;color:{clr};width:40px;text-align:right;">{score_txt}</span>'
                            f'</div>',
                            unsafe_allow_html=True,
                        )

                if ld.get("sources"):
                    links = " ".join(
                        f'<a href="{s["url"]}" target="_blank" style="display:inline-block;background:#111827;border:1px solid #243450;'
                        f'border-radius:6px;padding:4px 12px;font-size:.72rem;color:#60a5fa;text-decoration:none;font-weight:700;">'
                        f'Ver em {s["source"].upper()} &rarr;</a>'
                        for s in ld["sources"]
                    )
                    st.markdown(f'<div style="margin-top:12px;display:flex;gap:8px;flex-wrap:wrap;">{links}</div>', unsafe_allow_html=True)


# ══════════════════════════════════════════════════════════════════════════════
#  PAGE: CRM
# ══════════════════════════════════════════════════════════════════════════════
elif page == "&#128203;  CRM":

    st.markdown(
        '<div style="padding:1.5rem 0 1.2rem;border-bottom:1px solid #1a2640;margin-bottom:1.5rem;">'
        '<div style="font-size:.62rem;font-weight:700;color:#3b82f6;letter-spacing:1px;text-transform:uppercase;margin-bottom:4px;">Gestao Comercial</div>'
        '<div style="font-size:1.55rem;font-weight:900;color:#f1f5f9;">Pipeline de Negociacao</div>'
        '<div style="font-size:.82rem;color:#56697e;margin-top:4px;">Acompanhamento de contactos, visitas e propostas</div>'
        '</div>',
        unsafe_allow_html=True,
    )

    from crm.manager import CRMManager, STAGES
    crm     = CRMManager()
    summary = crm.get_pipeline_summary()
    stage_cfg = {
        "novo":       ("📥 Novo",          "#3b82f6"),
        "contactado": ("📞 Contactado",    "#8b5cf6"),
        "negociacao": ("🤝 Em Negociacao", "#f59e0b"),
        "ganho":      ("✅ Ganho",          "#10b981"),
        "perdido":    ("❌ Perdido",        "#475569"),
    }
    stage_keys = list(stage_cfg.keys())

    kcols = st.columns(len(stage_cfg), gap="small")
    for col, (sk, (sl, sc)) in zip(kcols, stage_cfg.items()):
        with col:
            st.markdown(
                f'<div class="kanban-card" style="border-top:3px solid {sc};">'
                f'<div style="font-size:2rem;font-weight:900;color:{sc};letter-spacing:-2px;">{summary.get(sk, 0)}</div>'
                f'<div style="font-size:.66rem;font-weight:700;color:#56697e;text-transform:uppercase;letter-spacing:.5px;margin-top:2px;">{sl}</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

    st.divider()
    nav_col, detail_col = st.columns([1, 3])
    with nav_col:
        sel_stage = st.radio("Fase:", stage_keys, format_func=lambda s: stage_cfg[s][0])
    with detail_col:
        stage_leads = crm.get_leads_by_stage(sel_stage)
        if _demo_filter is not None:
            stage_leads = [l for l in stage_leads if l.is_demo == _demo_filter]
        sl_color    = stage_cfg[sel_stage][1]
        st.caption(f"{len(stage_leads)} oportunidades em {stage_cfg[sel_stage][0]}")

        if not stage_leads:
            st.info(f"Sem oportunidades em '{stage_cfg[sel_stage][0]}'.")
        else:
            for lead in stage_leads[:20]:
                lbl       = lead.score_label or "COLD"
                exp_title = f"{label_emoji(lbl)} #{lead.id} · {lead.score} pts · {lead.typology or '?'} {lead.zone or '?'} · {fmt_price(lead.price)}"
                with st.expander(exp_title, expanded=False):
                    # Badge + title (use markdown for HTML)
                    owner_badge = '<span class="badge badge-owner">Owner</span>' if lead.is_owner else ""
                    demo_badge  = '<span class="badge badge-demo">DEMO</span> '  if lead.is_demo  else ""
                    ct_badge    = contact_badge(lead.contact_phone, lead.contact_email)
                    st.markdown(
                        f'<div style="margin-bottom:8px;">{demo_badge}{badge_html(lbl)} {ct_badge} {owner_badge}</div>'
                        f'<div style="font-weight:600;color:#94a3b8;font-size:.88rem;margin-bottom:10px;">{lead.title or "—"}</div>',
                        unsafe_allow_html=True,
                    )
                    # Motivation tags
                    desc = getattr(lead, "description", "") or ""
                    motives = detect_motivation(desc)
                    if motives:
                        motiv_html = " ".join(
                            f'<span style="background:rgba(139,92,246,.1);color:#a78bfa;border:1px solid rgba(139,92,246,.2);'
                            f'border-radius:20px;font-size:.64rem;font-weight:700;padding:2px 8px;">{e} {lb}</span>'
                            for lb, e in motives
                        )
                        st.markdown(f'<div style="margin-bottom:8px;">{motiv_html}</div>', unsafe_allow_html=True)
                    # Chips row
                    price_changes_chip = (
                        f'<span class="chip badge-drop" style="background:rgba(249,115,22,.08);color:#f97316;border-color:rgba(249,115,22,.2);">'
                        f'📉 {lead.price_changes} red.</span>'
                        if lead.price_changes else ""
                    )
                    st.markdown(
                        f'<div style="margin-bottom:8px;">'
                        f'{contact_chip(lead.contact_phone, lead.contact_email, getattr(lead, "contact_source", None))}'
                        f'{confidence_chip(getattr(lead, "contact_confidence", 0))}'
                        f'{source_label_pill("contacto", getattr(lead, "contact_source", None))}'
                        f'{owner_chip(lead.is_owner, lead.agency_name, getattr(lead, "owner_type", None))}'
                        f'<span class="chip">⏱ {lead.days_on_market} dias</span>'
                        f'{price_changes_chip}'
                        f'{source_label_pill("via", getattr(lead, "discovery_source", None))}</div>',
                        unsafe_allow_html=True,
                    )
                    if lead.address:
                        st.caption(f"📍 {lead.address}")
                    # Move stage + note
                    ea, eb = st.columns([3, 1])
                    with eb:
                        others = [s for s in stage_keys if s != sel_stage]
                        new_s  = st.selectbox("Mover para:", others,
                                               format_func=lambda s: stage_cfg[s][0],
                                               key=f"ss_{lead.id}")
                        if st.button("Mover", key=f"mv_{lead.id}", use_container_width=True):
                            if crm.move_to_stage(lead.id, new_s):
                                st.success(f"Movido para {stage_cfg[new_s][0]}")
                                st.cache_data.clear()
                                st.rerun()
                    nc1, nc2 = st.columns([3, 1])
                    with nc1:
                        note_txt = st.text_area(
                            "Registar interaccao:", height=72, key=f"nt_{lead.id}",
                            placeholder="Ex: Proprietario confirmado. Aceita 270k. Visita marcada para sexta...",
                        )
                    with nc2:
                        note_type = st.selectbox(
                            "Tipo:", ["call", "email", "visit", "whatsapp", "internal"],
                            key=f"ntype_{lead.id}",
                            format_func=lambda t: {
                                "call": "📞 Chamada", "email": "✉️ Email",
                                "visit": "🏠 Visita", "whatsapp": "💬 WhatsApp",
                                "internal": "📝 Nota",
                            }[t],
                        )
                        st.write("")
                        if st.button("💾 Guardar", key=f"sv_{lead.id}", use_container_width=True):
                            if note_txt.strip():
                                crm.add_note(lead.id, note_txt.strip(), note_type)
                                st.success("Interaccao registada ✓")

    st.divider()
    st.markdown('<div class="lbl-section">Historico de Interaccoes</div>', unsafe_allow_html=True)
    recent = crm.get_recent_activity(limit=15)
    if not recent:
        st.caption("Sem interaccoes registadas.")
    for note in recent:
        ic = {"call": "📞", "email": "✉️", "visit": "🏠", "whatsapp": "💬", "internal": "📝"}.get(note.note_type, "📝")
        dt = note.created_at.strftime("%d/%m  %H:%M") if note.created_at else "—"
        st.markdown(
            f'<div class="activity-row">'
            f'<div style="font-size:.9rem;flex-shrink:0;">{ic}</div>'
            f'<div><div style="font-size:.7rem;color:#56697e;margin-bottom:2px;">Lead #{note.lead_id} · {dt}</div>'
            f'<div>{note.note[:160]}</div></div></div>',
            unsafe_allow_html=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
#  PAGE: MOTOR
# ══════════════════════════════════════════════════════════════════════════════
elif page == "&#128268;  Pre-Market":

    st.markdown(
        '<div style="padding:1.5rem 0 1.2rem;border-bottom:1px solid #1a2640;margin-bottom:1.5rem;">'
        '<div style="font-size:.62rem;font-weight:700;color:#a78bfa;letter-spacing:1px;text-transform:uppercase;margin-bottom:4px;">Inteligencia de Mercado</div>'
        '<div style="font-size:1.55rem;font-weight:900;color:#f1f5f9;">Sinais Pre-Mercado</div>'
        '<div style="font-size:.82rem;color:#56697e;margin-top:4px;">'
        'Proprietarios que podem vender antes de anunciar — licencas de obras, remodelacoes e mudancas profissionais'
        '</div>'
        '</div>',
        unsafe_allow_html=True,
    )

    # ── Load signals ──────────────────────────────────────────────────────────
    @st.cache_data(ttl=120)
    def load_premarket_signals():
        try:
            from storage.database import get_db
            from storage.models import PremktSignal
            from sqlalchemy import select, desc
            rows = []
            with get_db() as db:
                signals = (
                    db.execute(
                        select(PremktSignal)
                        .order_by(desc(PremktSignal.signal_score), desc(PremktSignal.created_at))
                        .limit(200)
                    ).scalars().all()
                )
                for s in signals:
                    rows.append({
                        "id":           s.id,
                        "signal_type":  s.signal_type,
                        "source":       s.source,
                        "signal_text":  s.signal_text,
                        "name":         s.name,
                        "company":      s.company,
                        "role":         s.role,
                        "location_raw": s.location_raw,
                        "zone":         s.zone,
                        "url":          s.url,
                        "signal_score": s.signal_score,
                        "promoted":     s.promoted,
                        "created_at":   s.created_at,
                    })
            return rows
        except Exception:
            return []

    signals = load_premarket_signals()

    # ── Signal type chip helper ───────────────────────────────────────────────
    _SIG_CHIP_CFG = {
        "building_permit":           ("1a2a3b", "a78bfa", "Licenca Obras",    "FF"),
        "renovation_ad_homeowner":   ("1a2a1a", "4ade80", "Anuncio Remodel.", "FF"),
        "renovation_ad_generic":     ("1a2a1a", "86efac", "Remodelacao",      "90"),
        "linkedin_city_change":      ("1a231f", "34d399", "Mudanca Cidade",   "FF"),
        "linkedin_job_change":       ("1a1f2a", "60a5fa", "Mudanca Prof.",    "AA"),
        "contractor_search_post":    ("211a2a", "c084fc", "Procura Empreit.", "FF"),
    }

    def signal_type_chip(signal_type: str) -> str:
        cfg  = _SIG_CHIP_CFG.get(signal_type, ("1a1a1a", "94a3b8", signal_type, "AA"))
        bg, fg, label, _ = cfg
        return (
            f'<span style="background:#{bg};color:#{fg};border:1px solid #{fg}44;'
            f'border-radius:5px;padding:2px 8px;font-size:.68rem;font-weight:700;'
            f'letter-spacing:.3px;">{label}</span>'
        )

    def source_chip(source: str) -> str:
        labels = {
            "olx":                  ("OLX",       "f97316"),
            "custojusto":           ("CustoJusto", "fb923c"),
            "cm_lisboa":            ("CM Lisboa",  "a78bfa"),
            "duckduckgo_linkedin":  ("LinkedIn",   "60a5fa"),
        }
        label, colour = labels.get(source, (source, "94a3b8"))
        return (
            f'<span style="color:#{colour};font-size:.68rem;font-weight:700;">'
            f'{label}</span>'
        )

    def score_bar(score: int) -> str:
        if score >= 75:
            colour = "a78bfa"
        elif score >= 55:
            colour = "4ade80"
        else:
            colour = "60a5fa"
        pct = min(score, 100)
        return (
            f'<div style="display:flex;align-items:center;gap:8px;">'
            f'<div style="width:50px;background:#16202f;border-radius:3px;height:5px;">'
            f'<div style="width:{pct}%;height:5px;background:#{colour};border-radius:3px;"></div>'
            f'</div>'
            f'<span style="font-size:.72rem;font-weight:800;color:#{colour};">{score}</span>'
            f'</div>'
        )

    # ── KPI row ───────────────────────────────────────────────────────────────
    total_sigs   = len(signals)
    permits      = sum(1 for s in signals if s["signal_type"] == "building_permit")
    renovations  = sum(1 for s in signals if "renovation" in s["signal_type"])
    linkedin_sig = sum(1 for s in signals if "linkedin" in s["signal_type"])
    promoted     = sum(1 for s in signals if s["promoted"])

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Total Sinais",       total_sigs)
    k2.metric("Licencas Obras",     permits,     help="CM Lisboa building permits (last 90 days)")
    k3.metric("Anuncios Remodel.",  renovations, help="OLX + CustoJusto renovation demand ads")
    k4.metric("Sinais LinkedIn",    linkedin_sig,help="Career/city change signals (DuckDuckGo)")
    k5.metric("Promovidos a Lead",  promoted)

    st.markdown("<hr/>", unsafe_allow_html=True)

    if not signals:
        st.markdown(
            '<div class="card" style="text-align:center;padding:2.5rem;">'
            '<div style="font-size:2rem;margin-bottom:.5rem;">📡</div>'
            '<div style="font-size:.9rem;font-weight:700;color:#f1f5f9;margin-bottom:.4rem;">'
            'Nenhum sinal pre-mercado encontrado</div>'
            '<div style="font-size:.78rem;color:#56697e;">Execute o scan para descobrir proprietarios que podem vender em breve.</div>'
            '</div>',
            unsafe_allow_html=True,
        )
        if st.button("Executar Pre-Market Scan", use_container_width=True):
            with st.spinner("A pesquisar sinais pre-mercado..."):
                try:
                    from premarket.enricher import PremktEnricher
                    result = PremktEnricher().run()
                    st.cache_data.clear()
                    st.success(f"+{result.new_signals} novos sinais encontrados")
                    st.rerun()
                except Exception as e:
                    st.error(f"Erro: {e}")
    else:
        # ── Filters ───────────────────────────────────────────────────────────
        fc1, fc2, fc3, fc4 = st.columns([2, 2, 2, 1])
        with fc1:
            _SIG_TYPES = ["Todos os tipos", "building_permit", "renovation_ad_homeowner",
                          "renovation_ad_generic", "linkedin_city_change",
                          "linkedin_job_change", "contractor_search_post"]
            sig_type_filter = st.selectbox("Tipo de sinal", _SIG_TYPES, key="pm_type")
        with fc2:
            zone_opts = ["Todas as zonas"] + sorted({s["zone"] for s in signals if s["zone"]})
            pm_zone   = st.selectbox("Zona", zone_opts, key="pm_zone")
        with fc3:
            src_opts  = ["Todas as fontes"] + sorted({s["source"] for s in signals})
            pm_src    = st.selectbox("Fonte", src_opts, key="pm_src")
        with fc4:
            pm_score_min = st.slider("Score min.", 0, 100, 0, key="pm_score")

        # Apply filters
        filtered = signals
        if sig_type_filter != "Todos os tipos":
            filtered = [s for s in filtered if s["signal_type"] == sig_type_filter]
        if pm_zone != "Todas as zonas":
            filtered = [s for s in filtered if s["zone"] == pm_zone]
        if pm_src != "Todas as fontes":
            filtered = [s for s in filtered if s["source"] == pm_src]
        filtered = [s for s in filtered if s["signal_score"] >= pm_score_min]

        st.markdown(
            f'<div style="font-size:.72rem;color:#56697e;margin:6px 0 16px;">'
            f'A mostrar <b style="color:#94a3b8">{len(filtered)}</b> de {total_sigs} sinais</div>',
            unsafe_allow_html=True,
        )

        # ── Signal cards ──────────────────────────────────────────────────────
        for sig in filtered[:80]:
            promoted_badge = (
                '<span style="background:#16202f;color:#4ade80;border:1px solid #4ade8044;'
                'border-radius:5px;padding:2px 7px;font-size:.62rem;font-weight:700;margin-left:6px;">'
                'PROMOVIDO</span>'
                if sig["promoted"] else ""
            )
            url_link = (
                f'<a href="{sig["url"]}" target="_blank" '
                f'style="font-size:.68rem;color:#60a5fa;text-decoration:none;margin-left:4px;">'
                f'ver fonte</a>'
                if sig["url"] else ""
            )
            person_line = ""
            if sig["name"]:
                person_parts = [f'<b style="color:#f1f5f9">{sig["name"]}</b>']
                if sig["role"]:
                    person_parts.append(f'<span style="color:#94a3b8">{sig["role"]}</span>')
                if sig["company"]:
                    person_parts.append(f'<span style="color:#56697e">@ {sig["company"]}</span>')
                person_line = (
                    f'<div style="font-size:.78rem;margin-top:4px;">'
                    + " &middot; ".join(person_parts)
                    + "</div>"
                )
            location_str = sig.get("location_raw") or sig.get("zone") or "—"
            date_str = (
                sig["created_at"].strftime("%d/%m/%Y")
                if sig.get("created_at") else "—"
            )

            st.markdown(
                f'<div class="card" style="margin-bottom:10px;">'
                f'<div style="display:flex;justify-content:space-between;align-items:flex-start;gap:12px;">'
                f'  <div style="flex:1;min-width:0;">'
                f'    <div style="margin-bottom:5px;">'
                f'      {signal_type_chip(sig["signal_type"])} '
                f'      {source_chip(sig["source"])}'
                f'      {promoted_badge}'
                f'    </div>'
                f'    <div style="font-size:.86rem;font-weight:600;color:#e2e8f0;line-height:1.4;'
                f'      overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">'
                f'      {sig["signal_text"][:120]}'
                f'    </div>'
                f'    {person_line}'
                f'    <div style="margin-top:6px;font-size:.7rem;color:#56697e;">'
                f'      <span style="margin-right:10px;">&#128205; {location_str}</span>'
                f'      <span style="margin-right:10px;">&#128197; {date_str}</span>'
                f'      {url_link}'
                f'    </div>'
                f'  </div>'
                f'  <div style="flex-shrink:0;">{score_bar(sig["signal_score"])}</div>'
                f'</div>'
                f'</div>',
                unsafe_allow_html=True,
            )

        st.markdown("<hr/>", unsafe_allow_html=True)

        # ── Run scan button ───────────────────────────────────────────────────
        pm_c1, pm_c2 = st.columns(2)
        with pm_c1:
            if st.button("Actualizar Sinais (novo scan)", use_container_width=True):
                with st.spinner("A pesquisar sinais pre-mercado..."):
                    try:
                        from premarket.enricher import PremktEnricher
                        result = PremktEnricher().run(zones=None)
                        st.cache_data.clear()
                        st.success(
                            f"+{result.new_signals} novos sinais | "
                            f"{result.skipped} duplicados ignorados"
                        )
                        st.rerun()
                    except Exception as e:
                        st.error(f"Erro: {e}")
        with pm_c2:
            if st.button("Apenas Licencas de Obras (CM Lisboa)", use_container_width=True):
                with st.spinner("A consultar CM Lisboa open data..."):
                    try:
                        from premarket.enricher import PremktEnricher
                        from premarket.sources.building_permits import BuildingPermitsSource
                        enricher = PremktEnricher()
                        enricher._sources = [BuildingPermitsSource()]
                        result = enricher.run(zones=["Lisboa"])
                        st.cache_data.clear()
                        st.success(
                            f"+{result.new_signals} novas licencas importadas | "
                            f"{result.skipped} ja existentes"
                        )
                        st.rerun()
                    except Exception as e:
                        st.error(f"Erro: {e}")

# ══════════════════════════════════════════════════════════════════════════════
elif page == "&#9881;  Motor":

    st.markdown(
        '<div style="padding:1.5rem 0 1.2rem;border-bottom:1px solid #1a2640;margin-bottom:1.5rem;">'
        '<div style="font-size:.62rem;font-weight:700;color:#3b82f6;letter-spacing:1px;text-transform:uppercase;margin-bottom:4px;">Centro de Operacoes</div>'
        '<div style="font-size:1.55rem;font-weight:900;color:#f1f5f9;">Motor de Inteligencia Imobiliaria</div>'
        '<div style="font-size:.82rem;color:#56697e;margin-top:4px;">Recolha automatica &#183; Normalizacao &#183; Identificacao de proprietarios &#183; Scoring &#183; Alertas HOT</div>'
        '</div>',
        unsafe_allow_html=True,
    )

    stats = load_stats()
    leads = load_leads(zone=zone_filter, typology=typo_filter, score_min=score_floor, is_demo=_demo_filter, contact=_contact_filter, owner_type=_owner_filter, csource_type=_csource_filter)

    # Pipeline flow visual
    st.markdown('<div class="lbl-section">Fluxo de Analise Automatica</div>', unsafe_allow_html=True)
    st.markdown(
        '<div class="pf-wrap">'
        '<div class="pf-step pf-step-active"><span class="pf-icon">&#128375;</span>'
        '<div class="pf-n">Passo 1</div><div class="pf-name">Recolher</div>'
        '<div class="pf-desc">OLX &#183; Imovirtual &#183; Idealista. Rate limiting e rotacao de user-agents.</div></div>'
        '<div class="pf-step"><span class="pf-icon">&#9881;</span>'
        '<div class="pf-n">Passo 2</div><div class="pf-name">Normalizar</div>'
        '<div class="pf-desc">Limpeza e padronizacao. Deduplicacao por fingerprint SHA-256.</div></div>'
        '<div class="pf-step"><span class="pf-icon">&#128100;</span>'
        '<div class="pf-n">Passo 3</div><div class="pf-name">Identificar</div>'
        '<div class="pf-desc">Proprietario directo vs. agencia. Deteccao de FSBO e sinais de urgencia.</div></div>'
        '<div class="pf-step"><span class="pf-icon">&#128202;</span>'
        '<div class="pf-n">Passo 4</div><div class="pf-name">Enriquecer</div>'
        '<div class="pf-desc">Benchmark EUR/m2 por zona. Geocoding. Motivo de venda. Historico de precos.</div></div>'
        '<div class="pf-step"><span class="pf-icon">&#127919;</span>'
        '<div class="pf-n">Passo 5</div><div class="pf-name">Pontuar</div>'
        '<div class="pf-desc">Score 0-100 em 6 dimensoes. HOT &gt;= 75, WARM &gt;= 50, COLD &lt; 50.</div></div>'
        '<div class="pf-step"><span class="pf-icon">&#128276;</span>'
        '<div class="pf-n">Passo 6</div><div class="pf-name">Alertar</div>'
        '<div class="pf-desc">Email + Telegram imediato para HOT. Relatorio diario 08:00.</div></div>'
        '</div>',
        unsafe_allow_html=True,
    )

    # Run button
    if st.button("▶  Executar Analise Completa de Mercado (todos os 6 passos)", use_container_width=True):
        prog  = st.progress(0, "A iniciar...")
        steps = [
            "Recolher dados...", "Normalizar e desduplicar...", "Identificar proprietarios...",
            "Enriquecer com benchmarks...", "Calcular pontuacoes...", "Verificar alertas HOT...",
        ]
        try:
            from pipeline.runner import PipelineRunner
            from scoring.scorer import Scorer
            runner = PipelineRunner()
            for i, msg in enumerate(steps[:-1], 1):
                prog.progress(int(i / 6 * 100), msg)
                if i == 1:
                    r = runner.run_full()
            prog.progress(83, "A calcular pontuacoes...")
            n_scored = Scorer().score_all_pending()
            prog.progress(100, "Concluido!")
            st.cache_data.clear()
            st.success(f"Analise concluida — {r.leads_created} novas · {r.leads_updated} actualizadas · {n_scored} pontuadas")
        except Exception as e:
            prog.empty()
            st.error(f"Erro: {e}")

    st.divider()

    # Stats summary
    by_src    = stats.get("by_source", {"olx": 0, "imovirtual": 0, "idealista": 0})
    total_src = sum(by_src.values()) or 1

    res1, res2 = st.columns(2, gap="large")

    with res1:
        st.markdown('<div class="lbl-section">Anuncios por Fonte</div>', unsafe_allow_html=True)
        src_rows = [
            ("OLX",        by_src.get("olx", 0),        "#f59e0b"),
            ("Imovirtual", by_src.get("imovirtual", 0), "#3b82f6"),
            ("Idealista",  by_src.get("idealista", 0),  "#8b5cf6"),
            ("Sapo",       by_src.get("sapo", 0),       "#10b981"),
        ]
        # Filter out zero-count sources so they don't clutter the bar chart
        src_rows = [(n, c, col) for n, c, col in src_rows if c > 0]
        total_src = sum(c for _, c, _ in src_rows) or 1
        bars_html = '<div style="background:#111827;border:1px solid #1a2640;border-radius:10px;padding:16px 20px;">'
        for name, count, color in src_rows:
            pct = int(count / total_src * 100)
            bars_html += (
                f'<div class="src-bar-row">'
                f'<span class="src-bar-name">{name}</span>'
                f'<div class="src-bar-track"><div class="src-bar-fill" style="width:{pct}%;background:{color};"></div></div>'
                f'<span class="src-bar-count">{count}</span>'
                f'</div>'
            )
        bars_html += (
            f'<div style="margin-top:12px;padding-top:10px;border-top:1px solid #1a2640;'
            f'display:flex;justify-content:space-between;align-items:center;">'
            f'<span style="font-size:.72rem;color:#56697e;">Apos deduplicacao</span>'
            f'<span style="font-size:1.05rem;font-weight:900;color:#f1f5f9;">{stats.get("total_active",0)} unicas</span>'
            f'</div></div>'
        )
        st.markdown(bars_html, unsafe_allow_html=True)

    with res2:
        st.markdown('<div class="lbl-section">Resumo de Inteligencia</div>', unsafe_allow_html=True)
        n_phone   = sum(1 for l in leads if l.get("has_phone"))
        n_email   = sum(1 for l in leads if l.get("has_email") and not l.get("has_phone"))
        n_noct    = sum(1 for l in leads if not l.get("has_contact"))
        cells = [
            ("HOT",              stats.get("hot_count", 0),        "#f43f5e"),
            ("WARM",             stats.get("warm_count", 0),       "#f59e0b"),
            ("Owner Directo",    stats.get("owner_count", 0),      "#10b981"),
            ("Com Reducao",      stats.get("price_drop_count", 0), "#f97316"),
            ("&#128222; Com Telefone",  n_phone,                   "#10b981"),
            ("&#9993; Com Email",       n_email,                   "#60a5fa"),
            ("&#10060; Sem Contacto",   n_noct,                    "#ef4444"),
            ("Com Urgencia",     stats.get("urgency_count", 0),    "#a78bfa"),
        ]
        grid_html = '<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;">'
        for lbl, val, clr in cells:
            grid_html += (
                f'<div class="intel-box">'
                f'<div class="intel-lbl">{lbl}</div>'
                f'<div class="intel-val" style="color:{clr};">{val}</div>'
                f'</div>'
            )
        grid_html += '</div>'
        st.markdown(grid_html, unsafe_allow_html=True)

    st.divider()

    # Alerts + Motivations
    al_col, mv_col = st.columns(2, gap="large")
    with al_col:
        st.markdown('<div class="lbl-section">Alertas Comerciais Activos</div>', unsafe_allow_html=True)
        all_leads = load_leads(is_demo=_demo_filter, contact=_contact_filter)
        alerts    = generate_alerts(all_leads)
        if alerts:
            for a in alerts:
                st.markdown(alert_card_html(a), unsafe_allow_html=True)
        else:
            st.caption("Sem alertas activos. Execute a analise de mercado.")

    with mv_col:
        st.markdown('<div class="lbl-section">Motivos de Venda Detectados</div>', unsafe_allow_html=True)
        from collections import Counter
        motive_counts: Counter = Counter()
        for lead in all_leads:
            for lb, _ in detect_motivation(lead.get("description", "")):
                motive_counts[lb] += 1
        if motive_counts:
            for lb, cnt in motive_counts.most_common(8):
                st.markdown(
                    f'<div style="display:flex;justify-content:space-between;align-items:center;'
                    f'padding:6px 0;border-bottom:1px solid #1a2640;font-size:.82rem;">'
                    f'<span style="background:rgba(139,92,246,.1);color:#a78bfa;border:1px solid rgba(139,92,246,.2);'
                    f'border-radius:20px;font-size:.64rem;font-weight:700;padding:2px 8px;">{lb}</span>'
                    f'<span style="color:#94a3b8;font-weight:700;">{cnt} propriedade{"s" if cnt > 1 else ""}</span>'
                    f'</div>',
                    unsafe_allow_html=True,
                )
        else:
            st.caption("Nenhum motivo identificado nas descricoes actuais.")

    st.divider()
    with st.expander("Controlo avancado — executar passos individuais"):
        col1, col2 = st.columns(2, gap="large")
        with col1:
            if st.button("Recolher dados (todos os portais)", use_container_width=True):
                with st.spinner("A recolher dados..."):
                    try:
                        from pipeline.runner import PipelineRunner
                        PipelineRunner()._run_scrapers(["olx", "imovirtual", "idealista"], ["Lisboa", "Cascais"])
                        st.success("Recolha concluida")
                    except Exception as e:
                        st.error(str(e))
            if st.button("Normalizar e enriquecer listagens", use_container_width=True):
                with st.spinner("A processar..."):
                    try:
                        from pipeline.runner import PipelineRunner
                        s = PipelineRunner().process_raw()
                        st.success(f"{s.leads_created} novas · {s.leads_updated} actualizadas")
                    except Exception as e:
                        st.error(str(e))
        with col2:
            if st.button("Recalcular todas as pontuacoes", use_container_width=True):
                with st.spinner("A calcular..."):
                    try:
                        from scoring.scorer import Scorer
                        n = Scorer().score_all_pending()
                        st.cache_data.clear()
                        st.success(f"{n} oportunidades actualizadas")
                    except Exception as e:
                        st.error(str(e))
            if st.button("Enviar alertas HOT agora", use_container_width=True):
                with st.spinner("A verificar..."):
                    try:
                        from alerts.notifier import Notifier
                        n = Notifier().check_and_alert_hot_leads()
                        st.success(f"{n} alertas enviados")
                    except Exception as e:
                        st.error(str(e))


# ══════════════════════════════════════════════════════════════════════════════
#  PAGE: EXPORTAR
# ══════════════════════════════════════════════════════════════════════════════
elif page == "&#128228;  Exportar":

    st.markdown(
        '<div style="padding:1.5rem 0 1.2rem;border-bottom:1px solid #1a2640;margin-bottom:1.5rem;">'
        '<div style="font-size:.62rem;font-weight:700;color:#3b82f6;letter-spacing:1px;text-transform:uppercase;margin-bottom:4px;">Exportar</div>'
        '<div style="font-size:1.55rem;font-weight:900;color:#f1f5f9;">Exportar Leads para o Cliente</div>'
        '<div style="font-size:.82rem;color:#56697e;margin-top:4px;">Gerar listas prontas a entregar — filtros do sidebar aplicados automaticamente</div>'
        '</div>',
        unsafe_allow_html=True,
    )

    # ── Row 1: Client-ready exports ──────────────────────────────────────
    ec1, ec2 = st.columns(2, gap="large")

    with ec1:
        st.markdown(
            '<div class="card" style="border-left:3px solid #10b981;">'
            '<div style="font-size:.95rem;font-weight:700;color:#10b981;margin-bottom:6px;">📋 Lista de Contactos</div>'
            '<div style="font-size:.78rem;color:#94a3b8;line-height:1.55;">'
            'Lista pronta para o cliente com nome, apelido, telefone, WhatsApp, zona, tipologia, '
            'preco, tipo de lead e insight. <strong>Exclui relay/OLX por defeito.</strong>'
            '</div></div>',
            unsafe_allow_html=True,
        )
        ct_score = st.slider("Score minimo", 0, 100, 0, key="ct_score")
        ct_mobile = st.checkbox("Apenas telemóvel real (9xx)", value=True, key="ct_mobile",
                                help="Exclui relay (6xx) e fixo (2xx) — só números contactáveis")
        ct_no_agency = st.checkbox("Excluir agências", value=True, key="ct_no_agency")

        if st.button("Gerar Lista de Contactos", use_container_width=True, type="primary"):
            with st.spinner("A gerar lista..."):
                try:
                    from reports.contact_export import generate_contact_list, export_contact_xlsx, export_contact_csv
                    from datetime import datetime as _dt

                    contacts = generate_contact_list(
                        score_min=ct_score,
                        zones=[zone_filter] if zone_filter else None,
                        include_agencies=not ct_no_agency,
                        mobile_only=ct_mobile,
                    )
                    if not contacts:
                        st.warning("Nenhum contacto encontrado com esses filtros.")
                    else:
                        ts = _dt.now().strftime("%Y%m%d_%H%M%S")
                        # Generate XLSX
                        xlsx_path = f"data/contactos_{ts}.xlsx"
                        export_contact_xlsx(contacts, xlsx_path)
                        csv_path = f"data/contactos_{ts}.csv"
                        export_contact_csv(contacts, csv_path)

                        st.success(f"{len(contacts)} contactos gerados")
                        dl1, dl2 = st.columns(2)
                        with dl1:
                            with open(xlsx_path, "rb") as f:
                                st.download_button(
                                    f"Descarregar Excel ({len(contacts)} leads)",
                                    f, file_name=Path(xlsx_path).name,
                                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                                    use_container_width=True,
                                )
                        with dl2:
                            with open(csv_path, "rb") as f:
                                st.download_button(
                                    f"Descarregar CSV ({len(contacts)} leads)",
                                    f, file_name=Path(csv_path).name,
                                    mime="text/csv",
                                    use_container_width=True,
                                )
                except Exception as e:
                    st.error(f"Erro: {e}")

    with ec2:
        st.markdown(
            '<div class="card" style="border-left:3px solid #f59e0b;">'
            '<div style="font-size:.95rem;font-weight:700;color:#f59e0b;margin-bottom:6px;">⭐ Lista Comercial Premium</div>'
            '<div style="font-size:.78rem;color:#94a3b8;line-height:1.55;">'
            'Excel com 3 separadores: <strong>Lista Premium</strong> (top proprietários com telemóvel), '
            '<strong>Lista Expandida</strong> (mais leads), e <strong>Resumo Executivo</strong> (KPIs).'
            '</div></div>',
            unsafe_allow_html=True,
        )
        cm_premium = st.number_input("Leads Premium (top)", value=30, min_value=5, max_value=100, key="cm_prem")
        cm_expanded = st.number_input("Leads Expandidos", value=100, min_value=10, max_value=500, key="cm_exp")

        if st.button("Gerar Lista Comercial", use_container_width=True, type="primary"):
            with st.spinner("A gerar lista comercial..."):
                try:
                    from reports.commercial_export import (
                        generate_premium_list, generate_expanded_list,
                        export_commercial_xlsx,
                    )
                    from datetime import datetime as _dt

                    ts = _dt.now().strftime("%Y%m%d_%H%M%S")
                    xlsx_path = f"data/leads_comercial_{ts}.xlsx"
                    premium = generate_premium_list(limit=cm_premium)
                    expanded = generate_expanded_list(
                        premium_phones={r.get("telefone") for r in premium if r.get("telefone")},
                        limit=cm_expanded,
                    )
                    summary = {
                        "premium_count": len(premium),
                        "expanded_count": len(expanded),
                        "generated_at": _dt.now().isoformat(),
                    }
                    export_commercial_xlsx(premium, expanded, summary, xlsx_path)
                    st.success(f"Lista comercial gerada: {len(premium)} premium + {len(expanded)} expandidos")
                    with open(xlsx_path, "rb") as f:
                        st.download_button(
                            f"Descarregar Excel Comercial",
                            f, file_name=Path(xlsx_path).name,
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True,
                        )
                except Exception as e:
                    st.error(f"Erro: {e}")

    st.divider()

    # ── Row 2: Technical exports ─────────────────────────────────────────
    st.markdown(
        '<div style="font-size:.82rem;font-weight:700;color:#56697e;margin-bottom:12px;">Exportações técnicas</div>',
        unsafe_allow_html=True,
    )
    ec3, ec4 = st.columns(2, gap="large")

    with ec3:
        st.markdown(
            '<div class="card"><div style="font-size:.85rem;font-weight:600;color:#94a3b8;margin-bottom:4px;">Relatório CSV (todos os campos)</div>'
            '<div style="font-size:.72rem;color:#56697e;">Exportação técnica com todos os campos da BD — para análise interna.</div></div>',
            unsafe_allow_html=True,
        )
        min_s = st.slider("Score minimo", 0, 100, 0, key="csv_sl")
        if st.button("Gerar CSV técnico", use_container_width=True):
            with st.spinner("A gerar..."):
                try:
                    from reports.generator import ReportGenerator
                    path = ReportGenerator().export_csv(score_min=min_s)
                    st.success(f"Ficheiro: `{path}`")
                    with open(path, "rb") as f:
                        st.download_button("Descarregar CSV", f,
                                           file_name=Path(path).name,
                                           mime="text/csv",
                                           use_container_width=True)
                except Exception as e:
                    st.error(str(e))

    with ec4:
        st.markdown(
            '<div class="card"><div style="font-size:.85rem;font-weight:600;color:#94a3b8;margin-bottom:4px;">JSON (integração)</div>'
            '<div style="font-size:.72rem;color:#56697e;">Formato estruturado para integração com sistemas externos.</div></div>',
            unsafe_allow_html=True,
        )
        st.write("")
        if st.button("Exportar WARM + HOT (JSON)", use_container_width=True):
            with st.spinner("A exportar..."):
                try:
                    from reports.generator import ReportGenerator
                    path = ReportGenerator().export_json(score_min=50)
                    st.success(f"Ficheiro: `{path}`")
                    with open(path, "rb") as f:
                        st.download_button("Descarregar JSON", f,
                                           file_name=Path(path).name,
                                           mime="application/json",
                                           use_container_width=True)
                except Exception as e:
                    st.error(str(e))

# betswag_gui.py
# AGGIUNTA: menu a tendina Multigol in "Predizioni"
# - Valori: 1-2,1-3,1-4,2-3,2-4,2-5,3-4,3-5,3-6,4-6
# - Tabella mostra MG% e Pick MG in base alla selezione
# - Dettaglio mostra la selezione Multigol scelta (range) + probabilità
# - Schedina usa il multigol selezionato come mercato MG
#
# FIX RICHIESTO (La Liga):
# - in fixtures "Athletic Club" -> in history "Ath Bilbao"
# - normalizzazione con rimozione accenti + alias
# - warning/log quando una squadra non viene trovata nello storico
#
# Requisiti:
#   pip install pandas numpy scipy requests
#
# Avvio:
#   python betswag_gui.py

import json
import math
import threading
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Set

import numpy as np
import pandas as pd
import requests
from scipy.optimize import minimize
from scipy.stats import poisson

import tkinter as tk
from tkinter import ttk, messagebox

from pathlib import Path

BASE_DIR = Path.home() / "Desktop" / "betswag"
BASE_DIR.mkdir(parents=True, exist_ok=True)

OU_THRESHOLDS = [0.5, 1.5, 2.5, 3.5, 4.5, 5.5]

# Multigol dropdown values requested
MG_RANGES = [
    (1, 2, "Multigol 1-2"),
    (1, 3, "Multigol 1-3"),
    (1, 4, "Multigol 1-4"),
    (2, 3, "Multigol 2-3"),
    (2, 4, "Multigol 2-4"),
    (2, 5, "Multigol 2-5"),
    (3, 4, "Multigol 3-4"),
    (3, 5, "Multigol 3-5"),
    (3, 6, "Multigol 3-6"),
    (4, 6, "Multigol 4-6"),
]
MG_LABELS = [x[2] for x in MG_RANGES]

LEAGUES = {
    "Serie A (Italia)": {
        "history_url": "https://www.football-data.co.uk/mmz4281/2526/I1.csv",
        "fixtures_url": "https://www.matchesio.com/competition/serie-a-it/export/csv/",
        "code": "IT_SA",
    },
    "Premier League (Inghilterra)": {
        "history_url": "https://www.football-data.co.uk/mmz4281/2526/E0.csv",
        "fixtures_url": "https://www.matchesio.com/competition/premier-league-gb-eng/export/csv/",
        "code": "EN_E0",
    },
    "La Liga (Spagna)": {
        "history_url": "https://www.football-data.co.uk/mmz4281/2526/SP1.csv",
        "fixtures_url": "https://www.matchesio.com/competition/la-liga-es/export/csv/",
        "code": "ES_SP1",
    },
    "Bundesliga (Germania)": {
        "history_url": "https://www.football-data.co.uk/mmz4281/2526/D1.csv",
        "fixtures_url": "https://www.matchesio.com/competition/bundesliga-de/export/csv/?season=63",
        "code": "DE_D1",
    },
    "Eredivisie (Olanda)": {
        "history_url": "https://www.football-data.co.uk/mmz4281/2526/N1.csv",
        "fixtures_url": "https://www.matchesio.com/competition/eredivisie-nl/export/csv/?season=340",
        "code": "NL_N1",
    },
    "Primeira Liga (Portogallo)": {
        "history_url": "https://www.football-data.co.uk/mmz4281/2526/P1.csv",
        "fixtures_url": "https://www.matchesio.com/competition/primeira-liga-pt/export/csv/?season=398",
        "code": "PT_P1",
    },
    "Ligue 1 (Francia)": {
        "history_url": "https://www.football-data.co.uk/mmz4281/2526/F1.csv",
        "fixtures_url": "https://www.matchesio.com/competition/ligue-1-fr/export/csv/?season=252",
        "code": "FR_F1",
    },
}

# ============================
# ALIAS FIX (fixtures -> history)
# ============================
# ============================
# ALIAS FIX (fixtures -> history)
# ============================
TEAM_ALIASES = {
    # --- Serie A ---
    "as roma": "roma",
    "ac milan": "milan",

    # --- Premier League ---
    "manchester city": "man city",
    "manchester united": "man united",
    # nott'm forest -> nott m forest (dopo pulizia) -> nottm forest
    "nottingham forest": "nottm forest",
    "nott m forest": "nottm forest",

    # --- La Liga ---
    "athletic club": "ath bilbao",
    "rayo vallecano": "vallecano",
    "atletico madrid": "ath madrid",
    "real betis": "betis",
    "real sociedad": "sociedad",
    "espanyol": "espanol",
    "celta vigo": "celta",

    # --- Bundesliga ---
    "fc augsburg": "augsburg",
    "fc heidenheim": "heidenheim",     # dopo rimozione "1 " diventa "fc heidenheim"
    "fc koln": "fc koln",              # lasciato neutro (serve se storico è proprio "fc koln")
    "vfl wolfsburg": "wolfsburg",
    "fsv mainz 05": "mainz",
    "hamburger sv": "hamburg",
    "bayern munchen": "bayern munich",
    "eintracht frankfurt": "ein frankfurt",
    "bayer leverkusen": "leverkusen",
    "1899 hoffenheim": "hoffenheim",
    "borussia dortmund": "dortmund",
    "sc freiburg": "freiburg",
    "vfb stuttgart": "stuttgart",
    # m'gladbach -> m gladbach (dopo pulizia) -> mgladbach
    "borussia monchengladbach": "mgladbach",
    "m gladbach": "mgladbach",
    # fc st pauli / fc st pauli (da "FC St. Pauli") -> st pauli
    "fc st pauli": "st pauli",

    # --- Eredivisie ---
    "nec nijmegen": "nijmegen",
    "fortuna sittard": "for sittard",
    "fc volendam": "volendam",
    "pec zwolle": "zwolle",

    # --- Primeira Liga ---
    "fc porto": "porto",
    "sporting cp": "sp lisbon",
    "sc braga": "sp braga",

    # --- Ligue 1 ---
    "stade brestois 29": "brest",
    "paris saint germain": "paris sg",
}



def league_paths(league_code: str) -> Dict[str, Path]:
    return {
        "history": BASE_DIR / f"history_{league_code}.csv",
        "history_meta": BASE_DIR / f"history_{league_code}.meta.json",
        "fixtures": BASE_DIR / f"fixtures_{league_code}.csv",
        "fixtures_meta": BASE_DIR / f"fixtures_{league_code}.meta.json",
    }

def thr_key(thr: float) -> str:
    return str(thr).replace(".", "_")

def mg_key(lo: int, hi: int) -> str:
    return f"{lo}_{hi}"

def mg_range_from_label(label: str) -> Tuple[int, int, str]:
    for lo, hi, lab in MG_RANGES:
        if lab == label:
            return lo, hi, lab
    return MG_RANGES[0]

def to_datetime_safe(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip()
    is_ymd = s.str.match(r"^\d{4}-\d{2}-\d{2}", na=False)
    out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    if is_ymd.any():
        out.loc[is_ymd] = pd.to_datetime(s.loc[is_ymd], errors="coerce", dayfirst=False)
    if (~is_ymd).any():
        out.loc[~is_ymd] = pd.to_datetime(s.loc[~is_ymd], errors="coerce", dayfirst=True)
    return out

def head_remote(url: str, timeout: int = 20) -> dict:
    headers = {"User-Agent": "betswag/1.0 (+python-requests)"}
    try:
        r = requests.head(url, allow_redirects=True, timeout=timeout, headers=headers)
        r.raise_for_status()
    except Exception:
        r = requests.get(url, allow_redirects=True, timeout=timeout, headers=headers, stream=True)
        r.raise_for_status()
    h = r.headers
    return {
        "url": r.url,
        "Last-Modified": h.get("Last-Modified"),
        "ETag": h.get("ETag"),
        "Content-Length": h.get("Content-Length"),
        "Date": h.get("Date"),
        "Content-Type": h.get("Content-Type"),
    }

def load_meta(path: Path) -> dict:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_meta(path: Path, meta: dict) -> None:
    with open(path, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, ensure_ascii=False)

def has_remote_changed(new_meta: dict, old_meta: dict) -> bool:
    if not old_meta:
        return True
    for k in ("ETag", "Last-Modified", "Content-Length"):
        nv = new_meta.get(k)
        ov = old_meta.get(k)
        if nv and ov:
            return nv != ov
    return True

def download_file(url: str, dest: Path, meta_path: Path, force_download: bool, log) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    new_meta = head_remote(url)
    old_meta = load_meta(meta_path)

    log(f"Controllo: {new_meta.get('url')}")
    log(f"  Last-Modified: {new_meta.get('Last-Modified')} | ETag: {new_meta.get('ETag')} | Len: {new_meta.get('Content-Length')}")

    changed = has_remote_changed(new_meta, old_meta)
    need_download = force_download or changed or (not dest.exists())

    if need_download:
        log(f"Scarico -> {dest.name}")
        headers = {"User-Agent": "betswag/1.0 (+python-requests)"}
        r = requests.get(url, stream=True, timeout=90, headers=headers, allow_redirects=True)
        r.raise_for_status()
        ctype = (r.headers.get("Content-Type") or "").lower()
        if "text/html" in ctype:
            raise RuntimeError(f"Il link ha restituito HTML invece di CSV: {url}")
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)
        save_meta(meta_path, new_meta)
        log("OK (aggiornato).")
    else:
        log("OK (già aggiornato).")
    return dest

# ============================
# FIX: normalizzazione + alias
# ============================
import re
import unicodedata

def norm_team(s: str) -> str:
    if s is None:
        return ""

    x = str(s).strip().lower()

    # 1) rimuovi accenti (Köln -> Koln, München -> Munchen)
    x = unicodedata.normalize("NFKD", x).encode("ascii", "ignore").decode("ascii")

    # 2) sostituisci punteggiatura con spazio (.,'-/() ecc.)
    x = re.sub(r"[^a-z0-9]+", " ", x)

    # 3) normalizza spazi multipli
    x = " ".join(x.split())

    # 4) rimuovi eventuale prefisso "1" (1 fc koln -> fc koln)
    #    IMPORTANTISSIMO per: 1. FC Köln / 1. FC Heidenheim
    if x.startswith("1 "):
        x = x[2:].strip()

    # 5) applica alias (fixtures -> history)
    return TEAM_ALIASES.get(x, x)


def time_weight(days_ago: float, half_life: float) -> float:
    return 0.5 ** (days_ago / half_life) if half_life > 0 else 1.0

def poisson_matrix(lam_home: float, lam_away: float, max_goals: int = 10) -> np.ndarray:
    hg = poisson.pmf(np.arange(max_goals + 1), lam_home)
    ag = poisson.pmf(np.arange(max_goals + 1), lam_away)
    return np.outer(hg, ag)

def probs_from_matrix(mat: np.ndarray, mg_ranges: List[Tuple[int, int]] = None) -> Dict[str, float]:
    max_g = mat.shape[0] - 1
    i = np.arange(max_g + 1).reshape(-1, 1)
    j = np.arange(max_g + 1).reshape(1, -1)
    total = i + j

    p1 = float(mat[i > j].sum())
    px = float(mat[i == j].sum())
    p2 = float(mat[i < j].sum())
    pgg = float(mat[(i > 0) & (j > 0)].sum())

    out = {
        "p_1": p1,
        "p_x": px,
        "p_2": p2,
        "p_btts": pgg,
        "p_ng": 1.0 - pgg,
        "p_1x": p1 + px,
        "p_x2": px + p2,
        "p_12": p1 + p2,
    }

    denom = p1 + p2
    out["p_dnb_1"] = p1 / denom if denom > 0 else np.nan
    out["p_dnb_2"] = p2 / denom if denom > 0 else np.nan

    for thr in OU_THRESHOLDS:
        pover = float(mat[total > thr].sum())
        out[f"p_over_{thr}"] = pover
        out[f"p_under_{thr}"] = 1.0 - pover

    if mg_ranges is None:
        mg_ranges = [(lo, hi) for lo, hi, _ in MG_RANGES]
    for lo, hi in mg_ranges:
        p = float(mat[(total >= lo) & (total <= hi)].sum())
        out[f"p_mg_{mg_key(lo, hi)}"] = p

    return out

def top_correct_scores(mat: np.ndarray, topn: int = 8) -> List[Tuple[str, float]]:
    scores = []
    for a in range(mat.shape[0]):
        for b in range(mat.shape[1]):
            scores.append((f"{a}-{b}", float(mat[a, b])))
    scores.sort(key=lambda x: x[1], reverse=True)
    return scores[:topn]

@dataclass
class FittedModel:
    teams: List[str]
    team_to_idx: Dict[str, int]
    attack: np.ndarray
    defense: np.ndarray
    home_adv: float
    base_rate: float
    half_life_days: float
    max_goals: int

def fit_model(history_csv: Path, half_life_days: float, max_goals: int, log=None) -> FittedModel:
    df = pd.read_csv(history_csv, sep=None, engine="python")
    required = {"Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG"}
    if not required.issubset(df.columns):
        raise ValueError(f"{history_csv.name} manca colonne minime: {sorted(list(required))}")

    df["Date"] = to_datetime_safe(df["Date"])
    df = df.dropna(subset=["Date", "HomeTeam", "AwayTeam", "FTHG", "FTAG"]).copy()

    df["HomeTeam_n"] = df["HomeTeam"].map(norm_team)
    df["AwayTeam_n"] = df["AwayTeam"].map(norm_team)

    teams = sorted(set(df["HomeTeam_n"]).union(set(df["AwayTeam_n"])))
    
    if log:
        log("Teams nello storico:")
    for t in teams:
        log(f"  - {t}")

    team_to_idx = {t: i for i, t in enumerate(teams)}
    n = len(teams)

    ref_date = df["Date"].max()
    df["days_ago"] = (ref_date - df["Date"]).dt.days.astype(float)
    df["w"] = df["days_ago"].apply(lambda d: time_weight(d, half_life_days))

    def unpack(theta: np.ndarray):
        base = theta[0]
        ha = theta[1]
        a_free = theta[2 : 2 + (n - 1)]
        d_free = theta[2 + (n - 1) : 2 + 2 * (n - 1)]
        attack = np.zeros(n)
        defense = np.zeros(n)
        attack[:-1] = a_free
        defense[:-1] = d_free
        attack[-1] = -attack[:-1].sum()
        defense[-1] = -defense[:-1].sum()
        return base, ha, attack, defense

    def neg_loglik(theta: np.ndarray) -> float:
        base, ha, attack, defense = unpack(theta)
        ll = 0.0
        for _, r in df.iterrows():
            hi = team_to_idx[r["HomeTeam_n"]]
            ai = team_to_idx[r["AwayTeam_n"]]
            w = float(r["w"])
            lam_h = math.exp(base + ha + attack[hi] - defense[ai])
            lam_a = math.exp(base + attack[ai] - defense[hi])
            gh = float(r["FTHG"])
            ga = float(r["FTAG"])
            ll_h = gh * math.log(lam_h + 1e-12) - lam_h - math.lgamma(gh + 1.0)
            ll_a = ga * math.log(lam_a + 1e-12) - lam_a - math.lgamma(ga + 1.0)
            ll += w * (ll_h + ll_a)
        reg = 0.01 * np.sum(theta[2:] ** 2)
        return -(ll - reg)

    mean_goals = (df["FTHG"].mean() + df["FTAG"].mean()) / 2.0
    base0 = math.log(max(mean_goals, 0.8))
    ha0 = 0.15

    theta0 = np.zeros(2 + 2 * (n - 1))
    theta0[0] = base0
    theta0[1] = ha0

    if log:
        log(f"Allenamento Poisson… (squadre: {n}, half-life: {half_life_days}g)")

    res = minimize(neg_loglik, theta0, method="L-BFGS-B")
    if not res.success:
        raise RuntimeError(f"Ottimizzazione fallita: {res.message}")

    base, ha, attack, defense = unpack(res.x)
    return FittedModel(teams, team_to_idx, attack, defense, ha, base, half_life_days, max_goals)

# ============================
# NEW: log warning se squadra non trovata
# ============================
def predict_fixture(
    model: FittedModel,
    home_team: str,
    away_team: str,
    log=None,
    warned: Optional[Set[str]] = None,
) -> Dict:
    ht_raw, at_raw = home_team, away_team
    ht = norm_team(home_team)
    at = norm_team(away_team)

    hi = model.team_to_idx.get(ht, None)
    ai = model.team_to_idx.get(at, None)

    # warning una sola volta per team (evita spam nel log)
    if warned is not None:
        if hi is None and ht not in warned:
            warned.add(ht)
            if log:
                log(f"[WARN] Team NON trovata nello storico: '{ht_raw}' -> '{ht}' (uso valori medi)")
        if ai is None and at not in warned:
            warned.add(at)
            if log:
                log(f"[WARN] Team NON trovata nello storico: '{at_raw}' -> '{at}' (uso valori medi)")

    a_h = model.attack[hi] if hi is not None else 0.0
    d_h = model.defense[hi] if hi is not None else 0.0
    a_a = model.attack[ai] if ai is not None else 0.0
    d_a = model.defense[ai] if ai is not None else 0.0

    lam_h = math.exp(model.base_rate + model.home_adv + a_h - d_a)
    lam_a = math.exp(model.base_rate + a_a - d_h)

    mat = poisson_matrix(lam_h, lam_a, max_goals=model.max_goals)
    probs = probs_from_matrix(mat, mg_ranges=[(lo, hi) for lo, hi, _ in MG_RANGES])
    cs = top_correct_scores(mat, topn=8)

    return {"xg_home": lam_h, "xg_away": lam_a, **probs, "top_correct_scores": cs}

def parse_fixture_datetime(fx: pd.DataFrame) -> pd.Series:
    d = to_datetime_safe(fx["date"].astype(str))
    t = fx["time"].astype(str).str.strip()
    dt = pd.to_datetime(d.dt.strftime("%Y-%m-%d") + " " + t.replace({"nan": "", "NaN": ""}), errors="coerce")
    return dt.fillna(d)

def is_played_row(row: pd.Series, dt: Optional[pd.Timestamp]) -> bool:
    status = str(row.get("status", "")).strip().lower()
    result = str(row.get("result", "")).strip().lower()
    played_markers = {"played", "finished", "ft", "full time", "completed", "final"}
    scheduled_markers = {"to be played", "scheduled", "not started", "ns", "upcoming"}
    if result not in ("", "nan", "none", "-", "null") and len(result) > 0:
        return True
    if status in played_markers:
        return True
    if status in scheduled_markers:
        return False
    if isinstance(dt, pd.Timestamp) and not pd.isna(dt):
        now = pd.Timestamp.now()
        if dt < (now - pd.Timedelta(hours=2)):
            return True
    return False

def load_fixtures_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, sep=None, engine="python")
    cols = {c.strip().lower(): c for c in df.columns}

    def pick_col(*names):
        for n in names:
            if n in cols:
                return cols[n]
        return None

    c_date_time = pick_col("date / time", "date/time", "datetime", "date time")
    c_date = pick_col("date")
    c_time = pick_col("time")
    c_md = pick_col("matchday", "round", "giornata")
    c_home = pick_col("home team", "home_team", "home")
    c_away = pick_col("away team", "away_team", "away")
    c_stadium = pick_col("stadium", "venue")
    c_status = pick_col("status")
    c_result = pick_col("result", "score")

    if c_date_time and (not c_date or not c_time):
        dt = df[c_date_time].astype(str)
        parts = dt.str.split(r"\s+", expand=True)
        date_token = None
        for col in parts.columns[::-1]:
            if parts[col].astype(str).str.contains(r"\d{1,2}/\d{1,2}/\d{2,4}", regex=True).any() or \
               parts[col].astype(str).str.contains(r"^\d{4}-\d{2}-\d{2}$", regex=True).any():
                date_token = col
                break
        if date_token is None:
            date_token = parts.columns[-2] if parts.shape[1] >= 2 else parts.columns[0]
        time_token = parts.columns[-1]
        df["date"] = parts[date_token]
        df["time"] = parts[time_token]
    else:
        df["date"] = df[c_date] if c_date else ""
        df["time"] = df[c_time] if c_time else ""

    df["matchday"] = df[c_md] if c_md else ""
    df["home_team"] = df[c_home] if c_home else ""
    df["away_team"] = df[c_away] if c_away else ""
    df["stadium"] = df[c_stadium] if c_stadium else ""
    df["status"] = df[c_status] if c_status else ""
    df["result"] = df[c_result] if c_result else ""

    return df[["date", "time", "matchday", "home_team", "away_team", "stadium", "status", "result"]].copy()

def build_outputs_df(league_name: str, fx_future: pd.DataFrame, kickoff_dt: pd.Series, preds: List[Dict]) -> pd.DataFrame:
    rows = []
    for i in range(len(fx_future)):
        row = fx_future.iloc[i]
        dt = kickoff_dt.iloc[i]
        pred = preds[i]

        best_1x2 = max([("1", pred["p_1"]), ("X", pred["p_x"]), ("2", pred["p_2"])], key=lambda x: x[1])
        best_dc = max([("1X", pred["p_1x"]), ("X2", pred["p_x2"]), ("12", pred["p_12"])], key=lambda x: x[1])
        best_btts = ("NG", pred["p_ng"]) if pred["p_ng"] >= pred["p_btts"] else ("GG", pred["p_btts"])

        mg_probs = {lab: float(pred[f"p_mg_{mg_key(lo, hi)}"]) for lo, hi, lab in MG_RANGES}
        cs_str = "; ".join([f"{s}:{p*100:.1f}%" for s, p in pred["top_correct_scores"]])

        out = {
            "league": league_name,
            "date": row["date"],
            "time": row["time"],
            "matchday": row["matchday"],
            "home_team": row["home_team"],
            "away_team": row["away_team"],
            "stadium": row["stadium"],
            "kickoff": "" if pd.isna(dt) else dt.strftime("%Y-%m-%d %H:%M"),

            "xg_home": float(pred["xg_home"]),
            "xg_away": float(pred["xg_away"]),

            "p1": float(pred["p_1"]),
            "px": float(pred["p_x"]),
            "p2": float(pred["p_2"]),
            "pick_1x2": best_1x2[0],
            "pick_1x2_prob": float(best_1x2[1]),

            "p1x": float(pred["p_1x"]),
            "px2": float(pred["p_x2"]),
            "p12": float(pred["p_12"]),
            "pick_dc": best_dc[0],
            "pick_dc_prob": float(best_dc[1]),

            "pgg": float(pred["p_btts"]),
            "png": float(pred["p_ng"]),
            "pick_btts": best_btts[0],
            "pick_btts_prob": float(max(pred["p_btts"], pred["p_ng"])),

            "top_scores": cs_str,
        }

        for thr in OU_THRESHOLDS:
            k = thr_key(thr)
            overp = float(pred[f"p_over_{thr}"])
            underp = float(pred[f"p_under_{thr}"])
            out[f"p_over_{k}"] = overp
            out[f"p_under_{k}"] = underp
            out[f"pick_ou_{k}"] = (f"Over {thr}" if overp > underp else f"Under {thr}")
            out[f"pick_ou_{k}_prob"] = float(max(overp, underp))

        for lo, hi, lab in MG_RANGES:
            out[f"p_{mg_key(lo, hi)}"] = mg_probs[lab]

        rows.append(out)

    return pd.DataFrame(rows)

# ==============================
# SCHEDINA HELPERS
# ==============================
def risk_label(p_combo: float) -> str:
    if p_combo >= 0.35:
        return "BASSO"
    if p_combo >= 0.15:
        return "MEDIO"
    return "ALTO"

def markets_for_row(r: pd.Series, ou_thr: float, mg_label: str) -> List[Tuple[str, str, float]]:
    k = thr_key(ou_thr)
    lo, hi, lab = mg_range_from_label(mg_label)
    mgp = float(r.get(f"p_{mg_key(lo, hi)}", np.nan))

    return [
        ("1X2", f"1X2: {r['pick_1x2']}", float(r["pick_1x2_prob"])),
        ("DC", f"DC: {r['pick_dc']}", float(r["pick_dc_prob"])),
        ("OU", f"O/U {ou_thr}: {r[f'pick_ou_{k}']}", float(r[f"pick_ou_{k}_prob"])),
        ("BTTS", f"BTTS: {r['pick_btts']}", float(r["pick_btts_prob"])),
        ("MG", f"{lab}", mgp),
    ]

def choose_pick_standard(r: pd.Series, ou_thr: float, mg_label: str) -> Tuple[str, float, str]:
    opts = markets_for_row(r, ou_thr, mg_label)
    mtype, label, prob = max(opts, key=lambda x: x[2])
    return label, prob, mtype

def choose_pick_value(r: pd.Series, ou_thr: float, mg_label: str, used_counts: Dict[str, int], penalty: float = 0.03) -> Tuple[str, float, str]:
    best = None
    for mtype, label, prob in markets_for_row(r, ou_thr, mg_label):
        score = prob - penalty * used_counts.get(mtype, 0)
        cand = (score, prob, mtype, label)
        if best is None or cand[0] > best[0]:
            best = cand
    _, prob, mtype, label = best
    return label, prob, mtype

def generate_slip(
    df_all: pd.DataFrame,
    n_events: int,
    ou_thr: float,
    league_filter: str,
    mode: str,
    conservative_min_prob: float,
    mg_label: str,
) -> Tuple[pd.DataFrame, str]:
    if df_all is None or df_all.empty:
        raise ValueError("Nessuna predizione disponibile. Esegui prima l'analisi.")

    tmp = df_all.copy()
    if league_filter and league_filter != "Tutti":
        tmp = tmp[tmp["league"] == league_filter].copy()
        if tmp.empty:
            raise ValueError("Non ci sono partite per il campionato selezionato.")

    tmp["kickoff_dt"] = pd.to_datetime(tmp["kickoff"], errors="coerce")
    tmp = tmp.dropna(subset=["kickoff_dt"])

    now = pd.Timestamp.now()
    tmp = tmp[tmp["kickoff_dt"] >= now - pd.Timedelta(minutes=1)].copy()
    if tmp.empty:
        raise ValueError("Non ci sono partite future con kickoff valido.")

    tmp["delta"] = (tmp["kickoff_dt"] - now).abs()
    tmp = tmp.sort_values(["delta", "kickoff_dt"]).reset_index(drop=True)

    selected_rows = []
    picks = []
    used_counts: Dict[str, int] = {}

    for _, r in tmp.iterrows():
        if len(selected_rows) >= n_events:
            break

        if mode == "Conservativa":
            label, prob, mtype = choose_pick_standard(r, ou_thr, mg_label)
            if prob < conservative_min_prob:
                continue
            selected_rows.append(r)
            picks.append((label, prob, mtype))
            used_counts[mtype] = used_counts.get(mtype, 0) + 1

        elif mode == "Value (diversificata)":
            label, prob, mtype = choose_pick_value(r, ou_thr, mg_label, used_counts, penalty=0.03)
            selected_rows.append(r)
            picks.append((label, prob, mtype))
            used_counts[mtype] = used_counts.get(mtype, 0) + 1

        else:
            label, prob, mtype = choose_pick_standard(r, ou_thr, mg_label)
            selected_rows.append(r)
            picks.append((label, prob, mtype))
            used_counts[mtype] = used_counts.get(mtype, 0) + 1

    if not selected_rows:
        if mode == "Conservativa":
            raise ValueError(f"Nessun evento supera la soglia conservativa ({conservative_min_prob*100:.0f}%). Abbassa la soglia.")
        raise ValueError("Impossibile generare schedina (nessun evento).")

    slip_df = pd.DataFrame(selected_rows).reset_index(drop=True)

    title_league = "MIX (tutti i campionati)" if (league_filter == "Tutti" or not league_filter) else league_filter
    lines = []
    lines.append(f"SCHEDINA — Modalità: {mode}")
    lines.append(f"Leghe: {title_league} | Eventi richiesti: {n_events} | Eventi presi: {len(slip_df)} | Soglia O/U: {ou_thr}")
    lines.append(f"Multigol selezionato: {mg_label}")
    if mode == "Conservativa":
        lines.append(f"Soglia Conservativa: >= {conservative_min_prob*100:.0f}%")
    lines.append(f"Generata: {now.strftime('%Y-%m-%d %H:%M')}")
    lines.append("")

    for i in range(len(slip_df)):
        r = slip_df.iloc[i]
        label, prob, _mtype = picks[i]
        lines.append(f"{i+1}) [{r['league']}] {r['kickoff']}  {r['home_team']} vs {r['away_team']}")
        lines.append(f"    PICK: {label}  ({prob*100:.1f}%)")
        lines.append("")

    p_combo = 1.0
    for _, prob, _ in picks:
        p_combo *= float(prob)

    lines.append(f"Probabilità complessiva (indicativa): {p_combo*100:.2f}%")
    lines.append(f"Rischio stimato: {risk_label(p_combo)}")

    if mode == "Value (diversificata)":
        lines.append("")
        lines.append("Distribuzione mercati (VALUE): " + ", ".join([f"{k}:{v}" for k, v in sorted(used_counts.items())]))

    return slip_df, "\n".join(lines)

# ==============================
# GUI
# ==============================
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Betswag — Predictor")
        self.geometry("1300x840")
        self.minsize(1080, 680)

        self.style = ttk.Style(self)
        try:
            self.style.theme_use("clam")
        except Exception:
            pass
        self.option_add("*Font", ("Segoe UI", 10))

        self.all_results: Optional[pd.DataFrame] = None

        self._build_header()
        self._build_main_tabs()

    def _build_header(self):
        header = ttk.Frame(self, padding=(16, 14, 16, 10))
        header.pack(fill="x")
        ttk.Label(header, text="Betswag", font=("Segoe UI", 18, "bold")).pack(side="left")
        ttk.Label(header, text="Predictor (Poisson) — tutte le leghe", foreground="#666").pack(side="left", padx=(12, 0))
        self.run_btn = ttk.Button(header, text="Esegui analisi", command=self.on_run_all)
        self.run_btn.pack(side="right")

    def _build_main_tabs(self):
        root = ttk.Frame(self, padding=(16, 0, 16, 16))
        root.pack(fill="both", expand=True)

        self.main_tabs = ttk.Notebook(root)
        self.main_tabs.pack(fill="both", expand=True)

        self.tab_predictions = ttk.Frame(self.main_tabs, padding=(10, 10))
        self.main_tabs.add(self.tab_predictions, text="Predizioni")

        self.tab_settings = ttk.Frame(self.main_tabs, padding=(10, 10))
        self.main_tabs.add(self.tab_settings, text="Impostazioni")

        self._build_predictions_ui(self.tab_predictions)
        self._build_settings_ui(self.tab_settings)

    def _build_predictions_ui(self, parent: ttk.Frame):
        topbar = ttk.Frame(parent)
        topbar.pack(fill="x", pady=(0, 10))

        ttk.Label(topbar, text="Campionato:").pack(side="left")
        self.pred_league_var = tk.StringVar(value="Tutti")
        self.pred_league_combo = ttk.Combobox(topbar, textvariable=self.pred_league_var, values=["Tutti"] + list(LEAGUES.keys()), width=34, state="readonly")
        self.pred_league_combo.pack(side="left", padx=(10, 14))
        self.pred_league_combo.bind("<<ComboboxSelected>>", lambda _e: self.refresh_table())

        ttk.Label(topbar, text="Soglia O/U:").pack(side="left")
        self.ou_thr_var = tk.StringVar(value="2.5")
        self.ou_combo = ttk.Combobox(topbar, textvariable=self.ou_thr_var, values=[str(x) for x in OU_THRESHOLDS], width=6, state="readonly")
        self.ou_combo.pack(side="left", padx=(10, 14))
        self.ou_combo.bind("<<ComboboxSelected>>", lambda _e: self.refresh_table())

        ttk.Label(topbar, text="Multigol:").pack(side="left")
        self.mg_var = tk.StringVar(value="Multigol 2-3")
        self.mg_combo = ttk.Combobox(topbar, textvariable=self.mg_var, values=MG_LABELS, width=14, state="readonly")
        self.mg_combo.pack(side="left", padx=(10, 14))
        self.mg_combo.bind("<<ComboboxSelected>>", lambda _e: self.refresh_table())

        ttk.Label(topbar, text="Eventi schedina:").pack(side="left")
        self.slip_n_var = tk.IntVar(value=5)
        ttk.Spinbox(topbar, from_=1, to=50, textvariable=self.slip_n_var, width=6).pack(side="left", padx=(10, 12))

        ttk.Label(topbar, text="Modalità:").pack(side="left")
        self.slip_mode_var = tk.StringVar(value="Standard (Best)")
        self.slip_mode_combo = ttk.Combobox(topbar, textvariable=self.slip_mode_var,
                                            values=["Standard (Best)", "Conservativa", "Value (diversificata)"],
                                            width=22, state="readonly")
        self.slip_mode_combo.pack(side="left", padx=(10, 12))

        ttk.Label(topbar, text="Soglia cons.:").pack(side="left")
        self.cons_thr_var = tk.IntVar(value=85)
        ttk.Spinbox(topbar, from_=50, to=99, textvariable=self.cons_thr_var, width=5).pack(side="left", padx=(10, 6))
        ttk.Label(topbar, text="%").pack(side="left")

        self.slip_btn = ttk.Button(topbar, text="Genera Schedina", command=self.on_generate_slip)
        self.slip_btn.pack(side="left", padx=(16, 0))

        tabs = ttk.Notebook(parent)
        tabs.pack(fill="both", expand=True)

        tab_table = ttk.Frame(tabs, padding=(8, 8))
        tabs.add(tab_table, text="Risultati (tabella)")

        table_frame = ttk.Frame(tab_table)
        table_frame.pack(fill="both", expand=True)

        cols = (
            "league", "date", "time", "home", "away",
            "p1", "px", "p2", "pick1x2",
            "dc1x", "dcx2", "dc12", "pickdc",
            "under", "over", "pickou",
            "ng", "gg", "pickbtts",
            "mgp", "mgpick",
        )
        self.tree = ttk.Treeview(table_frame, columns=cols, show="headings", height=18)

        self.tree.heading("league", text="Campionato")
        self.tree.column("league", width=190, anchor="w", stretch=False)

        self.tree.heading("date", text="Data")
        self.tree.heading("time", text="Ora")
        self.tree.column("date", width=90, anchor="center", stretch=False)
        self.tree.column("time", width=60, anchor="center", stretch=False)

        self.tree.heading("home", text="Casa")
        self.tree.heading("away", text="Trasferta")
        self.tree.column("home", width=210, anchor="w", stretch=True)
        self.tree.column("away", width=210, anchor="w", stretch=True)

        for c, t in [("p1", "1%"), ("px", "X%"), ("p2", "2%")]:
            self.tree.heading(c, text=t)
            self.tree.column(c, width=55, anchor="center", stretch=False)

        self.tree.heading("pick1x2", text="Pick 1X2")
        self.tree.column("pick1x2", width=110, anchor="center", stretch=False)

        for c, t in [("dc1x", "1X%"), ("dcx2", "X2%"), ("dc12", "12%")]:
            self.tree.heading(c, text=t)
            self.tree.column(c, width=65, anchor="center", stretch=False)

        self.tree.heading("pickdc", text="Pick DC")
        self.tree.column("pickdc", width=105, anchor="center", stretch=False)

        self.tree.heading("under", text="U%")
        self.tree.heading("over", text="O%")
        self.tree.heading("pickou", text="Pick O/U")
        self.tree.column("under", width=75, anchor="center", stretch=False)
        self.tree.column("over", width=75, anchor="center", stretch=False)
        self.tree.column("pickou", width=150, anchor="center", stretch=False)

        self.tree.heading("ng", text="NG%")
        self.tree.heading("gg", text="GG%")
        self.tree.heading("pickbtts", text="Pick BTTS")
        self.tree.column("ng", width=55, anchor="center", stretch=False)
        self.tree.column("gg", width=55, anchor="center", stretch=False)
        self.tree.column("pickbtts", width=110, anchor="center", stretch=False)

        self.tree.heading("mgp", text="MG%")
        self.tree.heading("mgpick", text="Pick MG")
        self.tree.column("mgp", width=70, anchor="center", stretch=False)
        self.tree.column("mgpick", width=120, anchor="center", stretch=False)

        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(table_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        table_frame.rowconfigure(0, weight=1)
        table_frame.columnconfigure(0, weight=1)

        self.tree.bind("<<TreeviewSelect>>", self.on_select_row)

        tab_detail = ttk.Frame(tabs, padding=(8, 8))
        tabs.add(tab_detail, text="Dettaglio")

        self.detail_title = ttk.Label(tab_detail, text="Seleziona una partita nella tabella", font=("Segoe UI", 14, "bold"))
        self.detail_title.pack(anchor="w", pady=(0, 8))

        self.detail_text = tk.Text(tab_detail, wrap="word", borderwidth=0)
        self.detail_text.pack(fill="both", expand=True)
        self.detail_text.configure(state="disabled")

        tab_slip = ttk.Frame(tabs, padding=(8, 8))
        tabs.add(tab_slip, text="Schedina")

        self.slip_text = tk.Text(tab_slip, wrap="word", borderwidth=0)
        self.slip_text.pack(fill="both", expand=True)
        self.slip_text.configure(state="disabled")

    def _build_settings_ui(self, parent: ttk.Frame):
        top = ttk.Frame(parent)
        top.pack(fill="x", pady=(0, 10))

        settings = ttk.Labelframe(top, text="Impostazioni", padding=(12, 10))
        settings.pack(side="left", fill="both", expand=True, padx=(0, 10))

        self.force_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(settings, text="Forza download (tutte le leghe)", variable=self.force_var).grid(row=0, column=0, sticky="w")

        params = ttk.Frame(settings)
        params.grid(row=1, column=0, sticky="w", pady=(10, 0))

        ttk.Label(params, text="Half-life (giorni)").grid(row=0, column=0, sticky="w")
        self.halflife_var = tk.IntVar(value=180)
        ttk.Spinbox(params, from_=30, to=3650, textvariable=self.halflife_var, width=10).grid(row=0, column=1, padx=(10, 0), sticky="w")

        ttk.Label(params, text="Max goals").grid(row=1, column=0, sticky="w", pady=(8, 0))
        self.maxgoals_var = tk.IntVar(value=10)
        ttk.Spinbox(params, from_=6, to=14, textvariable=self.maxgoals_var, width=10).grid(row=1, column=1, padx=(10, 0), sticky="w", pady=(8, 0))

        info = ttk.Labelframe(top, text="Info", padding=(12, 10))
        info.pack(side="left", fill="both")
        ttk.Label(info, text=f"Cartella:\n{BASE_DIR}", justify="left").pack(anchor="w")
        ttk.Label(info, text="Esegui Analisi scarica e calcola per TUTTE le leghe.\nFiltra in Predizioni.", foreground="#666", justify="left").pack(anchor="w", pady=(10, 0))

        logbox = ttk.Labelframe(parent, text="Log", padding=(10, 8))
        logbox.pack(fill="both", expand=True)
        self.log_text = tk.Text(logbox, wrap="word", borderwidth=0)
        self.log_text.pack(fill="both", expand=True)
        self.log_text.configure(state="disabled")

    def log(self, msg: str):
        self.log_text.configure(state="normal")
        self.log_text.insert("end", msg + "\n")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def set_detail(self, text: str):
        self.detail_text.configure(state="normal")
        self.detail_text.delete("1.0", "end")
        self.detail_text.insert("end", text)
        self.detail_text.configure(state="disabled")

    def set_slip(self, text: str):
        self.slip_text.configure(state="normal")
        self.slip_text.delete("1.0", "end")
        self.slip_text.insert("end", text)
        self.slip_text.configure(state="disabled")

    def clear_tree(self):
        for item in self.tree.get_children():
            self.tree.delete(item)

    def get_selected_thr(self) -> float:
        try:
            return float(self.ou_thr_var.get())
        except Exception:
            return 2.5

    def get_selected_mg_label(self) -> str:
        return self.mg_var.get().strip() if self.mg_var.get() else MG_LABELS[0]

    def current_filter_league(self) -> str:
        return self.pred_league_var.get().strip() if self.pred_league_var.get() else "Tutti"

    def pct(self, x: float) -> float:
        return float(x) * 100.0

    def refresh_table(self):
        self.clear_tree()
        if self.all_results is None or self.all_results.empty:
            return

        thr = self.get_selected_thr()
        k = thr_key(thr)

        mg_label = self.get_selected_mg_label()
        lo, hi, _ = mg_range_from_label(mg_label)
        mg_col = f"p_{mg_key(lo, hi)}"

        df = self.all_results
        lf = self.current_filter_league()
        if lf != "Tutti":
            df = df[df["league"] == lf].copy()

        df["kickoff_dt"] = pd.to_datetime(df["kickoff"], errors="coerce")
        df = df.sort_values(["kickoff_dt"], na_position="last").drop(columns=["kickoff_dt"]).reset_index(drop=True)

        for idx, r in df.iterrows():
            mgp = float(r.get(mg_col, np.nan))
            self.tree.insert(
                "",
                "end",
                iid=str(idx),
                values=(
                    r["league"],
                    r["date"],
                    r["time"],
                    r["home_team"],
                    r["away_team"],
                    f"{self.pct(r['p1']):.1f}",
                    f"{self.pct(r['px']):.1f}",
                    f"{self.pct(r['p2']):.1f}",
                    f"{r['pick_1x2']} ({self.pct(r['pick_1x2_prob']):.0f}%)",
                    f"{self.pct(r['p1x']):.1f}",
                    f"{self.pct(r['px2']):.1f}",
                    f"{self.pct(r['p12']):.1f}",
                    f"{r['pick_dc']} ({self.pct(r['pick_dc_prob']):.0f}%)",
                    f"{self.pct(r[f'p_under_{k}']):.1f}",
                    f"{self.pct(r[f'p_over_{k}']):.1f}",
                    f"{r[f'pick_ou_{k}']} ({self.pct(r[f'pick_ou_{k}_prob']):.0f}%)",
                    f"{self.pct(r['png']):.1f}",
                    f"{self.pct(r['pgg']):.1f}",
                    f"{r['pick_btts']} ({self.pct(r['pick_btts_prob']):.0f}%)",
                    f"{self.pct(mgp):.0f}" if not np.isnan(mgp) else "",
                    mg_label,
                ),
            )

        thr_txt = f"{thr}".rstrip("0").rstrip(".")
        self.tree.heading("under", text=f"U{thr_txt}%")
        self.tree.heading("over", text=f"O{thr_txt}%")
        self.tree.heading("pickou", text=f"Pick O/U {thr_txt}")

    def on_select_row(self, _evt=None):
        if self.all_results is None or self.all_results.empty:
            return
        sel = self.tree.selection()
        if not sel:
            return
        idx = int(sel[0])

        df = self.all_results
        lf = self.current_filter_league()
        if lf != "Tutti":
            df = df[df["league"] == lf].copy()
        df["kickoff_dt"] = pd.to_datetime(df["kickoff"], errors="coerce")
        df = df.sort_values(["kickoff_dt"], na_position="last").drop(columns=["kickoff_dt"]).reset_index(drop=True)
        if idx >= len(df):
            return
        r = df.iloc[idx]

        thr = self.get_selected_thr()
        k = thr_key(thr)

        mg_label = self.get_selected_mg_label()
        lo, hi, _ = mg_range_from_label(mg_label)
        mg_col = f"p_{mg_key(lo, hi)}"
        mgp = float(r.get(mg_col, np.nan))

        title = f"[{r['league']}] {r['date']} {r['time']} — {r['home_team']} vs {r['away_team']}"
        detail = []
        detail.append(title)
        detail.append(f"Stadio: {r['stadium']}")
        detail.append(f"Kickoff: {r['kickoff']}")
        detail.append("")
        detail.append(f"xG attesi: {r['xg_home']:.2f} - {r['xg_away']:.2f}")
        detail.append("")
        detail.append(f"1X2: 1 {self.pct(r['p1']):.1f}% | X {self.pct(r['px']):.1f}% | 2 {self.pct(r['p2']):.1f}% -> {r['pick_1x2']} ({self.pct(r['pick_1x2_prob']):.1f}%)")
        detail.append(f"DC: 1X {self.pct(r['p1x']):.1f}% | X2 {self.pct(r['px2']):.1f}% | 12 {self.pct(r['p12']):.1f}% -> {r['pick_dc']} ({self.pct(r['pick_dc_prob']):.1f}%)")
        detail.append(f"O/U {thr}: Over {self.pct(r[f'p_over_{k}']):.1f}% | Under {self.pct(r[f'p_under_{k}']):.1f}% -> {r[f'pick_ou_{k}']} ({self.pct(r[f'pick_ou_{k}_prob']):.1f}%)")
        detail.append(f"BTTS: GG {self.pct(r['pgg']):.1f}% | NG {self.pct(r['png']):.1f}% -> {r['pick_btts']} ({self.pct(r['pick_btts_prob']):.1f}%)")
        if not np.isnan(mgp):
            detail.append(f"{mg_label}: {self.pct(mgp):.1f}%")
        detail.append("")
        detail.append(f"Top risultati esatti: {r['top_scores']}")

        self.detail_title.configure(text=title)
        self.set_detail("\n".join(detail))

    def on_generate_slip(self):
        if self.all_results is None or self.all_results.empty:
            messagebox.showinfo("Schedina", "Esegui prima Analisi.")
            return

        try:
            n = int(self.slip_n_var.get())
        except Exception:
            n = 5

        lf = self.current_filter_league()
        thr = self.get_selected_thr()
        mg_label = self.get_selected_mg_label()
        mode = self.slip_mode_var.get().strip()
        cons_thr = float(self.cons_thr_var.get()) / 100.0

        try:
            slip_df, slip_text = generate_slip(self.all_results, n, thr, lf, mode, cons_thr, mg_label)
            self.set_slip(slip_text)

            safe_mode = mode.replace(" ", "_").replace("(", "").replace(")", "").replace("/", "_").replace(":", "")
            safe_league = "MIX" if lf == "Tutti" else LEAGUES[lf]["code"]
            outp = BASE_DIR / f"slip_{safe_league}_{safe_mode}.txt"
            with open(outp, "w", encoding="utf-8") as f:
                f.write(slip_text)

            self.log(f"Scheda generata ({len(slip_df)} eventi) -> {outp.name}")
        except Exception as e:
            messagebox.showerror("Schedina", str(e))

    def on_run_all(self):
        self.run_btn.configure(state="disabled")
        self.log("===== Avvio analisi: TUTTI i campionati =====")

        force = bool(self.force_var.get())
        half_life = float(self.halflife_var.get())
        max_goals = int(self.maxgoals_var.get())

        def worker():
            all_dfs = []
            warned_teams: Set[str] = set()
            try:
                for league_name, info in LEAGUES.items():
                    code = info["code"]
                    p = league_paths(code)

                    self.log(f"\n--- {league_name} ---")
                    self.log("History:")
                    download_file(info["history_url"], p["history"], p["history_meta"], force, self.log)
                    self.log("Fixtures:")
                    download_file(info["fixtures_url"], p["fixtures"], p["fixtures_meta"], force, self.log)

                    fx = load_fixtures_csv(p["fixtures"])
                    kickoff_dt = parse_fixture_datetime(fx)

                    keep = []
                    for i in range(len(fx)):
                        keep.append(not is_played_row(fx.iloc[i], kickoff_dt.iloc[i]))
                    keep = pd.Series(keep)

                    fx_future = fx.loc[keep].reset_index(drop=True)
                    kickoff_future = kickoff_dt.loc[keep].reset_index(drop=True)

                    self.log(f"Partite future: {len(fx_future)} (saltate già giocate: {len(fx)-len(fx_future)})")
                    if len(fx_future) == 0:
                        continue

                    self.log("Fit modello + predizioni…")
                    model = fit_model(p["history"], half_life_days=half_life, max_goals=max_goals, log=self.log)

                    preds = [
                        predict_fixture(
                            model,
                            r["home_team"],
                            r["away_team"],
                            log=self.log,
                            warned=warned_teams
                        )
                        for _, r in fx_future.iterrows()
                    ]

                    df_out = build_outputs_df(league_name, fx_future, kickoff_future, preds)
                    all_dfs.append(df_out)

                if not all_dfs:
                    self.log("Nessuna predizione generata (forse fixtures vuote).")
                    self.after(0, lambda: messagebox.showinfo("Info", "Nessuna partita futura trovata."))
                    return

                self.all_results = pd.concat(all_dfs, ignore_index=True)
                master_csv = BASE_DIR / "predictions_ALL.csv"
                self.all_results.to_csv(master_csv, index=False)

                self.after(0, self.refresh_table)
                self.after(0, lambda: self.main_tabs.select(self.tab_predictions))
                self.log(f"\nOK. Salvato anche {master_csv.name}")

            except Exception as e:
                self.after(0, lambda: messagebox.showerror("Errore", str(e)))
                self.log(f"ERRORE: {e}")
            finally:
                self.after(0, lambda: self.run_btn.configure(state="normal"))

        threading.Thread(target=worker, daemon=True).start()


if __name__ == "__main__":
    app = App()
    app.mainloop()

#!/usr/bin/env python3
"""
NBA Edge Model — Nightly Results Logger
Runs via GitHub Actions at 9am ET daily.
Pulls previous day's finalized NBA games from BallDontLie,
fetches closing lines from The Odds API,
computes ATS/OU outcomes for fatigue-flagged games,
appends to data/results.json.
"""

import os
import json
import time as _time
import requests
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

SGO_KEY  = None  # SGO disabled — using BDL + The Odds API
BDL_KEY  = os.environ["BDL_API_KEY"]
ODDS_KEY = os.environ["ODDS_API_KEY"]

BDL_BASE  = "https://api.balldontlie.io/v1"
ODDS_BASE = "https://api.the-odds-api.com/v4"
SPORT     = "basketball_nba"

BDL_HEADERS  = {"Authorization": BDL_KEY}

ET = ZoneInfo("America/New_York")

# ── FATIGUE MODEL (mirrors nba_edge_v2.html exactly) ─────────────
import math

ARENAS = {
    "ATL":{"lat":33.7573,"lon":-84.3963,"tz_name":"America/New_York"},
    "BOS":{"lat":42.3662,"lon":-71.0621,"tz_name":"America/New_York"},
    "BKN":{"lat":40.6826,"lon":-73.9754,"tz_name":"America/New_York"},
    "CHA":{"lat":35.2251,"lon":-80.8392,"tz_name":"America/New_York"},
    "CHI":{"lat":41.8807,"lon":-87.6742,"tz_name":"America/Chicago"},
    "CLE":{"lat":41.4965,"lon":-81.6882,"tz_name":"America/New_York"},
    "DAL":{"lat":32.7905,"lon":-96.8103,"tz_name":"America/Chicago"},
    "DEN":{"lat":39.7487,"lon":-105.0077,"tz_name":"America/Denver"},
    "DET":{"lat":42.3410,"lon":-83.0552,"tz_name":"America/New_York"},
    "GSW":{"lat":37.7680,"lon":-122.3877,"tz_name":"America/Los_Angeles"},
    "HOU":{"lat":29.7508,"lon":-95.3621,"tz_name":"America/Chicago"},
    "IND":{"lat":39.7640,"lon":-86.1555,"tz_name":"America/New_York"},
    "LAC":{"lat":33.8958,"lon":-118.3386,"tz_name":"America/Los_Angeles"},
    "LAL":{"lat":34.0430,"lon":-118.2673,"tz_name":"America/Los_Angeles"},
    "MEM":{"lat":35.1383,"lon":-90.0505,"tz_name":"America/Chicago"},
    "MIA":{"lat":25.7814,"lon":-80.1870,"tz_name":"America/New_York"},
    "MIL":{"lat":43.0450,"lon":-87.9170,"tz_name":"America/Chicago"},
    "MIN":{"lat":44.9795,"lon":-93.2762,"tz_name":"America/Chicago"},
    "NOP":{"lat":29.9490,"lon":-90.0812,"tz_name":"America/Chicago"},
    "NYK":{"lat":40.7505,"lon":-73.9934,"tz_name":"America/New_York"},
    "OKC":{"lat":35.4634,"lon":-97.5151,"tz_name":"America/Chicago"},
    "ORL":{"lat":28.5392,"lon":-81.3839,"tz_name":"America/New_York"},
    "PHI":{"lat":39.9012,"lon":-75.1720,"tz_name":"America/New_York"},
    "PHX":{"lat":33.4457,"lon":-112.0712,"tz_name":"America/Phoenix"},
    "POR":{"lat":45.5316,"lon":-122.6668,"tz_name":"America/Los_Angeles"},
    "SAC":{"lat":38.5802,"lon":-121.4997,"tz_name":"America/Los_Angeles"},
    "SAS":{"lat":29.4270,"lon":-98.4375,"tz_name":"America/Chicago"},
    "TOR":{"lat":43.6435,"lon":-79.3791,"tz_name":"America/Toronto"},
    "UTA":{"lat":40.7683,"lon":-111.9011,"tz_name":"America/Denver"},
    "WAS":{"lat":38.8981,"lon":-77.0209,"tz_name":"America/New_York"},
}
ALTITUDE_ARENAS = {"DEN", "UTA"}

# Full team names for The Odds API matching
TEAM_NAMES = {
    "ATL":"Atlanta Hawks",          "BOS":"Boston Celtics",
    "BKN":"Brooklyn Nets",          "CHA":"Charlotte Hornets",
    "CHI":"Chicago Bulls",          "CLE":"Cleveland Cavaliers",
    "DAL":"Dallas Mavericks",       "DEN":"Denver Nuggets",
    "DET":"Detroit Pistons",        "GSW":"Golden State Warriors",
    "HOU":"Houston Rockets",        "IND":"Indiana Pacers",
    "LAC":"Los Angeles Clippers",   "LAL":"Los Angeles Lakers",
    "MEM":"Memphis Grizzlies",      "MIA":"Miami Heat",
    "MIL":"Milwaukee Bucks",        "MIN":"Minnesota Timberwolves",
    "NOP":"New Orleans Pelicans",   "NYK":"New York Knicks",
    "OKC":"Oklahoma City Thunder",  "ORL":"Orlando Magic",
    "PHI":"Philadelphia 76ers",     "PHX":"Phoenix Suns",
    "POR":"Portland Trail Blazers", "SAC":"Sacramento Kings",
    "SAS":"San Antonio Spurs",      "TOR":"Toronto Raptors",
    "UTA":"Utah Jazz",              "WAS":"Washington Wizards",
}

def haversine(lat1, lon1, lat2, lon2):
    R = 3959
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return round(R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a)))

def get_dist(a, b):
    if a == b: return 0
    if set([a,b]) == {"LAL","LAC"}: return 12
    if set([a,b]) == {"NYK","BKN"}: return 8
    if a not in ARENAS or b not in ARENAS: return 0
    return haversine(ARENAS[a]["lat"], ARENAS[a]["lon"], ARENAS[b]["lat"], ARENAS[b]["lon"])

def get_utc_offset(arena, date_str):
    from zoneinfo import ZoneInfo
    tz_name = ARENAS.get(arena, {}).get("tz_name")
    if not tz_name: return -6
    tz = ZoneInfo(tz_name)
    y, m, d = map(int, date_str.split("-"))
    dt = datetime(y, m, d, 17, 0, 0, tzinfo=timezone.utc)
    local = dt.astimezone(tz)
    offset = local.utcoffset().total_seconds() / 3600
    return offset

def estimate_btb_sleep(from_arena, to_arena, prev_tip_local_hr=20.0, date_str=None):
    dist = get_dist(from_arena, to_arena)
    flight_hrs = dist / 500
    to_tz   = get_utc_offset(to_arena,   date_str or "2026-01-01")
    from_tz = get_utc_offset(from_arena, date_str or "2026-01-01")
    tz_shift = to_tz - from_tz
    game_end = prev_tip_local_hr + 2.5
    departure_dest = game_end + 2.5 + tz_shift
    landing_dest   = departure_dest + flight_hrs
    hotel_arrival  = landing_dest + 0.75
    wake_up        = 34.0  # 10am next day
    hotel_sleep    = max(0, wake_up - hotel_arrival)
    plane_after_midnight = max(0, landing_dest - max(departure_dest, 24.0))
    plane_sleep    = plane_after_midnight * 0.5
    total          = hotel_sleep + plane_sleep
    return {"dist": dist, "flight_hrs": round(flight_hrs,1),
            "total": round(total,1), "tz_delta": tz_shift}

def compute_fatigue_score(scenario, is_btb, effective_sleep, tz_delta, prev_late, density_tag, altitude_penalty=0):
    base = 0
    if is_btb:
        base = {"A":5,"C":4,"B":3,"home-home":2}.get(scenario, 2)
    sleep_mod = 0
    if is_btb and effective_sleep is not None:
        if   effective_sleep < 2: sleep_mod = 5
        elif effective_sleep < 4: sleep_mod = 3
        elif effective_sleep < 6: sleep_mod = 2
        elif effective_sleep < 7: sleep_mod = 1
    tz_mod      = min(tz_delta * 0.5, 1.5) if tz_delta > 0 else 0
    late_mod    = 0.5 if (is_btb and prev_late) else 0
    density_mod = 2 if density_tag == "4-in-6" else 1 if density_tag == "3-in-4" else 0
    return min(10, max(0, round((base + sleep_mod + tz_mod + late_mod + density_mod + altitude_penalty) * 10) / 10))

def analyze_fatigue(team, is_home, days_rest, prev_arena, home_team, was_home_last,
                    games_in4=1, games_in6=1, prev_tip_hr=19.5, prev_late=False,
                    recent_altitude=False, date_str=None):
    if days_rest is None:
        return {"score": 0, "scenario": None, "detail": "Rest unknown"}

    density_tag = None
    if games_in4 >= 3:
        density_tag = "4-in-6" if games_in6 >= 4 else "3-in-4"
    elif games_in6 >= 4:
        density_tag = "4-in-6"

    alt_penalty = 1.0 if (not is_home and home_team in ALTITUDE_ARENAS and not recent_altitude) else 0
    is_btb = days_rest == 0

    if is_home:
        if not is_btb:
            score = compute_fatigue_score(None, False, 99, 0, False, density_tag, 0)
            return {"score": score, "scenario": None, "detail": "Home court", "is_btb": False}
        if not was_home_last:
            s = estimate_btb_sleep(prev_arena or team, home_team, prev_tip_hr, date_str)
            adj = s["total"]
            score = compute_fatigue_score("C", True, adj, s["tz_delta"], prev_late, density_tag, 0)
            return {"score": score, "scenario": "C", "detail": f"BTB away→home · {s['dist']}mi", "is_btb": True, "sleep": adj}
        else:
            prev_adj = prev_tip_hr if prev_tip_hr >= 12 else prev_tip_hr + 24
            hh_sleep = max(0, 34.0 - (prev_adj + 3.5))
            score = compute_fatigue_score("home-home", True, hh_sleep, 0, prev_late, density_tag, 0)
            return {"score": score, "scenario": "home-home", "detail": "Home BTB", "is_btb": True, "sleep": round(hh_sleep,1)}

    if days_rest >= 2:
        score = compute_fatigue_score(None, False, 99, 0, False, density_tag, alt_penalty)
        return {"score": score, "scenario": None, "detail": "Road, full rest", "is_btb": False}

    if days_rest == 1:
        dist = get_dist(prev_arena or team, home_team)
        tz_delta = get_utc_offset(home_team, date_str or "2026-01-01") - get_utc_offset(prev_arena or team, date_str or "2026-01-01")
        severe = tz_delta >= 2 and dist > 1800
        score = compute_fatigue_score(None, False, 99, tz_delta if severe else 0, False, density_tag, alt_penalty)
        return {"score": score, "scenario": None, "detail": f"Road 1d rest {'(body clock)' if severe else ''}", "is_btb": False}

    # BTB away
    if was_home_last:
        dist = get_dist(team, home_team)
        tz_delta = get_utc_offset(home_team, date_str or "2026-01-01") - get_utc_offset(team, date_str or "2026-01-01")
        prev_adj = prev_tip_hr if prev_tip_hr >= 12 else prev_tip_hr + 24
        raw_sleep = max(0, 34.0 - (prev_adj + 3.5))
        body_clock = tz_delta * 0.3 if tz_delta > 0 else 0
        adj = max(0, raw_sleep - body_clock)
        score = compute_fatigue_score("B", True, adj, tz_delta, prev_late, density_tag, alt_penalty)
        return {"score": score, "scenario": "B", "detail": f"BTB home→away · {dist}mi", "is_btb": True, "sleep": round(adj,1)}

    # Scenario A
    from_arena = prev_arena or team
    s = estimate_btb_sleep(from_arena, home_team, prev_tip_hr, date_str)
    adj = max(0, s["total"])
    score = compute_fatigue_score("A", True, adj, s["tz_delta"], prev_late, density_tag, alt_penalty)
    return {"score": score, "scenario": "A", "detail": f"BTB road trip · {s['dist']}mi", "is_btb": True, "sleep": round(adj,1)}

def get_betting_signals(away_f, home_f):
    """
    Mirrors getBettingSignals() in nba_edge_v2.html exactly.
    Returns a list of dicts — can contain both 'spread' and 'under'.
    """
    signals = []
    diff  = away_f["score"] - home_f["score"]  # positive = away worse
    delta = abs(diff)

    # UNDER signal: both >= 5, away = Scenario A (road-trip BTB only)
    if (away_f["score"] >= 5 and home_f["score"] >= 5
            and away_f.get("is_btb") and away_f.get("scenario") == "A"):
        strong = home_f.get("scenario") in ("home-home", "C")
        signals.append({"type": "under", "confidence": "+++" if strong else "++"})

    # SPREAD signal: home more fatigued (diff < 0), home BTB non-B, delta >= 4
    if (diff < 0 and delta >= 4
            and home_f.get("is_btb") and home_f.get("scenario") != "B"):
        signals.append({"type": "spread"})

    return signals

# ── BDL FETCH HELPERS ────────────────────────────────────────────

def bdl_get(endpoint, params):
    r = requests.get(f"{BDL_BASE}{endpoint}", headers=BDL_HEADERS, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def fetch_bdl_games(date_str):
    """Fetch all finalized NBA games for a given date (YYYY-MM-DD)."""
    data = bdl_get("/games", {"dates[]": date_str, "per_page": 100})
    return [g for g in data.get("data", []) if g.get("status") == "Final"]

def fetch_bdl_history(start_date_str, end_date_str):
    """Fetch all games between two dates for fatigue history."""
    from datetime import date
    start = date.fromisoformat(start_date_str)
    end   = date.fromisoformat(end_date_str)
    dates = []
    cur = start
    while cur < end:
        dates.append(cur.isoformat())
        cur += timedelta(days=1)

    all_games = []
    chunk = 7  # one week at a time to stay within param limits
    for i in range(0, len(dates), chunk):
        batch = dates[i:i+chunk]
        params = [("dates[]", d) for d in batch] + [("per_page", 100)]
        r = requests.get(f"{BDL_BASE}/games", headers=BDL_HEADERS, params=params, timeout=20)
        r.raise_for_status()
        all_games.extend(r.json().get("data", []))
        _time.sleep(1)  # stay under BDL rate limit
    return all_games

# ── ODDS API: CLOSING LINES ───────────────────────────────────────

def _avg_point(bookmakers, market_key, outcome_name):
    vals = []
    for bk in bookmakers:
        for mkt in bk.get("markets", []):
            if mkt["key"] != market_key:
                continue
            for oc in mkt.get("outcomes", []):
                if oc["name"] == outcome_name and oc.get("point") is not None:
                    vals.append(oc["point"])
    return round(sum(vals) / len(vals), 1) if vals else None

def _avg_price(bookmakers, market_key, outcome_name):
    vals = []
    for bk in bookmakers:
        for mkt in bk.get("markets", []):
            if mkt["key"] != market_key:
                continue
            for oc in mkt.get("outcomes", []):
                if oc["name"] == outcome_name and oc.get("price") is not None:
                    vals.append(oc["price"])
    return round(sum(vals) / len(vals)) if vals else None

def fetch_odds_lines(date_str):
    """
    Fetch closing lines from The Odds API historical endpoint.
    Snapshot at 19:00 UTC (~2pm ET) captures pre-game closing lines.
    Cost: 30 credits (3 markets x 1 region).
    """
    y, m, d = map(int, date_str.split("-"))
    snapshot = datetime(y, m, d, 19, 0, 0, tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    params = {
        "apiKey":    ODDS_KEY,
        "regions":   "us",
        "markets":   "spreads,totals,h2h",
        "oddsFormat":"american",
        "date":      snapshot,
    }
    try:
        r = requests.get(
            f"{ODDS_BASE}/historical/sports/{SPORT}/odds",
            params=params, timeout=20
        )
        remaining = r.headers.get("x-requests-remaining", "?")
        used      = r.headers.get("x-requests-used", "?")
        print(f"  [Odds API] credits used={used} remaining={remaining}")
        if r.status_code != 200:
            print(f"  Odds API error {r.status_code}: {r.text[:150]}")
            return {}
        events = r.json().get("data", [])
    except Exception as e:
        print(f"  Odds API exception: {e}")
        return {}

    lines = {}
    for ev in events:
        away_full = ev.get("away_team", "")
        home_full = ev.get("home_team", "")
        away_abbr = next((k for k, v in TEAM_NAMES.items() if v == away_full), None)
        home_abbr = next((k for k, v in TEAM_NAMES.items() if v == home_full), None)
        if not away_abbr or not home_abbr:
            continue
        bks = ev.get("bookmakers", [])
        lines[(away_abbr, home_abbr)] = {
            "close_spread": _avg_point(bks, "spreads", home_full),
            "close_total":  _avg_point(bks, "totals",  "Over"),
            "home_ml":      _avg_price(bks, "h2h",     home_full),
            "away_ml":      _avg_price(bks, "h2h",     away_full),
        }
    return lines

# ── MAIN ──────────────────────────────────────────────────────────

def main():
    # Yesterday in ET
    now_et    = datetime.now(ET)
    yesterday = (now_et - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"Processing games for {yesterday}")

    # ── 1. Fetch yesterday's finalized games from BDL ─────────────
    games = fetch_bdl_games(yesterday)
    if not games:
        print(f"No finalized NBA games found for {yesterday}")
        return
    print(f"Found {len(games)} finalized games")

    # ── 2. Fetch closing lines from The Odds API ──────────────────
    # One call covers all games for the date. Snapshot at 19:00 UTC
    # captures pre-game closing lines (~2pm ET).
    lines = fetch_odds_lines(yesterday)
    print(f"Fetched closing lines for {len(lines)} matchups from The Odds API")

    # ── 3. Fetch 21-day history for fatigue/rest calculation ──────
    y, m, d  = map(int, yesterday.split("-"))
    hist_end = yesterday  # exclusive: stop before yesterday's games
    hist_start = (datetime(y, m, d) - timedelta(days=21)).strftime("%Y-%m-%d")
    history_games = fetch_bdl_history(hist_start, hist_end)
    print(f"Fetched {len(history_games)} history games for fatigue calc")

    # Build team history keyed by team abbreviation.
    # BDL provides a "datetime" UTC ISO string for tip time — used for rest/BTB calc.
    team_history = {}
    for g in history_games:
        home_abbr = g["home_team"]["abbreviation"]
        away_abbr = g["visitor_team"]["abbreviation"]
        # BDL returns a "datetime" field with the actual UTC tip time ISO string.
        # Fall back to date + noon UTC only if missing.
        starts_at = g.get("datetime") or (g["date"][:10] + "T17:00:00Z")
        rec = {
            "starts_at":  starts_at,
            "home_abbr":  home_abbr,
            "away_abbr":  away_abbr,
            "home_score": g.get("home_team_score"),
            "away_score": g.get("visitor_team_score"),
        }
        team_history.setdefault(home_abbr, []).append(rec)
        team_history.setdefault(away_abbr, []).append(rec)

    for abbr_key in team_history:
        team_history[abbr_key].sort(key=lambda x: x["starts_at"])

    def calc_rest(team_abbr, target_date_str):
        games_hist = team_history.get(team_abbr, [])

        def et_date(starts_at_str):
            dt = datetime.fromisoformat(starts_at_str.replace("Z", "+00:00"))
            return dt.astimezone(ET).date()

        target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()
        played = [g for g in games_hist if et_date(g["starts_at"]) < target_date]

        if not played:
            return {"days_rest": None, "prev_arena": None, "was_home_last": None,
                    "games_in4": 1, "games_in6": 1, "prev_tip_hr": 19.5,
                    "prev_late": False, "recent_altitude": False}

        last = played[-1]
        last_et_date = et_date(last["starts_at"])
        days_rest = max(0, (target_date - last_et_date).days - 1)

        was_home  = last["home_abbr"] == team_abbr
        home_abbr = last["home_abbr"]

        # Parse actual tip time from the BDL datetime field (UTC ISO string)
        last_dt = datetime.fromisoformat(last["starts_at"].replace("Z", "+00:00"))
        last_local = last_dt.astimezone(ZoneInfo(ARENAS.get(home_abbr, {}).get("tz_name", "America/New_York")))
        prev_tip_hr = last_local.hour + last_local.minute / 60
        if prev_tip_hr < 12:
            prev_tip_hr += 24  # push into 0-48 scale for midnight crossover
        prev_late = prev_tip_hr >= 21.5

        def count_in(n):
            cutoff = target_date - timedelta(days=n)
            return sum(1 for g in played if et_date(g["starts_at"]) >= cutoff)

        games_in3 = count_in(3)
        games_in5 = count_in(5)

        cutoff4 = target_date - timedelta(days=4)
        recent_altitude = any(
            et_date(g["starts_at"]) >= cutoff4
            and g["home_abbr"] in ALTITUDE_ARENAS
            for g in played
        )

        return {
            "days_rest":       days_rest,
            "prev_arena":      home_abbr,
            "was_home_last":   was_home,
            "games_in4":       games_in3 + 1,
            "games_in6":       games_in5 + 1,
            "prev_tip_hr":     prev_tip_hr,
            "prev_late":       prev_late,
            "recent_altitude": recent_altitude,
        }

    # ── 4. Load existing results ───────────────────────────────────
    results_path = os.path.join(os.path.dirname(__file__), "..", "data", "results.json")
    results_path = os.path.normpath(results_path)
    try:
        with open(results_path) as f:
            existing = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        existing = {"games": [], "meta": {"last_updated": "", "total_flagged": 0}}

    existing_ids = {g["event_id"] for g in existing["games"]}
    new_games = []

    # ── 5. Process each game ───────────────────────────────────────
    for game in games:
        eid = str(game["id"])
        if eid in existing_ids:
            print(f"  Skip {eid} (already logged)")
            continue

        home = game["home_team"]["abbreviation"]
        away = game["visitor_team"]["abbreviation"]

        home_score = float(game.get("home_team_score") or 0)
        away_score = float(game.get("visitor_team_score") or 0)

        if not home_score and not away_score:
            print(f"  Skip {away} @ {home}: no final score from BDL")
            continue

        # Fatigue scores
        hr = calc_rest(home, yesterday)
        ar = calc_rest(away, yesterday)

        home_f = analyze_fatigue(home, True,  hr["days_rest"], hr["prev_arena"], home,
                                 hr["was_home_last"], hr["games_in4"], hr["games_in6"],
                                 hr["prev_tip_hr"], hr["prev_late"], hr["recent_altitude"], yesterday)
        away_f = analyze_fatigue(away, False, ar["days_rest"], ar["prev_arena"], home,
                                 ar["was_home_last"], ar["games_in4"], ar["games_in6"],
                                 ar["prev_tip_hr"], ar["prev_late"], ar["recent_altitude"], yesterday)

        away_fat = round(away_f["score"], 1)
        home_fat = round(home_f["score"], 1)
        diff     = round(away_fat - home_fat, 1)

        # Only log if a v2.0 betting signal fires
        signals = get_betting_signals(away_f, home_f)
        if not signals:
            print(f"  Skip {away} @ {home}...")
            continue

        signal_types = [s["type"] for s in signals]
        has_spread  = "spread" in signal_types
        has_under   = "under"  in signal_types
        signal_type = "both" if (has_spread and has_under) else signal_types[0]

        if diff > 0:
            edge = "HOME"; flagged_team = away
        elif diff < 0:
            edge = "AWAY"; flagged_team = home
        else:
            edge = "EVEN"; flagged_team = ""

        both_tired = away_fat >= 5 and home_fat >= 5

        # Closing lines from Odds API
        ln           = lines.get((away, home), {})
        close_spread = ln.get("close_spread")
        close_total  = ln.get("close_total")

        if close_spread is None:
            print(f"  Warning: no closing line from Odds API for {away} @ {home}")

        # ATS grading
        ats_result = None
        if close_spread is not None:
            net = (home_score - away_score) + close_spread
            ats_result = "push" if net == 0 else ("home" if net > 0 else "away")

        # O/U grading
        ou_result = None
        if close_total is not None:
            total_final = home_score + away_score
            if total_final > close_total:   ou_result = "over"
            elif total_final < close_total: ou_result = "under"
            else:                           ou_result = "push"

        # Spread bet result: bet is on AWAY team (fatigued road team + points)
        edge_ats = None
        if has_spread and ats_result:
            edge_ats = "WIN" if ats_result == "away" else (
                "PUSH" if ats_result == "push" else "LOSS")

        under_result = None
        if has_under and ou_result:
            under_result = "WIN" if ou_result == "under" else (
                "PUSH" if ou_result == "push" else "LOSS")

        rec = {
            "event_id":      eid,
            "date":          yesterday,
            "starts_at":     yesterday + "T20:00:00Z",  # BDL has no tip time
            "matchup":       f"{away} @ {home}",
            "away":          away,
            "home":          home,
            "away_score":    int(away_score),
            "home_score":    int(home_score),
            "away_fatigue":  away_fat,
            "home_fatigue":  home_fat,
            "away_scenario": away_f.get("scenario"),
            "home_scenario": home_f.get("scenario"),
            "away_detail":   away_f.get("detail",""),
            "home_detail":   home_f.get("detail",""),
            "fatigue_diff":  abs(diff),
            "edge":          edge,
            "flagged_team":  flagged_team,
            "both_tired":    both_tired,
            "signal_type":   signal_type,
            "close_spread":  close_spread,
            "close_total":   close_total,
            "ats_result":    ats_result,
            "ou_result":     ou_result,
            "edge_ats":      edge_ats,
            "under_result":  under_result,
        }

        new_games.append(rec)
        print(f"  LOGGED: {away} @ {home} | signal={signal_type} | away={away_fat} home={home_fat} | ATS={edge_ats} OU={under_result}")

    if new_games:
        existing["games"].extend(new_games)
        existing["games"].sort(key=lambda x: x["date"])
        existing["meta"]["last_updated"]  = now_et.strftime("%Y-%m-%d %H:%M ET")
        existing["meta"]["total_flagged"] = len(existing["games"])

        os.makedirs(os.path.dirname(results_path), exist_ok=True)
        with open(results_path, "w") as f:
            json.dump(existing, f, indent=2)
        print(f"\nWrote {len(new_games)} new games to results.json ({existing['meta']['total_flagged']} total)")
    else:
        print("\nNo new flagged games to log today")

if __name__ == "__main__":
    main()

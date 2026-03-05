#!/usr/bin/env python3
"""
NBA Edge Model — Nightly Results Logger
Runs via GitHub Actions at 9am ET daily.
Pulls previous day's finalized NBA games from SGO,
computes ATS/OU outcomes for fatigue-flagged games,
appends to data/results.json.
"""

import os
import json
import requests
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

SGO_KEY = os.environ["SGO_API_KEY"]
BASE    = "https://api.sportsgameodds.com/v2"
HEADERS = {"x-api-key": SGO_KEY}

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

# Map SGO teamID suffixes to our abbreviations
SGO_TEAM_MAP = {
    "ATLANTA_HAWKS_NBA": "ATL", "BOSTON_CELTICS_NBA": "BOS",
    "BROOKLYN_NETS_NBA": "BKN", "CHARLOTTE_HORNETS_NBA": "CHA",
    "CHICAGO_BULLS_NBA": "CHI", "CLEVELAND_CAVALIERS_NBA": "CLE",
    "DALLAS_MAVERICKS_NBA": "DAL", "DENVER_NUGGETS_NBA": "DEN",
    "DETROIT_PISTONS_NBA": "DET", "GOLDEN_STATE_WARRIORS_NBA": "GSW",
    "HOUSTON_ROCKETS_NBA": "HOU", "INDIANA_PACERS_NBA": "IND",
    "LOS_ANGELES_CLIPPERS_NBA": "LAC", "LOS_ANGELES_LAKERS_NBA": "LAL",
    "MEMPHIS_GRIZZLIES_NBA": "MEM", "MIAMI_HEAT_NBA": "MIA",
    "MILWAUKEE_BUCKS_NBA": "MIL", "MINNESOTA_TIMBERWOLVES_NBA": "MIN",
    "NEW_ORLEANS_PELICANS_NBA": "NOP", "NEW_YORK_KNICKS_NBA": "NYK",
    "OKLAHOMA_CITY_THUNDER_NBA": "OKC", "ORLANDO_MAGIC_NBA": "ORL",
    "PHILADELPHIA_76ERS_NBA": "PHI", "PHOENIX_SUNS_NBA": "PHX",
    "PORTLAND_TRAIL_BLAZERS_NBA": "POR", "SACRAMENTO_KINGS_NBA": "SAC",
    "SAN_ANTONIO_SPURS_NBA": "SAS", "TORONTO_RAPTORS_NBA": "TOR",
    "UTAH_JAZZ_NBA": "UTA", "WASHINGTON_WIZARDS_NBA": "WAS",
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
        if   effective_sleep < 4: sleep_mod = 4
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

# ── SGO FETCH HELPERS ─────────────────────────────────────────────

def sgo_get(endpoint, params):
    r = requests.get(f"{BASE}{endpoint}", headers=HEADERS, params=params, timeout=20)
    r.raise_for_status()
    return r.json()

def fetch_all_events(params):
    events = []
    cursor = None
    while True:
        p = {**params, "limit": 50}
        if cursor:
            p["cursor"] = cursor
        data = sgo_get("/events/", p)
        events.extend(data.get("data", []))
        cursor = data.get("nextCursor")
        if not cursor:
            break
    return events

# ── TEAM ABBR FROM SGO EVENT ──────────────────────────────────────

def abbr(team_obj):
    tid = team_obj.get("teamID", "")
    if tid in SGO_TEAM_MAP:
        return SGO_TEAM_MAP[tid]
    # fallback: use short name
    return team_obj.get("names", {}).get("short", tid[:3].upper())

# ── OUTCOME COMPUTATION ───────────────────────────────────────────

def compute_outcomes(event):
    """
    SGO puts closeSpread, closeOverUnder, and score directly on the odd object
    (top level, not nested in byBookmaker). Per the handling-odds guide:
      oddObject.score         = final stat value for that market
      oddObject.closeOverUnder = closing line for OU markets
      oddObject.closeSpread    = closing line for spread markets
    We use the consensus/book values. byBookmaker is NOT needed for grading.
    """
    odds = event.get("odds", {})

    # --- Spread (home perspective) ---
    sp_odd = odds.get("points-home-game-sp-home")
    close_spread = None
    if sp_odd:
        for field in ("closeSpread", "closeBookSpread", "bookSpread"):
            v = sp_odd.get(field)
            if v is not None:
                close_spread = float(v)
                break

    # --- Total: use the over odd; its score = total points scored ---
    ou_odd = odds.get("points-all-game-ou-over")
    close_total = None
    total_scored = None
    if ou_odd:
        for field in ("closeOverUnder", "closeBookOverUnder", "bookOverUnder"):
            v = ou_odd.get(field)
            if v is not None:
                close_total = float(v)
                break
        s = ou_odd.get("score")
        if s is not None:
            total_scored = float(s)

    # --- Spread score: use home spread odd's score (= home_pts - away_pts + spread) ---
    # Actually SGO score on a spread odd = the raw stat (home points for home-sp-home).
    # We need home margin, which we already have from final scores passed in separately.
    # So ATS grading uses close_spread + final scores (computed below in caller).

    # O/U result — if SGO gave us total_scored via score field, use it
    ou_result = None
    if close_total is not None and total_scored is not None:
        if total_scored > close_total:   ou_result = "over"
        elif total_scored < close_total: ou_result = "under"
        else:                            ou_result = "push"

    return {
        "close_spread":  close_spread,
        "close_total":   close_total,
        "total_scored":  total_scored,
        "ou_result":     ou_result,
    }

# ── MAIN ──────────────────────────────────────────────────────────

def main():
    # Yesterday in ET
    now_et    = datetime.now(ET)
    yesterday = (now_et - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"Processing games for {yesterday}")

    # Fetch finalized NBA events from yesterday.
    # Use explicit UTC datetime strings so West Coast games
    # (which tip ~10:30pm ET = 3:30am UTC next day) are never missed.
    yesterday_start_utc = (
        datetime(*map(int, yesterday.split("-")), 5, 0, 0, tzinfo=timezone.utc)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    today_end_utc = (
        datetime(*map(int, yesterday.split("-")), 5, 0, 0, tzinfo=timezone.utc)
        + timedelta(days=1)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    events = fetch_all_events({
        "leagueID":      "NBA",
        "finalized":     "true",
        "startsAfter":   yesterday_start_utc,
        "startsBefore":  today_end_utc,
        "oddID":         "points-home-game-sp-home,points-all-game-ou-over",
        "expandResults": "true",
    })

    if not events:
        print(f"No finalized NBA games found for {yesterday}")
        return

    print(f"Found {len(events)} finalized games")

    # Fetch last 21 days of games for fatigue rest calculation.
    # No odds needed for history — just game results for schedule/rest data.
    # Use UTC datetime strings (same as main fetch) so late West Coast games
    # aren't dropped from team history due to UTC date boundary.
    three_weeks_ago_utc = (
        datetime(*map(int, yesterday.split("-")), 5, 0, 0, tzinfo=timezone.utc)
        - timedelta(days=21)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    yesterday_end_utc = yesterday_start_utc  # stop before yesterday's games
    history_events = fetch_all_events({
        "leagueID":     "NBA",
        "finalized":    "true",
        "startsAfter":  three_weeks_ago_utc,
        "startsBefore": yesterday_end_utc,
    })
    print(f"Fetched {len(history_events)} history games for fatigue calc")

    # Build a simple game history per team for rest calculation
    # keyed by teamID → sorted list of {date, home_teamID, away_teamID, starts_at, home_score, away_score}
    team_history = {}
    for e in history_events:
        home_tid = e["teams"]["home"]["teamID"]
        away_tid = e["teams"]["away"]["teamID"]
        starts   = e["status"].get("startsAt","")
        results  = e.get("results", {})
        h_pts    = results.get("home",{}).get("score") or results.get("points-home-game",{}).get("score")
        a_pts    = results.get("away",{}).get("score") or results.get("points-away-game",{}).get("score")
        rec = {"starts_at": starts, "home_teamID": home_tid, "away_teamID": away_tid,
               "home_score": h_pts, "away_score": a_pts}
        for tid in [home_tid, away_tid]:
            team_history.setdefault(tid, []).append(rec)

    for tid in team_history:
        team_history[tid].sort(key=lambda x: x["starts_at"])

    def calc_rest(team_id, target_date_str):
        games = team_history.get(team_id, [])
        if not games:
            return {"days_rest": None, "prev_arena": None, "was_home_last": None,
                    "games_in4": 1, "games_in6": 1, "prev_tip_hr": 19.5,
                    "prev_late": False, "recent_altitude": False}

        # Convert starts_at to ET calendar date for each game.
        # Using UTC timestamps to compare calendar dates causes BTB misclassification:
        # a 10:30pm ET tip = 3:30am UTC next day, so comparing against UTC midnight
        # puts it on the wrong calendar date.
        def et_date(starts_at_str):
            dt = datetime.fromisoformat(starts_at_str.replace("Z", "+00:00"))
            return dt.astimezone(ET).date()

        target_date = datetime.strptime(target_date_str, "%Y-%m-%d").date()

        # Only include games played before target date (ET calendar)
        played = [g for g in games if et_date(g["starts_at"]) < target_date]
        if not played:
            return {"days_rest": None, "prev_arena": None, "was_home_last": None,
                    "games_in4": 1, "games_in6": 1, "prev_tip_hr": 19.5,
                    "prev_late": False, "recent_altitude": False}

        last = played[-1]
        last_et_date = et_date(last["starts_at"])
        days_rest = (target_date - last_et_date).days - 1  # 0 = BTB, 1 = one day off, etc.
        days_rest = max(0, days_rest)

        was_home = last["home_teamID"] == team_id
        home_abbr = SGO_TEAM_MAP.get(last["home_teamID"], "")

        # prev tip local hr — use ET conversion of the actual UTC game time
        last_dt = datetime.fromisoformat(last["starts_at"].replace("Z", "+00:00"))
        last_local = last_dt.astimezone(ZoneInfo(ARENAS.get(home_abbr, {}).get("tz_name", "America/New_York")))
        prev_tip_hr = last_local.hour + last_local.minute / 60
        if prev_tip_hr < 12:
            prev_tip_hr += 24  # push into 0-48 scale (handles midnight crossover)
        prev_late = prev_tip_hr >= 21.5

        # Density: count games in last N calendar days (ET) before target
        def count_in(n):
            cutoff = target_date - timedelta(days=n)
            return sum(1 for g in played if et_date(g["starts_at"]) >= cutoff)

        games_in3 = count_in(3)
        games_in5 = count_in(5)

        # Altitude visit: did this team play in DEN or UTA in the last 4 calendar days?
        cutoff4 = target_date - timedelta(days=4)
        recent_altitude = any(
            et_date(g["starts_at"]) >= cutoff4
            and SGO_TEAM_MAP.get(g["home_teamID"], "") in ALTITUDE_ARENAS
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

    # Load existing results
    results_path = os.path.join(os.path.dirname(__file__), "..", "data", "results.json")
    results_path = os.path.normpath(results_path)
    try:
        with open(results_path) as f:
            existing = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        existing = {"games": [], "meta": {"last_updated": "", "total_flagged": 0}}

    existing_ids = {g["event_id"] for g in existing["games"]}
    new_games = []

    for event in events:
        eid = event["eventID"]
        if eid in existing_ids:
            print(f"  Skip {eid} (already logged)")
            continue

        home_obj = event["teams"]["home"]
        away_obj = event["teams"]["away"]
        home_tid = home_obj["teamID"]
        away_tid = away_obj["teamID"]
        home     = abbr(home_obj)
        away     = abbr(away_obj)

        # Final scores: SGO puts score on the odd object itself (per handling-odds guide).
        # points-all-game-ou-over.score = total points scored (both teams combined).
        # points-home-game-sp-home.score = home team points.
        # Derive away score = total - home.
        odds_raw = event.get("odds", {})
        ou_odd   = odds_raw.get("points-all-game-ou-over", {})
        sp_odd   = odds_raw.get("points-home-game-sp-home", {})

        total_pts = ou_odd.get("score")
        home_pts  = sp_odd.get("score")

        # Fallback: try event.results if odd scores aren't populated
        if total_pts is None or home_pts is None:
            results_raw = event.get("results", {})
            if home_pts is None:
                home_pts = (results_raw.get("home", {}) or {}).get("score")
            if total_pts is None:
                away_pts_r = (results_raw.get("away", {}) or {}).get("score")
                if home_pts is not None and away_pts_r is not None:
                    total_pts = float(home_pts) + float(away_pts_r)

        if total_pts is None or home_pts is None:
            print(f"  Skip {away} @ {home}: no final score in odds or results")
            continue

        home_score = float(home_pts)
        away_score = float(total_pts) - home_score

        # Fatigue scores
        hr = calc_rest(home_tid, yesterday)
        ar = calc_rest(away_tid, yesterday)

        home_f = analyze_fatigue(home, True,  hr["days_rest"], hr["prev_arena"], home,
                                 hr["was_home_last"], hr["games_in4"], hr["games_in6"],
                                 hr["prev_tip_hr"], hr["prev_late"], hr["recent_altitude"], yesterday)
        away_f = analyze_fatigue(away, False, ar["days_rest"], ar["prev_arena"], home,
                                 ar["was_home_last"], ar["games_in4"], ar["games_in6"],
                                 ar["prev_tip_hr"], ar["prev_late"], ar["recent_altitude"], yesterday)

        away_fat = round(away_f["score"], 1)
        home_fat = round(home_f["score"], 1)
        max_fat  = max(away_fat, home_fat)
        diff     = round(away_fat - home_fat, 1)  # positive = away more fatigued = home edge

        # Only log if model flagged it (max score >= 5, diff >= 2)
        FLAG_THRESHOLD = 5
        DIFF_THRESHOLD = 2
        both_tired_check = away_fat >= FLAG_THRESHOLD and home_fat >= FLAG_THRESHOLD
        if max_fat < FLAG_THRESHOLD or (abs(diff) < DIFF_THRESHOLD and not both_tired_check):
            print(f"  Skip {away} @ {home}...")
            continue

        # Edge direction
        if diff > 0:
            edge = "HOME"
            flagged_team = away
        elif diff < 0:
            edge = "AWAY"
            flagged_team = home
        else:
            edge = "EVEN"
            flagged_team = ""

        # Both tired under flag
        both_tired = away_fat >= FLAG_THRESHOLD and home_fat >= FLAG_THRESHOLD

        # Outcomes — compute_outcomes reads closeSpread/closeOverUnder/score
        # from the odd object top level (not byBookmaker).
        outcomes = compute_outcomes(event)

        # ATS: home covers if (home_score - away_score) + close_spread > 0
        ats_result = None
        close_spread = outcomes["close_spread"]
        if close_spread is not None:
            margin = home_score - away_score
            net = margin + close_spread
            ats_result = "push" if net == 0 else ("home" if net > 0 else "away")

        # O/U: use score from odd if available, else compute from final scores
        ou_result = outcomes["ou_result"]
        close_total = outcomes["close_total"]
        if ou_result is None and close_total is not None:
            total_final = home_score + away_score
            if total_final > close_total:   ou_result = "over"
            elif total_final < close_total: ou_result = "under"
            else:                           ou_result = "push"

        # Edge ATS result
        edge_ats = None
        if ats_result and edge in ("HOME","AWAY"):
            edge_ats = "WIN" if ats_result == edge.lower() else (
                       "PUSH" if ats_result == "push" else "LOSS")

        under_result = None
        if both_tired and ou_result:
            under_result = "WIN" if ou_result == "under" else ("PUSH" if ou_result == "push" else "LOSS")

        starts_at = event["status"].get("startsAt","")

        rec = {
            "event_id":      eid,
            "date":          yesterday,
            "starts_at":     starts_at,
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
            "close_spread":  close_spread,
            "close_total":   close_total,
            "ats_result":    ats_result,
            "ou_result":     ou_result,
            "edge_ats":      edge_ats,
            "under_result":  under_result,
        }

        new_games.append(rec)
        print(f"  LOGGED: {away} @ {home} | away={away_fat} home={home_fat} edge={edge} | ATS={edge_ats} OU={ou_result}")

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

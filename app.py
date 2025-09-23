
import streamlit as st
import sqlite3
import pandas as pd
import random
from datetime import date, datetime, timedelta
from typing import List, Tuple, Optional, Dict

DB_PATH = "data/sondagsholdet.db"

# ---------------- DB ----------------
def conn():
    c = sqlite3.connect(DB_PATH)
    c.execute("PRAGMA foreign_keys = ON;")
    return c

def init_db():
    c = conn(); cur = c.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS players(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT UNIQUE NOT NULL
    );
    CREATE TABLE IF NOT EXISTS sessions(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      session_date TEXT NOT NULL UNIQUE
    );
    CREATE TABLE IF NOT EXISTS attendance(
      session_id INTEGER NOT NULL,
      player_id INTEGER NOT NULL,
      PRIMARY KEY(session_id, player_id),
      FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE,
      FOREIGN KEY(player_id) REFERENCES players(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS matches(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      session_id INTEGER NOT NULL,
      is_doubles INTEGER NOT NULL,
      side1_p1 INTEGER NOT NULL,
      side1_p2 INTEGER,
      side2_p1 INTEGER NOT NULL,
      side2_p2 INTEGER,
      winning_side INTEGER NOT NULL,
      score1 INTEGER,
      score2 INTEGER,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
    );
    """)
    c.commit(); c.close()

def get_or_create_session(d: date) -> int:
    c = conn(); cur = c.cursor()
    ds = d.isoformat()
    cur.execute("INSERT OR IGNORE INTO sessions(session_date) VALUES (?);", (ds,))
    c.commit()
    cur.execute("SELECT id FROM sessions WHERE session_date=?;", (ds,))
    sid = cur.fetchone()[0]
    c.close()
    return sid

def list_players() -> List[Tuple[int,str]]:
    c = conn(); cur = c.cursor()
    cur.execute("SELECT id,name FROM players ORDER BY name COLLATE NOCASE;")
    rows = cur.fetchall(); c.close(); return rows

def add_player(name: str) -> Optional[int]:
    name = (name or '').strip()
    if not name: return None
    c = conn(); cur = c.cursor()
    cur.execute("INSERT OR IGNORE INTO players(name) VALUES (?);", (name,))
    c.commit()
    cur.execute("SELECT id FROM players WHERE name=?;", (name,))
    row = cur.fetchone(); c.close()
    return row[0] if row else None

def record_attendance(session_id: int, player_ids: List[int]):
    c = conn(); cur = c.cursor()
    cur.execute("DELETE FROM attendance WHERE session_id=?;", (session_id,))
    for pid in player_ids:
        cur.execute("INSERT OR IGNORE INTO attendance(session_id, player_id) VALUES (?,?);", (session_id, pid))
    c.commit(); c.close()

def list_attendance(session_id: int) -> List[int]:
    c = conn(); cur = c.cursor()
    cur.execute("SELECT player_id FROM attendance WHERE session_id=?;", (session_id,))
    rows = [r[0] for r in cur.fetchall()]
    c.close(); return rows

def delete_session_data(session_id: int):
    """Delete matches + attendance + the session row (safe cleanup of 'dagens data')."""
    c = conn(); cur = c.cursor()
    cur.execute("DELETE FROM matches WHERE session_id=?;", (session_id,))
    cur.execute("DELETE FROM attendance WHERE session_id=?;", (session_id,))
    cur.execute("DELETE FROM sessions WHERE id=?;", (session_id,))
    c.commit(); c.close()

def reset_all():
    """Drop everything (hard reset)."""
    c = conn(); cur = c.cursor()
    cur.executescript("""
    DROP TABLE IF EXISTS matches;
    DROP TABLE IF EXISTS attendance;
    DROP TABLE IF EXISTS sessions;
    DROP TABLE IF EXISTS players;
    """)
    c.commit(); c.close()
    init_db()

def normalize_side_tuple(is_doubles: int, side: Tuple[int, Optional[int]]) -> Tuple[int, Optional[int]]:
    """Return side as sorted (p1<=p2) tuple for robust comparison (order independent)."""
    p = [side[0]] + ([side[1]] if side[1] else [])
    p = sorted([x for x in p if x is not None])
    if is_doubles:
        return (p[0], p[1])
    else:
        return (p[0], None)

def match_exists(session_id: int, is_doubles: int, side1: Tuple[int, Optional[int]], side2: Tuple[int, Optional[int]], score1: int, score2: int) -> bool:
    """
    Prevent saving same match repeatedly.
    Rule: within the same session, if there already exists a match with the SAME unordered participants and the SAME score (either 11-7 or 7-11 etc.), it's a duplicate.
    """
    s1 = normalize_side_tuple(is_doubles, side1)
    s2 = normalize_side_tuple(is_doubles, side2)
    # create canonical order: smaller first by tuple
    if s2 < s1:
        s1, s2 = s2, s1
        score1, score2 = score2, score1  # align score with sides flip
    c = conn(); c.row_factory = sqlite3.Row; cur = c.cursor()
    cur.execute("""
        SELECT side1_p1, side1_p2, side2_p1, side2_p2, score1, score2
        FROM matches
        WHERE session_id=? AND is_doubles=?
    """, (session_id, is_doubles))
    rows = cur.fetchall()
    c.close()
    for r in rows:
        a = normalize_side_tuple(is_doubles, (r["side1_p1"], r["side1_p2"]))
        b = normalize_side_tuple(is_doubles, (r["side2_p1"], r["side2_p2"]))
        ra, rb = (a, b) if a <= b else (b, a)
        # flip score accordingly
        rsc1, rsc2 = (r["score1"], r["score2"]) if (a, b)==(ra, rb) else (r["score2"], r["score1"])
        if ra == s1 and rb == s2 and rsc1 == score1 and rsc2 == score2:
            return True
    return False

def save_match(session_id: int, is_doubles: int, side1: Tuple[int, Optional[int]], side2: Tuple[int, Optional[int]], winner: int, score1: int, score2: int) -> bool:
    """Returns True if saved, False if rejected by duplicate guard."""
    if match_exists(session_id, is_doubles, side1, side2, score1, score2):
        return False
    c = conn(); cur = c.cursor()
    cur.execute("""
        INSERT INTO matches(session_id, is_doubles, side1_p1, side1_p2, side2_p1, side2_p2, winning_side, score1, score2)
        VALUES (?,?,?,?,?,?,?,?,?);
    """, (session_id, is_doubles, side1[0], side1[1], side2[0], side2[1], winner, score1, score2))
    c.commit(); c.close()
    return True

# ---------------- Stats ----------------
def compute_standings(year: int) -> pd.DataFrame:
    c = conn(); cur = c.cursor()
    cur.execute("SELECT id,name FROM players;")
    players = dict(cur.fetchall())
    if not players:
        c.close(); return pd.DataFrame()
    cur.execute("SELECT id FROM sessions WHERE strftime('%Y', session_date)=?;", (str(year),))
    sids = [r[0] for r in cur.fetchall()]
    if not sids:
        c.close(); return pd.DataFrame()
    sid_tuple = "(" + ",".join("?"*len(sids)) + ")"
    cur.execute(f"SELECT player_id, COUNT(*) FROM attendance WHERE session_id IN {sid_tuple} GROUP BY player_id;", sids)
    attendance = dict(cur.fetchall())
    cur.execute(f"SELECT is_doubles, side1_p1, side1_p2, side2_p1, side2_p2, winning_side FROM matches WHERE session_id IN {sid_tuple};", sids)
    rows = cur.fetchall()
    wins = {pid:0 for pid in players}; losses = {pid:0 for pid in players}; played = {pid:0 for pid in players}
    for is_d, s1p1, s1p2, s2p1, s2p2, wside in rows:
        s1 = [s1p1] + ([s1p2] if s1p2 else [])
        s2 = [s2p1] + ([s2p2] if s2p2 else [])
        for p in s1+s2: played[p]=played.get(p,0)+1
        winners = s1 if wside==1 else s2
        losers  = s2 if wside==1 else s1
        for p in winners: wins[p]=wins.get(p,0)+1
        for p in losers: losses[p]=losses.get(p,0)+1
    data = []
    for pid,name in players.items():
        att = attendance.get(pid,0); mp=played.get(pid,0); w=wins.get(pid,0); l=losses.get(pid,0)
        total = att*1 + w*3
        winpct = round((w/mp)*100,1) if mp>0 else 0.0
        data.append([name, att, mp, w, l, winpct, total, pid])
    df = pd.DataFrame(data, columns=["Spiller","Fremmøder","Kampe","Sejre","Nederlag","Sejr-%","Point i alt","pid"])
    df = df.sort_values(["Point i alt","Sejre","Spiller"], ascending=[False,False,True]).reset_index(drop=True)
    c.close(); return df

def compute_rivals(year: int) -> Dict[int, Dict]:
    c = conn(); cur = c.cursor()
    cur.execute("SELECT id,name FROM players;"); players = dict(cur.fetchall())
    result = {pid: None for pid in players}
    if not players: c.close(); return result
    cur.execute("SELECT id FROM sessions WHERE strftime('%Y', session_date)=?;", (str(year),))
    sids=[r[0] for r in cur.fetchall()]
    if not sids: c.close(); return result
    sid_tuple = "(" + ",".join("?"*len(sids)) + ")"
    cur.execute(f"SELECT is_doubles, side1_p1, side1_p2, side2_p1, side2_p2, winning_side FROM matches WHERE session_id IN {sid_tuple};", sids)
    rows = cur.fetchall()
    h2h = {pid:{} for pid in players}
    for is_d, a1,a2,b1,b2,wside in rows:
        s1 = [a1] + ([a2] if a2 else [])
        s2 = [b1] + ([b2] if b2 else [])
        winners = set(s1 if wside==1 else s2)
        for A in s1:
            for B in s2:
                h2h.setdefault(A, {}).setdefault(B, [0,0])
                if A in winners: h2h[A][B][0]+=1
                else: h2h[A][B][1]+=1
        for A in s2:
            for B in s1:
                h2h.setdefault(A, {}).setdefault(B, [0,0])
                if A in winners: h2h[A][B][0]+=1
                else: h2h[A][B][1]+=1
    for A in players:
        candidates = []
        for B,(w,l) in h2h.get(A,{}).items():
            tot = w+l
            if tot>=3:
                wr = w/tot
                score = abs(wr-0.5)
                candidates.append((score, -tot, B, w, l, wr))
        if not candidates: continue
        candidates.sort()
        score, neg, B, w, l, wr = candidates[0]
        result[A] = {"opponent_id": B, "w": w, "l": l, "meetings": -neg, "winpct": round(wr*100,1)}
    c.close(); return result

# ---------------- Round generator ----------------
def make_round_matches(att_ids: List[int], courts: int, mix_mode: str) -> List[Dict]:
    """Maximize players on court per runde by choosing d doubles and s singles within available courts."""
    if not att_ids or courts <= 0: return []
    ids = att_ids[:]

    # order: Snake uses name order; Random shuffles
    c = conn(); cur = c.cursor()
    q = "SELECT id,name FROM players WHERE id IN (%s)" % ",".join("?"*len(ids))
    cur.execute(q, ids)
    names = {pid: n for pid,n in cur.fetchall()}
    c.close()
    if mix_mode == "Snake (balanceret)":
        ids.sort(key=lambda p: names.get(p,"").lower())
    else:
        random.shuffle(ids)

    N = len(ids)
    best = None  # (used_players, doubles, singles)
    max_doubles = min(N // 4, courts)
    for d in range(max_doubles, -1, -1):
        rem_players = N - 4*d
        rem_courts = courts - d
        s = min(rem_players // 2, rem_courts)
        used = 4*d + 2*s
        cand = (used, d, s)
        if best is None or used > best[0] or (used == best[0] and d > best[1]):
            best = cand
    used, d, s = best

    doubles_players = ids[:4*d]
    singles_players = ids[4*d:4*d+2*s]

    matches = []
    # build doubles matches (pair adjacent into teams, then teams into matches)
    teams = []
    for i in range(0, len(doubles_players), 2):
        teams.append( (doubles_players[i], doubles_players[i+1]) )
    for j in range(0, len(teams), 2):
        if j+1 < len(teams):
            t1, t2 = teams[j], teams[j+1]
            matches.append({"is_doubles":1, "side1":(t1[0], t1[1]), "side2":(t2[0], t2[1])})

    # build singles matches
    for k in range(0, len(singles_players), 2):
        p1 = singles_players[k]; p2 = singles_players[k+1]
        matches.append({"is_doubles":0, "side1":(p1, None), "side2":(p2, None)})

    return matches

# ---------------- UI ----------------
st.set_page_config(page_title="Søndagsholdet F/S", layout="wide")
st.title("Søndagsholdet F/S")

import os
os.makedirs("data", exist_ok=True)
init_db()

with st.sidebar:
    st.header("Indstillinger")
    # Add player
    with st.form("add_player_form", clear_on_submit=True):
        nm = st.text_input("Tilføj spiller")
        submitted = st.form_submit_button("Gem spiller")
        if submitted and nm.strip():
            pid = add_player(nm)
            if pid: st.success(f"Tilføjet: {nm}")
            else: st.warning("Kunne ikke tilføje.")
    # Backups
    if os.path.exists(DB_PATH):
        with open(DB_PATH, "rb") as f:
            st.download_button("Download database (.db)", data=f.read(), file_name="sondagsholdet.db")
    # CSV export of matches
    c = conn()
    try:
        df_all = pd.read_sql_query("SELECT * FROM matches", c)
    except Exception:
        df_all = pd.DataFrame()
    finally:
        c.close()
    if not df_all.empty:
        st.download_button("Download alle kampe (CSV)", data=df_all.to_csv(index=False).encode("utf-8"), file_name="kampe.csv", mime="text/csv")
    uploaded = st.file_uploader("Upload database (.db)", type=["db"])
    if uploaded is not None:
        with open(DB_PATH, "wb") as f:
            f.write(uploaded.getbuffer())
        st.success("Database gendannet. Genindlæs siden for at se ændringer.")
    # Dangerous ops
    with st.expander("Ryd data"):
        st.caption("Brug disse knapper med omtanke i testfasen.")
        if st.button("Ryd dagens data (matches + fremmøde for valgt dato)"):
            # We'll delete the session below after we know which date is chosen.
            st.session_state["_delete_today_requested"] = True
        if st.button("Ryd ALT (drop hele databasen)"):
            reset_all()
            st.success("Alt er ryddet.")

# Session & attendance
col1, col2 = st.columns([1,1])
with col1:
    sel_date = st.date_input("Dato", value=date.today())
    # If user asked to delete today's data:
    if st.session_state.get("_delete_today_requested"):
        # Only run once per click
        st.session_state["_delete_today_requested"] = False
        # Try to find session by date; if doesn't exist, just ignore
        c = conn(); cur = c.cursor()
        cur.execute("SELECT id FROM sessions WHERE session_date=?;", (sel_date.isoformat(),))
        row = cur.fetchone()
        if row:
            delete_session_data(row[0])
            st.success("Dagens data ryddet.")
        else:
            st.info("Der var ingen data for den valgte dato.")
    session_id = get_or_create_session(sel_date)
with col2:
    mix_mode = st.radio("Mixing", ["Random","Snake (balanceret)"], horizontal=True)

players = list_players()
pid2name = {pid:name for pid,name in players}

st.subheader("Fremmøde")
picked = st.multiselect("Vælg spillere", [name for _,name in players], default=[pid2name.get(pid) for pid in list_attendance(session_id)])
picked_ids = [pid for pid,name in players if name in picked]
if st.button("Gem fremmøde"):
    record_attendance(session_id, picked_ids)
    st.success("Fremmøde gemt.")

# Round generator
st.subheader("Start spil")
courts = st.number_input("Antal baner", min_value=1, max_value=6, value=2, step=1)
att_ids = list_attendance(session_id)

if st.button("Start runde"):
    if len(att_ids) < 2:
        st.warning("For få spillere.")
    else:
        matches = make_round_matches(att_ids, int(courts), mix_mode)
        if not matches:
            st.warning("Kunne ikke planlægge kampe til denne runde.")
        else:
            st.session_state["matches"] = matches
            st.session_state["current_session"] = session_id
            st.success(f"Runde startet med {len(matches)} kampe.")

# Active matches table
if "matches" in st.session_state and st.session_state.get("current_session")==session_id:
    st.subheader("Aktive kampe")
    for idx, m in enumerate(st.session_state["matches"], start=1):
        s1 = m["side1"]; s2 = m["side2"]
        is_d = m["is_doubles"]
        s1_names = " & ".join([pid2name.get(s1[0],"?")] + ([pid2name.get(s1[1],"?")] if s1[1] else []))
        s2_names = " & ".join([pid2name.get(s2[0],"?")] + ([pid2name.get(s2[1],"?")] if s2[1] else []))
        c1, c2, c3 = st.columns([3,3,2])
        with c1:
            st.write(f"{s1_names}  vs  {s2_names}  ({'Doubles' if is_d else 'Singles'})")
        with c2:
            sc1 = st.number_input(f"Score {s1_names}", min_value=0, max_value=11, value=11, step=1, key=f"sc1_{idx}")
            sc2 = st.number_input(f"Score {s2_names}", min_value=0, max_value=11, value=7, step=1, key=f"sc2_{idx}")
        with c3:
            if st.button("Gem resultat", key=f"save_{idx}"):
                if sc1 == sc2:
                    st.warning("Ingen uafgjort. Justér score.")
                else:
                    winner = 1 if sc1 > sc2 else 2
                    ok = save_match(session_id, is_d, s1, s2, winner, int(sc1), int(sc2))
                    if ok:
                        st.success("Kamp gemt.")
                    else:
                        st.info("Den kamp er allerede gemt (samme spillere og score i dag).")

# Archive
st.subheader("Kamp-arkiv")
year_choice = st.number_input("År", min_value=2000, max_value=2100, value=date.today().year, step=1)
c = conn(); c.row_factory = sqlite3.Row; cur = c.cursor()
cur.execute("SELECT id FROM sessions WHERE strftime('%Y', session_date)=?;", (str(year_choice),))
sids = [r[0] for r in cur.fetchall()]
rows = []
if sids:
    sid_tuple = "(" + ",".join("?"*len(sids)) + ")"
    cur.execute(f"SELECT * FROM matches WHERE session_id IN {sid_tuple} ORDER BY id DESC;", sids)
    rows = cur.fetchall()
c.close()

# Filters
colf1, colf2, colf3, colf4 = st.columns([1,1,1,1])
with colf1:
    filter_player = st.selectbox("Spiller (valgfrit)", options=["- Alle -"] + [n for _,n in players])
with colf2:
    ftype = st.selectbox("Type", ["Alle","Singles","Doubles"])
with colf3:
    fresult = st.selectbox("Resultat (for valgt spiller)", ["Alle","Vundet","Tabt"])
with colf4:
    fsearch = st.text_input("Søg (navne)")

def row_from_match(r, perspective_pid: Optional[int]=None):
    is_d = bool(r["is_doubles"])
    s1 = [r["side1_p1"]] + ([r["side1_p2"]] if r["side1_p2"] else [])
    s2 = [r["side2_p1"]] + ([r["side2_p2"]] if r["side2_p2"] else [])
    s1_names = " & ".join([pid2name.get(p,"?") for p in s1])
    s2_names = " & ".join([pid2name.get(p,"?") for p in s2])
    winner = 1 if r["score1"]>r["score2"] else 2
    # date
    c = conn(); cur = c.cursor()
    cur.execute("SELECT session_date FROM sessions WHERE id=?;", (r["session_id"],))
    drow = cur.fetchone(); c.close()
    dstr = drow[0] if drow else ""
    vinkel = ""
    if perspective_pid and (perspective_pid in s1 or perspective_pid in s2):
        won = (winner==1 and perspective_pid in s1) or (winner==2 and perspective_pid in s2)
        vinkel = "Vundet" if won else "Tabt"
    return {"Dato": dstr, "Type": "Doubles" if is_d else "Singles", "Side 1": s1_names, "Side 2": s2_names, "Resultat": f"{r['score1']}-{r['score2']}", "Vinkel": vinkel}

perspective_pid = None
if filter_player != "- Alle -":
    for pid,name in players:
        if name == filter_player:
            perspective_pid = pid; break

table_rows = []
for r in rows:
    if ftype!="Alle":
        is_d = bool(r["is_doubles"])
        if ftype=="Singles" and is_d: continue
        if ftype=="Doubles" and not is_d: continue
    row = row_from_match(r, perspective_pid)
    if fsearch and fsearch.lower() not in (row["Side 1"] + " " + row["Side 2"]).lower():
        continue
    if perspective_pid and fresult!="Alle" and row["Vinkel"]!=fresult:
        continue
    if perspective_pid:
        n = pid2name[perspective_pid]
        if n not in (row["Side 1"] + " " + row["Side 2"]):
            continue
    table_rows.append(row)

if table_rows:
    df_arch = pd.DataFrame(table_rows)
    st.dataframe(df_arch, use_container_width=True)
    st.download_button("Download arkiv (CSV)", data=df_arch.to_csv(index=False).encode("utf-8"), file_name=f"kamp_arkiv_{year_choice}.csv", mime="text/csv")
else:
    st.caption("Ingen kampe matcher filtrene.")

# League
st.subheader("Liga")
df = compute_standings(year_choice)
if not df.empty:
    # add Rival column
    rivals = compute_rivals(year_choice)
    rival_txt = []
    for _,r in df.iterrows():
        pid = int(r["pid"])
        info = rivals.get(pid)
        if info and info.get("opponent_id"):
            opp = info["opponent_id"]
            rival_txt.append(f"{pid2name.get(opp,'?')} ({info['w']}-{info['l']}, {info['winpct']}%, {info['meetings']} kampe)")
        else:
            rival_txt.append("(for få møder)")
    out = df[["Spiller","Fremmøder","Kampe","Sejre","Nederlag","Sejr-%","Point i alt"]].copy()
    out["Rival"] = rival_txt
    st.dataframe(out, use_container_width=True)
    st.download_button("Download liga (CSV)", data=out.to_csv(index=False).encode("utf-8"), file_name=f"liga_{year_choice}.csv", mime="text/csv")
else:
    st.caption("Ingen data endnu.")

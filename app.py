import streamlit as st
import sqlite3
import pandas as pd
import random
from datetime import date
from typing import List, Tuple, Optional, Dict

DB_PATH = "data/sondagsholdet.db"

# -------------- DB + Migration --------------
def conn():
    c = sqlite3.connect(DB_PATH)
    c.execute("PRAGMA foreign_keys = ON;")
    return c

def table_columns(c, table):
    cur = c.cursor()
    try:
        cur.execute(f"PRAGMA table_info({table});")
        cols = [r[1] for r in cur.fetchall()]
        return cols
    except sqlite3.OperationalError:
        return []

def legacy_matches_columns(c):
    cols = table_columns(c, "matches")
    # Legacy v4 had these columns
    needed = {"id","session_id","is_doubles","side1_p1","side1_p2","side2_p1","side2_p2","winning_side","score1","score2","created_at"}
    return needed.issubset(set(cols))

def migrate_if_needed():
    os.makedirs("data", exist_ok=True)
    c = conn(); cur = c.cursor()
    # Ensure players table exists (so PRAGMAs don't fail)
    cur.execute("CREATE TABLE IF NOT EXISTS players(id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE NOT NULL);")

    # ---- sessions: add sport column if missing ----
    sess_cols = table_columns(c, "sessions")
    if sess_cols and ("sport" not in sess_cols):
        # Rename old table
        cur.execute("ALTER TABLE sessions RENAME TO sessions_old;")
        # Create new sessions with sport + same IDs
        cur.execute("""
            CREATE TABLE sessions(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              session_date TEXT NOT NULL,
              sport TEXT NOT NULL,
              UNIQUE(session_date, sport)
            );
        """)
        # Copy over with sport='Pickleball' (legacy data var Pickleball)
        cur.execute("SELECT id, session_date FROM sessions_old;")
        rows = cur.fetchall()
        for sid, d in rows:
            cur.execute("INSERT INTO sessions(id, session_date, sport) VALUES (?,?,?);", (sid, d, "Pickleball"))
        cur.execute("DROP TABLE sessions_old;")
        c.commit()

    # Ensure sessions table exists if it didn't
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sessions(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          session_date TEXT NOT NULL,
          sport TEXT NOT NULL,
          UNIQUE(session_date, sport)
        );
    """)

    # ---- attendance table (legacy compatible) ----
    cur.execute("""
        CREATE TABLE IF NOT EXISTS attendance(
          session_id INTEGER NOT NULL,
          player_id INTEGER NOT NULL,
          PRIMARY KEY(session_id, player_id),
          FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE,
          FOREIGN KEY(player_id) REFERENCES players(id) ON DELETE CASCADE
        );
    """)

    # ---- matches: migrate legacy v4 to new schema (matches + match_players) ----
    # New schema check
    new_matches_cols = table_columns(c, "matches")
    has_new_schema = set(["id","session_id","sport","team_size","score1","score2","winning_side","created_at"]).issubset(set(new_matches_cols))

    if not has_new_schema and legacy_matches_columns(c):
        # Rename legacy
        cur.execute("ALTER TABLE matches RENAME TO matches_old;")
        # Create new
        cur.execute("""
            CREATE TABLE matches(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              session_id INTEGER NOT NULL,
              sport TEXT NOT NULL,
              team_size INTEGER NOT NULL,
              score1 INTEGER,
              score2 INTEGER,
              winning_side INTEGER NOT NULL,
              created_at TEXT DEFAULT CURRENT_TIMESTAMP,
              FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS match_players(
              match_id INTEGER NOT NULL,
              side INTEGER NOT NULL,
              player_id INTEGER NOT NULL,
              PRIMARY KEY(match_id, side, player_id),
              FOREIGN KEY(match_id) REFERENCES matches(id) ON DELETE CASCADE,
              FOREIGN KEY(player_id) REFERENCES players(id) ON DELETE CASCADE
            );
        """)
        # Read from old and insert
        cur.execute("SELECT id, session_id, is_doubles, side1_p1, side1_p2, side2_p1, side2_p2, winning_side, score1, score2, created_at FROM matches_old;")
        rows = cur.fetchall()
        for (mid, sid, is_d, a1,a2,b1,b2, wside, sc1, sc2, created) in rows:
            team_size = 2 if is_d==1 else 1
            sport = "Pickleball"
            cur.execute("INSERT INTO matches(id, session_id, sport, team_size, score1, score2, winning_side, created_at) VALUES (?,?,?,?,?,?,?,?);",
                        (mid, sid, sport, team_size, sc1, sc2, wside, created))
            # match_players
            side1 = [a1] + ([a2] if a2 else [])
            side2 = [b1] + ([b2] if b2 else [])
            for p in side1:
                cur.execute("INSERT OR IGNORE INTO match_players(match_id, side, player_id) VALUES (?,?,?)", (mid,1,p))
            for p in side2:
                cur.execute("INSERT OR IGNORE INTO match_players(match_id, side, player_id) VALUES (?,?,?)", (mid,2,p))
        cur.execute("DROP TABLE matches_old;")
        c.commit()
    else:
        # Ensure new schema exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS matches(
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              session_id INTEGER NOT NULL,
              sport TEXT NOT NULL,
              team_size INTEGER NOT NULL,
              score1 INTEGER,
              score2 INTEGER,
              winning_side INTEGER NOT NULL,
              created_at TEXT DEFAULT CURRENT_TIMESTAMP,
              FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS match_players(
              match_id INTEGER NOT NULL,
              side INTEGER NOT NULL,
              player_id INTEGER NOT NULL,
              PRIMARY KEY(match_id, side, player_id),
              FOREIGN KEY(match_id) REFERENCES matches(id) ON DELETE CASCADE,
              FOREIGN KEY(player_id) REFERENCES players(id) ON DELETE CASCADE
            );
        """)

    c.commit(); c.close()

def get_or_create_session(d: date, sport: str) -> int:
    c = conn(); cur = c.cursor()
    ds = d.isoformat()
    cur.execute("INSERT OR IGNORE INTO sessions(session_date, sport) VALUES (?,?);", (ds, sport))
    c.commit()
    cur.execute("SELECT id FROM sessions WHERE session_date=? AND sport=?;", (ds, sport))
    sid = cur.fetchone()[0]
    c.close()
    return sid

def list_players():
    c = conn(); cur = c.cursor()
    cur.execute("SELECT id,name FROM players ORDER BY name COLLATE NOCASE;")
    rows = cur.fetchall(); c.close(); return rows

def add_player(name: str):
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
    c = conn(); cur = c.cursor()
    cur.execute("DELETE FROM match_players WHERE match_id IN (SELECT id FROM matches WHERE session_id=?);", (session_id,))
    cur.execute("DELETE FROM matches WHERE session_id=?;", (session_id,))
    cur.execute("DELETE FROM attendance WHERE session_id=?;", (session_id,))
    cur.execute("DELETE FROM sessions WHERE id=?;", (session_id,))
    c.commit(); c.close()

def reset_all():
    c = conn(); cur = c.cursor()
    cur.executescript("""
    DROP TABLE IF EXISTS match_players;
    DROP TABLE IF EXISTS matches;
    DROP TABLE IF EXISTS attendance;
    DROP TABLE IF EXISTS sessions;
    DROP TABLE IF EXISTS players;
    """)
    c.commit(); c.close()
    migrate_if_needed()  # recreate

# -------------- Duplicate guard --------------
def canonical_side(players: List[int]) -> Tuple[int,...]:
    return tuple(sorted(players))

def match_duplicate_exists(session_id: int, sport: str, team_size: int, side1: List[int], side2: List[int], score1: int, score2: int) -> bool:
    s1 = canonical_side(side1); s2 = canonical_side(side2)
    if s2 < s1:
        s1, s2 = s2, s1
        score1, score2 = score2, score1
    c = conn(); c.row_factory = sqlite3.Row; cur = c.cursor()
    cur.execute("SELECT id, score1, score2 FROM matches WHERE session_id=? AND sport=? AND team_size=?;", (session_id, sport, team_size))
    mats = cur.fetchall()
    if not mats:
        c.close(); return False
    ids = [m["id"] for m in mats]
    ph = ",".join("?"*len(ids))
    cur.execute(f"SELECT match_id, side, player_id FROM match_players WHERE match_id IN ({ph})", ids)
    rows = cur.fetchall()
    c.close()
    by_match = {}
    for r in rows:
        by_match.setdefault(r["match_id"], {1:[],2:[]})[r["side"]].append(r["player_id"])
    for m in mats:
        mp = by_match.get(m["id"], {1:[],2:[]})
        a = canonical_side(mp.get(1,[])); b = canonical_side(mp.get(2,[]))
        ra, rb = (a,b) if a<=b else (b,a)
        rsc1, rsc2 = (m["score1"], m["score2"]) if (a,b)==(ra,rb) else (m["score2"], m["score1"])
        if ra==s1 and rb==s2 and rsc1==score1 and rsc2==score2:
            return True
    return False

def save_match(session_id: int, sport: str, team_size: int, side1: List[int], side2: List[int], score1: int, score2: int) -> bool:
    if match_duplicate_exists(session_id, sport, team_size, side1, side2, score1, score2):
        return False
    winner = 1 if score1>score2 else 2
    c = conn(); cur = c.cursor()
    cur.execute("""
        INSERT INTO matches(session_id, sport, team_size, score1, score2, winning_side) VALUES (?,?,?,?,?,?);
    """, (session_id, sport, team_size, score1, score2, winner))
    mid = cur.lastrowid
    for pid in side1:
        cur.execute("INSERT INTO match_players(match_id, side, player_id) VALUES (?,?,?)", (mid,1,pid))
    for pid in side2:
        cur.execute("INSERT INTO match_players(match_id, side, player_id) VALUES (?,?,?)", (mid,2,pid))
    c.commit(); c.close()
    return True

# -------------- Stats --------------
def compute_standings(year: int, sport: str) -> pd.DataFrame:
    c = conn(); cur = c.cursor()
    cur.execute("SELECT id,name FROM players;")
    players = dict(cur.fetchall())
    if not players:
        c.close(); return pd.DataFrame()
    cur.execute("SELECT id FROM sessions WHERE strftime('%Y', session_date)=? AND sport=?;", (str(year), sport))
    sids = [r[0] for r in cur.fetchall()]
    if not sids:
        c.close(); return pd.DataFrame()
    sid_tuple = "(" + ",".join("?"*len(sids)) + ")"
    cur.execute(f"SELECT player_id, COUNT(*) FROM attendance WHERE session_id IN {sid_tuple} GROUP BY player_id;", sids)
    attendance = dict(cur.fetchall())
    cur.execute(f"SELECT id, winning_side FROM matches WHERE session_id IN {sid_tuple};", sids)
    rows = cur.fetchall()
    wins = {pid:0 for pid in players}; losses = {pid:0 for pid in players}; played = {pid:0 for pid in players}
    for (mid, wside) in rows:
        cur.execute("SELECT side, player_id FROM match_players WHERE match_id=?;", (mid,))
        parts = cur.fetchall()
        s1 = [p[1] for p in parts if p[0]==1]; s2 = [p[1] for p in parts if p[0]==2]
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
        data.append([name, att, mp, w, l, winpct, total])
    df = pd.DataFrame(data, columns=["Spiller","Fremmøder","Kampe","Sejre","Nederlag","Sejr-%","Point i alt"])
    df = df.sort_values(["Point i alt","Sejre","Spiller"], ascending=[False,False,True]).reset_index(drop=True)
    c.close(); return df

# -------------- Round generator --------------
def make_round_matches(att_ids: List[int], courts: int, team_size: int, mix_mode: str) -> List[Dict]:
    if not att_ids or courts<=0 or team_size<=0: return []
    ids = att_ids[:]

    c = conn(); cur = c.cursor()
    q = "SELECT id,name FROM players WHERE id IN (%s)" % ",".join("?"*len(ids))
    cur.execute(q, ids)
    names = {pid: n for pid,n in cur.fetchall()}
    c.close()
    if mix_mode == "Snake (balanceret)":
        ids.sort(key=lambda p: names.get(p,"").lower())
    else:
        random.shuffle(ids)

    per_match = 2*team_size
    max_matches = min(len(ids)//per_match, courts)
    matches = []
    used = 0
    for m in range(max_matches):
        chunk = ids[used:used+per_match]
        used += per_match
        side1 = chunk[:team_size]
        side2 = chunk[team_size:]
        matches.append({"side1": side1, "side2": side2})
    return matches

# -------------- UI Per sport --------------
def sport_tab_ui(sport: str, default_team_size: int, team_min: int, team_max: int):
    st.header(sport)
    col1, col2, col3 = st.columns([1,1,1])
    with col1:
        sel_date = st.date_input("Dato", value=date.today(), key=f"date_{sport}")
        session_id = get_or_create_session(sel_date, sport)
    with col2:
        team_size = st.number_input("Holdstørrelse", min_value=team_min, max_value=team_max, value=default_team_size, step=1, key=f"teamsize_{sport}")
    with col3:
        courts = st.number_input("Baner", min_value=1, max_value=8, value=2, step=1, key=f"courts_{sport}")
    # Attendance
    players = list_players()
    pid2name = {pid:name for pid,name in players}
    st.subheader("Fremmøde")
    picked = st.multiselect("Vælg spillere", [name for _,name in players], default=[pid2name.get(pid) for pid in list_attendance(session_id)], key=f"att_{sport}")
    picked_ids = [pid for pid,name in players if name in picked]
    if st.button("Gem fremmøde", key=f"save_att_{sport}"):
        record_attendance(session_id, picked_ids)
        st.success("Fremmøde gemt.")
    # Controls
    st.subheader("Start spil")
    mix_mode = st.radio("Mixing", ["Random","Snake (balanceret)"], horizontal=True, key=f"mix_{sport}")
    att_ids = list_attendance(session_id)
    if st.button("Start runde", key=f"start_round_{sport}"):
        if len(att_ids) < 2*team_size:
            st.warning(f"For få spillere til {team_size}v{team_size}.")
        else:
            matches = make_round_matches(att_ids, int(courts), int(team_size), mix_mode)
            if not matches:
                st.warning("Kunne ikke planlægge kampe til denne runde.")
            else:
                st.session_state[f"matches_{sport}"] = matches
                st.session_state[f"session_{sport}"] = session_id
                st.success(f"Runde startet med {len(matches)} kampe.")
    # Active matches
    key_matches = f"matches_{sport}"
    if key_matches in st.session_state and st.session_state.get(f"session_{sport}")==session_id:
        st.subheader("Aktive kampe")
        for idx, m in enumerate(st.session_state[key_matches], start=1):
            s1 = m["side1"]; s2 = m["side2"]
            s1_names = " & ".join(pid2name.get(p,"?") for p in s1)
            s2_names = " & ".join(pid2name.get(p,"?") for p in s2)
            c1, c2, c3 = st.columns([3,3,2])
            with c1:
                st.write(f"{s1_names}  vs  {s2_names}")
            with c2:
                default1 = 21 if sport=='Badminton' else 11
                default2 = 19 if sport=='Badminton' else 7
                sc1 = st.number_input(f"Score {s1_names}", min_value=0, max_value=50, value=default1, step=1, key=f"sc1_{sport}_{idx}")
                sc2 = st.number_input(f"Score {s2_names}", min_value=0, max_value=50, value=default2, step=1, key=f"sc2_{sport}_{idx}")
            with c3:
                if st.button("Gem resultat", key=f"save_{sport}_{idx}"):
                    if sc1 == sc2:
                        st.warning("Ingen uafgjort. Justér score.")
                    else:
                        ok = save_match(session_id, sport, int(team_size), s1, s2, int(sc1), int(sc2))
                        if ok:
                            st.success("Kamp gemt.")
                        else:
                            st.info("Den kamp er allerede gemt (samme deltagere og score i dag).")
    # Archive
    st.subheader("Kamp-arkiv")
    year_choice = st.number_input("År", min_value=2000, max_value=2100, value=date.today().year, step=1, key=f"year_{sport}")
    c = conn(); c.row_factory = sqlite3.Row; cur = c.cursor()
    cur.execute("SELECT id FROM sessions WHERE strftime('%Y', session_date)=? AND sport=?;", (str(year_choice), sport))
    sids = [r[0] for r in cur.fetchall()]
    rows = []
    if sids:
        sid_tuple = "(" + ",".join("?"*len(sids)) + ")"
        cur.execute(f"SELECT id, score1, score2, winning_side FROM matches WHERE session_id IN {sid_tuple} ORDER BY id DESC;", sids)
        mats = cur.fetchall()
        for m in mats:
            mid = m["id"]
            cur.execute("SELECT side, player_id FROM match_players WHERE match_id=?;", (mid,))
            parts = cur.fetchall()
            s1 = [p[1] for p in parts if p[0]==1]; s2 = [p[1] for p in parts if p[0]==2]
            rows.append((mid, m["score1"], m["score2"], m["winning_side"], s1, s2))
    c.close()
    players = list_players()
    pid2name = {pid:name for pid,name in players}
    table_rows = []
    for mid, sc1, sc2, wside, s1, s2 in rows:
        s1_names = " & ".join(pid2name.get(p,"?") for p in s1)
        s2_names = " & ".join(pid2name.get(p,"?") for p in s2)
        c = conn(); cur = c.cursor()
        cur.execute("SELECT session_date FROM sessions WHERE id=(SELECT session_id FROM matches WHERE id=?);", (mid,))
        drow = cur.fetchone(); c.close()
        dstr = drow[0] if drow else ""
        table_rows.append({"Dato": dstr, "Side 1": s1_names, "Side 2": s2_names, "Resultat": f"{sc1}-{sc2}"})
    if table_rows:
        df_arch = pd.DataFrame(table_rows)
        st.dataframe(df_arch, use_container_width=True)
        st.download_button("Download arkiv (CSV)", data=df_arch.to_csv(index=False).encode("utf-8"), file_name=f"{sport.lower()}_arkiv_{year_choice}.csv", mime="text/csv")
    else:
        st.caption("Ingen kampe endnu for det valgte år.")
    # League
    st.subheader("Liga")
    df = compute_standings(year_choice, sport)
    if not df.empty:
        out = df[["Spiller","Fremmøder","Kampe","Sejre","Nederlag","Sejr-%","Point i alt"]]
        st.dataframe(out, use_container_width=True)
        st.download_button("Download liga (CSV)", data=out.to_csv(index=False).encode("utf-8"), file_name=f"{sport.lower()}_liga_{year_choice}.csv", mime="text/csv")
    else:
        st.caption("Ingen data i ligaen endnu.")

# -------------- App UI --------------
st.set_page_config(page_title="Søndagsholdet F/S", layout="wide")
st.title("Søndagsholdet F/S")

import os
os.makedirs("data", exist_ok=True)
migrate_if_needed()

with st.sidebar:
    st.header("Indstillinger")
    with st.form("add_player_form", clear_on_submit=True):
        nm = st.text_input("Tilføj spiller")
        submitted = st.form_submit_button("Gem spiller")
        if submitted and nm.strip():
            pid = add_player(nm)
            if pid: st.success(f"Tilføjet: {nm}")
    # Backups
    if os.path.exists(DB_PATH):
        with open(DB_PATH, "rb") as f:
            st.download_button("Download database (.db)", data=f.read(), file_name="sondagsholdet.db")
    # CSV export (all matches)
    c = conn()
    try:
        df_all = pd.read_sql_query("""
            SELECT m.id, s.session_date, s.sport, m.team_size, m.score1, m.score2, m.winning_side
            FROM matches m JOIN sessions s ON s.id = m.session_id
            ORDER BY m.id DESC
        """, c)
    except Exception:
        df_all = pd.DataFrame()
    finally:
        c.close()
    if not df_all.empty:
        st.download_button("Download alle kampe (CSV)", data=df_all.to_csv(index=False).encode("utf-8"), file_name="alle_kampe.csv", mime="text/csv")
    uploaded = st.file_uploader("Upload database (.db)", type=["db"])
    if uploaded is not None:
        with open(DB_PATH, "wb") as f:
            f.write(uploaded.getbuffer())
        st.success("Database gendannet. Genindlæs siden.")
    # Ryd data
    with st.expander("Ryd data"):
        st.caption("Slet testdata under udvikling.")
        del_sport = st.selectbox("Sport", ["Pickleball","Badminton","Volleyball","Indørs fodbold","Indørs hockey"], key="del_sport")
        del_date = st.date_input("Dato", value=date.today(), key="del_date")
        if st.button("Ryd dagens data for valgt sport"):
            c = conn(); cur = c.cursor()
            cur.execute("SELECT id FROM sessions WHERE session_date=? AND sport=?;", (del_date.isoformat(), del_sport))
            row = cur.fetchone()
            if row:
                delete_session_data(row[0])
                st.success("Dagens data ryddet.")
            else:
                st.info("Ingen session fundet for den dato/sport.")
            c.close()
        if st.button("Ryd ALT (drop database)"):
            reset_all()
            st.success("Alt er ryddet.")

tabs = st.tabs(["Pickleball","Badminton","Volleyball","Indørs fodbold","Indørs hockey"])

with tabs[0]:
    sport_tab_ui("Pickleball", default_team_size=2, team_min=1, team_max=2)
with tabs[1]:
    sport_tab_ui("Badminton", default_team_size=2, team_min=1, team_max=2)
with tabs[2]:
    sport_tab_ui("Volleyball", default_team_size=6, team_min=2, team_max=6)
with tabs[3]:
    sport_tab_ui("Indørs fodbold", default_team_size=5, team_min=3, team_max=6)
with tabs[4]:
    sport_tab_ui("Indørs hockey", default_team_size=3, team_min=2, team_max=5)

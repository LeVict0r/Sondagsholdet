
import streamlit as st
import sqlite3
from datetime import date
from typing import List, Tuple, Optional, Dict
import random
import pandas as pd

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
    /* New: rounds & round_matches for per-round scheduling */
    CREATE TABLE IF NOT EXISTS rounds(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      session_id INTEGER NOT NULL,
      round_index INTEGER NOT NULL,
      courts INTEGER NOT NULL,
      is_active INTEGER NOT NULL DEFAULT 1,
      created_at TEXT DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(session_id, round_index),
      FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
    );
    CREATE TABLE IF NOT EXISTS round_matches(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      round_id INTEGER NOT NULL,
      court INTEGER NOT NULL,
      is_singles INTEGER NOT NULL,
      s1p1 INTEGER NOT NULL,
      s1p2 INTEGER,
      s2p1 INTEGER NOT NULL,
      s2p2 INTEGER,
      completed INTEGER NOT NULL DEFAULT 0,
      score1 INTEGER,
      score2 INTEGER,
      FOREIGN KEY(round_id) REFERENCES rounds(id) ON DELETE CASCADE
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
    name = (name or "").strip()
    if not name: return None
    c = conn(); cur = c.cursor()
    cur.execute("INSERT OR IGNORE INTO players(name) VALUES (?);", (name,))
    c.commit()
    cur.execute("SELECT id FROM players WHERE name=?;", (name,))
    row = cur.fetchone(); c.close()
    return row[0] if row else None

def record_attendance(session_id: int, player_ids: List[int]):
    c = conn(); cur = c.cursor()
    for pid in player_ids:
        cur.execute("INSERT OR IGNORE INTO attendance(session_id, player_id) VALUES (?,?);", (session_id, pid))
    # remove unchecked handled outside if needed
    c.commit(); c.close()

def list_attendance(session_id: int) -> List[int]:
    c = conn(); cur = c.cursor()
    cur.execute("SELECT player_id FROM attendance WHERE session_id=?;", (session_id,))
    rows = [r[0] for r in cur.fetchall()]
    c.close(); return rows

def remove_attendance(session_id: int, player_id: int):
    c = conn(); cur = c.cursor()
    cur.execute("DELETE FROM attendance WHERE session_id=? AND player_id=?;", (session_id, player_id))
    c.commit(); c.close()

def get_active_round(session_id: int) -> Optional[int]:
    c = conn(); cur = c.cursor()
    cur.execute("SELECT id FROM rounds WHERE session_id=? AND is_active=1 ORDER BY round_index DESC LIMIT 1;", (session_id,))
    row = cur.fetchone(); c.close()
    return row[0] if row else None

def list_round_matches(round_id: int) -> List[sqlite3.Row]:
    c = conn(); c.row_factory = sqlite3.Row; cur = c.cursor()
    cur.execute("SELECT * FROM round_matches WHERE round_id=? ORDER BY court ASC, id ASC;", (round_id,))
    rows = cur.fetchall(); c.close(); return rows

def create_round(session_id: int, courts: int, pairs: List[Tuple[int,Optional[int],int,Optional[int]]]) -> int:
    """pairs: list of (s1p1,s1p2,s2p1,s2p2) in the order to assign to courts (chunked by courts size)."""
    c = conn(); cur = c.cursor()
    # next index
    cur.execute("SELECT COALESCE(MAX(round_index),0)+1 FROM rounds WHERE session_id=?;", (session_id,))
    ridx = cur.fetchone()[0]
    cur.execute("INSERT INTO rounds(session_id, round_index, courts, is_active) VALUES (?,?,?,1);", (session_id, ridx, courts))
    round_id = cur.lastrowid
    # insert matches
    court_num = 1
    for m in pairs:
        s1p1, s1p2, s2p1, s2p2 = m
        is_singles = 0 if (s1p2 or s2p2) else 1
        cur.execute("""
            INSERT INTO round_matches(round_id, court, is_singles, s1p1, s1p2, s2p1, s2p2)
            VALUES (?,?,?,?,?,?,?);
        """, (round_id, court_num, int(is_singles), s1p1, s1p2, s2p1, s2p2))
        court_num += 1
        if court_num > courts:
            court_num = 1
    c.commit(); c.close()
    return round_id

def complete_match_and_log(round_match_id: int, score1: int, score2: int):
    # mark round match completed + add to matches table
    c = conn(); c.row_factory = sqlite3.Row; cur = c.cursor()
    cur.execute("SELECT * FROM round_matches WHERE id=?;", (round_match_id,))
    rm = cur.fetchone()
    if not rm:
        c.close(); return
    # fetch session_id via round
    cur.execute("SELECT session_id FROM rounds WHERE id=?;", (rm["round_id"],))
    sid = cur.fetchone()[0]
    winning_side = 1 if score1 > score2 else 2
    cur.execute("""
        INSERT INTO matches(session_id, is_doubles, side1_p1, side1_p2, side2_p1, side2_p2, winning_side, score1, score2)
        VALUES (?,?,?,?,?,?,?,?,?);
    """, (sid, 0 if rm["is_singles"] else 1, rm["s1p1"], rm["s1p2"], rm["s2p1"], rm["s2p2"], winning_side, score1, score2))
    cur.execute("UPDATE round_matches SET completed=1, score1=?, score2=? WHERE id=?;", (score1, score2, round_match_id))
    c.commit(); c.close()

def close_round_if_done(round_id: int):
    c = conn(); cur = c.cursor()
    cur.execute("SELECT COUNT(*) FROM round_matches WHERE round_id=? AND completed=0;", (round_id,))
    left = cur.fetchone()[0]
    if left == 0:
        cur.execute("UPDATE rounds SET is_active=0 WHERE id=?;", (round_id,))
    c.commit(); c.close()

# ----------- STANDINGS / RIVAL -----------
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
        s1 = [s1p1] + ([s1p2] if is_d and s1p2 else [])
        s2 = [s2p1] + ([s2p2] if is_d and s2p2 else [])
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
        pps = round(total/att,2) if att>0 else 0.0
        ppm = round((w*3)/mp,2) if mp>0 else 0.0
        data.append([name, att, mp, w, l, winpct, total, pps, ppm, pid])
    df = pd.DataFrame(data, columns=["Spiller","Fremmøder","Kampe","Sejre","Nederlag","Sejr-%","Point i alt","Point/Træning","Point/Kamp","pid"])
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
        s1 = [a1] + ([a2] if is_d and a2 else [])
        s2 = [b1] + ([b2] if is_d and b2 else [])
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

# --------- SCHEDULER (round-based with odd rotation) ---------
def count_player_round_appearances(session_id: int) -> Dict[int,int]:
    """How many matches in rounds has each player played this session (to balance 'sidde over')."""
    c = conn(); c.row_factory = sqlite3.Row; cur = c.cursor()
    cur.execute("SELECT id FROM rounds WHERE session_id=?;", (session_id,))
    rids = [r[0] for r in cur.fetchall()]
    counts = {}
    if rids:
        rid_tuple = "(" + ",".join("?"*len(rids)) + ")"
        cur.execute(f"SELECT s1p1,s1p2,s2p1,s2p2 FROM round_matches WHERE round_id IN {rid_tuple};", rids)
        rows = cur.fetchall()
        for r in rows:
            for pid in [r["s1p1"], r["s1p2"], r["s2p1"], r["s2p2"]]:
                if pid:
                    counts[pid] = counts.get(pid,0)+1
    c.close(); return counts

def last_round_partners(session_id: int) -> Dict[int,int]:
    """Return a dict mapping player -> last partner id for last active or last round in session (for doubles)."""
    c = conn(); c.row_factory = sqlite3.Row; cur = c.cursor()
    cur.execute("SELECT id FROM rounds WHERE session_id=? ORDER BY round_index DESC LIMIT 1;", (session_id,))
    row = cur.fetchone()
    if not row:
        c.close(); return {}
    rid = row["id"]
    cur.execute("SELECT * FROM round_matches WHERE round_id=?;", (rid,))
    partners = {}
    for rm in cur.fetchall():
        if rm["is_singles"]: continue
        for a,b in [(rm["s1p1"], rm["s1p2"]), (rm["s2p1"], rm["s2p2"])]:
            if a and b:
                partners[a]=b; partners[b]=a
    c.close(); return partners

def build_round_pairs(att_ids: List[int], courts: int, use_singles_slot: bool, session_id: int) -> Tuple[List[Tuple[int,Optional[int],int,Optional[int]]], List[int]]:
    """
    Returns (pairs, sitting_out_list)
    pairs: list of (s1p1,s1p2,s2p1,s2p2) sized to courts (we will assign to courts in create_round)
    If odd number of players for doubles, rotate one to sit-out; if 4k+2 and courts>=3 and use_singles_slot -> one singles match included.
    """
    ids = att_ids.copy()
    random.shuffle(ids)  # initial shuffle for variety
    n = len(ids)
    sitting_out = []

    # determine singles slot eligibility
    singles_allowed = use_singles_slot and (n % 4 == 2) and (courts >= 3)

    # Compute play counts to choose fair sit-out
    played_counts = count_player_round_appearances(session_id)
    def next_sit_out(candidates: List[int]) -> int:
        # choose the one with fewest appearances; tiebreak by name/order
        cands = sorted(candidates, key=lambda pid: (played_counts.get(pid,0), pid))
        return cands[0]

    pairs = []

    if n % 2 == 1:
        # one must sit out
        so = next_sit_out(ids)
        sitting_out.append(so)
        ids.remove(so)
        n -= 1

    if n % 4 == 2 and not singles_allowed:
        # need one more to sit out so we have multiple of 4 for doubles
        so2 = next_sit_out(ids)
        if so2 not in sitting_out:
            sitting_out.append(so2)
        ids.remove(so2)
        n -= 2

    # partner memory: avoid last partner if possible
    last_part = last_round_partners(session_id)
    # make doubles teams from ids
    teams: List[Tuple[int,Optional[int]]] = []
    if singles_allowed and (n % 4 == 2):
        # make as many doubles as possible; last two become singles team entries (None partners)
        # form teams greedily avoiding last partners
        remaining = ids[:-2]
        singles = ids[-2:]
    else:
        remaining = ids
        singles = []

    # pair remaining into doubles
    used = set()
    rem_sorted = remaining[:]
    # simple heuristic: sort by how recently/least played to balance
    rem_sorted.sort(key=lambda pid: (played_counts.get(pid,0), pid))
    for pid in rem_sorted:
        if pid in used: continue
        # choose partner
        candidates = [q for q in rem_sorted if q not in used and q != pid]
        # avoid last partner if possible
        avoid = last_part.get(pid)
        pick = None
        for q in candidates:
            if q != avoid:
                pick = q; break
        if pick is None and candidates:
            pick = candidates[0]
        if pick is not None:
            teams.append((pid, pick))
            used.add(pid); used.add(pick)

    # if any leftover (shouldn't happen), drop them to sitting_out
    leftovers = [p for p in rem_sorted if p not in used]
    for p in leftovers:
        sitting_out.append(p)

    # Add singles pseudo-team if enabled
    if len(singles) == 2:
        teams.append((singles[0], None))  # team X (single)
        teams.append((singles[1], None))  # team Y (single)

    # now create matches limited by courts: greedy pair adjacent teams into matches
    # If singles entries exist, they'll pair either vs each other (single vs single) or vs doubles creating singles matches.
    # For simplicity in "round": we create up to 'courts' matches.
    # Strategy: prefer doubles vs doubles first; if singles entries exist and courts left, schedule single vs single.
    doubles_teams = [t for t in teams if t[1] is not None]
    single_entries = [t for t in teams if t[1] is None]

    # create matches
    court_slots = courts
    i = 0
    # pair doubles teams
    while len(doubles_teams) >= 2 and court_slots > 0:
        t1 = doubles_teams.pop(0)
        t2 = doubles_teams.pop(0)
        pairs.append((t1[0], t1[1], t2[0], t2[1]))
        court_slots -= 1

    # handle singles
    if court_slots > 0 and len(single_entries) >= 2:
        # single vs single
        s1 = single_entries.pop(0)[0]
        s2 = single_entries.pop(0)[0]
        pairs.append((s1, None, s2, None))
        court_slots -= 1

    # If still courts and one single left and one doubles left, we can schedule ONE singles vs ONE of doubles players?
    # To keep it simple and fair, we leave that for next round. (Keeps UI og flow simpelt).

    return pairs, sitting_out

# ---------------- UI ----------------
st.set_page_config(page_title="Søndagsholdet F/S – Onepager v2", layout="wide")
st.title("Søndagsholdet F/S – Onepager v2")

import os
os.makedirs("data", exist_ok=True)
init_db()

with st.sidebar:
    st.header("⚙️ Setup")
    with st.expander("➕ Tilføj spiller"):
        nm = st.text_input("Navn")
        if st.button("Tilføj"):
            pid = add_player(nm)
            if pid: st.success(f"Tilføjet: {nm}")
            else: st.warning("Ugyldigt navn.")
    with st.expander("🧹 Ryd data (fare)"):
        if st.checkbox("Jeg forstår, at alt slettes permanent."):
            if st.button("Nulstil DB"):
                c = conn(); cur = c.cursor()
                cur.executescript("""
                DROP TABLE IF EXISTS round_matches;
                DROP TABLE IF EXISTS rounds;
                DROP TABLE IF EXISTS matches;
                DROP TABLE IF EXISTS attendance;
                DROP TABLE IF EXISTS sessions;
                DROP TABLE IF EXISTS players;
                """)
                c.commit(); c.close()
                init_db()
                st.success("Database nulstillet.")

colL, colR = st.columns([1,1])
with colL:
    sel_date = st.date_input("Dato", value=date.today())
    session_id = get_or_create_session(sel_date)
with colR:
    courts = st.number_input("Antal baner", min_value=1, max_value=6, value=2, step=1)

players = list_players()
pid2name = {pid:name for pid,name in players}

# Attendance
st.subheader("📋 Fremmøde")
att_ids_existing = set(list_attendance(session_id))
opt_names = [name for _,name in players]
default_names = [pid2name[pid] for pid in att_ids_existing if pid in pid2name]
picked = st.multiselect("Vælg spillere, der er mødt", opt_names, default=default_names)
picked_ids = [pid for pid,name in players if name in picked]
colA, colB = st.columns([1,1])
with colA:
    if st.button("Opdatér fremmøde"):
        record_attendance(session_id, picked_ids)
        # remove unchecked
        for pid,_ in players:
            if pid in att_ids_existing and pid not in picked_ids:
                remove_attendance(session_id, pid)
        st.success("Fremmøde opdateret (1 point gives automatisk i stillingen).")
with colB:
    use_singles_slot = st.toggle("Aktivér singles-slot ved 4k+2 spillere og 3+ baner", value=True, help="Hvis I er fx 6/10/14 og har mindst 3 baner, kan to spillere få en singlekamp i runden.")

att_ids = list_attendance(session_id)

st.markdown("---")

# Round control
st.subheader("🎯 Runder (remix pr. runde + fair rotation ved ulige)")
active_round_id = get_active_round(session_id)
if active_round_id:
    st.info("Der er en aktiv runde. Registrér resultaterne herunder. Når alle kampe er gemt, lukker runden automatisk.")
else:
    st.caption("Ingen aktiv runde. Tryk 'Lav næste runde' når I er klar.")

col1, col2 = st.columns([1,1])
with col1:
    mix_mode = st.radio("Mixing", ["Snake (balanceret)","Random"], horizontal=True)
with col2:
    if st.button("🔁 Lav næste runde"):
        if len(att_ids) < 2:
            st.error("For få spillere tjekket ind.")
        else:
            # Build pairs and create round
            # For snake mode, vi sorterer forudsigeligt (navn), random ellers
            att = att_ids[:]
            if mix_mode == "Snake (balanceret)":
                att = [pid for pid,_ in sorted([(pid,pid2name[pid]) for pid in att], key=lambda x:x[1].lower())]
            else:
                random.shuffle(att)
            pairs, sitting_out = build_round_pairs(att, courts, use_singles_slot, session_id)
            if not pairs:
                st.warning("Ingen kampe kunne planlægges i denne runde (måske for få spillere i forhold til baner).")
            else:
                rid = create_round(session_id, courts, pairs)
                if sitting_out:
                    st.success("Runde oprettet. Sidder over: " + ", ".join(pid2name.get(p,'?') for p in sitting_out))
                else:
                    st.success("Runde oprettet. Alle spiller i denne runde.")

# Show active round matches
active_round_id = get_active_round(session_id)
if active_round_id:
    st.markdown("#### Dagens runde – registrér vindere")
    rms = list_round_matches(active_round_id)
    for rm in rms:
        c1, c2, c3 = st.columns([3,3,2])
        s1_names = " & ".join([pid2name.get(rm["s1p1"],"?")] + ([pid2name.get(rm["s1p2"],"?")] if rm["s1p2"] else []))
        s2_names = " & ".join([pid2name.get(rm["s2p1"],"?")] + ([pid2name.get(rm["s2p2"],"?")] if rm["s2p2"] else []))
        with c1:
            st.write(f"**Bane {rm['court']}** — {s1_names}  vs  {s2_names}  ({'Singles' if rm['is_singles'] else 'Doubles'})")
        with c2:
            sc1 = st.number_input(f"Score {s1_names}", min_value=0, max_value=11, value=11, step=1, key=f"s1_{rm['id']}")
            sc2 = st.number_input(f"Score {s2_names}", min_value=0, max_value=11, value=7, step=1, key=f"s2_{rm['id']}")
            if sc1 == sc2:
                st.warning("Uafgjort er ikke tilladt – justér scoren.", icon="⚠️")
        with c3:
            if rm["completed"]:
                st.success(f"Gemte: {rm['score1']}-{rm['score2']}")
            else:
                if st.button("Gem kamp", key=f"save_{rm['id']}"):
                    if sc1 == sc2:
                        st.error("Uafgjort er ikke tilladt.")
                    else:
                        complete_match_and_log(rm["id"], int(sc1), int(sc2))
                        st.success("Kamp gemt.")
    # After loop, attempt to close round if all completed
    if st.button("🏁 Tjek og afslut runde"):
        close_round_if_done(active_round_id)
        nr = get_active_round(session_id)
        if nr:
            st.info("Der er stadig kampe i runden, der ikke er gemt.")
        else:
            st.success("Runden er afsluttet. Klar til at lave næste runde.")

st.markdown("---")

# Archive
st.subheader("🗂️ Kamp-arkiv")
year_choice = st.number_input("År", min_value=2000, max_value=2100, value=date.today().year, step=1)
# fetch rows
c = conn(); c.row_factory = sqlite3.Row; cur = c.cursor()
cur.execute("SELECT id FROM sessions WHERE strftime('%Y', session_date)=?;", (str(year_choice),))
sids = [r[0] for r in cur.fetchall()]
rows = []
if sids:
    sid_tuple = "(" + ",".join("?"*len(sids)) + ")"
    cur.execute(f"SELECT * FROM matches WHERE session_id IN {sid_tuple} ORDER BY id DESC;", sids)
    rows = cur.fetchall()
c.close()

# filters
colf1, colf2, colf3, colf4 = st.columns([1,1,1,1])
with colf1:
    filter_player = st.selectbox("Filtrér spiller (valgfrit)", options=["- Alle -"] + [n for _,n in players])
with colf2:
    ftype = st.selectbox("Type", ["Alle","Singles","Doubles"])
with colf3:
    fresult = st.selectbox("Resultat (for valgt spiller)", ["Alle","Vundet","Tabt"])
with colf4:
    fsearch = st.text_input("Søg (navne)")

pid2name = {pid:name for pid,name in players}
def match_to_row(r, perspective_pid: Optional[int]=None):
    is_d = bool(r["is_doubles"])
    s1 = [r["side1_p1"]] + ([r["side1_p2"]] if is_d and r["side1_p2"] else [])
    s2 = [r["side2_p1"]] + ([r["side2_p2"]] if is_d and r["side2_p2"] else [])
    side1_names = " & ".join([pid2name.get(p,"?") for p in s1])
    side2_names = " & ".join([pid2name.get(p,"?") for p in s2])
    winner = 1 if r["score1"]>r["score2"] else 2
    # session date
    c = conn(); cur = c.cursor()
    cur.execute("SELECT session_date FROM sessions WHERE id=?;", (r["session_id"],))
    drow = cur.fetchone(); c.close()
    dstr = drow[0] if drow else ""
    outcome = ""
    mark = ""
    if perspective_pid:
        won = (winner==1 and perspective_pid in s1) or (winner==2 and perspective_pid in s2)
        if (perspective_pid in s1) or (perspective_pid in s2):
            outcome = "Vundet" if won else "Tabt"
            mark = "✅" if won else "❌"
    return {"Dato": dstr, "Type":"Doubles" if is_d else "Singles", "Side 1":side1_names, "Side 2":side2_names, "Resultat":f"{r['score1']}-{r['score2']}", "Vinkel": outcome, "Markering": mark}

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
    row = match_to_row(r, perspective_pid)
    if fsearch and fsearch.lower() not in (row["Side 1"] + " " + row["Side 2"]).lower():
        continue
    if perspective_pid and fresult!="Alle" and row["Vinkel"]!=fresult:
        continue
    if perspective_pid:
        if pid2name[perspective_pid] not in (row["Side 1"] + " " + row["Side 2"]):
            continue
    table_rows.append(row)

if table_rows:
    df_arch = pd.DataFrame(table_rows)
    st.dataframe(df_arch, use_container_width=True)
else:
    st.caption("Ingen kampe matcher filtrene.")

st.markdown("---")

# Live standings + Rival
st.subheader("📊 Live-liga + Rival")
df = compute_standings(year_choice)
if not df.empty:
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
    out = df[["Spiller","Fremmøder","Kampe","Sejre","Nederlag","Sejr-%","Point i alt","Point/Træning","Point/Kamp"]].copy()
    out["Rival"] = rival_txt
    st.dataframe(out, use_container_width=True)
    st.download_button("⬇️ Download liga (CSV)", data=out.to_csv(index=False).encode("utf-8"),
                       file_name=f"liga_{year_choice}.csv", mime="text/csv")
else:
    st.caption("Ingen data endnu.")

# MVP of selected date
st.subheader("🏆 Aften-MVP")
def compute_mvp(sid: int) -> List[int]:
    c = conn(); cur = c.cursor()
    cur.execute("SELECT player_id FROM attendance WHERE session_id=?;", (sid,))
    pts = {}
    for (pid,) in cur.fetchall():
        pts[pid] = pts.get(pid,0)+1
    cur.execute("SELECT is_doubles, side1_p1, side1_p2, side2_p1, side2_p2, winning_side FROM matches WHERE session_id=?;", (sid,))
    for is_d, a1,a2,b1,b2,wside in cur.fetchall():
        winners = ([a1] + ([a2] if is_d and a2 else [])) if wside==1 else ([b1] + ([b2] if is_d and b2 else []))
        for p in winners:
            pts[p] = pts.get(p,0)+3
    if not pts: c.close(); return []
    m = max(pts.values()); mvps = [pid for pid,v in pts.items() if v==m]
    c.close(); return mvps

mvps = compute_mvp(session_id)
if mvps:
    st.success("Aftenens MVP: " + ", ".join(pid2name.get(p,'?') for p in mvps))
else:
    st.caption("MVP vises når der er registreret fremmøde/kampe i dag.")

import streamlit as st
import sqlite3
from datetime import date
from typing import List, Tuple, Optional, Dict, Set
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
    /* Rounds pre-generated for a pool */
    CREATE TABLE IF NOT EXISTS rounds(
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      session_id INTEGER NOT NULL,
      round_index INTEGER NOT NULL,
      courts INTEGER NOT NULL,
      is_active INTEGER NOT NULL DEFAULT 0,
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
    for pid in player_ids:
        cur.execute("INSERT OR IGNORE INTO attendance(session_id, player_id) VALUES (?,?);", (session_id, pid))
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

def active_round(session_id: int) -> Optional[int]:
    c = conn(); cur = c.cursor()
    cur.execute("SELECT id FROM rounds WHERE session_id=? AND is_active=1 ORDER BY round_index LIMIT 1;", (session_id,))
    row = cur.fetchone(); c.close()
    return row[0] if row else None

def set_round_active(session_id: int, rid: int):
    c = conn(); cur = c.cursor()
    cur.execute("UPDATE rounds SET is_active=0 WHERE session_id=?;", (session_id,))
    cur.execute("UPDATE rounds SET is_active=1 WHERE id=?;", (rid,))
    c.commit(); c.close()

def next_inactive_round(session_id: int) -> Optional[int]:
    c = conn(); cur = c.cursor()
    cur.execute("SELECT id FROM rounds WHERE session_id=? AND is_active=0 ORDER BY round_index LIMIT 1;", (session_id,))
    row = cur.fetchone(); c.close()
    return row[0] if row else None

def clear_rounds(session_id: int):
    c = conn(); cur = c.cursor()
    cur.execute("DELETE FROM round_matches WHERE round_id IN (SELECT id FROM rounds WHERE session_id=?)", (session_id,))
    cur.execute("DELETE FROM rounds WHERE session_id=?", (session_id,))
    c.commit(); c.close()

def list_round_matches(rid: int) -> List[sqlite3.Row]:
    c = conn(); c.row_factory = sqlite3.Row; cur = c.cursor()
    cur.execute("SELECT * FROM round_matches WHERE round_id=? ORDER BY court ASC, id ASC;", (rid,))
    rows = cur.fetchall(); c.close(); return rows

def complete_match_and_log(round_match_id: int, score1: int, score2: int):
    c = conn(); c.row_factory = sqlite3.Row; cur = c.cursor()
    cur.execute("SELECT * FROM round_matches WHERE id=?;", (round_match_id,))
    rm = cur.fetchone()
    if not rm:
        c.close(); return
    cur.execute("SELECT session_id FROM rounds WHERE id=?;", (rm["round_id"],))
    sid = cur.fetchone()[0]
    winning_side = 1 if score1 > score2 else 2
    cur.execute("""
        INSERT INTO matches(session_id, is_doubles, side1_p1, side1_p2, side2_p1, side2_p2, winning_side, score1, score2)
        VALUES (?,?,?,?,?,?,?,?,?);
    """, (sid, 0 if rm["is_singles"] else 1, rm["s1p1"], rm["s1p2"], rm["s2p1"], rm["s2p2"], winning_side, score1, score2))
    cur.execute("UPDATE round_matches SET completed=1, score1=?, score2=? WHERE id=?;", (score1, score2, round_match_id))
    c.commit(); c.close()

def round_completed(rid: int) -> bool:
    c = conn(); cur = c.cursor()
    cur.execute("SELECT COUNT(*) FROM round_matches WHERE round_id=? AND completed=0;", (rid,))
    left = cur.fetchone()[0]; c.close()
    return left == 0

# ------------- STANDINGS / RIVAL -------------
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

# ------------- POOL SCHEDULER (all rounds up-front) -------------
def make_teams(att_ids: List[int], mode: str) -> List[Tuple[int, Optional[int]]]:
    ids = att_ids[:]
    if mode == "Snake (balanceret)":
        # deterministic by name order will be applied outside; here just keep order
        pass
    else:
        random.shuffle(ids)
    # pair as doubles by adjacent; if odd -> one sits over per round (handled by pacḱing)
    teams = []
    i = 0
    while i+1 < len(ids):
        teams.append((ids[i], ids[i+1]))
        i += 2
    if i < len(ids):  # odd leftover -> single entry
        teams.append((ids[i], None))
    return teams

def all_vs_all_pairs(teams: List[Tuple[int, Optional[int]]]) -> List[Tuple[Tuple[int,Optional[int]], Tuple[int,Optional[int]]]]:
    M = []
    for i in range(len(teams)):
        for j in range(i+1, len(teams)):
            M.append((teams[i], teams[j]))
    return M

def players_in_team(team: Tuple[int, Optional[int]]) -> Set[int]:
    return {team[0]} | ({team[1]} if team[1] else set())

def pack_into_rounds(session_id: int, courts: int, matches: List[Tuple[Tuple[int,Optional[int]], Tuple[int,Optional[int]]]]) -> List[List[Tuple[Tuple[int,Optional[int]], Tuple[int,Optional[int]]]]]:
    """Greedy: build rounds with up to 'courts' simultaneous matches, no overlapping players within a round."""
    rounds: List[List[Tuple[Tuple[int,Optional[int]], Tuple[int,Optional[int]]]]] = []
    remaining = matches[:]
    # simple fairness: shuffle order a bit
    random.shuffle(remaining)
    while remaining:
        used: Set[int] = set()
        this_round: List[Tuple[Tuple[int,Optional[int]], Tuple[int,Optional[int]]]] = []
        i = 0
        while i < len(remaining) and len(this_round) < courts:
            a,b = remaining[i]
            A = players_in_team(a)
            B = players_in_team(b)
            if used.isdisjoint(A) and used.isdisjoint(B):
                this_round.append((a,b))
                used |= A | B
                remaining.pop(i)
            else:
                i += 1
        if not this_round:
            # if we got stuck (due to many conflicts), force take first and continue
            a,b = remaining.pop(0)
            this_round.append((a,b))
        rounds.append(this_round)
    return rounds

def create_pool_rounds(session_id: int, courts: int, att_ids: List[int], mix_mode: str):
    """Generate teams -> all-vs-all matches -> pack into rounds -> persist to DB; activate first round."""
    # sort by name for snake-like balance
    ids = att_ids[:]
    # get names
    c = conn(); cur = c.cursor()
    cur.execute("SELECT id,name FROM players WHERE id IN (%s)" % ",".join("?"*len(ids)), ids)
    order = {pid: name for pid, name in cur.fetchall()}
    c.close()
    if mix_mode == "Snake (balanceret)":
        ids = sorted(ids, key=lambda pid: order.get(pid,"").lower())
    else:
        random.shuffle(ids)
    teams = make_teams(ids, mix_mode)
    matches = all_vs_all_pairs(teams)
    # Convert singles-vs-doubles into TWO singles? Simplicity: keep som den er (single vs double tillades ikke) → vi lader leftover single stå over og håber på jævnt felt.
    # Bedre: hvis sidste team er single, konverter alle deres matchups til singles mod én fra modstanderholdet -> men det kræver ekstra logik.
    # For v3: forbud mod single-entry i pool: hvis sidste er single, dropper vi den spiller for denne pulje (sidder over første runde). De kommer med i næste pulje/runde.
    # Derfor: hvis sidste team er (pid,None), drop det team fra denne pool og informer i UI.
    dropped_single_pid = None
    if teams and teams[-1][1] is None:
        dropped_single_pid = teams[-1][0]
        teams = teams[:-1]
        matches = all_vs_all_pairs(teams)

    rounds = pack_into_rounds(session_id, courts, matches)

    # Persist
    clear_rounds(session_id)
    c = conn(); cur = c.cursor()
    for idx, rd in enumerate(rounds, start=1):
        cur.execute("INSERT INTO rounds(session_id, round_index, courts, is_active) VALUES (?,?,?,0);", (session_id, idx, courts))
        rid = cur.lastrowid
        court = 1
        for (t1, t2) in rd:
            s1p1, s1p2 = t1[0], t1[1]
            s2p1, s2p2 = t2[0], t2[1]
            is_singles = 0 if (s1p2 or s2p2) else 1
            cur.execute("""
                INSERT INTO round_matches(round_id, court, is_singles, s1p1, s1p2, s2p1, s2p2)
                VALUES (?,?,?,?,?,?,?);
            """, (rid, court, int(is_singles), s1p1, s1p2, s2p1, s2p2))
            court += 1
            if court > courts: court = 1
    c.commit(); c.close()
    # Activate first
    c = conn(); cur = c.cursor()
    cur.execute("SELECT id FROM rounds WHERE session_id=? ORDER BY round_index LIMIT 1;", (session_id,))
    row = cur.fetchone(); c.close()
    if row:
        set_round_active(session_id, row[0])
    return dropped_single_pid

# ---------------- UI ----------------
st.set_page_config(page_title="Søndagsholdet F/S – Onepager v3", layout="wide")
st.title("Søndagsholdet F/S – Onepager v3 (Pulje med flere baner)")

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
    courts = st.number_input("Antal baner", min_value=1, max_value=6, value=3, step=1)

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
        # fjern dem, der ikke længere er valgt
        for pid,_ in players:
            if pid in att_ids_existing and pid not in picked_ids:
                remove_attendance(session_id, pid)
        st.success("Fremmøde opdateret (giver 1 point).")
with colB:
    mix_mode = st.radio("Mixing af hold", ["Snake (balanceret)","Random"], horizontal=True)

att_ids = list_attendance(session_id)

st.markdown("---")

# Pool control: generate ALL rounds based on courts
st.subheader("🎯 Start pulje (alle runder genereres på forhånd)")
if st.button("🚀 Start pulje med nuværende fremmøde"):
    if len(att_ids) < 4:
        st.error("For få spillere til en pulje (min. 4).")
    else:
        dropped = create_pool_rounds(session_id, courts, att_ids, mix_mode)
        if dropped:
            st.warning(f"Uligt antal – {pid2name.get(dropped,'?')} sidder over i denne pulje. (Kommer med i næste pulje/næste gang).")
        rid = active_round(session_id)
        if rid:
            st.success("Pulje oprettet. Runder er lagt i kø. Første runde er aktiv.")

active_rid = active_round(session_id)
if active_rid:
    st.markdown("#### Aktiv runde – registrér vindere for hver bane")
    rms = list_round_matches(active_rid)
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
    # Controls to go to next round
    coln1, coln2 = st.columns([1,1])
    with coln1:
        if st.button("🏁 Afslut runde og gå til næste"):
            if round_completed(active_rid):
                nr = next_inactive_round(session_id)
                if nr:
                    set_round_active(session_id, nr)
                    st.success("Næste runde er nu aktiv.")
                else:
                    st.info("Puljen er færdig – ingen flere runder.")
            else:
                st.warning("Der er stadig kampe i runden, som ikke er gemt.")
    with coln2:
        if st.button("🧹 Afbryd/ryd pulje"):
            clear_rounds(session_id)
            st.success("Puljen er ryddet. Du kan starte en ny.")

st.markdown("---")

# Archive
st.subheader("🗂️ Kamp-arkiv")
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

colf1, colf2, colf3, colf4 = st.columns([1,1,1,1])
with colf1:
    filter_player = st.selectbox("Filtrér spiller (valgfrit)", options=["- Alle -"] + [n for _,n in players])
with colf2:
    ftype = st.selectbox("Type", ["Alle","Singles","Doubles"])
with colf3:
    fresult = st.selectbox("Resultat (for valgt spiller)", ["Alle","Vundet","Tabt"])
with colf4:
    fsearch = st.text_input("Søg (navne)")

def match_to_row(r, perspective_pid: Optional[int]=None):
    is_d = bool(r["is_doubles"])
    s1 = [r["side1_p1"]] + ([r["side1_p2"]] if is_d and r["side1_p2"] else [])
    s2 = [r["side2_p1"]] + ([r["side2_p2"]] if is_d and r["side2_p2"] else [])
    side1_names = " & ".join([pid2name.get(p,"?") for p in s1])
    side2_names = " & ".join([pid2name.get(p,"?") for p in s2])
    winner = 1 if r["score1"]>r["score2"] else 2
    # date
    c = conn(); cur = c.cursor()
    cur.execute("SELECT session_date FROM sessions WHERE id=?;", (r["session_id"],))
    drow = cur.fetchone(); c.close()
    dstr = drow[0] if drow else ""
    outcome = ""; mark = ""
    if perspective_pid:
        involved = perspective_pid in s1 or perspective_pid in s2
        if involved:
            won = (winner==1 and perspective_pid in s1) or (winner==2 and perspective_pid in s2)
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
'''

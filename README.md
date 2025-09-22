
# Søndagsholdet F/S – Onepager v2

Nyt i v2:
- **Antal baner** (1–6) styrer rundeplanen.
- **Ulige fremmøde** → automatisk *sidde-over rotation* (fair og uden gentagelser).
- **Remix pr. runde** af makkerpar (undgår sidste runde-partnere hvis muligt).
- **Singles-slot (valgfrit)** når spillerantal = `4k+2` **og** baner ≥ 3 (to single-spillere får kamp i runden).
- **Per-runde visning**: opret runde → registrér vindere → runden lukker automatisk, lav ny runde.
- Live-liga & Rival som før. Kamp-arkiv som før.

## Kør

```bash
pip install -r requirements.txt
streamlit run app.py
```

Data ligger i `data/sondagsholdet.db`.

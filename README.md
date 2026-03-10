# Cermaq Dashboard – Backend API

FastAPI-backend som henter fôringsdata fra ScaleAQ og serverer dem til dashboardet.
Kjører på Railway med automatisk sync hvert 10. minutt.

---

## Filstruktur

```
cermaq-backend/
├── main.py              # FastAPI app + scheduler
├── db.py                # Database-oppsett (SQLite)
├── scaleaq_client.py    # ScaleAQ OAuth + API-kall
├── sync.py              # Sync-jobb (kjøres hvert 10 min)
├── routers/
│   ├── feed.py          # /api/feed/* endepunkter
│   └── meta.py          # /api/meta/* endepunkter
├── requirements.txt
├── railway.toml
└── .env.example
```

---

## Deploy på Railway (første gang)

### 1. Opprett Railway-konto
Gå til [railway.app](https://railway.app) og logg inn med GitHub.

### 2. Last opp koden til GitHub
```bash
cd cermaq-backend
git init
git add .
git commit -m "Initial backend"
# Opprett repo på github.com, deretter:
git remote add origin https://github.com/DITT_ORG/cermaq-backend.git
git push -u origin main
```

### 3. Deploy på Railway
1. Klikk **New Project → Deploy from GitHub repo**
2. Velg `cermaq-backend`
3. Railway oppdager Python automatisk

### 4. Legg til environment variables
I Railway-dashboardet → Settings → Variables:

```
SCALEAQ_USERNAME   =  din@epost.no
SCALEAQ_PASSWORD   =  ditt_passord
DB_PATH            =  /data/cermaq.db
SYNC_LOOKBACK_DAYS =  2
```

### 5. Legg til persistent volume
Railway → Storage → Add Volume:
- Mount path: `/data`

*(Uten dette mister du data ved restart)*

### 6. Seed metadata (kjøres én gang)
Etter deploy, kall dette endepunktet for å hente site/unit-IDs fra ScaleAQ:
```
POST https://din-app.railway.app/api/meta/sync-meta
```
Du kan gjøre dette i nettleseren via `/docs` (Swagger UI).

### 7. Notat deg Railway-URL-en
```
https://cermaq-backend-production.up.railway.app
```

---

## Oppdatering av data

Sync kjøres **automatisk hvert 10. minutt** etter oppstart.

Vil du trigge manuelt:
```
POST /api/meta/sync-now
```

Historisk backfill (f.eks. 30 dager tilbake):
```bash
# SSH inn i Railway-containeren, eller kjør lokalt med riktig .env:
python -c "import asyncio; from sync import backfill; asyncio.run(backfill(30))"
```

---

## API-endepunkter

| Metode | URL | Beskrivelse |
|--------|-----|-------------|
| GET | `/health` | Helsesjekk |
| GET | `/api/feed/dashboard/{unit_id}?from_date=&to_date=` | Alt dashboard trenger for én merd |
| GET | `/api/feed/daily/{unit_id}?from_date=&to_date=` | Daglige totaler |
| GET | `/api/feed/hourly/{unit_id}?from_date=&to_date=` | Timesprofil |
| GET | `/api/feed/10min/{unit_id}?date=` | 10-minutters rådata |
| GET | `/api/feed/sync-status` | Sist synket per merd |
| GET | `/api/meta/units` | Alle merder |
| GET | `/api/meta/sites` | Alle lokaliteter |
| POST | `/api/meta/sync-meta` | Hent site/unit-IDs fra ScaleAQ |
| POST | `/api/meta/sync-now` | Trigger manuell sync |
| GET | `/docs` | Swagger UI (interaktiv API-dok) |

---

## Koble dashboardet til API-et

I `cermaq_dashboard_horsvagen.html`, bytt ut den hardkodede `HV_FEED_DATA`-konstanten
med en `fetch()` til Railway:

```javascript
const API_BASE = 'https://din-app.railway.app';

async function loadFeedData(unitId, fromDate, toDate) {
  const resp = await fetch(
    `${API_BASE}/api/feed/dashboard/${unitId}?from_date=${fromDate}&to_date=${toDate}`
  );
  return resp.json();
}

// Kall ved oppstart:
const feedData = await loadFeedData('Fram01', '2026-02-01', '2026-03-10');
```

Responsen har nøyaktig samme struktur som den eksisterende `HV_FEED_DATA`-objektet.

---

## Lokal utvikling

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env             # Fyll inn ScaleAQ-credentials
uvicorn main:app --reload
# Åpne http://localhost:8000/docs
```

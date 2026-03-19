# EDR Scraping — Données Asset Management Allemand

Système complet de scraping, stockage et visualisation des données liées à la gestion d'actifs en Allemagne.

## Architecture

```
python/bdd/edr/
├── main.py                 # Point d'entrée CLI
├── requirements.txt        # Dépendances
├── config/
│   ├── settings.py         # Configuration centralisée
│   └── parser.py           # Parseur du fichier de liens
├── database/
│   ├── models.py           # ORM SQLAlchemy
│   └── schema.sql          # Schéma SQL brut
├── scraper/
│   ├── engine.py           # Moteur async (aiohttp + Playwright)
│   └── spiders/
│       ├── funds_spider.py      # Fondsweb, Morningstar
│       ├── regulator_spider.py  # BaFin, ESMA
│       └── market_spider.py     # BVI, Bundesbank
├── processing/
│   ├── pipeline.py         # Nettoyage et normalisation
│   └── analytics.py        # Rapports et agrégations Pandas
└── dashboard/
    └── app.py              # Dashboard Streamlit

data/bdd/edr/
├── db/                     # Base SQLite (edr_scraping.db)
└── links/
    └── scraping_links.txt  # ~1590 URLs, 9 secteurs
```

## Installation

```bash
cd ~/python/bdd/edr
pip install -r requirements.txt
playwright install chromium
```

## Utilisation

```bash
# Initialiser la DB + charger les sources
python main.py init

# Scraper toutes les sources
python main.py scrape

# Scraper par priorité / secteur / méthode
python main.py scrape --priority high
python main.py scrape --sector 1
python main.py scrape --method playwright

# Pipeline de nettoyage
python main.py pipeline

# Rapports analytiques (CSV)
python main.py analytics

# Dashboard Streamlit
python main.py dashboard

# Exécution complète (init → scrape → pipeline → analytics)
python main.py full
```

## Secteurs couverts

| # | Secteur | Exemples |
|---|---------|----------|
| 1 | Cadre réglementaire | BaFin, ESMA, MiFID II, SFDR |
| 2 | Structure du marché | BVI, Bundesbank, Morningstar DE |
| 3 | Produits financiers | Fondsweb, JustETF, ETF comparatifs |
| 4 | Sociétés locales AM | DWS, Union Investment, Deka |
| 5 | Sociétés internationales AM | BlackRock, Vanguard, Amundi |
| 6 | Macro & relance | Bundesfinanzministerium, BCE |
| 7 | Presse & classements | Citywire, Fund Forum, Handelsblatt |
| 8 | Agrégateurs de données | Bloomberg, Refinitiv, MSCI |
| 9 | Tendances & comportement | Statista, BVI Altersvorsorge |

## Configuration

- **SQLite** (par défaut) : `data/bdd/edr/db/edr_scraping.db`
- **PostgreSQL** : définir `EDR_USE_POSTGRES=1` + les variables `EDR_PG_*`

## Stack technique

- **Scraping** : aiohttp + BeautifulSoup (statique), Playwright (dynamique JS), pdfplumber (PDF)
- **BDD** : SQLAlchemy 2.0 / SQLite / PostgreSQL
- **Traitement** : Pandas, dédoublonnage, normalisation multi-format
- **Visualisation** : Streamlit + Plotly (7 pages)
- **Fonctionnalités** : scraping incrémental (SHA-256), détection automatique d'URLs, rate limiting par domaine

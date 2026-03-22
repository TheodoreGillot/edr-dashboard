# ──────────────────────────────────────────────────────────────────────────────
# EDR Asset Management Scraping — Configuration
# ──────────────────────────────────────────────────────────────────────────────
import os
from pathlib import Path

# ── Chemins ──────────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
# EDR_DB_DIR peut être overridé par variable d'env (local) ou on utilise data/ du projet (cloud)
_default_db_dir = str(PROJECT_ROOT / "data")
DB_DIR = Path(os.getenv("EDR_DB_DIR", _default_db_dir))
LINKS_FILE = Path(os.getenv("EDR_LINKS_FILE", "/home/theod/data/bdd/edr/links/scraping_links.txt"))
DATA_DIR = PROJECT_ROOT / "data"

DB_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ── PostgreSQL ───────────────────────────────────────────────────────────────
PG_HOST = os.getenv("EDR_PG_HOST", "localhost")
PG_PORT = int(os.getenv("EDR_PG_PORT", "5432"))
PG_DB = os.getenv("EDR_PG_DB", "edr_scraping")
PG_USER = os.getenv("EDR_PG_USER", "edr")
PG_PASS = os.getenv("EDR_PG_PASS", "edr_secret")
PG_DSN = f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"

# SQLite fallback (dev + cloud)
SQLITE_PATH = DB_DIR / "edr_dashboard.db"
SQLITE_DSN = f"sqlite:///{SQLITE_PATH}"

USE_POSTGRES = os.getenv("EDR_USE_POSTGRES", "0") == "1"
DB_DSN = PG_DSN if USE_POSTGRES else SQLITE_DSN

# ── Scraping ─────────────────────────────────────────────────────────────────
REQUEST_TIMEOUT = int(os.getenv("EDR_TIMEOUT", "30"))
MAX_CONCURRENT = int(os.getenv("EDR_MAX_CONCURRENT", "8"))
RETRY_TIMES = int(os.getenv("EDR_RETRY_TIMES", "3"))
DOWNLOAD_DELAY = float(os.getenv("EDR_DOWNLOAD_DELAY", "1.5"))

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

# ── Proxy (optionnel) ────────────────────────────────────────────────────────
PROXY_LIST = [p.strip() for p in os.getenv("EDR_PROXIES", "").split(",") if p.strip()]

# ── Taxonomie des secteurs ───────────────────────────────────────────────────
SECTORS = {
    1: "Cadre Légal & Réglementaire",
    2: "Structure du Marché Allemand",
    3: "Produits Financiers Déjà en Place",
    4: "Asset Managers Locaux",
    5: "Asset Managers Internationaux",
    6: "Plan de Relance & Macro",
    7: "Presse & Classements de Fonds",
    8: "Agrégateurs de Données",
    9: "Tendances Produits & Comportement",
    10: "Actifs Non Cotés",
}

# ── Classification des types de source ───────────────────────────────────────
SOURCE_TYPE_RULES = {
    "regulateur": ["bafin.de", "esma.europa.eu", "eur-lex.europa.eu", "bundesnetzagentur.de",
                    "europarl.europa.eu", "ec.europa.eu", "bundesrat.de", "bundestag.de",
                    "bundesfinanzministerium.de", "bmwk.de", "bmbf.de", "bmas.de",
                    "gesetze-im-internet.de", "bundesanzeiger.de", "bzst.de"],
    "donnees_marche": ["bvi.de", "bundesbank.de", "destatis.de", "efama.org", "ecb.europa.eu",
                       "eurostat", "stoxx.com", "msci.com", "ice.com", "markit.com",
                       "boerse-frankfurt.de", "sipri.org", "iiss.org"],
    "plateforme": ["fondsweb.com", "morningstar.de", "morningstar.com", "finanzen.net",
                   "onvista.de", "comdirect.de", "flatex.de", "trade-republic.com",
                   "scalable.capital", "justetf.com", "extraetf.com", "fondsdepotbank.de",
                   "ffb.de", "ebase.com", "allfunds.com", "mfex.com", "fundsquare.net",
                   "fundinfo.com", "fww.de", "scope-explorer.com"],
    "asset_manager": ["dws.com", "allianzgi.de", "union-investment.de", "unioninvestment.de",
                      "deka.de", "fvs.de", "flossbach-von-storch.com", "berenberg.de",
                      "lbbw-am.de", "metzler.com", "lupusalpha.de", "mainfirst.com",
                      "acatis.de", "blackrock.com", "ishares.com", "amundi.de", "pimco.de",
                      "fidelity.de", "jpmorganassetmanagement.de", "vanguard.de",
                      "pictet", "axa-im.de", "bnpparibas-am.com", "candriam.com",
                      "ubs.com", "lombardodier.com", "franklintempleton.de",
                      "schroders.com", "invesco.com", "edram.com", "edr-am.com",
                      "la-francaise.com", "group.edram.com"],
    "presse": ["handelsblatt.com", "boersen-zeitung.de", "faz.net", "wiwo.de",
               "spiegel.de", "manager-magazin.de", "capital.de", "stern.de", "n-tv.de",
               "dasinvestment.com", "fondsprofessionell.de", "fundresearch.de",
               "institutional-money.com", "portfolio-institutionell.de",
               "euro-am-sonntag.de", "boerse-online.de", "citywire.de",
               "ignites-europe.com", "funds-europe.com", "ipe.com", "ft.com",
               "reuters.com", "bloomberg.com", "economist.com"],
    "recherche": ["ifo.de", "diw.de", "zew.de", "rwi-essen.de", "ifs-kiel.de",
                  "halle-institute.de", "cesifo.org", "iwkoeln.de", "ifm-bonn.org",
                  "svr-wirtschaft.de", "sachverstaendigenrat", "bruegel.org"],
    "institutionnel": ["aba-online.de", "gdv.de", "bayerische-versorgungskammer.de",
                       "vbl.de", "stiftungen.org", "bdi.eu", "dihk.de",
                       "pensions-industrie.de", "versorgungswerke.de"],
    "non_cote": ["bvk.de", "invest-europe.eu", "preqin.com", "pitchbook.com",
                 "alt-credit.com", "eltif.com", "eltif-platform.de",
                 "pe-magazin.de", "vc-magazin.de", "private-equity-forum.de",
                 "inrev.org", "moonfare.com", "tikehau-capital.com",
                 "ardian.com", "eurazeo.com", "golding-capital.com",
                 "bridgepoint.eu", "equistone.eu", "dbag.de",
                 "intermediate-capital.com", "hayfin.com", "pemberton-am.com",
                 "macquarie.com", "brookfield.com", "meridiam.com",
                 "liqid.de", "finvia.de", "astorius.de",
                 "dealroom.co", "crunchbase.com", "mergermarket.com",
                 "vdpresearch.de", "bulwiengesa.de", "patrizia.ag"],
}

# ── Priorités de scraping ───────────────────────────────────────────────────
SCRAPING_PRIORITY = {
    "high": {
        "frequency_hours": 24,
        "domains": ["bvi.de", "morningstar.de", "fondsweb.com", "bafin.de",
                     "scope-explorer.com", "bundesbank.de", "esma.europa.eu",
                     "dasinvestment.com", "fondsprofessionell.de",
                     "institutional-money.com", "handelsblatt.com",
                     "bvk.de", "invest-europe.eu", "eltif.com",
                     "alt-credit.com", "inrev.org", "efama.org"]
    },
    "medium": {
        "frequency_hours": 72,
        "domains": ["dws.com", "allianzgi.de", "amundi.de", "pimco.de",
                     "fidelity.de", "blackrock.com",
                     "portfolio-institutionell.de", "finanzen.net",
                     "preqin.com", "pitchbook.com", "dealroom.co",
                     "ardian.com", "tikehau-capital.com", "moonfare.com",
                     "dbag.de", "patrizia.ag", "macquarie.com",
                     "fundresearch.de", "fww.de", "scope-fonds.de"]
    },
    "low": {
        "frequency_hours": 168,
        "domains": []  # tout le reste
    }
}

# ── Nature technique heuristiques ────────────────────────────────────────────
DYNAMIC_JS_DOMAINS = [
    "morningstar.de", "morningstar.com", "fondsweb.com", "scope-explorer.com",
    "fww.de", "trade-republic.com", "scalable.capital", "comdirect.de",
    "onvista.de", "allfunds.com", "boerse-frankfurt.de"
]

PDF_PATTERNS = [".pdf", "/pdf/", "/blob/", "/download/", "/SharedDocs/"]

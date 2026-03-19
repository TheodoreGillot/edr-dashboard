#!/usr/bin/env python3
# ──────────────────────────────────────────────────────────────────────────────
# EDR Scraping — Point d'entrée principal
# ──────────────────────────────────────────────────────────────────────────────
"""
Usage:
    python main.py init          # Initialise la DB et charge les sources
    python main.py parse         # Parse le fichier de liens → taxonomie JSON
    python main.py scrape        # Lance le scraping complet
    python main.py scrape --priority high    # Scrape uniquement les sources prioritaires
    python main.py scrape --sector 1         # Scrape uniquement le secteur 1
    python main.py scrape --method requests_bs4   # Uniquement les sources HTML statiques
    python main.py pipeline      # Lance le pipeline de nettoyage
    python main.py analytics     # Génère les rapports analytiques
    python main.py dashboard     # Lance le dashboard Streamlit
    python main.py full          # init + scrape + pipeline + analytics
"""
import sys
import asyncio
import logging
from pathlib import Path

# Ajout du répertoire projet au path
sys.path.insert(0, str(Path(__file__).resolve().parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("edr.main")


def cmd_init():
    """Initialise la base de données et charge les sources."""
    from database.models import init_db, load_sources_from_parsed
    from config.parser import parse_links_file, export_taxonomy, get_stats
    import json

    logger.info("=== Initialisation ===")
    init_db()

    entries = parse_links_file()
    logger.info(f"Fichier parsé : {len(entries)} URLs extraites")

    load_sources_from_parsed(entries)

    export_taxonomy()

    stats = get_stats(entries)
    logger.info(f"Statistiques :\n{json.dumps(stats, ensure_ascii=False, indent=2)}")


def cmd_parse():
    """Parse le fichier de liens et exporte la taxonomie."""
    from config.parser import export_taxonomy, get_stats
    import json

    entries = export_taxonomy()
    stats = get_stats(entries)
    print(json.dumps(stats, ensure_ascii=False, indent=2))


def cmd_scrape(priority: str = None, sector: int = None, method: str = None):
    """Lance le scraping."""
    from database.models import get_session, Source
    from scraper.engine import ScraperEngine

    db = get_session()
    query = db.query(Source).filter(Source.actif == True)

    if priority:
        query = query.filter(Source.priorite == priority)
    if sector:
        query = query.filter(Source.secteur == sector)
    if method:
        query = query.filter(Source.methode_scraping == method)

    sources = query.all()
    db.close()

    if not sources:
        logger.warning("Aucune source à scraper avec ces filtres.")
        return

    logger.info(f"Scraping de {len(sources)} sources...")

    engine = ScraperEngine()
    results = asyncio.run(engine.run_batch(sources, incremental=True))
    logger.info(f"Résultat: {results}")


def cmd_pipeline():
    """Lance le pipeline de nettoyage."""
    from processing.pipeline import run_pipeline
    run_pipeline()


def cmd_apis():
    """Lance les clients API directs (Morningstar, BVI, Fondsweb)."""
    from scraper.api_clients import run_api_clients
    result = asyncio.run(run_api_clients())
    logger.info(f"API clients: {result}")


def cmd_analytics():
    """Génère les rapports analytiques."""
    from processing.analytics import export_all_reports
    export_all_reports()


def cmd_dashboard():
    """Lance le dashboard Streamlit."""
    import subprocess
    dashboard_path = Path(__file__).resolve().parent / "dashboard" / "app.py"
    subprocess.run([sys.executable, "-m", "streamlit", "run", str(dashboard_path),
                    "--server.port", "8501", "--server.headless", "true"])


def cmd_full():
    """Exécution complète du pipeline."""
    logger.info("=== EXÉCUTION COMPLÈTE ===")
    cmd_init()
    cmd_scrape()
    cmd_apis()
    cmd_pipeline()
    cmd_analytics()
    logger.info("=== TERMINÉ ===")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    command = sys.argv[1].lower()

    if command == "init":
        cmd_init()
    elif command == "parse":
        cmd_parse()
    elif command == "scrape":
        priority = None
        sector = None
        method = None
        args = sys.argv[2:]
        i = 0
        while i < len(args):
            if args[i] == "--priority" and i + 1 < len(args):
                priority = args[i + 1]
                i += 2
            elif args[i] == "--sector" and i + 1 < len(args):
                sector = int(args[i + 1])
                i += 2
            elif args[i] == "--method" and i + 1 < len(args):
                method = args[i + 1]
                i += 2
            else:
                i += 1
        cmd_scrape(priority=priority, sector=sector, method=method)
    elif command == "pipeline":
        cmd_pipeline()
    elif command == "apis":
        cmd_apis()
    elif command == "analytics":
        cmd_analytics()
    elif command == "dashboard":
        cmd_dashboard()
    elif command == "full":
        cmd_full()
    else:
        print(f"Commande inconnue: {command}")
        print(__doc__)
        sys.exit(1)


if __name__ == "__main__":
    main()

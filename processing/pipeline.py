# ──────────────────────────────────────────────────────────────────────────────
# EDR Scraping — Pipeline de nettoyage et normalisation
# ──────────────────────────────────────────────────────────────────────────────
import re
import logging
from datetime import datetime, date

import pandas as pd
from sqlalchemy import text

from database.models import engine, get_session, Fonds, Marche, ScrapeRaw

logger = logging.getLogger("edr.pipeline")


# ── Nettoyage générique ──────────────────────────────────────────────────────

def normalize_percent(value) -> float | None:
    """Normalise une valeur pourcentage (chaîne ou nombre)."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", ".").replace("%", "").replace(" ", "")
    text = re.sub(r"[^\d.\-]", "", text)
    try:
        return float(text)
    except ValueError:
        return None


def normalize_currency(value, unit: str = "EUR") -> float | None:
    """Normalise un montant monétaire en millions EUR."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    multiplier = 1.0
    if "Mrd" in text or "bn" in text.lower() or "billion" in text.lower():
        multiplier = 1000.0
    elif "Billionen" in text or "trillion" in text.lower():
        multiplier = 1_000_000.0
    text = re.sub(r"[^\d.,\-]", "", text)
    if "," in text and "." in text:
        text = text.replace(".", "").replace(",", ".")
    elif "," in text:
        text = text.replace(",", ".")
    try:
        return float(text) * multiplier
    except ValueError:
        return None


def normalize_date(value) -> date | None:
    """Normalise une date."""
    if value is None:
        return None
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    text = str(value).strip()
    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y", "%m/%Y", "%Y"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def clean_text(text: str | None) -> str | None:
    """Nettoyage basique de texte."""
    if not text:
        return None
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", text)
    return text[:10000] if text else None


# ── Pipeline Fonds ───────────────────────────────────────────────────────────

def clean_fonds_table():
    """Normalise les données dans la table fonds."""
    db = get_session()
    try:
        fonds = db.query(Fonds).all()
        updated = 0
        for f in fonds:
            changed = False
            for attr in ("perf_ytd_pct", "perf_1y_pct", "perf_3y_pct", "perf_5y_pct", "ter_pct"):
                val = getattr(f, attr)
                norm = normalize_percent(val)
                if norm != val:
                    setattr(f, attr, norm)
                    changed = True
            if f.aum_meur:
                norm = normalize_currency(f.aum_meur)
                if norm != f.aum_meur:
                    f.aum_meur = norm
                    changed = True
            if f.nom_fonds:
                f.nom_fonds = clean_text(f.nom_fonds)
            if changed:
                updated += 1
        db.commit()
        logger.info(f"Pipeline fonds: {updated}/{len(fonds)} mis à jour")
    finally:
        db.close()


def deduplicate_fonds():
    """Déduplique les fonds par ISIN + date."""
    db = get_session()
    try:
        # Garder l'entrée la plus récente par ISIN/date
        subq = text("""
            DELETE FROM fonds
            WHERE id NOT IN (
                SELECT MAX(id) FROM fonds
                WHERE isin IS NOT NULL
                GROUP BY isin, date_donnees
            )
            AND isin IS NOT NULL
        """)
        result = db.execute(subq)
        db.commit()
        logger.info(f"Déduplication: {result.rowcount} doublons supprimés")
    finally:
        db.close()


# ── Pipeline Marché ──────────────────────────────────────────────────────────

def clean_marche_table():
    """Normalise les données marché."""
    db = get_session()
    try:
        entries = db.query(Marche).all()
        for m in entries:
            if m.valeur:
                m.valeur = normalize_currency(m.valeur)
            if m.categorie:
                m.categorie = clean_text(m.categorie)
        db.commit()
        logger.info(f"Pipeline marché: {len(entries)} entrées normalisées")
    finally:
        db.close()


# ── Extraction structurée depuis scrape_raw ──────────────────────────────────

def extract_structured_data():
    """
    Lit le contenu brut de scrape_raw et extrait les données structurées
    vers les tables fonds, reglementation et marche via les spiders.
    """
    import asyncio
    from bs4 import BeautifulSoup
    from scraper.spiders.funds_spider import FondswebSpider, MorningstarSpider
    from scraper.spiders.regulator_spider import BaFinSpider, ESMASpider
    from scraper.spiders.market_spider import BVISpider, BundesbankSpider

    db = get_session()
    raw_rows = (
        db.query(ScrapeRaw)
        .filter(ScrapeRaw.contenu_text != "", ScrapeRaw.status_code == 200)
        .all()
    )
    logger.info(f"Extraction structurée sur {len(raw_rows)} pages OK...")

    funds_spider     = FondswebSpider()
    mstar_spider     = MorningstarSpider()
    bafin_spider     = BaFinSpider()
    esma_spider      = ESMASpider()
    bvi_spider       = BVISpider()
    buba_spider      = BundesbankSpider()

    fonds_total = reg_total = market_total = 0

    loop = asyncio.new_event_loop()

    def run(coro):
        return loop.run_until_complete(coro)

    for raw in raw_rows:
        url   = (raw.url or "").lower()
        html  = raw.contenu_html or raw.contenu_text or ""
        sid   = raw.source_id

        try:
            if "fondsweb" in url:
                items = run(funds_spider.parse_fund_page(html, raw.url))
                funds_spider.store_funds(items, sid)
                fonds_total += len(items)
            elif "morningstar" in url:
                items = run(mstar_spider.parse_search_results(html))
                mstar_spider.store_funds(items, sid)
                fonds_total += len(items)
            elif "bafin" in url:
                items = run(bafin_spider.parse_page(html, raw.url))
                bafin_spider.store_regulations(items, sid)
                reg_total += len(items)
            elif "esma" in url or "eur-lex" in url:
                items = run(esma_spider.parse_page(html, raw.url))
                esma_spider.store_regulations(items, sid)
                reg_total += len(items)
            elif "bvi" in url:
                items = run(bvi_spider.parse_stats_page(html, raw.url))
                bvi_spider.store_market_data(items, sid)
                market_total += len(items)
            elif "bundesbank" in url:
                items = run(buba_spider.parse_page(html, raw.url))
                buba_spider.store_market_data(items, sid)
                market_total += len(items)
        except Exception as e:
            logger.debug(f"Extraction échouée {raw.url}: {e}")

    loop.close()

    db.close()
    logger.info(
        f"Extraction terminée — fonds: {fonds_total}, "
        f"règlements: {reg_total}, marché: {market_total}"
    )


# ── Pipeline complète ────────────────────────────────────────────────────────

def run_pipeline():
    """Exécute l'intégralité du pipeline de nettoyage."""
    logger.info("Démarrage pipeline nettoyage...")
    extract_structured_data()
    clean_fonds_table()
    deduplicate_fonds()
    clean_marche_table()
    logger.info("Pipeline terminé")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_pipeline()

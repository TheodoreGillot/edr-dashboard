# ──────────────────────────────────────────────────────────────────────────────
# Parser du fichier scraping_links.txt → taxonomie structurée
# ──────────────────────────────────────────────────────────────────────────────
import re
import json
from pathlib import Path
from urllib.parse import urlparse
from config.settings import (
    LINKS_FILE, DATA_DIR, SECTORS, SOURCE_TYPE_RULES,
    DYNAMIC_JS_DOMAINS, PDF_PATTERNS, SCRAPING_PRIORITY
)


def parse_links_file(filepath: Path = LINKS_FILE) -> list[dict]:
    """Parse le fichier de liens et extrait URLs, secteurs, sous-catégories."""
    text = filepath.read_text(encoding="utf-8")
    entries = []
    current_sector = 0
    current_subcategory = "Général"

    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue

        # Détection secteur
        sector_match = re.match(r"^Secteur\s+(\d+)", line, re.IGNORECASE)
        if sector_match:
            current_sector = int(sector_match.group(1))
            continue

        sector_header = re.match(r"^SECTEUR\s+(\d+)", line)
        if sector_header:
            current_sector = int(sector_header.group(1))
            continue

        # Détection sous-catégorie (lignes en MAJUSCULES avec —)
        if re.match(r"^[A-ZÀÉÈÊËÎÏÔÙÛÜÇ\s&/—\-\(\)\.]{10,}$", line) and "http" not in line:
            current_subcategory = line.strip("_ ").strip()
            continue

        # Extraction URL | description
        url_match = re.match(r"(https?://[^\s|]+)\s*\|?\s*(.*)", line)
        if url_match:
            url = url_match.group(1).strip()
            description = url_match.group(2).strip()
            domain = urlparse(url).netloc.lower().replace("www.", "")

            entry = {
                "url": url,
                "description": description,
                "secteur": current_sector,
                "secteur_nom": SECTORS.get(current_sector, "Inconnu"),
                "sous_categorie": current_subcategory,
                "domain": domain,
                "type_source": _classify_source(domain),
                "nature_technique": _classify_technique(url, domain),
                "priorite": _classify_priority(domain),
                "methode_scraping": _determine_method(url, domain),
            }
            entries.append(entry)

    return entries


def _classify_source(domain: str) -> str:
    for stype, domains in SOURCE_TYPE_RULES.items():
        for d in domains:
            if d in domain:
                return stype
    return "autre"


def _classify_technique(url: str, domain: str) -> str:
    if any(p in url.lower() for p in PDF_PATTERNS):
        return "pdf"
    if any(d in domain for d in DYNAMIC_JS_DOMAINS):
        return "dynamique_js"
    return "html_statique"


def _classify_priority(domain: str) -> str:
    for level in ("high", "medium"):
        if any(d in domain for d in SCRAPING_PRIORITY[level]["domains"]):
            return level
    return "low"


def _determine_method(url: str, domain: str) -> str:
    if any(p in url.lower() for p in PDF_PATTERNS):
        return "pdf_parser"
    if any(d in domain for d in DYNAMIC_JS_DOMAINS):
        return "playwright"
    return "requests_bs4"


def build_taxonomy(entries: list[dict]) -> dict:
    """Construit une taxonomie hiérarchique secteur > sous-catégorie > URLs."""
    taxonomy = {}
    for e in entries:
        sec = f"{e['secteur']} — {e['secteur_nom']}"
        sub = e["sous_categorie"]
        taxonomy.setdefault(sec, {}).setdefault(sub, []).append({
            "url": e["url"],
            "description": e["description"],
            "type_source": e["type_source"],
            "nature_technique": e["nature_technique"],
            "priorite": e["priorite"],
            "methode_scraping": e["methode_scraping"],
        })
    return taxonomy


def export_taxonomy(output: Path = DATA_DIR / "taxonomy.json"):
    entries = parse_links_file()
    taxonomy = build_taxonomy(entries)
    output.write_text(json.dumps(taxonomy, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"[OK] Taxonomie exportée : {output} ({len(entries)} URLs)")
    return entries


def get_stats(entries: list[dict]) -> dict:
    from collections import Counter
    return {
        "total_urls": len(entries),
        "par_secteur": dict(Counter(e["secteur_nom"] for e in entries)),
        "par_type_source": dict(Counter(e["type_source"] for e in entries)),
        "par_nature_technique": dict(Counter(e["nature_technique"] for e in entries)),
        "par_methode": dict(Counter(e["methode_scraping"] for e in entries)),
        "par_priorite": dict(Counter(e["priorite"] for e in entries)),
    }


if __name__ == "__main__":
    entries = export_taxonomy()
    stats = get_stats(entries)
    print(json.dumps(stats, ensure_ascii=False, indent=2))

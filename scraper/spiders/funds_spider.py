# ──────────────────────────────────────────────────────────────────────────────
# Spider Fondsweb — Extraction données fonds (Playwright pour JS)
# ──────────────────────────────────────────────────────────────────────────────
import re
import logging
from datetime import date, datetime
from bs4 import BeautifulSoup

from database.models import get_session, Fonds, Source

logger = logging.getLogger("edr.spider.fondsweb")


class FondswebSpider:
    """Extraction structurée des données fonds depuis fondsweb.com."""

    BASE_URL = "https://www.fondsweb.com/de/suche?q={query}"
    RANKING_URL = "https://www.fondsweb.com/de/ranglisten/fonds/beliebteste"

    async def parse_fund_page(self, html: str, source_url: str) -> list[dict]:
        """Parse une page de résultats ou fiche fonds Fondsweb."""
        soup = BeautifulSoup(html, "html.parser")
        funds = []

        # Tableaux de classement
        for row in soup.select("table tbody tr, .fund-list-item, .ranking-item"):
            fund = self._extract_fund_row(row)
            if fund:
                funds.append(fund)

        # Fiche détaillée
        if not funds:
            fund = self._extract_detail_page(soup)
            if fund:
                funds.append(fund)

        return funds

    def _extract_fund_row(self, row) -> dict | None:
        """Extrait un fonds d'une ligne de tableau."""
        try:
            cells = row.find_all("td")
            if len(cells) < 3:
                return None

            name_el = row.select_one("a[href*='/de/'], .fund-name, td:first-child a")
            isin_el = row.select_one(".isin, [data-isin]")

            name = name_el.get_text(strip=True) if name_el else cells[0].get_text(strip=True)
            if not name or len(name) < 3:
                return None

            isin = None
            if isin_el:
                isin = isin_el.get("data-isin") or isin_el.get_text(strip=True)
            if not isin:
                isin_match = re.search(r'[A-Z]{2}[A-Z0-9]{9}\d', row.get_text())
                if isin_match:
                    isin = isin_match.group(0)

            return {
                "nom_fonds": name[:200],
                "isin": isin,
                "societe_gestion": self._extract_text(row, ".kag, .fund-company"),
                "categorie": self._extract_text(row, ".category, .fund-category"),
                "perf_ytd_pct": self._parse_percent(cells, -3),
                "perf_1y_pct": self._parse_percent(cells, -2),
                "perf_3y_pct": self._parse_percent(cells, -1),
                "date_donnees": date.today(),
            }
        except Exception as e:
            logger.debug(f"Extraction row échouée: {e}")
            return None

    def _extract_detail_page(self, soup: BeautifulSoup) -> dict | None:
        """Extrait les détails d'une fiche fonds individuelle."""
        try:
            name = soup.select_one("h1, .fund-name, .fondsname")
            if not name:
                return None

            text = soup.get_text()
            isin_match = re.search(r'ISIN[:\s]*([A-Z]{2}[A-Z0-9]{9}\d)', text)
            ter_match = re.search(r'TER[:\s]*([\d,\.]+)\s*%', text)
            aum_match = re.search(r'Fondsvolumen[:\s]*([\d.,]+)\s*(Mio|Mrd)', text)

            aum = None
            if aum_match:
                val = float(aum_match.group(1).replace(".", "").replace(",", "."))
                if "Mrd" in aum_match.group(2):
                    val *= 1000
                aum = val

            return {
                "nom_fonds": name.get_text(strip=True)[:200],
                "isin": isin_match.group(1) if isin_match else None,
                "ter_pct": float(ter_match.group(1).replace(",", ".")) if ter_match else None,
                "aum_meur": aum,
                "date_donnees": date.today(),
            }
        except Exception:
            return None

    def _extract_text(self, el, selector: str) -> str | None:
        found = el.select_one(selector)
        return found.get_text(strip=True) if found else None

    def _parse_percent(self, cells, index: int) -> float | None:
        try:
            text = cells[index].get_text(strip=True).replace(",", ".").replace("%", "").strip()
            return float(text) if text and text != "-" else None
        except (IndexError, ValueError):
            return None

    def store_funds(self, funds: list[dict], source_id: int):
        """Persiste les fonds extraits dans la DB."""
        db = get_session()
        inserted = 0
        try:
            for f in funds:
                if not f.get("nom_fonds"):
                    continue
                existing = None
                if f.get("isin") and f.get("date_donnees"):
                    existing = db.query(Fonds).filter_by(
                        isin=f["isin"], date_donnees=f["date_donnees"]
                    ).first()
                if existing:
                    for k, v in f.items():
                        if v is not None and hasattr(existing, k):
                            setattr(existing, k, v)
                else:
                    fonds = Fonds(source_id=source_id, **f)
                    db.add(fonds)
                    inserted += 1
            db.commit()
            logger.info(f"Fondsweb: {inserted} fonds insérés")
        except Exception as e:
            db.rollback()
            logger.error(f"Erreur store fonds: {e}")
        finally:
            db.close()


class MorningstarSpider:
    """Extraction données fonds depuis morningstar.de."""

    async def parse_search_results(self, html: str) -> list[dict]:
        soup = BeautifulSoup(html, "html.parser")
        funds = []

        for row in soup.select("table tbody tr, .search-results-item, [data-fund-row]"):
            fund = self._extract_row(row)
            if fund:
                funds.append(fund)
        return funds

    def _extract_row(self, row) -> dict | None:
        try:
            text = row.get_text()
            isin_match = re.search(r'([A-Z]{2}[A-Z0-9]{9}\d)', text)
            name_el = row.select_one("a, .fund-name, td:first-child")
            if not name_el:
                return None

            rating_el = row.select_one("[class*='star'], .rating")
            rating = None
            if rating_el:
                stars = rating_el.get("data-rating") or rating_el.get_text(strip=True)
                try:
                    rating = int(stars)
                except (ValueError, TypeError):
                    pass

            cells = row.find_all("td")
            return {
                "nom_fonds": name_el.get_text(strip=True)[:200],
                "isin": isin_match.group(1) if isin_match else None,
                "rating_morningstar": rating,
                "categorie": self._cell_text(cells, 2),
                "perf_1y_pct": self._parse_pct(cells, -2),
                "perf_3y_pct": self._parse_pct(cells, -1),
                "date_donnees": date.today(),
            }
        except Exception:
            return None

    def _cell_text(self, cells, idx) -> str | None:
        try:
            return cells[idx].get_text(strip=True) or None
        except IndexError:
            return None

    def _parse_pct(self, cells, idx) -> float | None:
        try:
            t = cells[idx].get_text(strip=True).replace(",", ".").replace("%", "")
            return float(t) if t and t != "-" else None
        except (IndexError, ValueError):
            return None

    def store_funds(self, funds: list[dict], source_id: int):
        db = get_session()
        inserted = 0
        try:
            for f in funds:
                if not f.get("nom_fonds"):
                    continue
                existing = None
                if f.get("isin") and f.get("date_donnees"):
                    existing = db.query(Fonds).filter_by(
                        isin=f["isin"], date_donnees=f["date_donnees"]
                    ).first()
                if existing:
                    for k, v in f.items():
                        if v is not None and hasattr(existing, k):
                            setattr(existing, k, v)
                else:
                    fonds = Fonds(source_id=source_id, **f)
                    db.add(fonds)
                    inserted += 1
            db.commit()
            logger.info(f"Morningstar: {inserted} fonds insérés")
        except Exception as e:
            db.rollback()
            logger.error(f"Erreur store Morningstar: {e}")
        finally:
            db.close()

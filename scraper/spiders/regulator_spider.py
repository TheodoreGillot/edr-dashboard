# ──────────────────────────────────────────────────────────────────────────────
# Spider Régulateur — BaFin, ESMA, EUR-Lex
# ──────────────────────────────────────────────────────────────────────────────
import re
import logging
from datetime import date
from bs4 import BeautifulSoup

from database.models import get_session, Reglementation

logger = logging.getLogger("edr.spider.regulator")


class BaFinSpider:
    """Extraction données réglementaires depuis bafin.de."""

    async def parse_page(self, html: str, source_url: str) -> list[dict]:
        soup = BeautifulSoup(html, "html.parser")
        regs = []

        # Pages de listes (Merkblätter, Rundschreiben, etc.)
        for item in soup.select("article, .list-item, .teaser, .publication-item, li.result"):
            reg = self._extract_item(item, source_url)
            if reg:
                regs.append(reg)

        # Page unique — détail d'un texte réglementaire
        if not regs:
            reg = self._extract_detail(soup, source_url)
            if reg:
                regs.append(reg)

        return regs

    def _extract_item(self, el, source_url: str) -> dict | None:
        try:
            title_el = el.select_one("h2, h3, a, .title, .heading")
            if not title_el:
                return None
            title = title_el.get_text(strip=True)
            if len(title) < 5:
                return None

            link = title_el.get("href") or ""
            if link and not link.startswith("http"):
                link = "https://www.bafin.de" + link

            date_el = el.select_one("time, .date, .publish-date")
            pub_date = self._parse_date(date_el.get_text(strip=True)) if date_el else None

            text = el.get_text(separator=" ", strip=True)[:2000]
            type_texte = self._detect_type(title, text)

            return {
                "titre": title[:500],
                "organisme": "BaFin",
                "type_texte": type_texte,
                "resume": text[:1000],
                "date_publication": pub_date,
                "url_document": link or source_url,
            }
        except Exception:
            return None

    def _extract_detail(self, soup: BeautifulSoup, source_url: str) -> dict | None:
        try:
            title = soup.select_one("h1, .page-title")
            if not title:
                return None
            text = soup.get_text(separator=" ", strip=True)[:5000]
            type_texte = self._detect_type(title.get_text(), text)

            # Extraire les contraintes mentionnées
            contraintes = []
            for pattern in [r"(?:müssen|verpflichtet|erforderlich|Pflicht)[^.]{10,100}\.", 
                          r"(?:Voraussetzung|Anforderung)[^.]{10,100}\."]:
                contraintes.extend(re.findall(pattern, text, re.IGNORECASE))

            return {
                "titre": title.get_text(strip=True)[:500],
                "organisme": "BaFin",
                "type_texte": type_texte,
                "resume": text[:2000],
                "contraintes": " | ".join(contraintes[:5]) if contraintes else None,
                "url_document": source_url,
            }
        except Exception:
            return None

    def _detect_type(self, title: str, text: str) -> str:
        combined = (title + " " + text).lower()
        if "merkblatt" in combined:
            return "merkblatt"
        if "rundschreiben" in combined:
            return "circulaire"
        if "richtlinie" in combined or "directive" in combined:
            return "directive"
        if "verordnung" in combined or "regulation" in combined or "règlement" in combined:
            return "reglement"
        if "guideline" in combined or "leitlinie" in combined:
            return "guideline"
        return "autre"

    def _parse_date(self, text: str) -> date | None:
        for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d/%m/%Y", "%d. %B %Y"):
            try:
                from datetime import datetime
                return datetime.strptime(text.strip(), fmt).date()
            except ValueError:
                continue
        # Regex fallback
        m = re.search(r"(\d{2})\.(\d{2})\.(\d{4})", text)
        if m:
            try:
                return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
            except ValueError:
                pass
        return None

    def store_regulations(self, regs: list[dict], source_id: int):
        db = get_session()
        inserted = 0
        try:
            for r in regs:
                if not r.get("titre"):
                    continue
                reg = Reglementation(source_id=source_id, **r)
                db.add(reg)
                inserted += 1
            db.commit()
            logger.info(f"BaFin: {inserted} entrées réglementaires insérées")
        except Exception as e:
            db.rollback()
            logger.error(f"Erreur store BaFin: {e}")
        finally:
            db.close()


class ESMASpider:
    """Extraction données depuis esma.europa.eu."""

    async def parse_page(self, html: str, source_url: str) -> list[dict]:
        soup = BeautifulSoup(html, "html.parser")
        regs = []

        for item in soup.select(".views-row, article, .node, .ecl-list-item, tr"):
            reg = self._extract(item, source_url)
            if reg:
                regs.append(reg)

        if not regs:
            title = soup.select_one("h1")
            if title:
                text = soup.get_text(separator=" ", strip=True)[:5000]
                regs.append({
                    "titre": title.get_text(strip=True)[:500],
                    "organisme": "ESMA",
                    "type_texte": "guideline",
                    "resume": text[:2000],
                    "url_document": source_url,
                })
        return regs

    def _extract(self, el, source_url) -> dict | None:
        try:
            title_el = el.select_one("a, h3, h2, .title")
            if not title_el:
                return None
            title = title_el.get_text(strip=True)
            if len(title) < 5:
                return None

            link = title_el.get("href", "")
            if link and not link.startswith("http"):
                link = "https://www.esma.europa.eu" + link

            return {
                "titre": title[:500],
                "organisme": "ESMA",
                "type_texte": "guideline",
                "resume": el.get_text(separator=" ", strip=True)[:1000],
                "url_document": link or source_url,
            }
        except Exception:
            return None

    def store_regulations(self, regs: list[dict], source_id: int):
        db = get_session()
        inserted = 0
        try:
            for r in regs:
                if not r.get("titre"):
                    continue
                reg = Reglementation(source_id=source_id, **r)
                db.add(reg)
                inserted += 1
            db.commit()
            logger.info(f"ESMA: {inserted} entrées insérées")
        except Exception as e:
            db.rollback()
            logger.error(f"Erreur store ESMA: {e}")
        finally:
            db.close()

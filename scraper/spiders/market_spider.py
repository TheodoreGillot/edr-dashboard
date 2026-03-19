# ──────────────────────────────────────────────────────────────────────────────
# Spider Marché — BVI, Bundesbank, EFAMA
# ──────────────────────────────────────────────────────────────────────────────
import re
import logging
from datetime import date
from bs4 import BeautifulSoup

from database.models import get_session, Marche

logger = logging.getLogger("edr.spider.market")


class BVISpider:
    """Extraction statistiques BVI (AUM, flux, ETF)."""

    async def parse_stats_page(self, html: str, source_url: str) -> list[dict]:
        soup = BeautifulSoup(html, "html.parser")
        data = []

        # Tableaux de données BVI
        for table in soup.select("table"):
            rows = table.select("tr")
            if len(rows) < 2:
                continue
            headers = [th.get_text(strip=True) for th in rows[0].select("th, td")]
            for row in rows[1:]:
                cells = [td.get_text(strip=True) for td in row.select("td")]
                if len(cells) < 2:
                    continue
                entry = self._parse_stat_row(headers, cells, source_url)
                if entry:
                    data.append(entry)

        # Extraire chiffres clés du texte
        text = soup.get_text()
        inline_data = self._extract_inline_stats(text)
        data.extend(inline_data)

        return data

    def _parse_stat_row(self, headers: list[str], cells: list[str], source_url: str) -> dict | None:
        try:
            categorie = cells[0] if cells else None
            valeur_text = cells[-1] if len(cells) > 1 else None
            if not valeur_text:
                return None

            valeur = self._parse_number(valeur_text)
            if valeur is None:
                return None

            metrique = "aum_total"
            if "absatz" in source_url.lower() or "flux" in source_url.lower():
                metrique = "flux_net"
            elif "etf" in source_url.lower():
                metrique = "aum_etf"

            return {
                "entite": "BVI",
                "metrique": metrique,
                "categorie": categorie,
                "valeur": valeur,
                "unite": "MEUR",
                "date_donnees": date.today(),
            }
        except Exception:
            return None

    def _extract_inline_stats(self, text: str) -> list[dict]:
        """Extrait les chiffres clés mentionnés dans le texte."""
        data = []
        patterns = [
            (r"Fondsvermögen[:\s]*([\d.,]+)\s*(Mrd|Mio|Billionen)", "aum_total"),
            (r"Mittelaufkommen[:\s]*([\d.,]+)\s*(Mrd|Mio)", "flux_net"),
            (r"Spezialfonds[:\s]*([\d.,]+)\s*(Mrd|Mio)", "aum_spezialfonds"),
            (r"Publikumsfonds[:\s]*([\d.,]+)\s*(Mrd|Mio)", "aum_publikumsfonds"),
            (r"ETF[:\s]*([\d.,]+)\s*(Mrd|Mio)", "aum_etf"),
        ]
        for pattern, metrique in patterns:
            m = re.search(pattern, text, re.IGNORECASE)
            if m:
                val = float(m.group(1).replace(".", "").replace(",", "."))
                unit = m.group(2)
                if "Billionen" in unit:
                    val *= 1_000_000
                elif "Mrd" in unit:
                    val *= 1000
                data.append({
                    "entite": "BVI",
                    "metrique": metrique,
                    "valeur": val,
                    "unite": "MEUR",
                    "date_donnees": date.today(),
                })
        return data

    def _parse_number(self, text: str) -> float | None:
        text = text.strip().replace(" ", "")
        text = re.sub(r"[^\d.,-]", "", text)
        if not text:
            return None
        try:
            if "," in text and "." in text:
                text = text.replace(".", "").replace(",", ".")
            elif "," in text:
                text = text.replace(",", ".")
            return float(text)
        except ValueError:
            return None

    def store_market_data(self, data: list[dict], source_id: int):
        db = get_session()
        inserted = 0
        try:
            for d in data:
                entry = Marche(source_id=source_id, **d)
                db.add(entry)
                inserted += 1
            db.commit()
            logger.info(f"BVI: {inserted} données marché insérées")
        except Exception as e:
            db.rollback()
            logger.error(f"Erreur store BVI: {e}")
        finally:
            db.close()


class BundesbankSpider:
    """Extraction statistiques Bundesbank."""

    async def parse_page(self, html: str, source_url: str) -> list[dict]:
        soup = BeautifulSoup(html, "html.parser")
        data = []

        for table in soup.select("table"):
            rows = table.select("tr")
            for row in rows[1:]:
                cells = [td.get_text(strip=True) for td in row.select("td")]
                if len(cells) >= 2:
                    val = self._parse_number(cells[-1])
                    if val is not None:
                        data.append({
                            "entite": "Bundesbank",
                            "metrique": "stat_fonds",
                            "categorie": cells[0][:200],
                            "valeur": val,
                            "unite": "MEUR",
                            "date_donnees": date.today(),
                        })
        return data

    def _parse_number(self, text: str) -> float | None:
        text = re.sub(r"[^\d.,-]", "", text.strip())
        if not text:
            return None
        try:
            if "," in text and "." in text:
                text = text.replace(".", "").replace(",", ".")
            elif "," in text:
                text = text.replace(",", ".")
            return float(text)
        except ValueError:
            return None

    def store_market_data(self, data: list[dict], source_id: int):
        db = get_session()
        try:
            for d in data:
                entry = Marche(source_id=source_id, **d)
                db.add(entry)
            db.commit()
        except Exception as e:
            db.rollback()
            logger.error(f"Erreur store Bundesbank: {e}")
        finally:
            db.close()

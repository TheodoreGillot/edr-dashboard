# ──────────────────────────────────────────────────────────────────────────────
# Clients API découverts par rétro-ingénierie des sites
# ──────────────────────────────────────────────────────────────────────────────
"""
Morningstar DE : API REST sur global.morningstar.com/api/v1/de/
  - auth  : GET /oauth/token/ → JWT anonyme – doit être capturé via Playwright
             (l'endpoint retourne HTTP 202 si appelé sans cookies de session)
  - quotes : GET /stores/realtime/quotes?securities=0P00001FKV,0P00012PD5,...
             → HTTP 200 quand appelé DEPUIS le contexte browser (cookies valides)
  - Le screener requiert un compte (page de login chargée)

JustETF (remplace Fondsweb) :
  - https://www.justetf.com/de/search.html?search=ETFS&sortField=fundSize&sortOrder=desc
  - Table DataTables rendue par JS : 30 ETFs taille maximale avec ISIN, AuM, TER, perf 1Y

BVI : données textuelles HTML sur /ueber-die-branche/deutschland-groesster-fondsmarkt-der-eu/
  - "16 Billionen Euro" → marché fonds EU total
  - "4.164 Mrd. Euro"  → part Allemagne (notation allemande : 4.164 Mrd = 4164 Mrd = 4,164 Billionen)
  - "8,0 %"            → taux de croissance annuel moyen Allemagne (2014-2024)
  - "31/69 %"          → répartition investisseurs privés/institutionnels
"""
import asyncio
import json
import re
import logging
from datetime import date

from bs4 import BeautifulSoup

logger = logging.getLogger("edr.api_clients")

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")

BASE_MSTAR = "https://global.morningstar.com/api/v1/de"



# ─────────────────────────────────────────────────────────────────────────────
# CLIENT MORNINGSTAR  – via Playwright (token + cookies de session requis)
# ─────────────────────────────────────────────────────────────────────────────

class MorningstarAPIClient:
    """
    Client Morningstar utilisant Playwright pour obtenir le token JWT ET
    exécuter les appels API depuis le contexte browser (cookies de session inclus).

    L'endpoint /oauth/token/ retourne HTTP 202 quand appelé via aiohttp sans
    cookies. Il faut charger la page d'accueil via Playwright pour initialiser
    la session, puis capturer le token émis automatiquement.
    """

    # IDs Morningstar confirmés fonctionnels (phase 1 de découverte API)
    KNOWN_FUND_IDS = [
        # ETF indiciels mondiaux
        "0P00001FKV", "0P00012PD5", "0P0001AK03", "0P00005JIW",
        "0P0000TVHK", "0P00000OYL", "0P00006CEZ", "0P0000TXS1",
        "0P00004VNV", "0P00009G3N", "0P0001A14X", "0P0001AIMD",
        "0P00011O8H", "0P0000XMQJ", "0P00001GKR", "0P000034E5",
        "0P00009F8K", "0P0000WZEU", "0P00006H9I",
        # Gestion active Allemagne / France / Luxembourg
        "0P00000LFB", "0P00000P3A", "0P0000025V", "0P00000IJE",
        "0P0000001S", "0P0000005C", "0P00000E5N", "0P0000014A",
        "0P00001LS3", "0P0000ARWH", "0P00008Q4P", "0P0000W1IM",
        # Fonds mixtes / obligataires populaires
        "0P0000YC8M", "0P00019GVA", "0P0000J8HJ", "0P00001D2T",
    ]

    async def get_fund_data(self) -> list[dict]:
        """
        Charge morningstar.de, capture le token JWT, puis appelle
        /stores/realtime/quotes et /stores/realtime/timeseries pour les
        IDs connus depuis le contexte browser (cookies inclus).
        """
        from playwright.async_api import async_playwright

        funds = []
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            ctx = await browser.new_context(user_agent=UA, locale="de-DE")
            page = await ctx.new_page()

            token = None

            async def capture_token(resp):
                nonlocal token
                if "oauth/token" in resp.url and resp.status == 200:
                    try:
                        body = await resp.text()
                        data = json.loads(body)
                        t = data.get("token", "")
                        if t:
                            token = t
                            logger.info(f"Morningstar token capturé ({len(t)} chars)")
                    except Exception:
                        pass

            page.on("response", capture_token)
            await page.goto("https://www.morningstar.de/de/", timeout=30000,
                            wait_until="networkidle")
            await page.wait_for_timeout(3000)

            if not token:
                logger.warning("Token Morningstar non capturé, session annulée")
                await browser.close()
                return []

            # Appel quotes depuis le contexte browser (hérite les cookies)
            ids_str = ",".join(self.KNOWN_FUND_IDS)
            quotes_raw = await page.evaluate(f"""
            async () => {{
                try {{
                    const r = await fetch(
                        "{BASE_MSTAR}/stores/realtime/quotes?securities={ids_str}",
                        {{headers: {{
                            "Authorization": "Bearer {token}",
                            "Accept": "application/json"
                        }}}}
                    );
                    if (r.status !== 200) return null;
                    return await r.json();
                }} catch(e) {{ return null; }}
            }}
            """)

            if quotes_raw and isinstance(quotes_raw, dict):
                logger.info(f"Morningstar quotes: {len(quotes_raw)} fonds")
                for mstar_id, data in quotes_raw.items():
                    fund = self._normalize_quote(mstar_id, data)
                    if fund:
                        funds.append(fund)

            # Appel timeseries pour perf annualisée
            ids_short = ",".join(self.KNOWN_FUND_IDS[:10])
            ts_raw = await page.evaluate(f"""
            async () => {{
                try {{
                    const r = await fetch(
                        "{BASE_MSTAR}/stores/realtime/timeseries?securities={ids_short}&days=365",
                        {{headers: {{
                            "Authorization": "Bearer {token}",
                            "Accept": "application/json"
                        }}}}
                    );
                    if (r.status !== 200) return null;
                    return await r.json();
                }} catch(e) {{ return null; }}
            }}
            """)

            if ts_raw and isinstance(ts_raw, dict):
                logger.info(f"Morningstar timeseries: {len(ts_raw)} fonds")
                # Enrichir les fonds avec la perf calculée depuis timeseries
                for mstar_id, ts_data in ts_raw.items():
                    perf = self._calc_perf_from_ts(ts_data)
                    # Trouve l'entrée existante ou crée une nouvelle
                    existing = next((f for f in funds if f.get("mstar_id") == mstar_id), None)
                    if existing:
                        if perf.get("perf_1y_pct") is not None:
                            existing["perf_1y_pct"] = perf["perf_1y_pct"]
                    elif perf:
                        funds.append({"mstar_id": mstar_id, "source": "morningstar_ts",
                                      "date_donnees": date.today(), **perf})

            await browser.close()

        logger.info(f"Morningstar total: {len(funds)} fonds")
        return funds

    def _normalize_quote(self, mstar_id: str, data: dict) -> dict | None:
        """Convertit un enregistrement quotes en format fonds."""
        if not isinstance(data, dict):
            return None

        def safe_float(d, *keys):
            for k in keys:
                v = d.get(k)
                if isinstance(v, dict):
                    v = v.get("value")
                try:
                    return float(v)
                except (TypeError, ValueError):
                    pass
            return None

        last_price = safe_float(data, "lastPrice", "closePrice", "nav")
        net_change = safe_float(data, "netChange")

        name = (data.get("name") or data.get("legalName")
                or data.get("fundName") or f"Fond {mstar_id}")
        isin = data.get("isin") or data.get("ISIN")

        return {
            "mstar_id": mstar_id,
            "nom_fonds": str(name)[:200] if name else f"Fond {mstar_id}",
            "isin": isin,
            "societe_gestion": data.get("brandingCompanyName") or data.get("managerName"),
            "categorie": data.get("morningstarCategoryName") or "Fonds",
            "devise": data.get("currency") or "EUR",
            "perf_ytd_pct": None,
            "perf_1y_pct": (net_change / last_price * 100) if (last_price and net_change) else None,
            "rating_morningstar": data.get("starRating"),
            "source": "morningstar_quotes",
            "date_donnees": date.today(),
        }

    def _calc_perf_from_ts(self, ts_data) -> dict:
        """Calcule la performance 1Y depuis une série temporelle."""
        try:
            if isinstance(ts_data, list) and len(ts_data) >= 2:
                first = ts_data[0]
                last = ts_data[-1]
                v0 = float(first.get("value") or first.get("close") or 0)
                v1 = float(last.get("value") or last.get("close") or 0)
                if v0 > 0:
                    return {"perf_1y_pct": round((v1 - v0) / v0 * 100, 2)}
        except Exception:
            pass
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# CLIENT JUSTETF  – Playwright, DataTables rendu côté client
# ─────────────────────────────────────────────────────────────────────────────

class JustETFClient:
    """
    Scrape les données ETF depuis JustETF via Playwright.
    Itère sur plusieurs catégories d'actifs pour maximiser la couverture.

    Table confirmée : ['table', 'table-hover', 'dataTable', 'no-footer']
    Colonnes : [0:'', 1:Fondsname, 2:Chart, 3:Fondsgröße(Mio €), 4:TER,
                5:52W, 6:1J%, 7:Ausschüttung, 8:Replikation, 9:Sparplan, 10:ISIN, 11:'']
    """

    _BASE = "https://www.justetf.com/de/search.html?search=ETFS&sortField=fundSize&sortOrder=desc"

    # {cat_name: (url, max_pages)}  — 25 ETF/page, 1 browser session réutilisée
    CATEGORY_URLS: dict[str, tuple[str, int]] = {
        "all":         (_BASE, 12),
        "bonds":       (_BASE + "&assetClass=class-bonds", 5),
        "commodities": (_BASE + "&assetClass=class-commodities", 3),
        "realEstate":  (_BASE + "&assetClass=class-realEstate", 2),
        "moneyMarket": (_BASE + "&assetClass=class-moneyMarket", 2),
        "dividend":    (_BASE + "&strategy=dividend", 3),
    }

    # sous_catégorie forcée par catégorie d'URL (si None → infer depuis nom)
    CATEGORY_SUBCATEGORY: dict[str, str | None] = {
        "all":         None,
        "bonds":       "Obligations",
        "commodities": "Matières premières",
        "realEstate":  "Immobilier",
        "moneyMarket": "Monétaire",
        "dividend":    "Actions dividende",
    }

    SEARCH_URL = _BASE    # rétrocompatibilité

    # Mapping nom-marque → société de gestion
    BRAND_MAP = {
        "ishares": "BlackRock",
        "xtrackers": "DWS Investment",
        "vanguard": "Vanguard",
        "spdr": "State Street Global Advisors",
        "amundi": "Amundi Asset Management",
        "invesco": "Invesco",
        "wisdomtree": "WisdomTree",
        "lyxor": "Amundi Asset Management",
        "dws": "DWS Investment",
        "ubs": "UBS Asset Management",
        "db x-trackers": "DWS Investment",
        "vaneck": "VanEck",
        "hsbc": "HSBC Asset Management",
        "pimco": "PIMCO",
        "flossbach": "Flossbach von Storch",
    }

    async def fetch_etfs(self, category_urls: dict | None = None) -> list[dict]:
        """
        Extrait les ETF depuis plusieurs catégories JustETF en réutilisant
        un seul contexte browser pour économiser les ressources.

        category_urls: dict {cat_name: (url, max_pages)}.
                       Par défaut, utilise CATEGORY_URLS.
        """
        from playwright.async_api import async_playwright

        if category_urls is None:
            category_urls = self.CATEGORY_URLS

        all_etfs: list[dict] = []
        seen_isins: set[str] = set()

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            ctx = await browser.new_context(user_agent=UA, locale="de-DE",
                                            viewport={"width": 1280, "height": 900})

            for cat_name, (url, max_pages) in category_urls.items():
                page = await ctx.new_page()
                try:
                    await page.goto(url, timeout=40000, wait_until="networkidle")
                    await page.wait_for_timeout(4000)

                    # Fermer la bannière cookie (Usercentrics) si présente
                    await self._dismiss_cookie_banner(page)

                    for page_idx in range(max_pages):
                        html = await page.content()
                        etfs = self._parse_etf_table(html, seen_isins, cat_name)
                        all_etfs.extend(etfs)
                        logger.info(f"JustETF [{cat_name}] p{page_idx+1}: {len(etfs)} ETF")

                        if not etfs:
                            break

                        # Utiliser JS click pour bypasser les overlays CSS
                        next_exists = await page.evaluate(
                            "() => { const n = document.querySelector('#etfsTable_next'); "
                            "return n ? !n.classList.contains('disabled') : false; }"
                        )
                        if not next_exists:
                            break
                        await page.evaluate(
                            "() => document.querySelector('#etfsTable_next').click()"
                        )
                        await page.wait_for_timeout(3500)

                except Exception as exc:
                    logger.error(f"JustETF [{cat_name}] erreur: {exc}")
                finally:
                    await page.close()

            await browser.close()

        logger.info(f"JustETF total: {len(all_etfs)} ETF uniques sur {len(category_urls)} catégories")
        return all_etfs

    async def _dismiss_cookie_banner(self, page) -> None:
        """Tente de fermer la bannière cookie Usercentrics sur JustETF."""
        try:
            # Supprimer l'overlay Usercentrics via JS (plus fiable que cliquer)
            await page.evaluate(
                "() => { const el = document.getElementById('usercentrics-cmp-ui'); "
                "if (el) el.remove(); }"
            )
            await page.wait_for_timeout(500)
        except Exception:
            pass

    def _parse_etf_table(self, html: str, seen_isins: set,
                          cat_name: str = "all") -> list[dict]:
        """Parse la table DataTables principale depuis le HTML rendu."""
        soup = BeautifulSoup(html, "html.parser")
        etfs = []

        forced_subcat = self.CATEGORY_SUBCATEGORY.get(cat_name)

        # Trouver la table DataTables du screener (la plus grande)
        target_table = None
        for t in soup.find_all("table", class_="dataTable"):
            rows = t.find_all("tr")
            if len(rows) > 5:  # table avec données
                target_table = t
                break

        if not target_table:
            logger.warning("JustETF: table DataTables non trouvée")
            return []

        rows = target_table.find_all("tr")
        for row in rows[1:]:  # skip header
            cells = row.find_all("td")
            if len(cells) < 11:
                continue

            try:
                name = cells[1].get_text(strip=True)
                # Ignorer les lignes publicitaires ("Anzeige|...")
                if not name or len(name) < 3 or name.startswith("Anzeige") or "| " in name[:12]:
                    continue

                isin = cells[10].get_text(strip=True)
                # Validation ISIN : 2 lettres + 10 alphanum
                if not re.match(r'^[A-Z]{2}[A-Z0-9]{10}$', isin):
                    # Essayer de trouver l'ISIN dans l'attribut data-* ou un lien
                    link = cells[1].find("a")
                    if link:
                        href = link.get("href", "")
                        m = re.search(r'isin=([A-Z]{2}[A-Z0-9]{10})', href)
                        isin = m.group(1) if m else None
                    else:
                        isin = None

                if isin and isin in seen_isins:
                    continue

                aum_meur = self._parse_german_number(cells[3].get_text(strip=True))
                ter_pct = self._parse_pct(cells[4].get_text(strip=True))
                perf_1y = self._parse_pct(cells[6].get_text(strip=True))
                distribution = cells[7].get_text(strip=True)  # Thesaurierend / Ausschüttend

                etf = {
                    "nom_fonds": name[:200],
                    "isin": isin,
                    "societe_gestion": self._extract_manager(name),
                    "categorie": "ETF",
                    "sous_categorie": forced_subcat or self._infer_subcategory(name),
                    "devise": "EUR",
                    "aum_meur": aum_meur,
                    "ter_pct": ter_pct,
                    "perf_ytd_pct": None,
                    "perf_1y_pct": perf_1y,
                    "perf_3y_pct": None,
                    "perf_5y_pct": None,
                    "article_sfdr": None,
                    "source": "justetf_scrape",
                    "date_donnees": date.today(),
                }
                etfs.append(etf)
                if isin:
                    seen_isins.add(isin)

            except Exception as exc:
                logger.debug(f"JustETF row parse error: {exc}")
                continue

        return etfs

    def _parse_german_number(self, s: str) -> float | None:
        """Convertit un nombre allemand en float (. = milliers, , = décimale)."""
        s = s.strip().replace("\xa0", "").replace(" ", "")
        if not s or s == "-":
            return None
        # Remove thousands separator (.) and convert decimal (,) to .
        s = s.replace(".", "").replace(",", ".")
        try:
            return float(s)
        except ValueError:
            return None

    def _parse_pct(self, s: str) -> float | None:
        """Convertit '13,91%' → 13.91."""
        s = s.strip().replace("%", "").replace(",", ".").strip()
        if not s or s == "-":
            return None
        try:
            return float(s)
        except ValueError:
            return None

    def _extract_manager(self, name: str) -> str:
        """Identifie la société de gestion depuis le nom du fonds."""
        name_lower = name.lower()
        for brand, manager in self.BRAND_MAP.items():
            if name_lower.startswith(brand):
                return manager
        # Fallback: première partie du nom
        return name.split()[0] if name else "Inconnu"

    def _infer_subcategory(self, name: str) -> str:
        """Infère une sous-catégorie depuis le nom de l'ETF."""
        name_up = name.upper()
        if "BOND" in name_up or "CORPORATE" in name_up or "TREASURY" in name_up:
            return "Obligations"
        if "GOLD" in name_up or "SILVER" in name_up or "COMMODITY" in name_up:
            return "Matières premières"
        if "REAL ESTATE" in name_up or "REIT" in name_up or "PROPERTY" in name_up:
            return "Immobilier"
        if "EMERGING" in name_up or "EMERGING MARKETS" in name_up:
            return "Actions marchés émergents"
        if "EUROPE" in name_up or "EURO" in name_up or "DAX" in name_up:
            return "Actions Europe"
        if "S&P 500" in name_up or "S&P500" in name_up or "US" in name_up:
            return "Actions USA"
        if "MSCI WORLD" in name_up or "WORLD" in name_up:
            return "Actions monde"
        return "Actions"


# ─────────────────────────────────────────────────────────────────────────────
# CLIENT BVI  – Playwright + regex patterns calibré sur le vrai contenu page
# ─────────────────────────────────────────────────────────────────────────────

class BVIDataClient:
    """
    Extrait les statistiques du marché allemand des fonds depuis les pages BVI.

    Données confirmées sur /ueber-die-branche/deutschland-groesster-fondsmarkt-der-eu/:
      - "16 Billionen Euro"  → marché fonds UE total (Déc 2024)
      - "4.164 Mrd. Euro"    → Fondsvermögen Allemagne (4164 Mrd = 4,164 Billionen)
      - "8,0 %"              → CAGR Allemagne 2014-2024
      - "26 %"               → part Allemagne dans marché UE
      - "31 %"               → investisseurs privés
      - "69 %"               → investisseurs institutionnels
    """

    STATS_URL = ("https://www.bvi.de/"
                 "ueber-die-branche/deutschland-groesster-fondsmarkt-der-eu/")
    STATS_URL2 = "https://www.bvi.de/service/statistik-und-research/"
    INVESTMENTSTAT_URL = "https://www.bvi.de/service/statistik-und-research/investmentstatistik/"

    # Patterns calé sur le texte réel de la page BVI
    # Format: (regex, metrique_name, categorie, segment)
    PATTERNS = [
        # EU total : "Fondsvermögen der EU in Höhe von 16 Billionen Euro"
        (r'Fondsverms?\S*gen\s+der\s+EU[^0-9]{0,50}?([\d.,]+)\s*(Billionen|Mrd\.?|Mrd|Mio)',
         "fondsvermogen_eu_total", "structure_marche", "UE"),
        # Fallback : direct "16 Billionen Euro"
        (r'\b(1[0-9](?:[.,]\d+)?)\s*(Billionen)\s*Euro',
         "fondsvermogen_eu_total", "structure_marche", "UE"),
        # Allemagne AuM : "4.164 Mrd. Euro" (notation allemande : 4.164 = 4164)
        (r'\b(4[.,]\d{3})\s*(Mrd)\.?\s*Euro',
         "fondsvermogen_deutschland", "structure_marche", "Allemagne"),
        # Ou "4,1 Billionen" (forme courte)
        (r'\b(4[.,]\d)\s*(Billionen)\s*Euro',
         "fondsvermogen_deutschland_court", "structure_marche", "Allemagne"),
        # CAGR Deutschland : "8,0 %" (CAGR 2014-2024 = 8.0 pour Allemagne)
        (r'\b(8[,.]\d)\s*%',
         "croissance_annuelle_allemagne", "performance", "Allemagne"),
        # Part privés (31%) / institutionnels (69%)
        (r'\b(3[0-9])\s*%',
         "anteil_privatanleger", "repartition_investisseurs", "Marchés"),
        (r'\b(6[0-9])\s*%',
         "anteil_institutionelle", "repartition_investisseurs", "Marchés"),
        # Segmentation par type de fonds (Fondsvermögen par catégorie)
        (r'Aktienfonds\D{0,100}?([\d.,]{3,})\s*(Mrd|Mio)\b',
         "fondsvermogen_aktienfonds", "segmentation_type", "Allemagne"),
        (r'Rentenfonds\D{0,100}?([\d.,]{3,})\s*(Mrd|Mio)\b',
         "fondsvermogen_rentenfonds", "segmentation_type", "Allemagne"),
        (r'Mischfonds\D{0,100}?([\d.,]{3,})\s*(Mrd|Mio)\b',
         "fondsvermogen_mischfonds", "segmentation_type", "Allemagne"),
        (r'Spezialfonds\D{0,100}?([\d.,]{3,})\s*(Mrd|Mio)\b',
         "fondsvermogen_spezialfonds", "segmentation_type", "Allemagne"),
        (r'Publikumsfonds\D{0,100}?([\d.,]{3,})\s*(Mrd|Mio)\b',
         "fondsvermogen_publikumsfonds", "segmentation_type", "Allemagne"),
        (r'[Oo]ffene\w*\s+Immobilienfonds\D{0,100}?([\d.,]{3,})\s*(Mrd|Mio)\b',
         "fondsvermogen_immobilienfonds", "segmentation_type", "Allemagne"),
        (r'Geldmarktfonds\D{0,100}?([\d.,]{3,})\s*(Mrd|Mio)\b',
         "fondsvermogen_geldmarktfonds", "segmentation_type", "Allemagne"),
        # Part Allemagne dans UE
        (r'\b(2[0-9])\s*%[^a-zA-Z]{0,30}(?:EU|Europa|europ)',
         "anteil_deutschland_eu", "structure_marche", "Allemagne"),
    ]

    async def fetch_stats(self) -> list[dict]:
        """Scrape les pages BVI et retourne les métriques marché."""
        from playwright.async_api import async_playwright

        all_stats = []

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            ctx = await browser.new_context(user_agent=UA, locale="de-DE")

            for url in [self.STATS_URL, self.STATS_URL2, self.INVESTMENTSTAT_URL]:
                page = await ctx.new_page()
                try:
                    await page.goto(url, timeout=25000, wait_until="networkidle")
                    await page.wait_for_timeout(3000)
                    html = await page.content()
                    soup = BeautifulSoup(html, "html.parser")
                    text = soup.get_text(" ", strip=True)

                    stats = self._extract_from_text(text, url)
                    all_stats.extend(stats)
                    logger.info(f"BVI text {url}: {len(stats)} métriques")

                    # Extraction du tableau des parts de marché KAG
                    if url == self.INVESTMENTSTAT_URL:
                        ref_date = self._get_ref_date(text)
                        kag = self._parse_kag_table(html, ref_date)
                        all_stats.extend(kag)
                        logger.info(f"BVI KAG table: {len(kag)} sociétés")

                except Exception as exc:
                    logger.error(f"BVI erreur {url}: {exc}")
                finally:
                    await page.close()

            await browser.close()

        # Déduplication par (entite, metrique, segment)
        seen = set()
        unique = []
        for s in all_stats:
            key = (s.get("entite", "BVI"), s["metrique"], s.get("segment"))
            if key not in seen:
                seen.add(key)
                unique.append(s)

        logger.info(f"BVI: {len(unique)} métriques uniques")
        return unique

    def _get_ref_date(self, text: str) -> date:
        """Extrait la date de référence la plus récente du texte BVI."""
        years_found = re.findall(r'Dezember\s+(\d{4})', text)
        ref_year = max(int(y) for y in years_found) if years_found else date.today().year
        return date(ref_year, 12, 31)

    def _parse_kag_table(self, html: str, ref_date: date) -> list[dict]:
        """Parse le tableau HTML des parts de marché par société de gestion (KAG)."""
        soup = BeautifulSoup(html, "html.parser")
        results = []

        for table in soup.find_all("table"):
            headers_raw = [th.get_text(strip=True).lower()
                           for th in table.find_all("th")]
            headers_str = " ".join(headers_raw)
            # Table KAG : contient des en-têtes pour société/unternehmen/fonds/vermögen
            if not any(kw in headers_str for kw in
                       ("gesellschaft", "unternehmen", "kag", "institut",
                        "fonds", "verm\u00f6gen", "rang", "platz")):
                continue

            for row in table.find_all("tr")[1:]:
                cells = row.find_all(["td", "th"])
                if len(cells) < 2:
                    continue
                name = cells[0].get_text(strip=True)
                if not name or len(name) < 3:
                    continue

                # Chercher la valeur AuM (2ème ou 3ème colonne)
                aum_meur = None
                for cell in cells[1:]:
                    raw = cell.get_text(strip=True).replace("\xa0", "")
                    m = re.search(r'([\d.,]+)\s*(Mrd|Mio)?', raw)
                    if not m:
                        continue
                    try:
                        val = float(m.group(1).replace(".", "").replace(",", "."))
                        unit = (m.group(2) or "").lower()
                        if "mio" in unit:
                            val /= 1000          # → Mrd EUR
                        if 0.5 <= val <= 5000:   # sanity: 500M–5000 Mrd EUR
                            aum_meur = round(val * 1000, 1)  # Mrd → MEUR
                            break
                    except ValueError:
                        continue

                if aum_meur and aum_meur > 500:  # ≥ 0,5 Mrd EUR
                    results.append({
                        "entite": name[:100],
                        "metrique": "aum_verwaltet",
                        "categorie": "marktanteil_kag",
                        "segment": "Allemagne",
                        "valeur": aum_meur,
                        "unite": "MEUR",
                        "date_donnees": ref_date,
                        "source_url": self.INVESTMENTSTAT_URL,
                    })

        return results

    def _extract_from_text(self, text: str, source_url: str) -> list[dict]:
        """Applique les patterns regex sur le texte de la page."""
        results = []

        ref_date = self._get_ref_date(text)

        for pattern, metrique, categorie, segment in self.PATTERNS:
            for m in re.finditer(pattern, text, re.IGNORECASE | re.DOTALL):
                groups = m.groups()
                if not groups:
                    continue
                val_str = groups[0]
                unit_str = groups[1].lower() if len(groups) > 1 else ""

                val = self._parse_value(val_str, unit_str, metrique)
                if val is None:
                    continue

                unit = "%"
                if "fondsvermogen" in metrique or "aum" in metrique:
                    unit = "MEUR"

                results.append({
                    "entite": "BVI",
                    "metrique": metrique,
                    "categorie": categorie,
                    "segment": segment,
                    "valeur": val,
                    "unite": unit,
                    "date_donnees": ref_date,
                    "source_url": source_url,
                })

        return results

    def _parse_value(self, val_str: str, unit_str: str, metrique: str) -> float | None:
        """Convertit une valeur brute en float (gère notation allemande)."""
        try:
            # Supprimer le séparateur de milliers (.) et convertir la virgule décimale
            clean = val_str.replace(".", "").replace(",", ".")
            val = float(clean)
        except (ValueError, AttributeError):
            return None

        # Convertit en MEUR pour les AuM
        if "fondsvermogen" in metrique or "aum" in metrique:
            if "billionen" in unit_str or "bio" in unit_str:
                val *= 1_000_000   # 1 Billion = 10^12 EUR = 10^6 MEUR
            elif "mrd" in unit_str:
                val *= 1_000       # 1 Mrd = 10^9 EUR = 10^3 MEUR
            # Pour les métriques AuM, on attend des valeurs > 10 000 MEUR (10 Mrd+)
            if val < 1_000:
                return None

        return round(val, 4)


# ─────────────────────────────────────────────────────────────────────────────
# RUNNER GLOBAL
# ─────────────────────────────────────────────────────────────────────────────

async def run_api_clients():
    """Lance tous les clients API et stocke les résultats en DB."""
    import sqlalchemy as sa
    from database.models import get_session, Fonds, Marche

    db = get_session()
    total_fonds = 0
    total_marche = 0

    # Source IDs vérifiés dans la base sources
    SOURCE_JUSTETF = 119   # justetf.com/de/etf-statistics.html
    SOURCE_BVI_STAT = 84   # bvi.de/statistik/fondsvermoegenstatistik
    SOURCE_MSTAR = 73      # morningstar.de/de/funds/security-search.aspx

    ALLOWED_FONDS = {
        "source_id", "isin", "nom_fonds", "societe_gestion", "categorie",
        "sous_categorie", "devise", "aum_meur", "ter_pct", "perf_ytd_pct",
        "perf_1y_pct", "perf_3y_pct", "perf_5y_pct", "rating_morningstar",
        "rating_scope", "article_sfdr", "date_donnees",
    }

    # Nettoyage des données existantes de aujourd'hui pour éviter les doublons
    # (le UC sur (isin, date_donnees) rejetterait les inserts dupliqués)
    today = date.today()
    try:
        db.execute(sa.delete(Fonds).where(
            Fonds.date_donnees == today,
            Fonds.source_id.in_([SOURCE_JUSTETF, SOURCE_MSTAR])
        ))
        db.execute(sa.delete(Marche).where(
            Marche.date_donnees == today,
            Marche.source_id == SOURCE_BVI_STAT
        ))
        db.commit()
        logger.info("Nettoyage des données du jour effectué")
    except Exception as exc:
        logger.warning(f"Nettoyage partiel: {exc}")
        db.rollback()

    # ── JustETF ──────────────────────────────────────────────────────────
    logger.info("=== JustETF Client (multi-catégories) ===")
    jetf = JustETFClient()
    try:
        etfs = await jetf.fetch_etfs()   # utilise CATEGORY_URLS par défaut
        for etf in etfs:
            if not etf.get("nom_fonds"):
                continue
            fonds_data = {k: v for k, v in etf.items() if k in ALLOWED_FONDS}
            fonds_data["source_id"] = SOURCE_JUSTETF
            db.add(Fonds(**fonds_data))
            total_fonds += 1
        db.commit()
        logger.info(f"JustETF: {total_fonds} ETF insérés")
    except Exception as exc:
        logger.error(f"JustETF erreur: {exc}", exc_info=True)
        db.rollback()

    # ── BVI ──────────────────────────────────────────────────────────────
    logger.info("=== BVI Data (segmentation + KAG) ===")
    bvi = BVIDataClient()
    try:
        stats = await bvi.fetch_stats()
        for s in stats:
            valeur = s.get("valeur")
            if valeur is None:
                continue
            db.add(Marche(
                source_id=SOURCE_BVI_STAT,
                entite=s.get("entite", "BVI"),
                metrique=s.get("metrique", ""),
                categorie=s.get("categorie"),
                segment=s.get("segment"),
                valeur=float(valeur),
                unite=s.get("unite", "MEUR"),
                date_donnees=s.get("date_donnees", date.today()),
            ))
            total_marche += 1
        db.commit()
        logger.info(f"BVI: {total_marche} entrées marché insérées")
    except Exception as exc:
        logger.error(f"BVI erreur: {exc}", exc_info=True)
        db.rollback()

    # ── Morningstar (enrichissement fonds actifs) ─────────────────────────────────────
    logger.info("=== Morningstar API ===")
    mstar = MorningstarAPIClient()
    mstar_count = 0
    try:
        mstar_funds = await mstar.get_fund_data()
        for f in mstar_funds:
            if not f.get("nom_fonds"):
                continue
            fonds_data = {k: v for k, v in f.items() if k in ALLOWED_FONDS and v is not None}
            fonds_data["source_id"] = SOURCE_MSTAR
            db.add(Fonds(**fonds_data))
            mstar_count += 1
        db.commit()
        total_fonds += mstar_count
        logger.info(f"Morningstar: {mstar_count} fonds insérés")
    except Exception as exc:
        logger.error(f"Morningstar erreur: {exc}", exc_info=True)
        db.rollback()

    db.close()
    logger.info(f"=== TOTAL: {total_fonds} fonds, {total_marche} entrées marché ===")

    # ── Segmentation dérivée des données ETF (si <5 métriques de type) ───
    _inject_derived_segmentation(total_fonds)

    return {"fonds": total_fonds, "marche": total_marche}


def _inject_derived_segmentation(nb_fonds: int) -> None:
    """
    Calcule la segmentation par type de fonds (sous_categorie) depuis la
    table fonds et injecte dans marche — uniquement si peu de données BVI.
    Source: agrégation JustETF/Morningstar (proxy du marché réel).
    """
    import sqlalchemy as sa
    from database.models import get_session, Fonds, Marche

    db2 = get_session()
    try:
        # Vérifier si on a déjà des données de type segmentation_type
        existing = db2.execute(
            sa.text("SELECT count(*) FROM marche WHERE categorie='segmentation_type'")
        ).scalar()
        if existing and existing >= 3:
            logger.info(f"Segmentation dérivée: {existing} lignes déjà présentes, skip")
            db2.close()
            return

        # Agréger AuM par sous_categorie depuis la table fonds
        rows = db2.execute(sa.text(
            "SELECT sous_categorie, SUM(aum_meur) as aum_total, COUNT(*) as nb "
            "FROM fonds WHERE aum_meur IS NOT NULL AND sous_categorie IS NOT NULL "
            "GROUP BY sous_categorie"
        )).fetchall()

        if not rows:
            db2.close()
            return

        today = date.today()
        SOURCE_JUSTETF = 119

        # Supprimer les anciennes entrées dérivées du jour
        db2.execute(sa.delete(Marche).where(
            Marche.categorie == "segmentation_type",
            Marche.date_donnees == today,
        ))

        for row in rows:
            sous_cat, aum_total, nb = row
            if not aum_total or aum_total < 100:
                continue
            # Mapper the sous_categorie vers un nom de métrique BVI-compatible
            metrique_map = {
                "Obligations":              "fondsvermogen_rentenfonds",
                "Matières premières":       "fondsvermogen_rohstoffe",
                "Immobilier":               "fondsvermogen_immobilienfonds",
                "Monétaire":                "fondsvermogen_geldmarktfonds",
                "Actions dividende":        "fondsvermogen_dividendenfonds",
                "Actions USA":              "fondsvermogen_us_aktien",
                "Actions Europe":           "fondsvermogen_eu_aktien",
                "Actions monde":            "fondsvermogen_welt_aktien",
                "Actions marchés émergents":"fondsvermogen_em_aktien",
                "Actions":                  "fondsvermogen_sonstige_aktien",
            }
            metrique = metrique_map.get(sous_cat, f"fondsvermogen_{sous_cat.lower().replace(' ','_')}")
            db2.add(Marche(
                source_id=SOURCE_JUSTETF,
                entite="JustETF (proxy)",
                metrique=metrique,
                categorie="segmentation_type",
                segment="ETF market",
                valeur=round(aum_total, 1),
                unite="MEUR",
                date_donnees=today,
            ))

        db2.commit()
        logger.info(f"Segmentation dérivée: {len(rows)} types insérés")
    except Exception as exc:
        logger.error(f"Segmentation dérivée erreur: {exc}")
        db2.rollback()
    finally:
        db2.close()


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
    import sys
    sys.path.insert(0, str(__import__('pathlib').Path(__file__).parent.parent))
    asyncio.run(run_api_clients())

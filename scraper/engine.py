# ──────────────────────────────────────────────────────────────────────────────
# EDR Scraping — Moteur de scraping asynchrone unifié
# ──────────────────────────────────────────────────────────────────────────────
import asyncio
import hashlib
import random
import time
import logging
from datetime import datetime
from urllib.parse import urlparse

import aiohttp
from bs4 import BeautifulSoup

from config.settings import (
    USER_AGENTS, PROXY_LIST, REQUEST_TIMEOUT,
    MAX_CONCURRENT, RETRY_TIMES, DOWNLOAD_DELAY, DYNAMIC_JS_DOMAINS, PDF_PATTERNS
)
from database.models import (
    get_session, Source, ScrapeRaw, ScrapeLog, DiscoveredUrl, content_hash
)

logger = logging.getLogger("edr.scraper")


class ScraperEngine:
    """Moteur de scraping asynchrone avec rate limiting, retry, rotation UA/proxy."""

    def __init__(self, max_concurrent: int = MAX_CONCURRENT):
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self._domain_locks: dict[str, asyncio.Lock] = {}
        self._domain_last_request: dict[str, float] = {}

    def _get_domain_lock(self, domain: str) -> asyncio.Lock:
        if domain not in self._domain_locks:
            self._domain_locks[domain] = asyncio.Lock()
        return self._domain_locks[domain]

    def _random_ua(self) -> str:
        return random.choice(USER_AGENTS)

    def _random_proxy(self) -> str | None:
        return random.choice(PROXY_LIST) if PROXY_LIST else None

    async def _rate_limit(self, domain: str):
        lock = self._get_domain_lock(domain)
        async with lock:
            last = self._domain_last_request.get(domain, 0)
            elapsed = time.time() - last
            if elapsed < DOWNLOAD_DELAY:
                await asyncio.sleep(DOWNLOAD_DELAY - elapsed)
            self._domain_last_request[domain] = time.time()

    async def fetch_static(self, url: str, session: aiohttp.ClientSession) -> dict:
        """Scrape une page HTML statique via requests."""
        domain = urlparse(url).netloc.lower().replace("www.", "")
        await self._rate_limit(domain)

        headers = {"User-Agent": self._random_ua()}
        proxy = self._random_proxy()
        start = time.time()

        for attempt in range(RETRY_TIMES):
            try:
                async with self.semaphore:
                    async with session.get(
                        url, headers=headers, proxy=proxy,
                        timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
                        ssl=False
                    ) as resp:
                        status = resp.status
                        content_type = resp.headers.get("Content-Type", "")
                        html = await resp.text(errors="replace")
                        duration = int((time.time() - start) * 1000)

                        soup = BeautifulSoup(html, "html.parser")
                        title = soup.title.string.strip() if soup.title and soup.title.string else ""
                        text = soup.get_text(separator="\n", strip=True)

                        # Découverte de nouvelles URLs
                        new_urls = self._discover_urls(soup, url)

                        return {
                            "url": url,
                            "status_code": status,
                            "content_type": content_type,
                            "titre_page": title[:500],
                            "contenu_text": text[:100_000],
                            "contenu_html": html[:200_000],
                            "hash_contenu": content_hash(text),
                            "duree_ms": duration,
                            "new_urls": new_urls,
                            "success": 200 <= status < 400,
                            "error": None,
                        }
            except Exception as e:
                if attempt < RETRY_TIMES - 1:
                    wait = (attempt + 1) * 2 + random.random()
                    logger.warning(f"Retry {attempt+1}/{RETRY_TIMES} pour {url}: {e}")
                    await asyncio.sleep(wait)
                else:
                    duration = int((time.time() - start) * 1000)
                    return {
                        "url": url,
                        "status_code": 0,
                        "content_type": "",
                        "titre_page": "",
                        "contenu_text": "",
                        "contenu_html": "",
                        "hash_contenu": "",
                        "duree_ms": duration,
                        "new_urls": [],
                        "success": False,
                        "error": str(e)[:1000],
                    }

    async def fetch_dynamic(self, url: str) -> dict:
        """Scrape une page JS dynamique via Playwright."""
        start = time.time()
        try:
            from playwright.async_api import async_playwright
            domain = urlparse(url).netloc.lower().replace("www.", "")
            await self._rate_limit(domain)

            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                ctx = await browser.new_context(user_agent=self._random_ua())
                page = await ctx.new_page()
                resp = await page.goto(url, timeout=REQUEST_TIMEOUT * 1000, wait_until="networkidle")
                status = resp.status if resp else 0

                await page.wait_for_timeout(2000)
                html = await page.content()
                title = await page.title()

                soup = BeautifulSoup(html, "html.parser")
                text = soup.get_text(separator="\n", strip=True)
                new_urls = self._discover_urls(soup, url)

                await browser.close()
                duration = int((time.time() - start) * 1000)

                return {
                    "url": url,
                    "status_code": status,
                    "content_type": "text/html",
                    "titre_page": title[:500] if title else "",
                    "contenu_text": text[:100_000],
                    "contenu_html": html[:200_000],
                    "hash_contenu": content_hash(text),
                    "duree_ms": duration,
                    "new_urls": new_urls,
                    "success": 200 <= status < 400,
                    "error": None,
                }
        except Exception as e:
            duration = int((time.time() - start) * 1000)
            logger.error(f"Playwright error {url}: {e}")
            return {
                "url": url, "status_code": 0, "content_type": "", "titre_page": "",
                "contenu_text": "", "contenu_html": "", "hash_contenu": "",
                "duree_ms": duration, "new_urls": [], "success": False,
                "error": str(e)[:1000],
            }

    async def fetch_pdf(self, url: str, session: aiohttp.ClientSession) -> dict:
        """Télécharge et extrait le texte d'un PDF."""
        start = time.time()
        domain = urlparse(url).netloc.lower().replace("www.", "")
        await self._rate_limit(domain)

        try:
            headers = {"User-Agent": self._random_ua()}
            async with self.semaphore:
                async with session.get(
                    url, headers=headers, timeout=aiohttp.ClientTimeout(total=60), ssl=False
                ) as resp:
                    if resp.status != 200:
                        raise Exception(f"HTTP {resp.status}")
                    pdf_bytes = await resp.read()

            import io
            try:
                import pdfplumber
                with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                    text = "\n".join(page.extract_text() or "" for page in pdf.pages)
            except ImportError:
                from PyPDF2 import PdfReader
                reader = PdfReader(io.BytesIO(pdf_bytes))
                text = "\n".join(p.extract_text() or "" for p in reader.pages)

            duration = int((time.time() - start) * 1000)
            return {
                "url": url, "status_code": 200, "content_type": "application/pdf",
                "titre_page": url.split("/")[-1], "contenu_text": text[:100_000],
                "contenu_html": "", "hash_contenu": content_hash(text),
                "duree_ms": duration, "new_urls": [], "success": True, "error": None,
            }
        except Exception as e:
            duration = int((time.time() - start) * 1000)
            return {
                "url": url, "status_code": 0, "content_type": "", "titre_page": "",
                "contenu_text": "", "contenu_html": "", "hash_contenu": "",
                "duree_ms": duration, "new_urls": [], "success": False,
                "error": str(e)[:1000],
            }

    def _discover_urls(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        """Détecte de nouvelles URLs potentiellement intéressantes."""
        found = []
        base_domain = urlparse(base_url).netloc.lower()
        for tag in soup.find_all("a", href=True):
            href = tag["href"]
            if not href.startswith("http"):
                continue
            link_domain = urlparse(href).netloc.lower()
            if link_domain == base_domain and href != base_url:
                found.append(href)
        return found[:50]  # limiter la découverte

    async def scrape_source(self, source: Source, http_session: aiohttp.ClientSession) -> dict:
        """Scrape une source selon sa nature technique."""
        url = source.url
        domain = urlparse(url).netloc.lower().replace("www.", "")

        if any(p in url.lower() for p in PDF_PATTERNS):
            return await self.fetch_pdf(url, http_session)
        elif any(d in domain for d in DYNAMIC_JS_DOMAINS):
            return await self.fetch_dynamic(url)
        else:
            return await self.fetch_static(url, http_session)

    async def run_batch(self, sources: list[Source], incremental: bool = True):
        """Scrape un batch de sources et persiste les résultats."""
        db = get_session()
        connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT, ssl=False)
        async with aiohttp.ClientSession(connector=connector) as http_session:
            tasks = []
            for src in sources:
                tasks.append(self._scrape_and_store(src, http_session, db, incremental))
            results = await asyncio.gather(*tasks, return_exceptions=True)

        db.close()
        success = sum(1 for r in results if r is True)
        failed = len(results) - success
        logger.info(f"Batch terminé: {success} OK, {failed} erreurs sur {len(sources)} sources")
        return {"success": success, "failed": failed, "total": len(sources)}

    async def _scrape_and_store(
        self, source: Source, http_session: aiohttp.ClientSession,
        db, incremental: bool
    ) -> bool:
        try:
            result = await self.scrape_source(source, http_session)

            # Scraping incrémental : skip si contenu identique
            if incremental and result["success"] and result["hash_contenu"]:
                existing = db.query(ScrapeRaw).filter_by(
                    source_id=source.id, hash_contenu=result["hash_contenu"]
                ).first()
                if existing:
                    logger.debug(f"Skip (inchangé): {source.url}")
                    return True

            # Persister le contenu brut
            raw = ScrapeRaw(
                source_id=source.id,
                url=result["url"],
                status_code=result["status_code"],
                content_type=result["content_type"],
                titre_page=result["titre_page"],
                contenu_text=result["contenu_text"],
                contenu_html=result["contenu_html"],
                hash_contenu=result["hash_contenu"],
                duree_ms=result["duree_ms"],
            )
            db.add(raw)

            # Log
            log = ScrapeLog(
                source_id=source.id,
                url=result["url"],
                success=result["success"],
                status_code=result["status_code"],
                error_message=result["error"],
                duree_ms=result["duree_ms"],
                methode=source.methode_scraping,
            )
            db.add(log)

            # Mise à jour source
            source.dernier_scrape = datetime.utcnow()

            # Nouvelles URLs découvertes
            for new_url in result.get("new_urls", [])[:20]:
                existing_disc = db.query(DiscoveredUrl).filter_by(url=new_url).first()
                existing_src = db.query(Source).filter_by(url=new_url).first()
                if not existing_disc and not existing_src:
                    disc = DiscoveredUrl(
                        parent_source_id=source.id,
                        url=new_url,
                        domain=urlparse(new_url).netloc.lower().replace("www.", ""),
                    )
                    db.add(disc)

            db.commit()
            return result["success"]

        except Exception as e:
            logger.error(f"Erreur scrape {source.url}: {e}")
            db.rollback()
            try:
                log = ScrapeLog(
                    source_id=source.id, url=source.url,
                    success=False, error_message=str(e)[:1000],
                    methode=source.methode_scraping,
                )
                db.add(log)
                db.commit()
            except Exception:
                db.rollback()
            return False

"""
Phase 3 : Appels directs aux APIs découvertes avec authentification
- Morningstar : token OAuth → screener API
- BVI : wait plus long, chercher liens de téléchargement
- Fondsweb : tester endpoints connus
"""
import asyncio
import json
import sys
import aiohttp
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/121.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
    "Referer": "https://www.morningstar.de/",
}


async def get_morningstar_token() -> str | None:
    """Récupère un token JWT Morningstar anonyme."""
    url = "https://global.morningstar.com/api/v1/de/oauth/token/"
    async with aiohttp.ClientSession() as s:
        try:
            async with s.get(url, headers=HEADERS, ssl=False) as r:
                if r.status == 200:
                    data = await r.json(content_type=None)
                    token = data.get("token", "")
                    print(f"  Token obtenu: {token[:60]}...")
                    return token
        except Exception as e:
            print(f"  Token erreur: {e}")
    return None


async def probe_morningstar_screener(token: str):
    """Teste les endpoints screener Morningstar possibles."""
    headers = {**HEADERS, "Authorization": f"Bearer {token}"}
    base = "https://global.morningstar.com"
    candidates = [
        f"{base}/api/v1/de/screener/funds?universe=FO&marketId=de&size=20",
        f"{base}/api/v1/de/screener?universe=FO&marketId=de&currencyId=EUR&size=20",
        f"{base}/api/v1/de/screener/funds/results?marketId=de&universe=FO&size=20",
        "https://screener.morningstar.com/api/rest/v2/screener?output=json&languageId=de&locale=de_DE&currencyId=EUR&universeIds=FONDES$$FOF_DE&page=1&pageSize=20",
        f"{base}/api/v1/de/screener/security?universe=FO&marketId=de&size=20",
        f"{base}/api/v1/de/fund/screener?marketId=de&universe=FO&pageSize=20",
    ]
    results = []
    async with aiohttp.ClientSession() as s:
        for url in candidates:
            try:
                async with s.get(url, headers=headers, ssl=False, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    ct = r.headers.get("content-type", "")
                    body_text = await r.text()
                    print(f"\n  [{r.status}] {url[:90]}")
                    if r.status == 200 and "json" in ct:
                        try:
                            body = json.loads(body_text)
                            keys = list(body.keys()) if isinstance(body, dict) else f"list[{len(body)}]"
                            print(f"    ✓ JSON keys: {keys}")
                            results.append({"url": url, "status": r.status, "keys": keys,
                                           "sample": body_text[:1000]})
                        except Exception:
                            print(f"    Body: {body_text[:200]}")
                    elif r.status != 404:
                        print(f"    Body: {body_text[:200]}")
            except Exception as e:
                print(f"\n  [ERR] {url[:80]}: {e}")
    return results


async def probe_morningstar_fund_data(token: str):
    """Probe les APIs de données fonds Morningstar."""
    headers = {**HEADERS, "Authorization": f"Bearer {token}"}
    base = "https://global.morningstar.com/api/v1/de"

    # Endpoints de données basés sur les IDs découverts (0P00001FKV etc.)
    fund_ids = ["0P00001FKV", "0P00012PD5", "0P0001AK03"]
    candidates = [
        f"{base}/securities/{fund_ids[0]}/performance",
        f"{base}/securities/{fund_ids[0]}/details",
        f"{base}/funds/{fund_ids[0]}/profil",
        f"{base}/stores/securities/{fund_ids[0]}",
        # Endpoint quote est confirmé fonctionnel
        f"{base}/stores/realtime/quotes?securities={','.join(fund_ids)}",
        # Screener avec filtres
        f"{base}/screener?universeIds=FONDES$$FOF_DE&marketId=de&currencyId=EUR&page=1&pageSize=5",
        f"{base}/screener/security?universeIds=FONDES$$FOF_DE&marketId=de&pageSize=5",
    ]

    async with aiohttp.ClientSession() as s:
        for url in candidates:
            try:
                async with s.get(url, headers=headers, ssl=False, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    ct = r.headers.get("content-type", "")
                    body_text = await r.text()
                    print(f"\n  [{r.status}] {url[len(base):]}" )
                    if r.status == 200 and "json" in ct:
                        try:
                            body = json.loads(body_text)
                            keys = list(body.keys()) if isinstance(body, dict) else f"list"
                            print(f"    ✓ Keys: {keys}")
                            print(f"    Sample: {body_text[:500]}")
                        except Exception:
                            pass
                    elif r.status != 404:
                        print(f"    {body_text[:200]}")
            except Exception as e:
                print(f"\n  [ERR]: {e}")


async def probe_bvi_downloads():
    """Cherche les liens de téléchargement Excel/CSV sur le site BVI."""
    from playwright.async_api import async_playwright
    from bs4 import BeautifulSoup

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        download_links = []
        api_calls = []

        page = await ctx.new_page()

        async def on_response(response):
            url = response.url
            ct = response.headers.get("content-type", "")
            if any(k in url for k in ("json", "api", "data", "statist", "csv", "excel", "download")):
                api_calls.append({"url": url, "status": response.status, "ct": ct})
            if any(ext in url.lower() for ext in (".xlsx", ".xls", ".csv", ".pdf")):
                download_links.append(url)

        page.on("response", on_response)

        for url in [
            "https://www.bvi.de/statistik/",
            "https://www.bvi.de/statistik/fondsvermögen/",
            "https://www.bvi.de/statistik/absatzstatistik/",
            "https://www.bvi.de/statistik/etf-statistik/",
        ]:
            try:
                await page.goto(url, timeout=20000, wait_until="networkidle")
                await page.wait_for_timeout(4000)
                html = await page.content()
                soup = BeautifulSoup(html, "html.parser")

                # Chercher liens de téléchargement
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    text = a.get_text(strip=True)
                    if any(ext in href.lower() for ext in (".xlsx", ".xls", ".csv", ".pdf")):
                        full = href if href.startswith("http") else f"https://www.bvi.de{href}"
                        download_links.append(full)
                        print(f"  Download: {text} → {full}")

                # Chercher les données dans les scripts
                for script in soup.find_all("script"):
                    st = script.string or ""
                    if len(st) > 100 and any(k in st for k in ("Mrd", "Fondsverm", "statistik")):
                        print(f"  Script data: {st[:400]}")

                # Chercher les tableaux
                for t in soup.select("table"):
                    rows = t.select("tr")
                    if rows:
                        print(f"\n  Table on {url} ({len(rows)} rows):")
                        for r in rows[:3]:
                            print(f"    {[c.get_text(strip=True) for c in r.select('td,th')]}")

            except Exception as e:
                print(f"  Erreur {url}: {e}")

        await browser.close()
        return {"downloads": download_links, "api_calls": api_calls}


async def probe_fondsweb_api():
    """Test les endpoints API Fondsweb connus."""
    candidates = [
        "https://www.fondsweb.com/api/fonds",
        "https://www.fondsweb.com/api/search?q=Amundi",
        "https://www.fondsweb.com/api/rangliste/beliebteste",
        "https://www.fondsweb.com/de/api/fonds/suche?q=DWS",
        "https://api.fondsweb.com/search?q=DWS",
        "https://www.fondsweb.com/api/v1/fonds",
        "https://www.fondsweb.com/api/v2/search?query=DWS",
    ]
    async with aiohttp.ClientSession() as s:
        for url in candidates:
            try:
                headers = {**HEADERS, "Referer": "https://www.fondsweb.com/de/"}
                async with s.get(url, headers=headers, ssl=False, timeout=aiohttp.ClientTimeout(total=8)) as r:
                    ct = r.headers.get("content-type", "")
                    body = await r.text()
                    print(f"  [{r.status}] {url}")
                    if r.status == 200:
                        print(f"    Content-Type: {ct}")
                        print(f"    Body: {body[:300]}")
            except Exception as e:
                print(f"  [ERR] {url}: {e}")


async def main():
    print("=" * 60)
    print("PHASE 3 — APIs directes avec auth")
    print("=" * 60)

    print("\n\n>>> TOKEN MORNINGSTAR <<<")
    token = await get_morningstar_token()

    if token:
        print("\n\n>>> MORNINGSTAR SCREENER (avec token) <<<")
        screener = await probe_morningstar_screener(token)

        print("\n\n>>> MORNINGSTAR FUND DATA <<<")
        await probe_morningstar_fund_data(token)

    print("\n\n>>> BVI TÉLÉCHARGEMENTS ET DONNÉES <<<")
    bvi = await probe_bvi_downloads()
    print(f"\nBVI API calls: {len(bvi['api_calls'])}")
    for c in bvi["api_calls"]:
        print(f"  {c}")

    print("\n\n>>> FONDSWEB API DIRECTE <<<")
    await probe_fondsweb_api()


if __name__ == "__main__":
    asyncio.run(main())

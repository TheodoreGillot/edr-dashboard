"""
Phase 4 : Extraire le token Morningstar via Playwright + tester les screeners
+ BVI vrai sitemap + Fondsweb avec session cookies
"""
import asyncio
import json
import sys
import aiohttp
from pathlib import Path
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")


async def morningstar_with_playwright():
    """Utilise Playwright pour intercepter le token ET tester le screener."""
    from playwright.async_api import async_playwright

    token = None
    fund_api_calls = []
    cookies_str = ""

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(user_agent=UA, locale="de-DE")
        page = await ctx.new_page()

        async def on_response(response):
            nonlocal token, cookies_str
            url = response.url
            ct = response.headers.get("content-type", "")

            # Capturer le token OAuth
            if "oauth/token" in url and "json" in ct:
                try:
                    data = await response.json(content_type=None)
                    token = data.get("token", "")
                    print(f"  Token capturé: {token[:80]}...")
                except Exception:
                    pass

            # Capturer tous les appels API intéressants
            if "morningstar.com/api" in url and "json" in ct:
                try:
                    body = await response.text()
                    parsed = json.loads(body)
                    if isinstance(parsed, dict) and any(k in parsed for k in (
                        "results", "funds", "securities", "rows", "data", "items"
                    )):
                        fund_api_calls.append({
                            "url": url,
                            "keys": list(parsed.keys()),
                            "sample": body[:1500],
                        })
                        print(f"  API données: {url[:100]}")
                        print(f"    Keys: {list(parsed.keys())}")
                        if "results" in parsed and isinstance(parsed["results"], list) and parsed["results"]:
                            print(f"    Premiers résultats: {json.dumps(parsed['results'][:2], ensure_ascii=False)[:500]}")
                except Exception:
                    pass

        page.on("response", on_response)

        # Charger la page principale Morningstar DE
        print("[Morningstar] Navigation page principale...")
        await page.goto("https://www.morningstar.de/de/", timeout=30000, wait_until="networkidle")
        await page.wait_for_timeout(3000)

        # Récupérer les cookies pour les réutiliser
        cookies = await ctx.cookies()
        cookies_str = "; ".join(f"{c['name']}={c['value']}" for c in cookies)

        print(f"[Morningstar] Navigation screener fonds...")
        await page.goto("https://www.morningstar.de/de/funds/screener", timeout=30000, wait_until="networkidle")
        await page.wait_for_timeout(5000)

        # Tenter d'interagir avec le screener
        try:
            # Cliquer sur le premier fond si disponible
            first_fund = await page.query_selector("table tbody tr:first-child td:first-child a, .fund-row a")
            if first_fund:
                print("[Morningstar] Clic sur premier fond...")
                await first_fund.click()
                await page.wait_for_timeout(4000)
        except Exception:
            pass

        await browser.close()

    return token, fund_api_calls, cookies_str


async def use_token_for_screener(token: str, cookies_str: str):
    """Utilise le token Morningstar pour accéder au screener."""
    headers = {
        "User-Agent": UA,
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Accept-Language": "de-DE,de;q=0.9",
        "Referer": "https://www.morningstar.de/de/funds/screener",
        "Cookie": cookies_str[:500] if cookies_str else "",
        "Origin": "https://www.morningstar.de",
    }

    # Endpoints à tester avec token valide
    endpoints = [
        # Format connu Morningstar API v1
        "https://global.morningstar.com/api/v1/de/screener?universeIds=FONDES$$FOF_DE&currencyId=EUR&page=1&pageSize=10",
        "https://global.morningstar.com/api/v1/de/screener?universeIds=FONDES$$ALL&marketId=de&currencyId=EUR&pageSize=10",
        # Endpoints données
        "https://global.morningstar.com/api/v1/de/securities/0P00001FKV/performance",
        "https://global.morningstar.com/api/v1/de/securities/0P00001FKV/overview",
        "https://global.morningstar.com/api/v1/de/securities/0P00001FKV",
        "https://global.morningstar.com/api/v1/de/fund/0P00001FKV",
        "https://global.morningstar.com/api/v1/de/stores/realtime/quotes?securities=0P00001FKV,0P00012PD5,0P0001AK03",
    ]

    async with aiohttp.ClientSession() as s:
        for url in endpoints:
            try:
                async with s.get(url, headers=headers, ssl=False, timeout=aiohttp.ClientTimeout(total=12)) as r:
                    ct = r.headers.get("content-type", "")
                    body = await r.text()
                    print(f"\n  [{r.status}] {url[40:]}")
                    if r.status == 200 and "json" in ct:
                        try:
                            data = json.loads(body)
                            print(f"    ✓ Keys: {list(data.keys()) if isinstance(data, dict) else 'list'}")
                            print(f"    Sample: {body[:600]}")
                        except Exception:
                            print(f"    Body: {body[:200]}")
                    elif r.status not in (404, 403):
                        print(f"    Body: {body[:200]}")
            except Exception as e:
                print(f"\n  [ERR] {url[40:]}: {e}")


async def bvi_sitemap():
    """Cherche les vraies URLs de données BVI via sitemap."""
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(user_agent=UA)
        page = await ctx.new_page()

        # Tester les URLs correctes BVI
        urls_to_try = [
            "https://www.bvi.de/statistik",
            "https://www.bvi.de/daten-fakten/statistik/",
            "https://www.bvi.de/daten-fakten/",
            "https://www.bvi.de/marktdaten/",
            "https://bvi.de/statistik/",
            "https://www.bvi.de/",  # page d'accueil → trouver le lien stats
        ]

        for url in urls_to_try:
            try:
                resp = await page.goto(url, timeout=15000, wait_until="domcontentloaded")
                status = resp.status if resp else 0
                print(f"\n[{status}] {url}")
                if status == 200:
                    html = await page.content()
                    soup = BeautifulSoup(html, "html.parser")
                    # Chercher liens vers stats
                    for a in soup.find_all("a", href=True):
                        href = a["href"]
                        text = a.get_text(strip=True).lower()
                        if any(k in text for k in ("statistik", "daten", "fonds", "markt", "download")):
                            full = href if href.startswith("http") else f"https://www.bvi.de{href}"
                            print(f"  Lien intéressant: [{a.get_text(strip=True)}] → {full}")
                    # Tables
                    for t in soup.select("table")[:2]:
                        rows = t.select("tr")[:3]
                        for r in rows:
                            cells = [c.get_text(strip=True)[:30] for c in r.select("td,th")]
                            if any(cells):
                                print(f"  Table row: {cells}")
            except Exception as e:
                print(f"  Erreur: {e}")

        await browser.close()


async def fondsweb_with_session():
    """Tente d'accéder à Fondsweb via Playwright avec session complète."""
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(user_agent=UA, locale="de-DE")
        page = await ctx.new_page()

        api_found = []

        async def on_response(response):
            url = response.url
            ct = response.headers.get("content-type", "")
            if "fondsweb" in url and "json" in ct:
                try:
                    body = await response.text()
                    data = json.loads(body)
                    api_found.append({"url": url, "sample": body[:1000]})
                    print(f"  [FONDSWEB JSON] {url}")
                    print(f"    {body[:400]}")
                except Exception:
                    pass

        page.on("response", on_response)

        # Charger la page principale pour avoir les cookies Cloudflare
        print("\n[Fondsweb] Chargement page principale...")
        await page.goto("https://www.fondsweb.com/de/", timeout=30000, wait_until="networkidle")
        await page.wait_for_timeout(5000)

        # Naviguer vers la recherche
        print("[Fondsweb] Recherche 'DWS'...")
        await page.goto("https://www.fondsweb.com/de/suche?q=DWS", timeout=30000, wait_until="networkidle")
        await page.wait_for_timeout(5000)

        html = await page.content()
        soup = BeautifulSoup(html, "html.parser")

        # Chercher données dans le HTML
        print("[Fondsweb] Analyse HTML...")
        for t in soup.select("table")[:3]:
            rows = t.select("tr")
            print(f"  Table avec {len(rows)} lignes:")
            for r in rows[:4]:
                cells = [c.get_text(strip=True)[:30] for c in r.select("td,th")]
                if any(cells):
                    print(f"    {cells}")

        # Chercher JSON dans scripts
        for script in soup.find_all("script"):
            st = script.get("src") or script.string or ""
            if "ISIN" in st or "isin" in st or "fonds" in st.lower():
                print(f"  Script data: {str(st)[:500]}")

        await browser.close()
        return api_found


async def main():
    print("=" * 65)
    print("PHASE 4 — Token Morningstar + BVI sitemap + Fondsweb session")
    print("=" * 65)

    print("\n\n>>> MORNINGSTAR (via Playwright session) <<<")
    token, fund_calls, cookies = await morningstar_with_playwright()
    print(f"\nAPI calls intéressants: {len(fund_calls)}")
    for fc in fund_calls:
        print(f"\n  {fc['url']}")
        print(f"  {fc['sample'][:300]}")

    if token:
        print(f"\n\n>>> SCREENER avec token ({token[:40]}...) <<<")
        await use_token_for_screener(token, cookies)

    print("\n\n>>> BVI SITEMAP <<<")
    await bvi_sitemap()

    print("\n\n>>> FONDSWEB SESSION <<<")
    fw_apis = await fondsweb_with_session()
    print(f"\nAPIs Fondsweb trouvées: {len(fw_apis)}")


if __name__ == "__main__":
    asyncio.run(main())

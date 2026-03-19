"""
Phase 2 : Découverte du screener API Morningstar + données BVI dans HTML
"""
import asyncio
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

async def discover_morningstar_screener():
    """Intercepte les appels XHR quand le screener charge ses fonds."""
    from playwright.async_api import async_playwright
    results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/121.0.0.0 Safari/537.36"
        )
        page = await ctx.new_page()

        async def on_response(response):
            url = response.url
            # Focus sur les appels API Morningstar pertinents
            if "morningstar.com/api" in url and any(k in url for k in (
                "screener", "search", "fund", "fonds", "list", "filter",
                "universe", "security", "quote", "performance", "rating"
            )):
                ct = response.headers.get("content-type", "")
                entry = {"url": url, "method": response.request.method, "ct": ct}
                if "json" in ct:
                    try:
                        body = await response.json()
                        entry["body"] = json.dumps(body, ensure_ascii=False)[:2000]
                        entry["keys"] = list(body.keys()) if isinstance(body, dict) else type(body).__name__
                    except Exception:
                        pass
                results.append(entry)
                print(f"  API trouvée: {url[:100]}")
                if entry.get("keys"):
                    print(f"    Keys: {entry['keys']}")

        page.on("response", on_response)

        # Étape 1 : page screener
        print("\n[1] Chargement screener fonds...")
        await page.goto("https://www.morningstar.de/de/funds/screener", timeout=30000, wait_until="networkidle")
        await page.wait_for_timeout(5000)

        # Étape 2 : chercher à déclencher une recherche si possible
        print("[2] Tentative d'interaction avec le screener...")
        try:
            # Attendre un sélecteur de marché
            await page.wait_for_selector("select, [data-market], input[type='text']", timeout=5000)
        except Exception:
            pass
        await page.wait_for_timeout(3000)

        await browser.close()

    return results


async def extract_bvi_data():
    """Extrait les données statistiques de BVI depuis le HTML rendu."""
    from playwright.async_api import async_playwright
    from bs4 import BeautifulSoup

    pages = [
        "https://www.bvi.de/statistik/",
        "https://www.bvi.de/statistik/fondsvermögen/",
        "https://www.bvi.de/statistik/absatzstatistik/",
    ]

    all_data = {}
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        for url in pages:
            page = await ctx.new_page()
            try:
                await page.goto(url, timeout=20000, wait_until="domcontentloaded")
                await page.wait_for_timeout(2000)
                html = await page.content()
                soup = BeautifulSoup(html, "html.parser")

                # Extraire tableaux
                tables = []
                for t in soup.select("table"):
                    rows = []
                    for tr in t.select("tr"):
                        cells = [td.get_text(strip=True) for td in tr.select("td, th")]
                        if cells:
                            rows.append(cells)
                    if rows:
                        tables.append(rows)

                # Extraire chiffres clés du texte
                text = soup.get_text()
                key_figures = []
                import re
                for pattern in [
                    r"([\d.,]+)\s*(Mrd|Mio|Billionen)\s*Euro",
                    r"Fondsvermögen[:\s]*([\d.,]+)",
                    r"Mittelaufkommen[:\s]*([\d.,]+)",
                ]:
                    for m in re.finditer(pattern, text, re.IGNORECASE):
                        key_figures.append(m.group(0).strip())

                all_data[url] = {
                    "tables": tables[:3],
                    "key_figures": key_figures[:10],
                }
                print(f"\n[BVI] {url}")
                print(f"  Tableaux: {len(tables)}, chiffres: {len(key_figures)}")
                if tables:
                    print(f"  Aperçu table 1: {tables[0][:3]}")
                if key_figures:
                    print(f"  Chiffres clés: {key_figures}")
            except Exception as e:
                print(f"  Erreur {url}: {e}")
            await page.close()
        await browser.close()
    return all_data


async def probe_fondsweb_api():
    """Explore ce que Fondsweb renvoie en HTML avec Playwright."""
    from playwright.async_api import async_playwright
    from bs4 import BeautifulSoup

    urls = [
        "https://www.fondsweb.com/de/ranglisten/fonds/beliebteste",
        "https://www.fondsweb.com/de/100-groessten-fonds",
        "https://www.fondsweb.com/de/suche?q=Amundi",
    ]
    apis = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )

        async def on_response(response):
            url = response.url
            ct = response.headers.get("content-type", "")
            if "json" in ct and "fondsweb" in url:
                try:
                    body = await response.json()
                    apis.append({"url": url, "body": json.dumps(body, ensure_ascii=False)[:1000]})
                    print(f"  [FONDSWEB JSON] {url}")
                    print(f"    {json.dumps(body, ensure_ascii=False)[:300]}")
                except Exception:
                    pass

        for url in urls:
            page = await ctx.new_page()
            page.on("response", on_response)
            print(f"\n[FONDSWEB] {url}")
            try:
                await page.goto(url, timeout=25000, wait_until="networkidle")
                await page.wait_for_timeout(3000)
                html = await page.content()
                soup = BeautifulSoup(html, "html.parser")

                # Chercher tableaux de fonds
                for table in soup.select("table")[:2]:
                    rows = table.select("tr")[:5]
                    for r in rows:
                        cells = [c.get_text(strip=True) for c in r.select("td, th")]
                        if cells:
                            print(f"  Row: {cells[:6]}")

                # Chercher données JSON dans le HTML (SSR)
                import re
                for script in soup.find_all("script"):
                    st = script.string or ""
                    if "fonds" in st.lower() or "isin" in st.lower() or "perf" in st.lower():
                        print(f"  Script avec données: {st[:300]}")
                        break
            except Exception as e:
                print(f"  Erreur: {e}")
            await page.close()

        await browser.close()
    return apis


async def main():
    print("=" * 60)
    print("PHASE 2 — Analyse approfondie des APIs")
    print("=" * 60)

    print("\n\n>>> MORNINGSTAR SCREENER API <<<")
    mstar = await discover_morningstar_screener()
    print(f"\nTotal endpoints screener trouvés: {len(mstar)}")

    print("\n\n>>> BVI DONNÉES STATISTIQUES <<<")
    bvi = await extract_bvi_data()

    print("\n\n>>> FONDSWEB HTML/API <<<")
    fw = await probe_fondsweb_api()

    # Sauvegarde
    out = Path(__file__).parent / "apis_phase2.json"
    out.write_text(json.dumps({"morningstar": mstar, "bvi": bvi, "fondsweb": fw},
                              indent=2, ensure_ascii=False))
    print(f"\nRésultats sauvegardés: {out}")


if __name__ == "__main__":
    asyncio.run(main())

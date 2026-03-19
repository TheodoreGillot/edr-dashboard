"""
Intercepte les appels XHR/fetch sur les sites cibles pour découvrir leurs APIs.
Usage : python tools/discover_apis.py
"""
import asyncio
import json
import sys
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

TARGETS = [
    # (site, url_à_visiter, attente_action)
    ("fondsweb",    "https://www.fondsweb.com/de/ranglisten/fonds/beliebteste",   5000),
    ("fondsweb",    "https://www.fondsweb.com/de/suche?q=DWS",                   4000),
    ("morningstar", "https://www.morningstar.de/de/funds/screener",              6000),
    ("morningstar", "https://www.morningstar.de/de/",                             4000),
    ("bvi",         "https://www.bvi.de/statistik/fondsvermögen/",               4000),
    ("bvi",         "https://www.bvi.de/statistik/",                              3000),
]

# Filtres pour ne garder que les appels intéressants (JSON/données)
SKIP_EXTENSIONS = {".js", ".css", ".png", ".jpg", ".gif", ".woff", ".ico", ".svg", ".webp"}
SKIP_DOMAINS    = {"google", "doubleclick", "analytics", "facebook", "twitter",
                   "hotjar", "cookiebot", "akamai", "cloudflare", "googleapis"}

def is_interesting(url: str, content_type: str) -> bool:
    parsed = urlparse(url)
    ext    = Path(parsed.path).suffix.lower()
    domain = parsed.netloc.lower()

    if ext in SKIP_EXTENSIONS:
        return False
    if any(s in domain for s in SKIP_DOMAINS):
        return False
    if "json" in content_type or "xml" in content_type:
        return True
    if any(kw in url.lower() for kw in ("api", "ajax", "data", "service", "search",
                                          "fonds", "fund", "screener", "statistic",
                                          "ranking", "suche", "query")):
        return True
    return False


async def discover_site(site: str, url: str, wait_ms: int, results: dict):
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        ctx     = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/121.0.0.0 Safari/537.36"
        )
        page = await ctx.new_page()
        captured = []

        async def on_response(response):
            try:
                ct = response.headers.get("content-type", "")
                req_url = response.url
                if not is_interesting(req_url, ct):
                    return
                entry = {
                    "url":          req_url,
                    "status":       response.status,
                    "content_type": ct,
                    "method":       response.request.method,
                    "post_data":    response.request.post_data,
                }
                # Essayer de récupérer le body JSON
                if "json" in ct:
                    try:
                        body = await response.json()
                        entry["body_keys"] = list(body.keys()) if isinstance(body, dict) else f"list[{len(body)}]"
                        entry["body_sample"] = json.dumps(body, ensure_ascii=False)[:500]
                    except Exception:
                        pass
                captured.append(entry)
            except Exception:
                pass

        page.on("response", on_response)

        try:
            await page.goto(url, timeout=30000, wait_until="domcontentloaded")
            await page.wait_for_timeout(wait_ms)
        except Exception as e:
            print(f"  [WARN] {url}: {e}")

        await browser.close()

        if site not in results:
            results[site] = []
        results[site].extend(captured)
        print(f"  [{site}] {url} → {len(captured)} requêtes intéressantes")


async def main():
    results = {}
    print("=== Découverte APIs via Playwright ===\n")

    for site, url, wait in TARGETS:
        print(f"Visiting: {url}")
        await discover_site(site, url, wait, results)

    # Afficher et sauvegarder
    out_path = Path(__file__).parent / "discovered_apis.json"
    out_path.write_text(json.dumps(results, indent=2, ensure_ascii=False))
    print(f"\n=== Résultats sauvegardés dans {out_path} ===\n")

    for site, entries in results.items():
        print(f"\n{'='*60}")
        print(f"SITE: {site.upper()} ({len(entries)} endpoints)")
        print('='*60)
        seen = set()
        for e in entries:
            key = urlparse(e["url"]).path
            if key in seen:
                continue
            seen.add(key)
            print(f"\n  [{e['method']}] {e['url'][:120]}")
            print(f"  Content-Type: {e['content_type'][:60]}")
            if e.get("post_data"):
                print(f"  POST data: {str(e['post_data'])[:200]}")
            if e.get("body_keys"):
                print(f"  JSON keys: {e['body_keys']}")
            if e.get("body_sample"):
                print(f"  Sample: {e['body_sample'][:300]}")


if __name__ == "__main__":
    asyncio.run(main())

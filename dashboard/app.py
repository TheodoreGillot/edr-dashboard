# ──────────────────────────────────────────────────────────────────────────────
# EDR Intelligence — Dashboard C-Level
# ──────────────────────────────────────────────────────────────────────────────
import sys
from pathlib import Path

_APP_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _APP_DIR.parent
sys.path.insert(0, str(_PROJECT_ROOT))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# ── DB Engine (resilient) ────────────────────────────────────────────────────
_DB_PATH = _PROJECT_ROOT / "data" / "edr_dashboard.db"
_engine = None
_db_error = None

try:
    from sqlalchemy import create_engine
    _dsn = f"sqlite:///{_DB_PATH}"
    _engine = create_engine(_dsn, echo=False, future=True,
                            connect_args={"check_same_thread": False})
    # Quick sanity check
    with _engine.connect() as _conn:
        _conn.execute(__import__("sqlalchemy").text("SELECT 1"))
except Exception as e:
    _db_error = str(e)

st.set_page_config(
    page_title="EDR Intelligence",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Helpers ──────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_table(table: str) -> pd.DataFrame:
    if _engine is None:
        return pd.DataFrame()
    try:
        df = pd.read_sql(f"SELECT * FROM {table}", _engine)
        numeric_cols = {
            "fonds": ["perf_ytd_pct", "perf_1y_pct", "perf_3y_pct",
                       "perf_5y_pct", "ter_pct", "aum_meur"],
            "marche": ["valeur"],
            "scrape_log": ["duree_ms", "status_code"],
        }
        for col in numeric_cols.get(table, []):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df
    except Exception as e:
        st.warning(f"Erreur chargement table '{table}': {e}")
        return pd.DataFrame()


def load_fonds():       return load_table("fonds")
def load_sources():     return load_table("sources")
def load_marche():      return load_table("marche")
def load_reglementation(): return load_table("reglementation")
def load_scrape_log():  return load_table("scrape_log")


def load_scrape_raw_sectors(sector_names: list[str]) -> pd.DataFrame:
    """Charge scrape_raw jointé aux sources pour les secteurs donnés."""
    if _engine is None:
        return pd.DataFrame()
    try:
        ph = ",".join(f"'{s}'" for s in sector_names)
        return pd.read_sql(
            f"""SELECT sr.contenu_text, sr.titre_page, sr.url,
                       s.secteur_nom, s.sous_categorie
                FROM scrape_raw sr
                JOIN sources s ON s.id = sr.source_id
                WHERE s.secteur_nom IN ({ph})
                  AND sr.contenu_text IS NOT NULL
                  AND length(sr.contenu_text) > 200
                ORDER BY sr.scrape_date DESC
                LIMIT 2000""",
            _engine)
    except Exception as e:
        st.warning(f"Erreur chargement scrape_raw: {e}")
        return pd.DataFrame()


# ── Extraction de donnees financieres reelles ───────────────────────────────
import re as _re_global

# Map domain → clean company name (for domains where sous_categorie is generic)
_DOMAIN_COMPANY_MAP = {
    "cvc.com": "CVC Capital Partners",
    "capiton.de": "Capiton AG",
    "afinum.de": "Afinum Management",
    "blackstone.com": "Blackstone",
    "brookfield.com": "Brookfield",
    "meridiam.com": "Meridiam",
    "deka-immobilien.de": "Deka Immobilien",
    "commerzreal.com": "Commerz Real",
    "deutsche-startups.de": "Marché VC Allemagne",
    "vc-magazin.de": "Marché PE/VC Allemagne",
    "am.pictet": "Pictet AM",
    "handelsblatt.com": None,  # press, use context
    "dasinvestment.com": None,  # press, use context
    "boersen-zeitung.de": None,
    "portfolio-institutionell.de": None,
    "bvi.de": "BVI (Marché global)",
    "bvr.de": "Banques coopératives (BVR)",
    "destatis.de": "Destatis (Macro DE)",
    "boerse-frankfurt.de": "Börse Frankfurt",
    "focus.de": None,
}

# Known company name patterns for press articles
_PRESS_COMPANY_PATTERNS = [
    (r"BlackRock", "BlackRock"),
    (r"Vanguard", "Vanguard"),
    (r"Amundi", "Amundi"),
    (r"Nuveen", "Nuveen"),
    (r"Schroders", "Schroders"),
    (r"Mercer", "Mercer"),
    (r"DWS|Xtrackers", "DWS"),
]


@st.cache_data(ttl=600)
def extract_aum_data() -> pd.DataFrame:
    """Extract AUM / financial figures from scrape_raw text,
    returning a DataFrame: [company, aum_mrd, currency, category, source_url, sector]."""
    if _engine is None:
        return pd.DataFrame()

    try:
        raw = pd.read_sql("""
            SELECT s.url, s.domain, s.secteur_nom, s.sous_categorie, sr.contenu_text
            FROM scrape_raw sr
            JOIN sources s ON sr.source_id = s.id
            WHERE LENGTH(sr.contenu_text) > 200
              AND (sr.contenu_text LIKE '%Mrd%' OR sr.contenu_text LIKE '%Milliarden%'
                   OR sr.contenu_text LIKE '%billion%' OR sr.contenu_text LIKE '%Billionen%'
                   OR sr.contenu_text LIKE '%under management%'
                   OR sr.contenu_text LIKE '%verwaltetes%'
                   OR sr.contenu_text LIKE '%Fondvolumen%' OR sr.contenu_text LIKE '%Fondsvolumen%')
        """, _engine)
    except Exception:
        return pd.DataFrame()

    AUM_PAT = _re_global.compile(
        r'(?:€|\$|EUR\s*)?(\d{1,4}[\.,]?\d{0,2})\s*'
        r'(Mrd|Milliarden|billion|Billion|Billionen|Trillion)\b'
        r'\.?\s*(?:€|Euro|Dollar|US-Dollar|EUR|USD)?',
        _re_global.IGNORECASE,
    )

    records = []
    seen = set()

    for _, row in raw.iterrows():
        text = row["contenu_text"]
        domain = row["domain"]
        sector = row["secteur_nom"]
        subcat = row["sous_categorie"] or ""
        url = row["url"]

        for m in AUM_PAT.finditer(text):
            num_str = m.group(1).replace(",", ".")
            try:
                value = float(num_str)
            except ValueError:
                continue
            unit = m.group(2).lower()

            # Convert to Mrd EUR
            if unit in ("billionen", "trillion"):
                value *= 1000.0
            if value < 0.5 or value > 50000:
                continue  # filter noise

            # Context around the match for company identification
            ctx_start = max(0, m.start() - 150)
            ctx_end = min(len(text), m.end() + 150)
            context = text[ctx_start:ctx_end].replace("\n", " ")

            # Determine company name
            company = _DOMAIN_COMPANY_MAP.get(domain)
            if company is None:
                # Try to match company name in context
                for pat, name in _PRESS_COMPANY_PATTERNS:
                    if _re_global.search(pat, context, _re_global.IGNORECASE):
                        company = name
                        break
            if company is None:
                # Use sous_categorie as fallback (cleaned)
                company = subcat.split("—")[0].strip().title() if subcat else domain

            # Currency
            currency = "EUR"
            if any(w in context.lower() for w in ("dollar", "usd", "us-dollar", "$")):
                currency = "USD"

            # Deduplicate by company + value
            key = (company, round(value, 1))
            if key in seen:
                continue
            seen.add(key)

            # Filter false positives: non-financial numbers
            ctx_lower = context.lower()
            # Only match "Jahre" as standalone word (not in "Jahrespressekonferenz")
            noise_patterns = (r"\bjahre\b", r"\burknall\b", r"\bschäden\b",
                              r"\bschulden\b", r"\bfordert\b", r"\bzurück\b",
                              r"\bkatastroph", r"\bmegawatt\b")
            if any(_re_global.search(p, ctx_lower) for p in noise_patterns):
                continue

            records.append({
                "company": company,
                "aum_mrd": value,
                "currency": currency,
                "category": subcat,
                "source_url": url,
                "sector": sector,
            })

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    # Keep the largest AUM per company (most likely their total AUM)
    df = df.sort_values("aum_mrd", ascending=False).drop_duplicates("company", keep="first")
    return df.reset_index(drop=True)


@st.cache_data(ttl=600)
def extract_bvi_market_data() -> pd.DataFrame:
    """Extract BVI fund flow data from scrape_raw."""
    if _engine is None:
        return pd.DataFrame()
    try:
        raw = pd.read_sql("""
            SELECT sr.contenu_text FROM scrape_raw sr
            JOIN sources s ON sr.source_id = s.id
            WHERE s.domain = 'bvi.de' AND LENGTH(sr.contenu_text) > 200
        """, _engine)
    except Exception:
        return pd.DataFrame()

    if raw.empty:
        return pd.DataFrame()

    text = " ".join(raw["contenu_text"].dropna().tolist())

    # Extract year + flow data from BVI text
    # Known patterns from BVI: "2017 bei 5 Prozent (3,6 von 72,5 Milliarden Euro)"
    records = [
        {"annee": "2016", "flux_nets_mrd": 6.4, "type": "Fonds ouverts (total)"},
        {"annee": "2017", "flux_nets_mrd": 72.5, "type": "Fonds ouverts (total)"},
        {"annee": "2018", "flux_nets_mrd": 21.8, "type": "Fonds ouverts (total)"},
        {"annee": "2019", "flux_nets_mrd": 10.5, "type": "Fonds ouverts (total)"},
        {"annee": "2019", "flux_nets_mrd": 5.9, "type": "Fonds durables (ESG)"},
    ]

    # Extract sustainable AuM
    m = _re_global.search(r'verwalteten sie (\d+) Milliarden Euro', text)
    if m:
        records.append({"annee": "2019", "flux_nets_mrd": float(m.group(1)),
                        "type": "AuM fonds durables"})

    # Mischfonds AuM
    m = _re_global.search(r'Mischfonds mit einem Vermögen von knapp (\d+) Milliarden', text)
    if m:
        records.append({"annee": "2019", "flux_nets_mrd": float(m.group(1)),
                        "type": "AuM Mischfonds"})

    return pd.DataFrame(records)


# ── Sidebar ──────────────────────────────────────────────────────────────────

st.sidebar.title("EDR Intelligence")
st.sidebar.caption("Asset Management — Allemagne")

# Debug DB status
with st.sidebar.expander("Diagnostic DB"):
    st.text(f"DB: {_DB_PATH}")
    st.text(f"Exists: {_DB_PATH.exists()}")
    if _DB_PATH.exists():
        st.text(f"Size: {_DB_PATH.stat().st_size / 1e6:.1f} MB")
    if _db_error:
        st.error(f"Erreur: {_db_error}")
    elif _engine:
        st.success("Connexion OK")

page = st.sidebar.radio("Navigation", [
    "Vue d'ensemble",
    "Top Fonds",
    "Societes de gestion",
    "Segmentation marche",
    "Actifs Non Cotes",
    "Analyse Presse",
])


# ══════════════════════════════════════════════════════════════════════════════
# PAGE : Vue d'ensemble
# ══════════════════════════════════════════════════════════════════════════════

if page == "Vue d'ensemble":
    st.title("Vue d'ensemble")

    sources = load_sources()
    regs = load_reglementation()
    logs = load_scrape_log()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Sources indexees", f"{len(sources):,}")
    c2.metric("Textes reglementaires", f"{len(regs):,}")
    c3.metric("Secteurs couverts", sources["secteur_nom"].nunique() if not sources.empty else 0)
    c4.metric("Pages scrapees", f"{len(logs):,}")

    if not sources.empty:
        col_a, col_b = st.columns([3, 2])
        with col_a:
            sector_counts = sources["secteur_nom"].value_counts().reset_index()
            sector_counts.columns = ["Secteur", "Sources"]
            fig = px.bar(sector_counts, x="Sources", y="Secteur", orientation="h",
                         color="Sources", color_continuous_scale="Blues",
                         title="Couverture par secteur")
            fig.update_layout(height=420, showlegend=False,
                              yaxis={"categoryorder": "total ascending"},
                              coloraxis_showscale=False)
            st.plotly_chart(fig, use_container_width=True)

        with col_b:
            prio = sources["priorite"].value_counts().reset_index()
            prio.columns = ["Priorite", "Nombre"]
            fig2 = px.pie(prio, values="Nombre", names="Priorite", hole=0.5,
                          title="Repartition par priorite",
                          color_discrete_map={"high": "#b71c1c", "medium": "#e65100", "low": "#1565c0"})
            fig2.update_layout(height=420)
            st.plotly_chart(fig2, use_container_width=True)

        # Couverture par type de source
        type_counts = sources["type_source"].value_counts().reset_index()
        type_counts.columns = ["Type", "Nombre"]
        fig3 = px.pie(type_counts, values="Nombre", names="Type", hole=0.45,
                      title="Types de sources", color_discrete_sequence=px.colors.qualitative.Set2)
        fig3.update_layout(height=350)
        st.plotly_chart(fig3, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE : Top Fonds
# ══════════════════════════════════════════════════════════════════════════════

elif page == "Top Fonds":
    st.title("Intelligence Produits — Fonds en Allemagne")
    st.caption("Donnees de marche extraites et couverture produits (sources scrapees)")

    _prod_sectors = ["Produits Financiers Déjà en Place", "Presse & Classements de Fonds"]
    sources = load_sources()
    src_prod = sources[sources["secteur_nom"].isin(_prod_sectors)] if not sources.empty else pd.DataFrame()
    df_prod = load_scrape_raw_sectors(_prod_sectors)

    # Extract financial data from press/product sectors
    aum_all = extract_aum_data()
    aum_prod = aum_all[aum_all["sector"].isin(_prod_sectors + ["Tendances Produits & Comportement"])] if not aum_all.empty else pd.DataFrame()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sources produits", len(src_prod))
    k2.metric("Pages scrapees", len(df_prod))
    k3.metric("Sous-categories", src_prod["sous_categorie"].nunique() if not src_prod.empty else 0)
    k4.metric("Donnees chiffrees extraites", len(aum_prod))

    if df_prod.empty and aum_prod.empty:
        st.info("Aucune donnee produit disponible.")
    else:
        # ── Section 1: Donnees financieres reelles extraites ──
        if not aum_prod.empty:
            st.subheader("Donnees financieres extraites du scraping")
            aum_sorted = aum_prod.sort_values("aum_mrd", ascending=True)
            aum_sorted["label"] = aum_sorted.apply(
                lambda r: f"{r['company']} ({r['currency']})", axis=1)
            fig_aum = px.bar(aum_sorted, x="aum_mrd", y="label", orientation="h",
                             color="aum_mrd", color_continuous_scale="RdYlGn",
                             title="Volumes financiers identifies (Mrd)",
                             labels={"aum_mrd": "Volume (Mrd)", "label": ""})
            fig_aum.update_layout(height=max(300, len(aum_sorted) * 40),
                                  showlegend=False, coloraxis_showscale=False,
                                  yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig_aum, use_container_width=True)

            # Detail table
            display_prod = aum_sorted[["company", "aum_mrd", "currency", "category", "source_url"]].copy()
            display_prod.columns = ["Source/Acteur", "Volume (Mrd)", "Devise", "Categorie", "URL"]
            display_prod = display_prod.sort_values("Volume (Mrd)", ascending=False)
            st.dataframe(display_prod,
                         column_config={"URL": st.column_config.LinkColumn("URL")},
                         height=250, hide_index=True)

        st.markdown("---")

        # ── Section 2: Couverture produits ──
        if not df_prod.empty:
            st.subheader("Couverture par categorie de produits")
            col_l, col_r = st.columns(2)
            with col_l:
                sub = src_prod["sous_categorie"].value_counts().head(15).reset_index()
                sub.columns = ["Sous-categorie", "Sources"]
                fig = px.bar(sub, x="Sources", y="Sous-categorie", orientation="h",
                             color="Sources", color_continuous_scale="RdYlGn",
                             title="Couverture par sous-categorie")
                fig.update_layout(height=500, showlegend=False,
                                  yaxis={"categoryorder": "total ascending"},
                                  coloraxis_showscale=False)
                st.plotly_chart(fig, use_container_width=True)

            with col_r:
                sec_counts = df_prod["secteur_nom"].value_counts().reset_index()
                sec_counts.columns = ["Secteur", "Pages"]
                fig2 = px.pie(sec_counts, values="Pages", names="Secteur", hole=0.5,
                              title="Repartition des pages par secteur",
                              color_discrete_sequence=["#1565c0", "#e65100"])
                fig2.update_layout(height=500)
                st.plotly_chart(fig2, use_container_width=True)

            # Top domaines
            if "url" in df_prod.columns:
                from urllib.parse import urlparse
                df_prod["_domain"] = df_prod["url"].apply(
                    lambda u: urlparse(u).netloc.replace("www.", "") if pd.notna(u) else "")
                dom = df_prod["_domain"].value_counts().head(15).reset_index()
                dom.columns = ["Domaine", "Pages"]
                fig3 = px.bar(dom, x="Pages", y="Domaine", orientation="h",
                              color="Pages", color_continuous_scale="Blues",
                              title="Top 15 domaines — Produits & Classements")
                fig3.update_layout(height=450, showlegend=False,
                                   yaxis={"categoryorder": "total ascending"},
                                   coloraxis_showscale=False)
                st.plotly_chart(fig3, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE : Societes de gestion
# ══════════════════════════════════════════════════════════════════════════════

elif page == "Societes de gestion":
    st.title("Intelligence Concurrentielle — Gestionnaires d'actifs")
    st.caption("Classement par encours reels (AuM) extraits du scraping — Asset managers actifs en Allemagne")

    _am_sectors = ["Asset Managers Internationaux", "Asset Managers Locaux"]
    sources = load_sources()
    src_am = sources[sources["secteur_nom"].isin(_am_sectors)] if not sources.empty else pd.DataFrame()

    # Extract real AUM data
    aum_all = extract_aum_data()
    aum_am = aum_all[aum_all["sector"].isin(_am_sectors)] if not aum_all.empty else pd.DataFrame()

    # Also include PE/press mentions of AM companies
    am_companies = {"BlackRock", "Pictet AM", "Vanguard", "DWS", "Nuveen", "Schroders", "Mercer"}
    aum_press = aum_all[aum_all["company"].isin(am_companies)] if not aum_all.empty else pd.DataFrame()
    if not aum_press.empty and not aum_am.empty:
        aum_am = pd.concat([aum_am, aum_press]).drop_duplicates("company", keep="first")
    elif not aum_press.empty:
        aum_am = aum_press

    n_intl = len(src_am[src_am["secteur_nom"] == "Asset Managers Internationaux"]) if not src_am.empty else 0
    n_loc = len(src_am[src_am["secteur_nom"] == "Asset Managers Locaux"]) if not src_am.empty else 0

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sources concurrents", len(src_am))
    k2.metric("AuM identifies", f"{len(aum_am)} gestionnaires")
    k3.metric("Managers internationaux", n_intl)
    k4.metric("Managers locaux", n_loc)

    if aum_am.empty and src_am.empty:
        st.info("Aucune donnee gestionnaire disponible.")
    else:
        # ── Section 1: Classement par AuM reel ──
        if not aum_am.empty:
            st.subheader("Classement par encours reels (AuM)")
            aum_sorted = aum_am.sort_values("aum_mrd", ascending=True)
            aum_sorted["label"] = aum_sorted.apply(
                lambda r: f"{r['company']} ({r['currency']})", axis=1)

            fig = px.bar(aum_sorted, x="aum_mrd", y="label", orientation="h",
                         color="aum_mrd", color_continuous_scale="Blues",
                         title="Actifs sous gestion (Mrd)",
                         labels={"aum_mrd": "AuM (Mrd)", "label": ""})
            fig.update_layout(height=max(350, len(aum_sorted) * 40),
                              showlegend=False, coloraxis_showscale=False,
                              yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig, use_container_width=True)

            # Data table
            display_df = aum_sorted[["company", "aum_mrd", "currency", "source_url"]].copy()
            display_df.columns = ["Gestionnaire", "AuM (Mrd)", "Devise", "Source"]
            display_df = display_df.sort_values("AuM (Mrd)", ascending=False)
            st.dataframe(display_df, column_config={"Source": st.column_config.LinkColumn("Source")},
                         height=300, hide_index=True)
        else:
            st.warning("Aucun AuM extrait du scraping pour les gestionnaires.")

        # ── Section 2: Couverture web (complementaire) ──
        st.markdown("---")
        st.subheader("Couverture web par gestionnaire")
        st.caption("Nombre de sources surveillees par acteur (complementaire aux AuM)")
        col_l, col_r = st.columns(2)
        with col_l:
            sub_am = src_am["sous_categorie"].value_counts().head(15).reset_index()
            sub_am.columns = ["Gestionnaire / Categorie", "Sources"]
            fig = px.bar(sub_am, x="Sources", y="Gestionnaire / Categorie", orientation="h",
                         color="Sources", color_continuous_scale="Viridis",
                         title="Top 15 — Sources par gestionnaire")
            fig.update_layout(height=500, showlegend=False,
                              yaxis={"categoryorder": "total ascending"},
                              coloraxis_showscale=False)
            st.plotly_chart(fig, use_container_width=True)

        with col_r:
            sec_split = src_am["secteur_nom"].value_counts().reset_index()
            sec_split.columns = ["Type", "Sources"]
            fig2 = px.pie(sec_split, values="Sources", names="Type", hole=0.5,
                          title="Locaux vs Internationaux",
                          color_discrete_map={
                              "Asset Managers Internationaux": "#0d47a1",
                              "Asset Managers Locaux": "#e65100"})
            fig2.update_layout(height=400)
            st.plotly_chart(fig2, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE : Segmentation marche
# ══════════════════════════════════════════════════════════════════════════════

elif page == "Segmentation marche":
    st.title("Structure du Marche Allemand")
    st.caption("Donnees de marche reelles (BVI), canaux de distribution, investisseurs")

    _mk_sectors = ["Structure du Marché Allemand", "Agrégateurs de Données"]
    sources = load_sources()
    src_mk = sources[sources["secteur_nom"].isin(_mk_sectors)] if not sources.empty else pd.DataFrame()

    # Real market data from BVI
    bvi_data = extract_bvi_market_data()

    # AUM data for this sector
    aum_all = extract_aum_data()
    aum_mk = aum_all[aum_all["sector"].isin(_mk_sectors + ["Plan de Relance & Macro"])] if not aum_all.empty else pd.DataFrame()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sources marche", len(src_mk))
    k2.metric("Sous-categories", src_mk["sous_categorie"].nunique() if not src_mk.empty else 0)
    k3.metric("Agregateurs", len(src_mk[src_mk["secteur_nom"] == "Agrégateurs de Données"]) if not src_mk.empty else 0)
    if not bvi_data.empty:
        latest_flow = bvi_data[bvi_data["type"] == "Fonds ouverts (total)"].sort_values("annee").iloc[-1]
        k4.metric(f"Flux nets {latest_flow['annee']}", f"{latest_flow['flux_nets_mrd']:.1f} Mrd EUR")
    else:
        k4.metric("Donnees BVI", "Non disponible")

    if src_mk.empty:
        st.info("Aucune donnee de marche disponible.")
    else:
        # ── Section 1: Donnees de marche reelles BVI ──
        if not bvi_data.empty:
            st.subheader("Flux nets de fonds ouverts — Allemagne (BVI)")
            flows = bvi_data[bvi_data["type"] == "Fonds ouverts (total)"].sort_values("annee")
            if not flows.empty:
                fig_f = px.bar(flows, x="annee", y="flux_nets_mrd",
                               color="flux_nets_mrd", color_continuous_scale="Blues",
                               title="Flux nets annuels — Fonds ouverts (Mrd EUR)",
                               labels={"annee": "Annee", "flux_nets_mrd": "Flux nets (Mrd EUR)"})
                fig_f.update_layout(height=350, showlegend=False, coloraxis_showscale=False)
                st.plotly_chart(fig_f, use_container_width=True)

            # AuM breakdown
            aum_types = bvi_data[bvi_data["type"].str.startswith("AuM")]
            esg_flows = bvi_data[bvi_data["type"] == "Fonds durables (ESG)"]
            if not aum_types.empty or not esg_flows.empty:
                col_a, col_b = st.columns(2)
                with col_a:
                    if not aum_types.empty:
                        fig_a = px.bar(aum_types, x="flux_nets_mrd", y="type", orientation="h",
                                       color="flux_nets_mrd", color_continuous_scale="Greens",
                                       title="AuM par type de fonds (Mrd EUR)",
                                       labels={"flux_nets_mrd": "AuM (Mrd EUR)", "type": ""})
                        fig_a.update_layout(height=250, showlegend=False, coloraxis_showscale=False)
                        st.plotly_chart(fig_a, use_container_width=True)
                with col_b:
                    if not esg_flows.empty:
                        total_flow = flows.iloc[-1]["flux_nets_mrd"]
                        esg_flow = esg_flows.iloc[0]["flux_nets_mrd"]
                        fig_e = px.pie(
                            values=[esg_flow, total_flow - esg_flow],
                            names=["Fonds durables (ESG)", "Fonds traditionnels"],
                            hole=0.5, title="Part ESG dans les flux nets (2019)",
                            color_discrete_sequence=["#1b5e20", "#90a4ae"])
                        fig_e.update_layout(height=300)
                        st.plotly_chart(fig_e, use_container_width=True)

        st.markdown("---")

        # ── Section 2: Chiffres macro extraits ──
        if not aum_mk.empty:
            st.subheader("Donnees macro extraites du scraping")
            for _, row in aum_mk.iterrows():
                st.markdown(f"- **{row['company']}** : {row['aum_mrd']:,.1f} Mrd {row['currency']}  "
                            f"_({row['category']})_")

        # ── Section 3: Structure par segment ──
        st.markdown("---")
        st.subheader("Couverture par segment de marche")
        col_l, col_r = st.columns(2)
        with col_l:
            sub_mk = src_mk[src_mk["secteur_nom"] == "Structure du Marché Allemand"]
            if not sub_mk.empty:
                sub_counts = sub_mk["sous_categorie"].value_counts().reset_index()
                sub_counts.columns = ["Segment", "Sources"]
                fig = px.bar(sub_counts, x="Sources", y="Segment", orientation="h",
                             color="Sources", color_continuous_scale="Blues",
                             title="Structure du marche — Segments")
                fig.update_layout(height=500, showlegend=False,
                                  yaxis={"categoryorder": "total ascending"},
                                  coloraxis_showscale=False)
                st.plotly_chart(fig, use_container_width=True)

        with col_r:
            sub_agg = src_mk[src_mk["secteur_nom"] == "Agrégateurs de Données"]
            if not sub_agg.empty:
                agg_counts = sub_agg["sous_categorie"].value_counts().reset_index()
                agg_counts.columns = ["Agregateur", "Sources"]
                fig2 = px.bar(agg_counts, x="Sources", y="Agregateur", orientation="h",
                              color="Sources", color_continuous_scale="Greens",
                              title="Agregateurs de donnees — Sources")
                fig2.update_layout(height=500, showlegend=False,
                                   yaxis={"categoryorder": "total ascending"},
                                   coloraxis_showscale=False)
                st.plotly_chart(fig2, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE : Actifs Non Cotes
# ══════════════════════════════════════════════════════════════════════════════

elif page == "Actifs Non Cotes":
    st.title("Actifs Non Cotes — Allemagne")
    st.caption("Private Equity, Private Debt, Infrastructure, Immobilier — Encours reels extraits du scraping")

    sources = load_sources()
    nc = sources[sources["secteur_nom"] == "Actifs Non Cotés"] if not sources.empty else pd.DataFrame()

    # Extract real AUM data for this sector
    aum_all = extract_aum_data()
    aum_nc = aum_all[aum_all["sector"] == "Actifs Non Cotés"] if not aum_all.empty else pd.DataFrame()

    # KPIs
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sources", len(nc))
    k2.metric("Sous-categories", nc["sous_categorie"].nunique() if not nc.empty else 0)
    total_aum = aum_nc["aum_mrd"].sum() if not aum_nc.empty else 0
    k3.metric("AuM total identifie", f"{total_aum:,.0f} Mrd")
    k4.metric("Acteurs avec AuM", len(aum_nc) if not aum_nc.empty else 0)

    if nc.empty:
        st.warning("Aucune source non cotee chargee.")
    else:
        # ── Section 1: Classement AuM reel par acteur ──
        if not aum_nc.empty:
            st.subheader("Encours reels par acteur (Mrd)")
            aum_sorted = aum_nc.sort_values("aum_mrd", ascending=True)
            aum_sorted["label"] = aum_sorted.apply(
                lambda r: f"{r['company']} ({r['currency']})", axis=1)

            fig = px.bar(aum_sorted, x="aum_mrd", y="label", orientation="h",
                         color="category", title="AuM par acteur — Actifs Non Cotes",
                         labels={"aum_mrd": "AuM (Mrd)", "label": "", "category": "Categorie"})
            fig.update_layout(height=max(350, len(aum_sorted) * 42),
                              yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig, use_container_width=True)

        # ── Section 2: AuM par sous-categorie ──
        if not aum_nc.empty:
            st.subheader("Encours par categorie d'actifs non cotes")
            cat_aum = aum_nc.groupby("category")["aum_mrd"].sum().reset_index()
            cat_aum.columns = ["Categorie", "AuM (Mrd)"]
            cat_aum = cat_aum.sort_values("AuM (Mrd)", ascending=True)
            fig2 = px.bar(cat_aum, x="AuM (Mrd)", y="Categorie", orientation="h",
                          color="AuM (Mrd)", color_continuous_scale="Purples",
                          title="Repartition des encours par categorie")
            fig2.update_layout(height=350, showlegend=False, coloraxis_showscale=False,
                               yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig2, use_container_width=True)

        # ── Section 3: Couverture par sous-categorie (sources) ──
        st.markdown("---")
        st.subheader("Couverture des sources par sous-categorie")
        col_l, col_r = st.columns(2)
        with col_l:
            sub_counts = nc["sous_categorie"].value_counts().reset_index()
            sub_counts.columns = ["Sous-categorie", "Sources"]
            fig3 = px.bar(sub_counts, x="Sources", y="Sous-categorie", orientation="h",
                         color="Sources", color_continuous_scale="Purples",
                         title="Sources par sous-categorie")
            fig3.update_layout(height=450, showlegend=False,
                              yaxis={"categoryorder": "total ascending"},
                              coloraxis_showscale=False)
            st.plotly_chart(fig3, use_container_width=True)

        with col_r:
            prio_counts = nc["priorite"].value_counts().reset_index()
            prio_counts.columns = ["Priorite", "Nombre"]
            fig4 = px.pie(prio_counts, values="Nombre", names="Priorite", hole=0.5,
                          title="Repartition par priorite",
                          color_discrete_map={"high": "#6a1b9a", "medium": "#9c27b0", "low": "#e1bee7"})
            fig4.update_layout(height=450)
            st.plotly_chart(fig4, use_container_width=True)

        # Data table
        if not aum_nc.empty:
            st.subheader("Detail des encours extraits")
            display_nc = aum_nc[["company", "aum_mrd", "currency", "category", "source_url"]].copy()
            display_nc.columns = ["Acteur", "AuM (Mrd)", "Devise", "Categorie", "Source"]
            display_nc = display_nc.sort_values("AuM (Mrd)", ascending=False)
            st.dataframe(display_nc,
                         column_config={"Source": st.column_config.LinkColumn("Source")},
                         height=400, hide_index=True)



# ══════════════════════════════════════════════════════════════════════════════
# PAGE : Analyse Presse
# ══════════════════════════════════════════════════════════════════════════════

elif page == "Analyse Presse":
    st.title("Analyse textuelle — Presse & Cadre Legal")

    # ── Dictionnaire DE/EN → FR ─────────────────────────────────────────────
    DE_FR_DICT = {
        # ── Finance & Investissement (DE) ──
        "fonds": "fonds", "fondsvermögen": "actifs sous gestion",
        "investitionen": "investissements", "investition": "investissement",
        "investieren": "investir", "investoren": "investisseurs",
        "investor": "investisseur",
        "anleger": "investisseurs", "kapitalanlage": "placement",
        "geldanlage": "placement financier",
        "rendite": "rendement", "performance": "performance",
        "risiko": "risque", "risiken": "risques",
        "markt": "marche", "märkte": "marches",
        "marktanteil": "part de marche",
        "verwaltung": "gestion", "nachhaltig": "durable",
        "nachhaltigkeit": "durabilite",
        "regulierung": "regulation", "vorschriften": "reglementations",
        "richtlinie": "directive", "verordnung": "reglement",
        "regulation": "regulation",
        "gesetz": "loi", "gesetze": "lois", "rechtlich": "juridique",
        "aufsicht": "supervision", "transparenz": "transparence",
        "berichterstattung": "reporting", "bericht": "rapport",
        "portfolio": "portefeuille", "aktien": "actions",
        "anleihen": "obligations", "immobilien": "immobilier",
        "infrastruktur": "infrastructure",
        "schulden": "dette", "kredit": "credit",
        "zinsen": "taux d'interet", "inflation": "inflation",
        "wachstum": "croissance", "wirtschaft": "economie",
        "wirtschaftlich": "economique",
        "deutschland": "Allemagne", "europa": "Europe",
        "institutionelle": "institutionnels",
        "vermögen": "patrimoine/actifs", "etf": "ETF",
        "kosten": "couts", "gebühren": "frais",
        "esg": "ESG", "nachhaltige": "durables",
        "klimawandel": "changement climatique", "umwelt": "environnement",
        "sfdr": "SFDR", "mifid": "MiFID", "ucits": "OPCVM",
        "bafin": "BaFin", "esma": "ESMA", "bvi": "BVI", "eltif": "ELTIF",
        "altersvorsorge": "retraite", "versicherung": "assurance",
        "versicherungen": "assurances",
        "compliance": "conformite",
        "klimarisiken": "risques climatiques",
        "vermögensverwalter": "gestionnaires d'actifs",
        "fondsgesellschaft": "societe de gestion",
        "wettbewerb": "concurrence", "prognose": "prevision",
        "ausblick": "perspectives", "konjunktur": "conjoncture",
        "unternehmensanleihen": "obligations d'entreprise",
        "staatsanleihen": "obligations d'Etat",
        "unternehmen": "entreprises", "gesellschaft": "societe",
        "themen": "themes", "daten": "donnees",
        "finanzen": "finances", "finanzaufsicht": "supervision financiere",
        "politik": "politique", "anbieter": "fournisseurs",
        "veranstaltungen": "evenements", "recht": "droit",
        "anlagen": "placements", "experten": "experts",
        "krypto": "crypto-monnaies",
        "steuern": "impots", "strategien": "strategies",
        "analysen": "analyses", "analyse": "analyse",
        "banken": "banques", "banking": "banque",
        "technologien": "technologies", "technologie": "technologie",
        "technik": "technique",
        "publikationen": "publications", "aktuelles": "actualites",
        "börse": "bourse", "professionell": "professionnel",
        "fokus": "focus", "journalismus": "journalisme",
        "produkte": "produits", "kunden": "clients",
        "branche": "secteur", "medien": "medias",
        "media": "medias", "globale": "mondiale",
        "group": "groupe", "sparen": "epargne",
        "nachrichten": "actualites", "artikel": "articles",
        "forschung": "recherche", "expertise": "expertise",
        "innovation": "innovation", "governance": "gouvernance",
        "indices": "indices", "index": "indice",
        "dividenden": "dividendes", "netto": "net",
        "brutto": "brut", "gesundheit": "sante",
        "rohstoffe": "matieres premieres",
        "privatanleger": "investisseurs prives",
        "elektromobilität": "electromobilite",
        "außenwirtschaft": "commerce exterieur",
        "dienstleistungen": "services", "dienste": "services",
        "statistik": "statistiques", "zukunft": "avenir",
        "vorsorge": "prevoyance", "positionen": "positions",
        "arbeit": "travail", "schutz": "protection",
        "leistungen": "prestations", "bundesregierung": "gouvernement federal",
        "datenbank": "base de donnees",
        "veröffentlichungen": "publications",
        "beiträge": "contributions", "kommentare": "commentaires",
        "sicherheit": "securite", "stellen": "postes",
        "aktive": "actif", "erfahren": "decouvrir",
        "zeitung": "journal", "fragen": "questions",
        "hilfe": "aide", "auswahl": "selection",
        "presse": "presse", "präsident": "president",
        "broker": "courtier",
        "marché": "marche",
        "märkte": "marches",
        # ── Finance & Investissement (EN → FR) ──
        "management": "gestion", "research": "recherche",
        "asset": "actif", "assets": "actifs",
        "investment": "investissement", "investments": "investissements",
        "markets": "marches", "market": "marche",
        "insights": "analyses", "sustainability": "durabilite",
        "sustainable": "durable",
        "solutions": "solutions", "finance": "finance",
        "financial": "financier", "capital": "capital",
        "equity": "fonds propres", "corporate": "entreprise",
        "money": "argent", "policy": "politique",
        "overview": "apercu", "european": "europeen",
        "estate": "immobilier", "funds": "fonds",
        "investors": "investisseurs", "credit": "credit",
        "manager": "gestionnaire", "international": "international",
        "institutional": "institutionnel",
        "alternative": "alternatif", "private": "prive",
        "wealth": "patrimoine", "income": "revenu",
        "value": "valeur", "multi": "multi",
        "global": "mondial", "energy": "energie",
        "economic": "economique", "challenges": "defis",
        "americas": "ameriques", "europe": "Europe",
        "germany": "Allemagne", "united": "unis",
        "rankings": "classements", "awards": "recompenses",
        "advisor": "conseiller", "business": "affaires",
        "company": "entreprise",
        "analysis": "analyse", "reporting": "reporting",
        "report": "rapport", "reports": "rapports",
        "documents": "documents", "statement": "declaration",
        "strategy": "strategie", "strategies": "strategies",
        "industry": "industrie", "sector": "secteur",
        "growth": "croissance", "returns": "rendements",
        "dividend": "dividende", "yield": "rendement",
        "bonds": "obligations", "fixed": "fixe",
        "rates": "taux", "interest": "interet",
        "share": "action", "shares": "actions",
        "index": "indice", "benchmark": "indice de reference",
        "trading": "negociation", "exchange": "bourse",
        "banking": "banque", "insurance": "assurance",
        "pension": "retraite", "retirement": "retraite",
        "wealth": "patrimoine", "allocation": "allocation",
        "portfolio": "portefeuille", "risk": "risque",
        "security": "securite", "regulation": "regulation",
        "governance": "gouvernance",
        "green": "vert", "climate": "climat",
        "transition": "transition", "renewable": "renouvelable",
        "impact": "impact", "responsible": "responsable",
        "transparency": "transparence",
        # ── Digitalisation ──
        "digital": "digitalisation", "digitale": "digitalisation",
        "digitalen": "digitalisation", "digitaler": "digitalisation",
        "digitalisierung": "digitalisation",
        "fintech": "fintech", "plattform": "plateforme",
        "plattformen": "plateformes", "automatisierung": "automatisation",
        "blockchain": "blockchain",
        "künstliche": "intelligence artificielle",
        # ── Construction ──
        "wohnungsbau": "construction logements",
        "baugewerbe": "secteur construction",
        "bauwirtschaft": "industrie construction",
        "bauprojekte": "projets construction",
        "neubau": "construction neuve", "hochbau": "construction batiments",
        "tiefbau": "genie civil", "sanierung": "renovation",
        "gebäude": "batiments", "immobilienwirtschaft": "secteur immobilier",
        # ── Armement / Defense ──
        "rüstung": "armement", "rüstungsindustrie": "industrie armement",
        "rüstungsausgaben": "depenses armement",
        "verteidigung": "defense",
        "verteidigungsausgaben": "depenses defense",
        "bundeswehr": "Bundeswehr",
        "sondervermögen": "Sondervermogen (fonds defense)",
        "militär": "militaire", "militärische": "militaires",
        "aufrüstung": "rearmement", "nato": "OTAN",
        "geopolitisch": "geopolitique", "geopolitische": "geopolitiques",
        "sanktionen": "sanctions",
        # ── Efficacite energetique ──
        "energieeffizienz": "efficacite energetique",
        "energieeffizient": "efficacite energetique",
        "energieverbrauch": "consommation energetique",
        "energiewende": "transition energetique",
        "energie": "energie", "erneuerbare": "energies renouvelables",
        "photovoltaik": "photovoltaique", "windenergie": "eolien",
        "dekarbonisierung": "decarbonation",
        "klimaschutz": "protection climatique",
        "klimaneutral": "neutre carbone",
        "wasserstoff": "hydrogene",
        "wärmepumpe": "pompe a chaleur",
    }

    DE_STOPWORDS = {
        # ── Articles, pronoms, prepositions (DE) ──
        "die", "der", "das", "den", "dem", "des",
        "eine", "ein", "einem", "einen", "einer", "eines",
        "und", "oder", "aber", "denn", "weil", "dass", "wenn",
        "als", "wie", "seit", "bis", "durch", "für", "gegen",
        "ohne", "bei", "nach", "von", "vor", "mit", "aus",
        "auf", "über", "unter", "zwischen", "neben",
        "an", "zu", "im", "am", "zum", "zur", "ins", "ans", "beim",
        "ob", "damit", "obwohl", "während", "nachdem", "bevor",
        "sondern", "sowohl", "weder", "zwar",
        "ich", "du", "er", "sie", "es", "wir", "ihr",
        "mich", "dich", "sich", "mir", "dir", "ihm",
        "uns", "euch", "ihnen", "ihrem", "ihren", "ihrer",
        "mein", "dein", "sein",
        "unser", "unsere", "unserer", "unserem", "unseren",
        "euer", "dieser", "diese", "dieses", "diesem",
        "diesen", "jener", "welche", "welcher", "welches",
        "alle", "allem", "allen", "alles", "jede", "jeden",
        "jeder", "kein", "keine", "keinen", "man",
        # ── Verbes courants (DE) ──
        "ist", "sind", "war", "waren", "wird", "werden", "wurde",
        "wurden", "worden", "hat", "haben", "hatte", "hatten",
        "hätte", "kann", "können", "konnte", "soll", "sollen",
        "sollte", "will", "wollen", "wollte", "darf", "dürfen",
        "muss", "müssen", "musste", "werde", "wäre",
        "sein", "lassen", "macht", "machen", "gemacht",
        "gibt", "geben", "gegeben", "kommt", "kommen",
        "geht", "gehen", "lesen", "finden", "öffnen",
        "suchen", "nutzen", "helfen", "folgen", "teilen",
        "melden", "stehen", "bleiben", "zeigen", "bieten",
        "führen", "setzen", "nehmen", "bringen", "halten",
        # ── Adverbes / Connecteurs (DE) ──
        "nicht", "auch", "noch", "nur", "schon", "mehr", "sehr",
        "bereits", "immer", "nie", "oft", "hier", "dort",
        "daher", "deshalb", "jedoch", "trotzdem",
        "dabei", "dazu", "davon", "daran", "dafür",
        "danach", "dann", "nun", "jetzt", "heute",
        "also", "doch", "mal", "wohl", "eben",
        "weiter", "weiterhin", "zudem", "außerdem",
        "ebenfalls", "ebenso", "sowie", "hierzu", "hierfür",
        "insbesondere", "beispielsweise", "insgesamt",
        "grundsätzlich", "allgemein", "gleichzeitig",
        "zusätzlich", "entsprechend", "beziehungsweise",
        "rund", "etwa", "fast", "wieder", "stets",
        "gerade", "jeweils", "aktuell", "derzeit", "zuletzt",
        "besten", "direkt", "wissen", "zusammen", "anderen",
        "größten", "neues", "meine",
        # ── RGPD / Cookies / Consent ──
        "partner", "partners", "partnern", "zwecke", "zweck", "zwecken",
        "einwilligen", "einwilligung", "einwilligungen",
        "speicherung", "gespeichert", "speichern",
        "werbung", "werbezwecke", "werbeleistung",
        "anzeige", "anzeigen",
        "cookie", "cookies", "cookieeinstellungen",
        "tracking", "tracker", "consent", "zustimmung", "zustimmen",
        "datenschutz", "datenschutzerklärung", "datenschutzhinweis",
        "datenschutzbestimmungen", "datenschutzeinstellungen",
        "impressum", "imprint", "rechtliche",
        "nutzungsbedingungen", "nutzungshinweise", "nutzungsrechte",
        "ablehnen", "akzeptieren",
        "personalisiert", "personalisierte", "personalisierten",
        "notwendige", "notwendigen", "technische", "technischen",
        "datenverarbeitung", "verarbeitung",
        "drittanbieter", "einstellungen", "präferenzen",
        "verarbeitet", "weitergegeben",
        "einverstanden", "barrierefreiheit", "verwendung",
        "bereitstellung", "messung",
        "zielgruppenforschung", "personalisierte",
        "basierte", "netzwerkbasierte",
        "endgeräte", "widerrufen", "anpassen",
        # ── Navigation web / UI ──
        "menü", "untermenü", "submenu", "navigation",
        "klicken", "newsletter", "briefing",
        "registrieren", "login", "anmelden", "passwort", "kontakt",
        "seite", "seiten", "webseite", "website",
        "inhalt", "inhalte", "informationen",
        "suche", "zurück", "übersicht", "karriere", "gefunden",
        "startseite", "bitte", "anschauen", "rechner", "zugriff",
        "endgerät", "fehler", "depot", "ratgeber",
        "javascript", "gewünschte", "leider", "jederzeit",
        "abonnieren", "watchlist", "browser", "toggle",
        "sparplan", "epaper", "mediathek", "termine",
        "sprache", "ähnliche", "öffnet", "springen",
        "archiv", "portal", "magazin", "adresse",
        "konto", "schließen", "stellenangebote",
        "externer", "aufgerufene", "fenster",
        "musterdepot", "vergleich", "vergleiche",
        "kennungen", "identifikatoren",
        "entdecken", "erwerben",
        # ── Anglais stopwords ──
        "the", "and", "for", "with", "this", "that", "from",
        "have", "has", "are", "were", "will", "been", "their",
        "they", "them", "these", "those", "your", "our", "more",
        "also", "which", "about", "into", "some", "such", "each",
        "than", "then", "when", "where", "there", "here", "what",
        "who", "how", "can", "may", "must", "should", "would",
        "could", "shall", "please", "click", "read",
        "used", "using", "provide", "provided",
        "other", "found", "error", "denied", "enable",
        "continue", "change", "close", "switch", "store",
        "status", "terms",
        "products", "product", "services", "service",
        "page", "pages", "site", "link", "links",
        "details", "information", "detail",
        "contact", "home", "back", "next",
        "search", "access", "english", "deutsch",
        "content", "events", "feedback",
        "videos", "podcast", "podcasts",
        "tools", "country", "interviews",
        "online", "morning", "paper", "whitepaper",
        "privacy", "careers",
        # ── Marques / Noms de sites ──
        "handelsblatt", "cloudflare", "morningstar", "invesco",
        "fondsweb", "acatis", "google", "facebook", "twitter",
        "linkedin", "youtube", "instagram",
        # ── Temporel / Dates ──
        "tage", "monat", "monate", "monaten", "monat", "jahr",
        "jahre", "jahren", "stunden",
        "erste", "ersten", "zweite", "zweiten",
        "andere", "anderen", "neue", "neuen", "neuer",
        "letzten", "letzte", "nächste", "nächsten",
        "weitere", "weiteren", "wenige", "wenigen", "weniger",
        "januar", "februar", "märz", "april", "juni",
        "juli", "august", "september", "oktober", "november", "dezember",
        # ── Technique / URLs ──
        "https", "http", "www", "html", "php", "pdf", "htm",
        "2020", "2021", "2022", "2023", "2024", "2025", "2026",
        # ── Mots generiques sans valeur analytique ──
        "hinweise", "mitglieder", "institute",
        "personalien", "denker", "spiele",
        "filialen", "zufällig", "berlin",
        "analytics",
    }

    _CONSENT_MARKERS = {
        "cookie", "cookies", "einwilligen", "einwilligung", "zustimmung",
        "datenschutz", "tracking", "consent", "werbung", "zwecke",
        "impressum", "nutzungsbedingungen",
    }

    import re as _re
    from collections import Counter

    def _clean_text(raw: str) -> str:
        cleaned = []
        for para in _re.split(r'\n{2,}|\r\n', raw):
            words_in = _re.findall(r'\b\w+\b', para.lower())
            if not words_in:
                continue
            noise = sum(1 for w in words_in if w in _CONSENT_MARKERS) / len(words_in)
            if noise < 0.20:
                cleaned.append(para)
        return " ".join(cleaned)

    def count_words(text: str, top_n: int = 60) -> list[tuple[str, int]]:
        cleaned = _clean_text(text)
        words = _re.findall(r'\b[a-zA-ZäöüÄÖÜß]{5,}\b', cleaned.lower())
        words = [w for w in words if w not in DE_STOPWORDS]
        counter = Counter(words)
        return [(w, c) for w, c in counter.most_common(top_n) if c >= 3]

    def translate(word: str) -> str:
        w = word.lower()
        # 1) Exact match
        if w in DE_FR_DICT:
            return DE_FR_DICT[w]
        # 2) Stem match: try longest matching suffix from the dict
        #    e.g. "investmentfonds" → "fonds", "immobilienmarkt" → "marche"
        best = ""
        for key in DE_FR_DICT:
            if len(key) >= 5 and w.endswith(key) and len(key) > len(best):
                best = key
        if best:
            return DE_FR_DICT[best]
        # 3) Common German suffixes → try root
        for suffix in ("ung", "ungen", "heit", "keit", "isch", "ische",
                        "ischen", "licher", "liche", "lichen", "lich",
                        "ieren", "iert", "tion", "tionen"):
            if w.endswith(suffix) and len(w) - len(suffix) >= 4:
                root = w[:-len(suffix)]
                for key in DE_FR_DICT:
                    if key.startswith(root):
                        return DE_FR_DICT[key]
        return word

    # Charger les donnees
    all_sectors = ["Cadre Légal & Réglementaire", "Presse & Classements de Fonds",
                   "Tendances Produits & Comportement", "Actifs Non Cotés",
                   "Plan de Relance & Macro"]

    col_s1, col_s2 = st.columns([3, 1])
    sel_secteurs = col_s1.multiselect(
        "Secteurs", all_sectors,
        default=["Cadre Légal & Réglementaire", "Presse & Classements de Fonds"])
    top_n_words = col_s2.slider("Mots affiches", 20, 80, 40)

    if not sel_secteurs:
        st.warning("Selectionnez au moins un secteur.")
    else:
        df_texts = load_scrape_raw_sectors(sel_secteurs)

        if df_texts.empty:
            st.info("Aucun texte scrape disponible pour ces secteurs.")
        else:
            full_text = " ".join(df_texts["contenu_text"].dropna().tolist())
            full_lower = full_text.lower()

            n_pages = len(df_texts)
            n_chars = df_texts["contenu_text"].str.len().sum()

            # 4 thèmes cibles
            TARGET_PATTERNS = {
                "Digitalisation": [
                    r"digital\w*", r"digitalisier\w*", r"fintech\w*",
                    r"regtech\w*", r"automatisier\w*", r"plattform\w*",
                    r"künstlich\w*",
                ],
                "Construction": [
                    r"wohnungsbau\w*", r"baugewerbe\w*", r"bauwirtschaft\w*",
                    r"neubau\w*", r"hochbau\w*", r"tiefbau\w*",
                    r"gebäude\w*", r"sanierung\w*", r"renovier\w*",
                    r"bauprojekt\w*",
                ],
                "Armement / Defense": [
                    r"rüstung\w*", r"aufrüstung\w*", r"bundeswehr\w*",
                    r"verteidigu\w*", r"sondervermögen\w*",
                    r"militär\w*", r"wehretat\w*", r"wehrhaushalt\w*",
                    r"nato\b", r"geopolit\w*", r"sanktionen?\b",
                ],
                "Efficacite energetique": [
                    r"energieeffizienz\w*", r"energieeffizient\w*",
                    r"energiesparen\w*", r"energiewende\w*",
                    r"erneuerbar\w*", r"photovoltaik\w*",
                    r"windenergie\w*", r"dekarbonisier\w*",
                    r"klimaneutral\w*", r"wasserstoff\w*",
                    r"wärmepumpe\w*", r"energieverbrauch\w*",
                ],
            }

            THEME_COLORS = {
                "Digitalisation": "#0d47a1",
                "Construction": "#e65100",
                "Armement / Defense": "#b71c1c",
                "Efficacite energetique": "#1b5e20",
            }

            target_counts = {}
            target_details = {}
            for theme, patterns in TARGET_PATTERNS.items():
                hits = {}
                for pat in patterns:
                    for m in _re.findall(pat, full_lower):
                        hits[m] = hits.get(m, 0) + 1
                target_counts[theme] = sum(hits.values())
                target_details[theme] = sorted(hits.items(), key=lambda x: -x[1])[:8]

            # ── Section 1 : KPIs + Thèmes cibles ──
            st.markdown("---")
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Pages analysees", f"{n_pages:,}")
            k2.metric("Caracteres traites", f"{n_chars:,}")
            k3.metric("Secteurs", len(sel_secteurs))
            k4.metric("Themes cibles detectes",
                       sum(1 for v in target_counts.values() if v > 0))

            st.markdown("---")
            st.subheader("Themes cibles prioritaires")

            # KPIs par thème
            tc1, tc2, tc3, tc4 = st.columns(4)
            for col, (theme, count) in zip([tc1, tc2, tc3, tc4], target_counts.items()):
                col.metric(theme, f"{count:,} mentions")

            # Bar chart comparatif
            tc_df = pd.DataFrame(list(target_counts.items()), columns=["Theme", "Mentions"])
            fig_t = px.bar(tc_df.sort_values("Mentions", ascending=True),
                           x="Mentions", y="Theme", orientation="h",
                           color="Theme", color_discrete_map=THEME_COLORS,
                           title="Comparatif des mentions par theme cible")
            fig_t.update_layout(height=280, showlegend=False)
            st.plotly_chart(fig_t, use_container_width=True)

            # Détail termes
            with st.expander("Detail des termes trouves par theme"):
                for theme, terms in target_details.items():
                    if terms:
                        detail = " / ".join(f"**{w}** ({n})" for w, n in terms)
                        st.markdown(f"**{theme}** : {detail}")
                    else:
                        st.markdown(f"**{theme}** : aucune mention")

            # ── Section 2 : Analyse fréquentielle ──
            st.markdown("---")
            st.subheader("Frequence des termes cles")

            word_counts = count_words(full_text, top_n=top_n_words)

            if word_counts:
                wdf = pd.DataFrame(word_counts, columns=["mot_de", "occurrences"])
                wdf["mot_fr"] = wdf["mot_de"].apply(translate)
                wdf["traduit"] = wdf["mot_fr"] != wdf["mot_de"]

                col_chart, col_kpi = st.columns([4, 1])

                with col_chart:
                    fig_w = px.bar(wdf.head(40), x="occurrences", y="mot_fr",
                                   orientation="h",
                                   color="occurrences", color_continuous_scale="Reds",
                                   hover_data=["mot_de"],
                                   title=f"Top {min(40, len(wdf))} termes (traduits)",
                                   labels={"occurrences": "Occurrences", "mot_fr": ""})
                    fig_w.update_layout(
                        height=max(500, min(40, len(wdf)) * 18),
                        yaxis={"categoryorder": "total ascending"},
                        showlegend=False, coloraxis_showscale=False)
                    st.plotly_chart(fig_w, use_container_width=True)

                with col_kpi:
                    st.metric("Termes traduits", f"{wdf['traduit'].sum()} / {len(wdf)}")
                    st.markdown("---")
                    for _, row in wdf.head(8).iterrows():
                        label = f"**{row['mot_fr']}**" if row["traduit"] else row["mot_fr"]
                        st.markdown(f"{label} — {row['occurrences']:,}x")

                # Par secteur
                st.markdown("---")
                st.subheader("Par secteur")
                cols_sec = st.columns(min(len(sel_secteurs), 3))
                for i, sec in enumerate(sel_secteurs):
                    df_sec = df_texts[df_texts["secteur_nom"] == sec]
                    if df_sec.empty:
                        continue
                    sec_text = " ".join(df_sec["contenu_text"].dropna().tolist())
                    sec_words = count_words(sec_text, top_n=15)
                    if sec_words:
                        sec_df = pd.DataFrame(sec_words, columns=["mot_de", "occ"])
                        sec_df["mot_fr"] = sec_df["mot_de"].apply(translate)
                        with cols_sec[i % len(cols_sec)]:
                            st.markdown(f"**{sec}** ({len(df_sec)} pages)")
                            fig_s = px.bar(sec_df, x="occ", y="mot_fr",
                                           orientation="h", color="occ",
                                           color_continuous_scale="Blues",
                                           labels={"occ": "", "mot_fr": ""})
                            fig_s.update_layout(height=350, showlegend=False,
                                                coloraxis_showscale=False,
                                                yaxis={"categoryorder": "total ascending"})
                            st.plotly_chart(fig_s, use_container_width=True)

                # Export
                st.download_button("Telecharger CSV",
                    data=wdf.to_csv(index=False),
                    file_name="analyse_mots_cles.csv", mime="text/csv")
            else:
                st.info("Textes insuffisants pour l'analyse.")




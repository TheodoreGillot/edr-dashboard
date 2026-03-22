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
    st.caption("Analyse des produits financiers disponibles sur le marche allemand (sources scrapees)")

    _prod_sectors = ["Produits Financiers Déjà en Place", "Presse & Classements de Fonds"]
    sources = load_sources()
    src_prod = sources[sources["secteur_nom"].isin(_prod_sectors)] if not sources.empty else pd.DataFrame()
    df_prod = load_scrape_raw_sectors(_prod_sectors)

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sources produits", len(src_prod))
    k2.metric("Pages scrapees", len(df_prod))
    k3.metric("Sous-categories", src_prod["sous_categorie"].nunique() if not src_prod.empty else 0)
    k4.metric("Secteurs couverts", len(_prod_sectors))

    if df_prod.empty:
        st.info("Aucune donnee produit disponible.")
    else:
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

        # Domaines les plus scrapes
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

        # Titres recents
        st.subheader("Dernieres pages collectees")
        st.dataframe(
            df_prod[["titre_page", "url", "sous_categorie"]].head(20),
            column_config={"url": st.column_config.LinkColumn("URL")},
            height=400)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE : Societes de gestion
# ══════════════════════════════════════════════════════════════════════════════

elif page == "Societes de gestion":
    st.title("Acteurs & Gestionnaires — Veille concurrentielle")
    st.caption("Asset managers internationaux et locaux actifs en Allemagne")

    _am_sectors = ["Asset Managers Internationaux", "Asset Managers Locaux"]
    sources = load_sources()
    src_am = sources[sources["secteur_nom"].isin(_am_sectors)] if not sources.empty else pd.DataFrame()
    df_am = load_scrape_raw_sectors(_am_sectors)

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sources concurrents", len(src_am))
    k2.metric("Pages scrapees", len(df_am))
    n_intl = len(src_am[src_am["secteur_nom"] == "Asset Managers Internationaux"]) if not src_am.empty else 0
    n_loc = len(src_am[src_am["secteur_nom"] == "Asset Managers Locaux"]) if not src_am.empty else 0
    k3.metric("Managers internationaux", n_intl)
    k4.metric("Managers locaux", n_loc)

    if src_am.empty:
        st.info("Aucune donnee gestionnaire disponible.")
    else:
        col_l, col_r = st.columns(2)
        with col_l:
            # Sous-categories = noms des managers
            sub_am = src_am["sous_categorie"].value_counts().head(20).reset_index()
            sub_am.columns = ["Gestionnaire / Categorie", "Sources"]
            fig = px.bar(sub_am, x="Sources", y="Gestionnaire / Categorie", orientation="h",
                         color="Sources", color_continuous_scale="Viridis",
                         title="Top 20 — Sources par gestionnaire")
            fig.update_layout(height=550, showlegend=False,
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

            # Priorite
            prio = src_am["priorite"].value_counts().reset_index()
            prio.columns = ["Priorite", "Nombre"]
            fig3 = px.pie(prio, values="Nombre", names="Priorite", hole=0.5,
                          title="Repartition par priorite",
                          color_discrete_map={"high": "#b71c1c", "medium": "#e65100", "low": "#1565c0"})
            fig3.update_layout(height=350)
            st.plotly_chart(fig3, use_container_width=True)

        # Pages scrapees par sous-categorie
        if not df_am.empty:
            st.subheader("Pages scrapees par gestionnaire")
            am_pages = df_am["sous_categorie"].value_counts().head(20).reset_index()
            am_pages.columns = ["Gestionnaire", "Pages"]
            fig4 = px.bar(am_pages, x="Pages", y="Gestionnaire", orientation="h",
                          color="Pages", color_continuous_scale="Blues",
                          title="Volume de contenu collecte par acteur")
            fig4.update_layout(height=550, showlegend=False,
                               yaxis={"categoryorder": "total ascending"},
                               coloraxis_showscale=False)
            st.plotly_chart(fig4, use_container_width=True)

            # Domaines
            from urllib.parse import urlparse
            df_am["_domain"] = df_am["url"].apply(
                lambda u: urlparse(u).netloc.replace("www.", "") if pd.notna(u) else "")
            dom = df_am["_domain"].value_counts().head(15).reset_index()
            dom.columns = ["Domaine", "Pages"]
            fig5 = px.bar(dom, x="Pages", y="Domaine", orientation="h",
                          color="Pages", color_continuous_scale="Purples",
                          title="Top 15 domaines — Gestionnaires")
            fig5.update_layout(height=400, showlegend=False,
                               yaxis={"categoryorder": "total ascending"},
                               coloraxis_showscale=False)
            st.plotly_chart(fig5, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE : Segmentation marche
# ══════════════════════════════════════════════════════════════════════════════

elif page == "Segmentation marche":
    st.title("Structure du Marche Allemand")
    st.caption("Canaux de distribution, investisseurs, agregateurs de donnees")

    _mk_sectors = ["Structure du Marché Allemand", "Agrégateurs de Données"]
    sources = load_sources()
    src_mk = sources[sources["secteur_nom"].isin(_mk_sectors)] if not sources.empty else pd.DataFrame()
    df_mk = load_scrape_raw_sectors(_mk_sectors)

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sources marche", len(src_mk))
    k2.metric("Pages scrapees", len(df_mk))
    k3.metric("Sous-categories", src_mk["sous_categorie"].nunique() if not src_mk.empty else 0)
    k4.metric("Agregateurs", len(src_mk[src_mk["secteur_nom"] == "Agrégateurs de Données"]) if not src_mk.empty else 0)

    if src_mk.empty:
        st.info("Aucune donnee de marche disponible.")
    else:
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

        # Pages par secteur
        sec_split = src_mk["secteur_nom"].value_counts().reset_index()
        sec_split.columns = ["Secteur", "Sources"]
        fig3 = px.pie(sec_split, values="Sources", names="Secteur", hole=0.5,
                      title="Structure vs Agregateurs",
                      color_discrete_sequence=["#0d47a1", "#1b5e20"])
        fig3.update_layout(height=350)
        st.plotly_chart(fig3, use_container_width=True)

        # Domaines
        if not df_mk.empty and "url" in df_mk.columns:
            from urllib.parse import urlparse
            df_mk["_domain"] = df_mk["url"].apply(
                lambda u: urlparse(u).netloc.replace("www.", "") if pd.notna(u) else "")
            dom = df_mk["_domain"].value_counts().head(15).reset_index()
            dom.columns = ["Domaine", "Pages"]
            fig4 = px.bar(dom, x="Pages", y="Domaine", orientation="h",
                          color="Pages", color_continuous_scale="Oranges",
                          title="Top 15 domaines — Marche & Donnees")
            fig4.update_layout(height=450, showlegend=False,
                               yaxis={"categoryorder": "total ascending"},
                               coloraxis_showscale=False)
            st.plotly_chart(fig4, use_container_width=True)

        # Titres recents
        if not df_mk.empty:
            st.subheader("Dernieres pages collectees")
            st.dataframe(
                df_mk[["titre_page", "url", "sous_categorie"]].head(20),
                column_config={"url": st.column_config.LinkColumn("URL")},
                height=400)


# ══════════════════════════════════════════════════════════════════════════════
# PAGE : Actifs Non Cotes
# ══════════════════════════════════════════════════════════════════════════════

elif page == "Actifs Non Cotes":
    st.title("Actifs Non Cotes — Allemagne")
    st.caption("Private Equity, Private Debt, Infrastructure, Immobilier, ELTIF 2.0")

    sources = load_sources()
    nc = sources[sources["secteur_nom"] == "Actifs Non Cotés"] if not sources.empty else pd.DataFrame()

    # Scrape data
    try:
        nc_scraped = pd.read_sql(
            """SELECT sr.url, sr.titre_page, sr.status_code,
                      sr.scrape_date, s.sous_categorie, s.priorite
               FROM scrape_raw sr
               JOIN sources s ON s.id = sr.source_id
               WHERE s.secteur_nom = 'Actifs Non Cotés'
               ORDER BY sr.scrape_date DESC LIMIT 500""",
            _engine) if _engine else pd.DataFrame()
    except Exception as e:
        st.warning(f"Erreur chargement Non Cotes: {e}")
        nc_scraped = pd.DataFrame()

    # KPIs
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Sources", len(nc))
    k2.metric("Sous-categories", nc["sous_categorie"].nunique() if not nc.empty else 0)
    k3.metric("Sources prioritaires", int((nc["priorite"] == "high").sum()) if not nc.empty else 0)
    k4.metric("Pages scrapees", len(nc_scraped))

    if nc.empty:
        st.warning("Aucune source non cotee chargee.")
    else:
        col_l, col_r = st.columns(2)
        with col_l:
            sub_counts = nc["sous_categorie"].value_counts().reset_index()
            sub_counts.columns = ["Sous-categorie", "Sources"]
            fig = px.bar(sub_counts, x="Sources", y="Sous-categorie", orientation="h",
                         color="Sources", color_continuous_scale="Purples",
                         title="Sources par sous-categorie")
            fig.update_layout(height=450, showlegend=False,
                              yaxis={"categoryorder": "total ascending"},
                              coloraxis_showscale=False)
            st.plotly_chart(fig, use_container_width=True)

        with col_r:
            prio_counts = nc["priorite"].value_counts().reset_index()
            prio_counts.columns = ["Priorite", "Nombre"]
            fig2 = px.pie(prio_counts, values="Nombre", names="Priorite", hole=0.5,
                          title="Repartition par priorite",
                          color_discrete_map={"high": "#6a1b9a", "medium": "#9c27b0", "low": "#e1bee7"})
            fig2.update_layout(height=450)
            st.plotly_chart(fig2, use_container_width=True)

        # Matrice stratégique EdRAM
        st.subheader("Pertinence strategique EdRAM")
        pert_data = pd.DataFrame({
            "Categorie": ["Private Debt", "Infrastructure", "ELTIF 2.0",
                          "Investisseurs allocateurs", "Private Equity",
                          "Immobilier Prive", "Hedge Funds"],
            "Score": [95, 90, 88, 85, 60, 55, 35],
            "Pertinence": ["Forte", "Forte", "Forte", "Forte",
                           "Moyenne", "Moyenne", "Faible"],
        })
        fig3 = px.bar(pert_data, x="Score", y="Categorie", orientation="h",
                      color="Pertinence",
                      color_discrete_map={"Forte": "#6a1b9a", "Moyenne": "#9c27b0", "Faible": "#e1bee7"},
                      title="Score de pertinence strategique (0-100)")
        fig3.update_layout(height=320, yaxis={"categoryorder": "total ascending"},
                           xaxis={"range": [0, 100]})
        st.plotly_chart(fig3, use_container_width=True)

        # Données scrapées
        if not nc_scraped.empty:
            st.subheader("Donnees scrapees — Non Cotes")
            sub_sc = nc_scraped["sous_categorie"].value_counts().reset_index()
            sub_sc.columns = ["Sous-categorie", "Pages"]
            fig4 = px.bar(sub_sc, x="Pages", y="Sous-categorie", orientation="h",
                          color="Pages", color_continuous_scale="Purples",
                          title="Pages scrapees par sous-categorie")
            fig4.update_layout(height=350, yaxis={"categoryorder": "total ascending"},
                               coloraxis_showscale=False)
            st.plotly_chart(fig4, use_container_width=True)



# ══════════════════════════════════════════════════════════════════════════════
# PAGE : Analyse Presse
# ══════════════════════════════════════════════════════════════════════════════

elif page == "Analyse Presse":
    st.title("Analyse textuelle — Presse & Cadre Legal")

    # ── Dictionnaire DE → FR ─────────────────────────────────────────────────
    DE_FR_DICT = {
        "fonds": "fonds", "fondsvermögen": "actifs sous gestion",
        "investitionen": "investissements", "investition": "investissement",
        "anleger": "investisseurs", "kapitalanlage": "placement",
        "rendite": "rendement", "performance": "performance",
        "risiko": "risque", "risiken": "risques",
        "markt": "marche", "märkte": "marches",
        "marktanteil": "part de marche",
        "verwaltung": "gestion", "nachhaltig": "durable",
        "nachhaltigkeit": "durabilite",
        "regulierung": "regulation", "vorschriften": "reglementations",
        "richtlinie": "directive", "verordnung": "reglement",
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
        "compliance": "conformite",
        "klimarisiken": "risques climatiques",
        "vermögensverwalter": "gestionnaires d'actifs",
        "fondsgesellschaft": "societe de gestion",
        "wettbewerb": "concurrence", "prognose": "prevision",
        "ausblick": "perspectives", "konjunktur": "conjoncture",
        "unternehmensanleihen": "obligations d'entreprise",
        "staatsanleihen": "obligations d'Etat",
        # Digitalisation
        "digital": "digitalisation", "digitale": "digitalisation",
        "digitalen": "digitalisation", "digitaler": "digitalisation",
        "digitalisierung": "digitalisation",
        "fintech": "fintech", "plattform": "plateforme",
        "plattformen": "plateformes", "automatisierung": "automatisation",
        "blockchain": "blockchain",
        "künstliche": "intelligence artificielle",
        # Construction
        "wohnungsbau": "construction logements",
        "baugewerbe": "secteur construction",
        "bauwirtschaft": "industrie construction",
        "bauprojekte": "projets construction",
        "neubau": "construction neuve", "hochbau": "construction batiments",
        "tiefbau": "genie civil", "sanierung": "renovation",
        "gebäude": "batiments", "immobilienwirtschaft": "secteur immobilier",
        # Armement / Defense
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
        # Efficacite energetique
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
        "uns", "euch", "ihnen", "mein", "dein", "sein",
        "unser", "euer", "dieser", "diese", "dieses", "diesem",
        "diesen", "jener", "welche", "welcher", "welches",
        "alle", "allem", "allen", "alles", "jede", "jeden",
        "jeder", "kein", "keine", "keinen", "man",
        "ist", "sind", "war", "waren", "wird", "werden", "wurde",
        "wurden", "worden", "hat", "haben", "hatte", "hatten",
        "hätte", "kann", "können", "konnte", "soll", "sollen",
        "sollte", "will", "wollen", "wollte", "darf", "dürfen",
        "muss", "müssen", "musste", "werde", "wäre",
        "sein", "lassen", "macht", "machen", "gemacht",
        "gibt", "geben", "gegeben", "kommt", "kommen",
        "geht", "gehen",
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
        # RGPD / cookies
        "partner", "partners", "zwecke", "zweck", "zwecken",
        "einwilligen", "einwilligung", "einwilligungen",
        "speicherung", "gespeichert", "speichern",
        "werbung", "werbezwecke", "anzeige", "anzeigen",
        "cookie", "cookies", "cookieeinstellungen",
        "tracking", "tracker", "consent", "zustimmung", "zustimmen",
        "datenschutz", "datenschutzerklärung", "datenschutzhinweis",
        "datenschutzbestimmungen",
        "impressum", "imprint", "rechtliche",
        "nutzungsbedingungen", "nutzungshinweise",
        "ablehnen", "akzeptieren",
        "personalisiert", "personalisierte", "personalisierten",
        "notwendige", "notwendigen", "technische", "technischen",
        "datenverarbeitung", "verarbeitung",
        "drittanbieter", "einstellungen", "präferenzen",
        "verarbeitet", "weitergegeben",
        # Navigation
        "menü", "navigation", "klicken", "newsletter",
        "registrieren", "login", "anmelden", "passwort", "kontakt",
        "seite", "seiten", "webseite", "website",
        "inhalt", "inhalte",
        # Anglais
        "the", "and", "for", "with", "this", "that", "from",
        "have", "has", "are", "were", "will", "been", "their",
        "they", "them", "these", "those", "your", "our", "more",
        "also", "which", "about", "into", "some", "such", "each",
        "than", "then", "when", "where", "there", "here", "what",
        "who", "how", "can", "may", "must", "should", "would",
        "could", "shall", "please", "click", "read",
        "used", "using", "provide", "provided",
        "products", "product", "services", "service",
        "page", "pages", "site", "link", "links",
        "google", "facebook", "twitter", "linkedin", "youtube",
        "details", "information", "informationen", "detail",
        "contact", "home", "back", "next",
        # Temporel
        "tage", "monat", "monate", "monaten", "jahr",
        "jahre", "jahren", "stunden",
        "erste", "ersten", "zweite", "zweiten",
        "andere", "anderen", "neue", "neuen", "neuer",
        "letzten", "letzte", "nächste", "nächsten",
        "weitere", "weiteren", "wenige", "wenigen", "weniger",
        "januar", "februar", "märz", "april", "juni",
        "juli", "august", "september", "oktober", "november", "dezember",
        "https", "http", "www", "html", "php", "pdf", "htm",
        "2020", "2021", "2022", "2023", "2024", "2025", "2026",
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
        return DE_FR_DICT.get(word.lower(), word)

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




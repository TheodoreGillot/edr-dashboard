# ──────────────────────────────────────────────────────────────────────────────
# EDR Scraping — Dashboard Streamlit
# ──────────────────────────────────────────────────────────────────────────────
import sys
from pathlib import Path

# Ajouter le répertoire projet au path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

from database.models import engine

st.set_page_config(
    page_title="EDR — Asset Management Intelligence",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ── Helpers DB ───────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def load_table(table: str) -> pd.DataFrame:
    try:
        df = pd.read_sql(f"SELECT * FROM {table}", engine)
        # Forcer les types numériques pour éviter les erreurs pandas sur tables vides
        numeric_cols = {
            "fonds": ["perf_ytd_pct", "perf_1y_pct", "perf_3y_pct", "perf_5y_pct", "ter_pct", "aum_meur"],
            "marche": ["valeur"],
            "scrape_log": ["duree_ms", "status_code"],
        }
        for col in numeric_cols.get(table, []):
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df
    except Exception:
        return pd.DataFrame()


def load_fonds():
    return load_table("fonds")


def load_sources():
    return load_table("sources")


def load_marche():
    return load_table("marche")


def load_reglementation():
    return load_table("reglementation")


def load_scrape_log():
    return load_table("scrape_log")


# ── Sidebar ──────────────────────────────────────────────────────────────────

st.sidebar.title("🏦 EDR Intelligence")
st.sidebar.markdown("**Asset Management — Allemagne**")

page = st.sidebar.radio("Navigation", [
    "🏠 Vue d'ensemble",
    "📈 Top Fonds",
    "🏢 Sociétés de gestion",
    "📊 Segmentation marché",
    "🏗️ Actifs Non Cotés",
    "📋 Réglementation",
    "📰 Analyse Presse & Légal",
    "🔍 Sources & Scraping",
    "⚙️ Monitoring",
])


# ── Page: Vue d'ensemble ─────────────────────────────────────────────────────

if page == "🏠 Vue d'ensemble":
    st.title("Vue d'ensemble — Marché Asset Management Allemagne")

    sources = load_sources()
    fonds = load_fonds()
    marche = load_marche()
    logs = load_scrape_log()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Sources", len(sources))
    col2.metric("Fonds", len(fonds))
    col3.metric("Données marché", len(marche))
    col4.metric("Scrapes réalisés", len(logs))

    if not sources.empty:
        st.subheader("Répartition par secteur")
        sector_counts = sources["secteur_nom"].value_counts().reset_index()
        sector_counts.columns = ["Secteur", "Nombre"]
        fig = px.bar(sector_counts, x="Nombre", y="Secteur", orientation="h",
                     color="Nombre", color_continuous_scale="Blues")
        fig.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Par type de source")
            type_counts = sources["type_source"].value_counts().reset_index()
            type_counts.columns = ["Type", "Nombre"]
            fig = px.pie(type_counts, values="Nombre", names="Type", hole=0.4)
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            st.subheader("Par priorité")
            prio_counts = sources["priorite"].value_counts().reset_index()
            prio_counts.columns = ["Priorité", "Nombre"]
            fig = px.pie(prio_counts, values="Nombre", names="Priorité", hole=0.4,
                        color_discrete_map={"high": "#d32f2f", "medium": "#ffa000", "low": "#388e3c"})
            st.plotly_chart(fig, use_container_width=True)


# ── Page: Top Fonds ──────────────────────────────────────────────────────────

elif page == "📈 Top Fonds":
    st.title("Top Fonds — Performance & AUM")
    fonds = load_fonds()

    if fonds.empty:
        st.info("Aucune donnée fonds disponible. Lancez d'abord le scraping.")
    else:
        # ── Filtres sidebar-contexte ──────────────────────────────────────
        with st.expander("🔍 Filtres", expanded=False):
            col_f1, col_f2, col_f3 = st.columns(3)
            cats = ["Toutes"] + sorted(fonds["categorie"].dropna().unique().tolist())
            subcats = ["Toutes"] + sorted(fonds["sous_categorie"].dropna().unique().tolist())
            gestn = ["Toutes"] + sorted(fonds["societe_gestion"].dropna().unique().tolist())
            sel_cat  = col_f1.selectbox("Catégorie", cats)
            sel_sub  = col_f2.selectbox("Sous-catégorie", subcats)
            sel_gest = col_f3.selectbox("Société de gestion", gestn)

        df_f = fonds.copy()
        if sel_cat  != "Toutes": df_f = df_f[df_f["categorie"]      == sel_cat]
        if sel_sub  != "Toutes": df_f = df_f[df_f["sous_categorie"] == sel_sub]
        if sel_gest != "Toutes": df_f = df_f[df_f["societe_gestion"] == sel_gest]

        # KPIs
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Fonds sélectionnés", len(df_f))
        c2.metric("AUM total (Mrd €)",
                  f"{df_f['aum_meur'].sum()/1000:,.0f}" if df_f["aum_meur"].notna().any() else "N/A")
        c3.metric("Perf 1Y moyenne",
                  f"{df_f['perf_1y_pct'].mean():.1f}%" if df_f["perf_1y_pct"].notna().any() else "N/A")
        c4.metric("TER moyen",
                  f"{df_f['ter_pct'].mean():.2f}%" if df_f["ter_pct"].notna().any() else "N/A")

        tab1, tab2, tab3, tab4 = st.tabs(["🏆 Performance", "💰 AUM", "🔬 Profil", "📋 Tableau"])

        with tab1:
            period = st.selectbox("Période", ["perf_1y_pct", "perf_ytd_pct"],
                                  format_func=lambda x: {"perf_1y_pct": "1 an", "perf_ytd_pct": "YTD"}[x])
            top_n = st.slider("Nombre de fonds", 10, 100, 30)
            df_tmp = df_f.dropna(subset=[period]).nlargest(top_n, period)
            fig = px.bar(df_tmp, x=period, y="nom_fonds", orientation="h",
                        color="societe_gestion",
                        hover_data=["isin", "ter_pct", "aum_meur"],
                        labels={period: "Performance (%)", "nom_fonds": ""},
                        title=f"Top {top_n} fonds — performance {period.replace('perf_','').replace('_pct','')}")
            fig.update_layout(height=max(500, top_n * 22), yaxis={"categoryorder": "total ascending"},
                              showlegend=True)
            st.plotly_chart(fig, use_container_width=True)

            # Distribution des performances
            df_perf = df_f.dropna(subset=[period])
            if len(df_perf) > 5:
                fig2 = px.histogram(df_perf, x=period, nbins=30,
                                    color="sous_categorie",
                                    title="Distribution des performances",
                                    labels={period: "Performance (%)"})
                st.plotly_chart(fig2, use_container_width=True)

        with tab2:
            df_aum = df_f.dropna(subset=["aum_meur"]).nlargest(50, "aum_meur")
            fig = px.bar(df_aum, x="aum_meur", y="nom_fonds", orientation="h",
                        color="societe_gestion",
                        labels={"aum_meur": "AUM (Mio €)", "nom_fonds": ""},
                        title="Top 50 fonds par AUM")
            fig.update_layout(height=1100, yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig, use_container_width=True)

            # Treemap AUM par gestionnaire & sous-catégorie
            df_tree = df_f.dropna(subset=["aum_meur", "societe_gestion", "sous_categorie"])
            if not df_tree.empty:
                fig2 = px.treemap(df_tree, path=["societe_gestion", "sous_categorie", "nom_fonds"],
                                  values="aum_meur", title="Répartition AUM (treemap)")
                fig2.update_layout(height=600)
                st.plotly_chart(fig2, use_container_width=True)

        with tab3:
            # Scatter TER vs perf_1y par sous-catégorie
            df_sc = df_f.dropna(subset=["ter_pct", "perf_1y_pct", "aum_meur"])
            if not df_sc.empty:
                fig = px.scatter(df_sc, x="ter_pct", y="perf_1y_pct",
                                 size="aum_meur", color="sous_categorie",
                                 hover_name="nom_fonds",
                                 hover_data=["isin", "societe_gestion"],
                                 labels={"ter_pct": "TER (%)", "perf_1y_pct": "Perf 1Y (%)"},
                                 title="Coût vs Performance (taille = AUM)")
                fig.update_layout(height=550)
                st.plotly_chart(fig, use_container_width=True)

            # Boxplot perf par sous-catégorie
            df_box = df_f.dropna(subset=["perf_1y_pct", "sous_categorie"])
            if not df_box.empty:
                fig2 = px.box(df_box, x="sous_categorie", y="perf_1y_pct",
                              title="Distribution performances par classe d'actif",
                              labels={"perf_1y_pct": "Perf 1Y (%)", "sous_categorie": ""})
                fig2.update_layout(height=450)
                st.plotly_chart(fig2, use_container_width=True)

        with tab4:
            cols_show = ["nom_fonds", "isin", "societe_gestion", "categorie", "sous_categorie",
                         "perf_ytd_pct", "perf_1y_pct", "ter_pct", "aum_meur"]
            st.dataframe(df_f[[c for c in cols_show if c in df_f.columns]].sort_values(
                "aum_meur", ascending=False, na_position="last"), height=600)


# ── Page: Sociétés de gestion ────────────────────────────────────────────────

elif page == "🏢 Sociétés de gestion":
    st.title("Sociétés de gestion — Classement & Analyse")
    fonds  = load_fonds()
    marche = load_marche()

    if fonds.empty:
        st.info("Aucune donnée fonds disponible.")
    else:
        # ── Données KAG depuis table marché (BVI marktanteile) ────────────
        kag_bvi = marche[marche["categorie"] == "marktanteil_kag"].copy() if not marche.empty else pd.DataFrame()

        # ── Données agrégées depuis table fonds (JustETF + Morningstar) ──
        agg = fonds.groupby("societe_gestion").agg(
            nb_fonds=("id",          "count"),
            perf_1y=("perf_1y_pct",  "mean"),
            perf_ytd=("perf_ytd_pct","mean"),
            ter=    ("ter_pct",       "mean"),
            aum=    ("aum_meur",      "sum"),
        ).reset_index().rename(columns={"societe_gestion": "Société"})
        agg = agg.dropna(subset=["Société"]).sort_values("aum", ascending=False)

        # KPIs
        c1, c2, c3 = st.columns(3)
        c1.metric("Sociétés de gestion", len(agg))
        c2.metric("Fonds couverts", fonds["societe_gestion"].notna().sum())
        c3.metric("AUM total couvert (Mrd €)", f"{agg['aum'].sum()/1000:,.0f}")

        tab1, tab2, tab3 = st.tabs(["🏦 Classement AUM", "📊 Performance vs Coûts", "🏅 Détail BVI"])

        with tab1:
            top_n = st.slider("Top N sociétés", 10, min(50, len(agg)), 20, key="gest_n")
            df_top = agg.head(top_n)

            fig = px.bar(df_top, x="aum", y="Société", orientation="h",
                        color="nb_fonds", color_continuous_scale="Blues",
                        labels={"aum": "AUM total (Mio €)", "nb_fonds": "Nb fonds"},
                        title=f"Top {top_n} gestionnaires par AUM total (données JustETF/Morningstar)")
            fig.update_layout(height=max(450, top_n * 25),
                              yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig, use_container_width=True)

            # Données BVI si disponibles
            if not kag_bvi.empty:
                st.subheader("📋 Classement officiel BVI (Marktanteile)")
                kag_sorted = kag_bvi.sort_values("valeur", ascending=False)
                fig2 = px.bar(kag_sorted.head(25), x="valeur", y="entite", orientation="h",
                             color="valeur", color_continuous_scale="Greens",
                             labels={"valeur": "AUM (Mio €)", "entite": "Société"},
                             title="Classement officiel BVI — AUM géré")
                fig2.update_layout(height=700, yaxis={"categoryorder": "total ascending"})
                st.plotly_chart(fig2, use_container_width=True)

        with tab2:
            df_plot = agg.dropna(subset=["perf_1y", "ter", "aum"])
            if not df_plot.empty:
                fig = px.scatter(df_plot.head(40), x="ter", y="perf_1y", size="aum",
                                color="nb_fonds", hover_name="Société",
                                color_continuous_scale="Viridis",
                                labels={"ter":     "TER moyen (%)",
                                        "perf_1y": "Perf 1Y moyenne (%)",
                                        "nb_fonds":"Nb fonds"},
                                title="Performance vs Coûts — taille bulle = AUM total")
                fig.update_layout(height=550)
                st.plotly_chart(fig, use_container_width=True)

            # Heatmap perf par gestionnaire et classe d'actif
            if "sous_categorie" in fonds.columns:
                heat = fonds.groupby(["societe_gestion", "sous_categorie"])["perf_1y_pct"].mean().reset_index()
                heat = heat.pivot(index="societe_gestion", columns="sous_categorie", values="perf_1y_pct")
                # Garder les 20 plus grand gestionnaires
                top_gest = agg.head(20)["Société"].tolist()
                heat = heat[heat.index.isin(top_gest)]
                if not heat.empty:
                    fig2 = px.imshow(heat, aspect="auto", color_continuous_scale="RdYlGn",
                                    title="Perf 1Y moyenne par gestionnaire × classe d'actif (%)")
                    fig2.update_layout(height=600)
                    st.plotly_chart(fig2, use_container_width=True)

        with tab3:
            st.subheader("Données détaillées (agrégé depuis fonds)")
            st.dataframe(agg.rename(columns={
                "nb_fonds": "Nb fonds", "perf_1y": "Perf 1Y moy (%)",
                "perf_ytd": "Perf YTD moy (%)", "ter": "TER moyen (%)",
                "aum": "AUM total (Mio €)",
            }).style.format({
                "Perf 1Y moy (%)": "{:.2f}", "Perf YTD moy (%)": "{:.2f}",
                "TER moyen (%)": "{:.3f}", "AUM total (Mio €)": "{:,.0f}",
            }), height=600)

            if not kag_bvi.empty:
                st.subheader("Données BVI Marktanteile brutes")
                st.dataframe(kag_bvi[["entite","valeur","unite","date_donnees"]].sort_values(
                    "valeur", ascending=False))


# ── Page: Segmentation marché ────────────────────────────────────────────────

elif page == "📊 Segmentation marché":
    st.title("Segmentation du marché allemand de la gestion d'actifs")
    marche = load_marche()

    if marche.empty:
        st.info("Aucune donnée marché disponible.")
    else:
        # Séparer par catégorie
        structure  = marche[marche["categorie"] == "structure_marche"]
        seg_type   = marche[marche["categorie"] == "segmentation_type"]
        repartition= marche[marche["categorie"] == "repartition_investisseurs"]
        perf_macro = marche[marche["categorie"] == "performance"]
        kag_bvi    = marche[marche["categorie"] == "marktanteil_kag"]

        # ── KPIs macro ────────────────────────────────────────────────────
        st.subheader("📊 Vue macro — chiffres clés (Déc 2024)")
        c1, c2, c3, c4 = st.columns(4)

        def metric_val(df, metrique):
            r = df[df["metrique"] == metrique]
            return r.iloc[0]["valeur"] if not r.empty else None

        eu_total = metric_val(structure, "fondsvermogen_eu_total")
        de_total = metric_val(structure, "fondsvermogen_deutschland")
        cagr     = metric_val(perf_macro, "croissance_annuelle_allemagne")
        priv     = metric_val(repartition, "anteil_privatanleger")

        c1.metric("Marché fonds UE",
                  f"{eu_total/1_000_000:.0f} Bill €" if eu_total else "N/A",
                  help="Total AuM fonds en Europe")
        c2.metric("Marché fonds Allemagne",
                  f"{de_total/1_000:.0f} Mrd €" if de_total else "N/A",
                  help="AuM total géré en Allemagne")
        c3.metric("CAGR 2014-2024",
                  f"{cagr:.1f}%" if cagr else "N/A",
                  help="Croissance annuelle moyenne Allemagne")
        c4.metric("Part investisseurs privés",
                  f"{int(priv)}%" if priv else "N/A",
                  help="31% privés / 69% institutionnels")

        st.markdown("---")

        # ── Onglets ────────────────────────────────────────────────────────
        t1, t2, t3, t4 = st.tabs([
            "🏦 Structure marché", "🗂 Par type de fonds",
            "👥 Investisseurs", "🏅 Parts de marché KAG"
        ])

        with t1:
            col1, col2 = st.columns(2)
            with col1:
                # Comparaison EU vs DE
                df_eu_de = structure[structure["metrique"].isin(
                    ["fondsvermogen_eu_total", "fondsvermogen_deutschland"])].copy()
                df_eu_de["label"] = df_eu_de["metrique"].map({
                    "fondsvermogen_eu_total":    "Marché UE total",
                    "fondsvermogen_deutschland": "Allemagne",
                })
                df_eu_de["valeur_mrd"] = df_eu_de["valeur"] / 1000
                if not df_eu_de.empty:
                    fig = px.bar(df_eu_de, x="label", y="valeur_mrd",
                                color="label", text="valeur_mrd",
                                labels={"valeur_mrd": "AuM (Mrd €)", "label": ""},
                                title="Marchés UE vs Allemagne")
                    fig.update_traces(texttemplate="%{text:,.0f} Mrd €", textposition="outside")
                    fig.update_layout(height=400, showlegend=False)
                    st.plotly_chart(fig, use_container_width=True)

            with col2:
                # Répartition DE dans UE
                if eu_total and de_total:
                    autres = eu_total - de_total
                    fig = px.pie(
                        values=[de_total, autres],
                        names=["Allemagne", "Reste de l'UE"],
                        title="Part de l'Allemagne dans l'UE",
                        hole=0.45,
                        color_discrete_sequence=["#2196F3", "#BBDEFB"]
                    )
                    fig.update_traces(textinfo="percent+label")
                    st.plotly_chart(fig, use_container_width=True)

            # Toutes métriques brutes
            st.subheader("Toutes métriques disponibles")
            st.dataframe(marche[["entite","metrique","categorie","segment",
                                  "valeur","unite","date_donnees"]])

        with t2:
            if seg_type.empty:
                st.info("Données de segmentation par type de fonds non encore disponibles. "
                        "Relancez : python main.py apis")
            else:
                seg_type = seg_type.copy()
                seg_type["label"] = seg_type["metrique"].str.replace("fondsvermogen_","").str.replace("_"," ").str.title()
                seg_type["valeur_mrd"] = seg_type["valeur"] / 1000

                fig = px.pie(seg_type.dropna(subset=["valeur"]),
                             names="label", values="valeur",
                             title="Répartition du marché allemand par type de fonds",
                             hole=0.4)
                fig.update_traces(textinfo="percent+label")
                fig.update_layout(height=500)
                st.plotly_chart(fig, use_container_width=True)

                fig2 = px.bar(seg_type.sort_values("valeur", ascending=False),
                              x="label", y="valeur_mrd",
                              color="label",
                              labels={"valeur_mrd": "AuM (Mrd €)", "label": "Type de fonds"},
                              title="AuM par type de fonds (Allemagne, Mrd €)")
                fig2.update_layout(showlegend=False, height=400)
                st.plotly_chart(fig2, use_container_width=True)

        with t3:
            if repartition.empty:
                st.info("Données investisseurs non disponibles.")
            else:
                rep = repartition.copy()
                rep["label"] = rep["metrique"].map({
                    "anteil_privatanleger":  "Investisseurs privés",
                    "anteil_institutionelle":"Investisseurs institutionnels",
                })
                fig = px.pie(rep, names="label", values="valeur",
                             title="Répartition privés / institutionnels (Allemagne)",
                             hole=0.45,
                             color_discrete_sequence=["#4CAF50", "#2196F3"])
                fig.update_traces(textinfo="percent+label")
                fig.update_layout(height=430)
                st.plotly_chart(fig, use_container_width=True)

                # Bar complémentaire
                col_p, col_i = st.columns(2)
                priv_pct = rep[rep["metrique"] == "anteil_privatanleger"]["valeur"].values
                inst_pct = rep[rep["metrique"] == "anteil_institutionelle"]["valeur"].values
                if priv_pct.size:
                    col_p.metric("Investisseurs privés",  f"{int(priv_pct[0])}%")
                if inst_pct.size:
                    col_i.metric("Investisseurs institutionnels", f"{int(inst_pct[0])}%")

        with t4:
            if kag_bvi.empty:
                st.info("Données parts de marché KAG non encore disponibles. "
                        "La page BVI Marktanteile sera scrappée au prochain `python main.py apis`.")
            else:
                kag_sorted = kag_bvi.sort_values("valeur", ascending=False)
                top25 = kag_sorted.head(25)

                fig = px.bar(top25, x="valeur", y="entite", orientation="h",
                            color="valeur", color_continuous_scale="Blues",
                            labels={"valeur": "AuM géré (Mio €)", "entite": "Société"},
                            title="Top 25 sociétés de gestion — Parts de marché (BVI)")
                fig.update_layout(height=700, yaxis={"categoryorder": "total ascending"})
                st.plotly_chart(fig, use_container_width=True)

                total_kag = kag_sorted["valeur"].sum()
                st.metric("AuM couvert par le classement", f"{total_kag/1000:,.0f} Mrd €")
                st.dataframe(kag_sorted[["entite","valeur","date_donnees"]].rename(columns={
                    "entite":"Société","valeur":"AuM (Mio €)","date_donnees":"Date"}))


# ── Page: Actifs Non Cotés ────────────────────────────────────────────────────

elif page == "🏗️ Actifs Non Cotés":
    st.title("Actifs Non Cotés en Allemagne")
    st.markdown(
        "Veille sur les marchés **Private Equity, Private Debt, Infrastructure, Immobilier, "
        "ELTIF 2.0** — segments stratégiques pour EdRAM en 2026."
    )

    sources = load_sources()
    nc_sources = sources[sources["secteur_nom"] == "Actifs Non Cotés"] if not sources.empty else pd.DataFrame()

    # KPIs globaux
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Sources non cotées", len(nc_sources))
    if not nc_sources.empty:
        col2.metric("Sous-catégories", nc_sources["sous_categorie"].nunique())
        col3.metric("Sources prioritaires (high)", (nc_sources["priorite"] == "high").sum())
        col4.metric("Actives", nc_sources["actif"].sum() if "actif" in nc_sources.columns else len(nc_sources))

    if nc_sources.empty:
        st.info("Aucune source non cotée. Relancez `python main.py init` pour charger les sources.")
    else:
        tabs = st.tabs([
            "📊 Vue globale", "🏷️ Par sous-catégorie", "🔗 Répertoire sources",
            "📈 Données scrapées"
        ])

        with tabs[0]:
            col_a, col_b = st.columns(2)
            with col_a:
                sub_counts = nc_sources["sous_categorie"].value_counts().reset_index()
                sub_counts.columns = ["Sous-catégorie", "Nb sources"]
                fig = px.bar(
                    sub_counts, x="Nb sources", y="Sous-catégorie", orientation="h",
                    color="Nb sources", color_continuous_scale="Purples",
                    title="Sources par sous-catégorie non cotée"
                )
                fig.update_layout(height=500, showlegend=False,
                                  yaxis={"categoryorder": "total ascending"})
                st.plotly_chart(fig, use_container_width=True)

            with col_b:
                prio_counts = nc_sources["priorite"].value_counts().reset_index()
                prio_counts.columns = ["Priorité", "Nombre"]
                fig2 = px.pie(
                    prio_counts, values="Nombre", names="Priorité", hole=0.45,
                    title="Répartition par priorité",
                    color_discrete_map={"high": "#7b1fa2", "medium": "#9c27b0", "low": "#e1bee7"}
                )
                st.plotly_chart(fig2, use_container_width=True)

            # Matrice pertinence EdRAM
            st.subheader("📌 Pertinence stratégique par catégorie pour EdRAM")
            pertinence_data = {
                "Catégorie": ["Private Debt", "Infrastructure", "ELTIF 2.0",
                               "Investisseurs allocateurs", "Agrégateurs cross",
                               "Private Equity", "Immobilier Privé", "Hedge Funds",
                               "Venture Capital", "Actifs Réels"],
                "Pertinence EdRAM": ["FORTE", "FORTE", "FORTE", "FORTE", "FORTE",
                                      "Moyenne", "Moyenne", "Moyenne", "Faible", "Faible"],
                "Raison": [
                    "Extension naturelle gamme obligataire",
                    "Sondervermögen catalyseur direct 2026",
                    "Nouveau marché retail non cotés DE",
                    "Cibles directes de distribution",
                    "Données de marché consolidées",
                    "Present mais non prioritaire",
                    "Demande institutionnelle DE forte",
                    "Diversification portefeuille",
                    "Faible synérgie gamme actuelle",
                    "Faible synérgie gamme actuelle",
                ],
                "Score": [5, 5, 5, 5, 4, 3, 3, 3, 1, 1],
            }
            df_pert = pd.DataFrame(pertinence_data).sort_values("Score", ascending=False)

            def _color_pertinence(row):
                color = {"FORTE": "#7b1fa2", "Moyenne": "#9c27b0", "Faible": "#ce93d8"}.get(row["Pertinence EdRAM"], "")
                return [f"background-color: {color}; color: white" if color else ""] * len(row)

            st.dataframe(
                df_pert.style.apply(_color_pertinence, axis=1),
                use_container_width=True,
            )

        with tabs[1]:
            subcats = sorted(nc_sources["sous_categorie"].dropna().unique())
            sel_sub = st.selectbox("Choisir une sous-catégorie", subcats)
            df_sub = nc_sources[nc_sources["sous_categorie"] == sel_sub]
            st.metric(f"Sources — {sel_sub}", len(df_sub))

            type_in_sub = df_sub["type_source"].value_counts().reset_index()
            type_in_sub.columns = ["Type", "Nb"]
            col_s1, col_s2 = st.columns([1, 2])
            with col_s1:
                fig = px.pie(type_in_sub, values="Nb", names="Type", hole=0.4,
                             title="Par type de source")
                st.plotly_chart(fig, use_container_width=True)
            with col_s2:
                st.dataframe(
                    df_sub[["url", "domain", "type_source", "methode_scraping", "priorite"]],
                    height=400,
                )

        with tabs[2]:
            # Filtres
            col_f1, col_f2 = st.columns(2)
            type_filter = col_f1.selectbox("Type source", ["Tous"] + sorted(nc_sources["type_source"].unique()))
            prio_filter = col_f2.selectbox("Priorité", ["Tous", "high", "medium", "low"])
            df_dir = nc_sources.copy()
            if type_filter != "Tous":
                df_dir = df_dir[df_dir["type_source"] == type_filter]
            if prio_filter != "Tous":
                df_dir = df_dir[df_dir["priorite"] == prio_filter]
            st.dataframe(
                df_dir[["url", "domain", "sous_categorie", "type_source",
                         "methode_scraping", "priorite", "dernier_scrape"]],
                height=600,
            )

        with tabs[3]:
            # Données raw scrapées pour les sources non cotées
            try:
                df_raw = pd.read_sql(
                    """
                    SELECT sr.url, sr.titre_page, sr.status_code,
                           sr.scrape_date, sr.duree_ms,
                           s.sous_categorie
                    FROM scrape_raw sr
                    JOIN sources s ON s.id = sr.source_id
                    WHERE s.secteur_nom = 'Actifs Non Cotés'
                    ORDER BY sr.scrape_date DESC
                    LIMIT 500
                    """,
                    engine
                )
            except Exception:
                df_raw = pd.DataFrame()

            if df_raw.empty:
                st.info("Aucune donnée scrapée pour le secteur non coté. Lancez le scraping.")
            else:
                st.metric("Pages scrapées (non cotés)", len(df_raw))
                success_rate = (df_raw["status_code"].between(200, 299)).mean() * 100
                st.metric("Taux succès HTTP", f"{success_rate:.1f}%")
                sub_scraped = df_raw["sous_categorie"].value_counts().reset_index()
                sub_scraped.columns = ["Sous-catégorie", "Pages scrapées"]
                fig = px.bar(sub_scraped, x="Pages scrapées", y="Sous-catégorie",
                             orientation="h", color="Pages scrapées",
                             color_continuous_scale="Purples",
                             title="Pages scrapées par sous-catégorie")
                fig.update_layout(height=400, yaxis={"categoryorder":"total ascending"})
                st.plotly_chart(fig, use_container_width=True)
                st.dataframe(df_raw[["url", "titre_page", "status_code",
                                     "sous_categorie", "scrape_date"]].head(200))


# ── Page: Analyse Presse & Légal ─────────────────────────────────────────────

elif page == "📰 Analyse Presse & Légal":
    st.title("Analyse textuelle — Presse & Cadre Légal")
    st.markdown(
        "Fréquence des mots-clés dans les pages scrapées des secteurs **Presse** et "
        "**Cadre Légal & Réglementaire** (Allemand → Français traduit automatiquement)."
    )

    # Dictionnaire de traduction allemand → français (termes finance/légal clés)
    DE_FR_DICT = {
        # Termes financiers généraux
        "fonds": "fonds", "fondsvermögen": "actifs sous gestion", "investitionen": "investissements",
        "anleger": "investisseurs", "anlegerinnen": "investisseurs", "kapitalanlage": "placement",
        "kapitalanlagen": "placements", "geldanlage": "épargne/placement", "rendite": "rendement",
        "performance": "performance", "risiko": "risque", "risiken": "risques",
        "markt": "marché", "märkte": "marchés", "marktanteil": "part de marché",
        "verwaltung": "gestion", "verwaltungsgebühr": "frais de gestion",
        "nachhaltig": "durable", "nachhaltigkeit": "durabilité",
        "regulierung": "régulation", "regulierungen": "régulations",
        "vorschriften": "réglementations", "richtlinie": "directive", "verordnung": "règlement",
        "gesetz": "loi", "gesetze": "lois", "rechtlich": "juridique",
        "aufsicht": "supervision", "aufsichtsbehörde": "autorité de supervision",
        "zulassung": "autorisation", "genehmigung": "approbation",
        "offenlegung": "divulgation", "transparenz": "transparence",
        "berichterstattung": "reporting", "bericht": "rapport", "berichte": "rapports",
        "jahresbericht": "rapport annuel", "halbjahresbericht": "rapport semestriel",
        "portfolio": "portefeuille", "portfolios": "portefeuilles",
        "aktien": "actions", "anleihen": "obligations", "renten": "obligations/rentes",
        "immobilien": "immobilier", "infrastruktur": "infrastructure",
        "alternativen": "alternatifs", "alternative": "alternatif",
        "private equity": "private equity", "privatmarkt": "marché privé",
        "schulden": "dette", "kredit": "crédit", "darlehen": "prêt",
        "zinsen": "taux d'intérêt", "zins": "intérêt", "zinssatz": "taux",
        "inflation": "inflation", "deflation": "déflation",
        "wachstum": "croissance", "wirtschaft": "économie", "wirtschaftlich": "économique",
        "deutschland": "Allemagne", "europa": "Europe", "europäisch": "européen",
        "institution": "institution", "institutionell": "institutionnel",
        "privatanleger": "investisseur privé", "retail": "particuliers",
        "zuflüsse": "flux entrants", "abflüsse": "flux sortants", "flüsse": "flux",
        "verwaltete": "géré(s)", "vermögen": "patrimoine/actifs",
        "sparplan": "plan d'épargne", "sparpläne": "plans d'épargne",
        "etf": "ETF", "indexfonds": "fonds indiciels", "aktiv": "actif", "passiv": "passif",
        "kosten": "coûts", "gebühren": "frais", "ter": "TER", "total expense ratio": "ratio de frais total",
        "vertrieb": "distribution", "vertriebskanal": "canal de distribution",
        "berater": "conseiller", "beratung": "conseil", "beratungsgebühr": "frais de conseil",
        "esg": "ESG", "nachhaltige": "durables", "grüne": "verts/vertes",
        "klimawandel": "changement climatique", "umwelt": "environnement",
        "sfdr": "SFDR", "artikel": "article", "offenlegungsverordnung": "SFDR",
        "mifid": "MiFID", "aifmd": "AIFMD", "ucits": "OPCVM", "aifm": "FIA",
        "bafin": "BaFin", "bundesbank": "Bundesbank", "esma": "ESMA", "efama": "EFAMA",
        "bvi": "BVI", "eltif": "ELTIF", "aif": "FIA", "spezialfonds": "fonds spéciaux",
        "sondervermögen": "Sondervermögen", "bundeswehr": "Bundeswehr",
        "altersvorsorge": "retraite", "rente": "retraite/pension", "pensionskasse": "caisse de pension",
        "betriebliche": "professionnel(le)", "versicherung": "assurance",
        "haftung": "responsabilité", "compliance": "conformité",
        "greenwashing": "greenwashing", "klimarisiken": "risques climatiques",
        "technologie": "technologie", "digitalisierung": "digitalisation",
        "künstliche intelligenz": "intelligence artificielle", "blockchain": "blockchain",
        "daten": "données", "plattform": "plateforme",
        "institutionelle": "institutionnels", "vermögensverwalter": "gestionnaires d'actifs",
        "fondsmanager": "gérants de fonds", "fondsgesellschaft": "société de gestion",
        "kapitalverwaltungsgesellschaft": "société de gestion", "kvg": "SGP",
        "banken": "banques", "spezialisten": "spécialistes",
        "wettbewerb": "concurrence", "marktentwicklung": "évolution du marché",
        "prognose": "prévision", "ausblick": "perspectives",
        "niedrigzins": "bas taux", "hochzins": "haut rendement", "zinswende": "retournement des taux",
        "rezession": "récession", "konjunktur": "conjoncture",
        "high yield": "haut rendement", "investment grade": "investment grade",
        "unternehmensanleihen": "obligations d'entreprise", "staatsanleihen": "obligations d'État",
        "covered bonds": "obligations sécurisées", "pfandbriefe": "Pfandbriefe",
    }

    # Stopwords allemands pour filtrage
    DE_STOPWORDS = {
        "die", "der", "das", "und", "ist", "in", "von", "mit", "für", "auf",
        "an", "zu", "im", "des", "bei", "als", "wie", "eine", "ein", "oder",
        "sich", "den", "dem", "nicht", "auch", "wird", "aus", "es", "am",
        "bis", "durch", "mehr", "nach", "zum", "haben", "wird", "werden",
        "wurde", "waren", "haben", "hatte", "kann", "können", "wird", "werde",
        "seine", "ihrer", "seinem", "ihrem", "dass", "ob", "wenn", "aber",
        "noch", "nur", "schon", "seit", "über", "unter", "zwischen", "gegen",
        "ohne", "während", "wegen", "dabei", "damit", "dazu", "dann", "so",
        "um", "sind", "hat", "sie", "er", "wir", "ihr", "ihre", "seinen",
        "diesen", "dieser", "dieses", "einen", "einem", "einer", "jeder",
        "alle", "allem", "viele", "vielen", "sowie", "dass", "welche",
        "diesem", "diesen", "hierzu", "hierfür", "dabei", "hieran",
        "https", "www", "com", "de", "en", "html", "pdf", "htm",
        "januar", "februar", "märz", "april", "mai", "juni",
        "juli", "august", "september", "oktober", "november", "dezember",
        "2024", "2025", "2026", "1", "2", "3", "4", "5", "6", "7", "8",
    }

    def count_words_from_text(text: str, top_n: int = 60) -> list[tuple[str, int]]:
        import re
        from collections import Counter
        words = re.findall(r'\b[a-zA-ZäöüÄÖÜß]{4,}\b', text.lower())
        words = [w for w in words if w not in DE_STOPWORDS and len(w) > 3]
        return Counter(words).most_common(top_n)

    def translate_word(word: str) -> str:
        """Traduit un mot allemand en français via dictionnaire."""
        return DE_FR_DICT.get(word.lower(), word)

    # Sélection des secteurs à analyser
    col_sel1, col_sel2 = st.columns(2)
    secteurs_dispo = ["Cadre Légal & Réglementaire", "Presse & Classements de Fonds",
                      "Tendances Produits & Comportement", "Actifs Non Cotés",
                      "Plan de Relance & Macro"]
    sel_secteurs = col_sel1.multiselect(
        "Secteurs à analyser",
        secteurs_dispo,
        default=["Cadre Légal & Réglementaire", "Presse & Classements de Fonds"],
    )
    top_n_words = col_sel2.slider("Nombre de mots à afficher", 20, 100, 40)

    if not sel_secteurs:
        st.warning("Sélectionnez au moins un secteur.")
    else:
        # Charger les textes scrapés
        try:
            placeholders = ",".join(f"'{s}'" for s in sel_secteurs)
            df_texts = pd.read_sql(
                f"""
                SELECT sr.contenu_text, sr.titre_page, sr.url,
                       s.secteur_nom, s.sous_categorie
                FROM scrape_raw sr
                JOIN sources s ON s.id = sr.source_id
                WHERE s.secteur_nom IN ({placeholders})
                  AND sr.contenu_text IS NOT NULL
                  AND length(sr.contenu_text) > 200
                ORDER BY sr.scrape_date DESC
                LIMIT 2000
                """,
                engine,
            )
        except Exception as e:
            df_texts = pd.DataFrame()
            st.warning(f"Erreur chargement données: {e}")

        if df_texts.empty:
            st.info(
                "Aucun texte scrapé disponible pour les secteurs sélectionnés. "
                "Lancez le scraping avec `python main.py scrape` puis revenez ici."
            )
        else:
            st.success(f"✅ {len(df_texts)} pages analysées — {df_texts['contenu_text'].str.len().sum():,} caractères")

            # Concaténer tous les textes
            full_text = " ".join(df_texts["contenu_text"].dropna().tolist())
            word_counts = count_words_from_text(full_text, top_n=top_n_words)

            if not word_counts:
                st.info("Textes insuffisants pour l'analyse de fréquence.")
            else:
                # Traduire les mots
                word_df = pd.DataFrame(word_counts, columns=["mot_allemand", "occurrences"])
                word_df["mot_francais"] = word_df["mot_allemand"].apply(translate_word)
                word_df["traduit"] = word_df["mot_francais"] != word_df["mot_allemand"]

                tab_wc, tab_by_sector, tab_raw = st.tabs([
                    "📊 Top mots-clés", "🗂 Par secteur", "📋 Tableau complet"
                ])

                with tab_wc:
                    col_chart, col_info = st.columns([3, 1])
                    with col_chart:
                        # Barplot horizontal
                        fig = px.bar(
                            word_df.head(40),
                            x="occurrences",
                            y="mot_francais",
                            orientation="h",
                            color="occurrences",
                            color_continuous_scale="Reds",
                            hover_data=["mot_allemand", "traduit"],
                            labels={"occurrences": "Occurrences",
                                    "mot_francais": "Terme (traduit)"},
                            title=f"Top {min(40, len(word_df))} termes les plus fréquents "
                                  f"({', '.join(sel_secteurs)})",
                        )
                        fig.update_layout(
                            height=max(600, len(word_df.head(40)) * 18),
                            yaxis={"categoryorder": "total ascending"},
                            showlegend=False,
                        )
                        st.plotly_chart(fig, use_container_width=True)
                    with col_info:
                        st.markdown("**Légende**")
                        st.markdown("- Les termes en **gras** sont des termes financiers traduits")
                        st.markdown(f"- {word_df['traduit'].sum()} termes traduits sur {len(word_df)}")
                        st.markdown("---")
                        st.markdown("**Top 10 termes**")
                        for _, row in word_df.head(10).iterrows():
                            st.markdown(
                                f"**{row['mot_francais']}** `{row['occurrences']}x`"
                                + (f" *(DE: {row['mot_allemand']})*" if row["traduit"] else "")
                            )

                with tab_by_sector:
                    for sec in sel_secteurs:
                        df_sec = df_texts[df_texts["secteur_nom"] == sec]
                        if df_sec.empty:
                            continue
                        st.subheader(f"📂 {sec} — {len(df_sec)} pages")
                        sec_text = " ".join(df_sec["contenu_text"].dropna().tolist())
                        sec_words = count_words_from_text(sec_text, top_n=20)
                        if sec_words:
                            sec_df = pd.DataFrame(sec_words, columns=["mot_allemand", "occurrences"])
                            sec_df["mot_francais"] = sec_df["mot_allemand"].apply(translate_word)
                            fig = px.bar(
                                sec_df, x="occurrences", y="mot_francais", orientation="h",
                                color="occurrences", color_continuous_scale="Blues",
                                title=f"Top 20 — {sec}",
                                labels={"occurrences": "Occurrences", "mot_francais": "Terme"},
                            )
                            fig.update_layout(
                                height=400, showlegend=False,
                                yaxis={"categoryorder": "total ascending"},
                            )
                            st.plotly_chart(fig, use_container_width=True)

                        # Sous-catégories
                        st.markdown("**Sous-catégories couvertes :**")
                        sub_coverage = df_sec["sous_categorie"].value_counts().reset_index()
                        sub_coverage.columns = ["Sous-catégorie", "Pages"]
                        st.dataframe(sub_coverage, use_container_width=True, height=200)
                        st.markdown("---")

                with tab_raw:
                    st.dataframe(
                        word_df.rename(columns={
                            "mot_allemand": "Terme (allemand)",
                            "mot_francais": "Terme (français)",
                            "occurrences": "Occurrences",
                            "traduit": "Traduit",
                        }),
                        height=600,
                        use_container_width=True,
                    )
                    st.download_button(
                        "⬇️ Télécharger CSV",
                        data=word_df.to_csv(index=False),
                        file_name="mots_cles_presse_legal.csv",
                        mime="text/csv",
                    )


# ── Page: Réglementation ─────────────────────────────────────────────────────

elif page == "📋 Réglementation":
    st.title("Veille réglementaire")
    regs = load_reglementation()

    if regs.empty:
        st.info("Aucune donnée réglementaire disponible.")
    else:
        orgs = ["Tous"] + sorted(regs["organisme"].dropna().unique().tolist())
        selected_org = st.selectbox("Organisme", orgs)
        if selected_org != "Tous":
            regs = regs[regs["organisme"] == selected_org]

        types = ["Tous"] + sorted(regs["type_texte"].dropna().unique().tolist())
        selected_type = st.selectbox("Type", types)
        if selected_type != "Tous":
            regs = regs[regs["type_texte"] == selected_type]

        for _, row in regs.iterrows():
            with st.expander(f"📄 {row.get('titre', 'Sans titre')[:100]}"):
                col1, col2 = st.columns(2)
                col1.markdown(f"**Organisme:** {row.get('organisme', 'N/A')}")
                col2.markdown(f"**Type:** {row.get('type_texte', 'N/A')}")
                if row.get("resume"):
                    st.markdown(row["resume"][:500])
                if row.get("contraintes"):
                    st.warning(f"⚠️ Contraintes: {row['contraintes'][:300]}")
                if row.get("url_document"):
                    st.markdown(f"[🔗 Document]({row['url_document']})")


# ── Page: Sources & Scraping ─────────────────────────────────────────────────

elif page == "🔍 Sources & Scraping":
    st.title("Sources enregistrées")
    sources = load_sources()

    if sources.empty:
        st.info("Aucune source enregistrée.")
    else:
        col1, col2, col3 = st.columns(3)
        with col1:
            secteur_filter = st.selectbox("Secteur", ["Tous"] + sorted(sources["secteur_nom"].unique().tolist()))
        with col2:
            type_filter = st.selectbox("Type", ["Tous"] + sorted(sources["type_source"].unique().tolist()))
        with col3:
            prio_filter = st.selectbox("Priorité", ["Tous", "high", "medium", "low"])

        df = sources.copy()
        if secteur_filter != "Tous":
            df = df[df["secteur_nom"] == secteur_filter]
        if type_filter != "Tous":
            df = df[df["type_source"] == type_filter]
        if prio_filter != "Tous":
            df = df[df["priorite"] == prio_filter]

        st.metric("Sources filtrées", len(df))
        st.dataframe(df[["url", "domain", "secteur_nom", "type_source", "nature_technique",
                         "methode_scraping", "priorite", "dernier_scrape"]], height=600)


# ── Page: Monitoring ─────────────────────────────────────────────────────────

elif page == "⚙️ Monitoring":
    st.title("Monitoring du scraping")
    logs = load_scrape_log()

    if logs.empty:
        st.info("Aucun log de scraping disponible.")
    else:
        col1, col2, col3 = st.columns(3)
        total = len(logs)
        success = logs["success"].sum() if "success" in logs.columns else 0
        col1.metric("Total scrapes", total)
        col2.metric("Succès", int(success))
        col3.metric("Taux succès", f"{success/total*100:.1f}%" if total > 0 else "N/A")

        if "scrape_date" in logs.columns:
            logs["scrape_date"] = pd.to_datetime(logs["scrape_date"])
            daily = logs.set_index("scrape_date").resample("D")["success"].agg(["count", "sum"])
            daily.columns = ["Total", "Succès"]
            fig = px.line(daily, y=["Total", "Succès"], title="Scrapes par jour")
            st.plotly_chart(fig, use_container_width=True)

        if "methode" in logs.columns:
            method_stats = logs.groupby("methode").agg(
                total=("id", "count"),
                succes=("success", "sum"),
            ).reset_index()
            method_stats["taux"] = (method_stats["succes"] / method_stats["total"] * 100).round(1)
            st.subheader("Par méthode")
            st.dataframe(method_stats)

        # Dernières erreurs
        errors = logs[logs["success"] == False].tail(20) if "success" in logs.columns else pd.DataFrame()
        if not errors.empty:
            st.subheader("Dernières erreurs")
            st.dataframe(errors[["url", "error_message", "status_code", "scrape_date"]].tail(15))

# ──────────────────────────────────────────────────────────────────────────────
# EDR Scraping — Analytics & Agrégations (Pandas)
# ──────────────────────────────────────────────────────────────────────────────
import logging
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from database.models import engine
from config.settings import DATA_DIR

logger = logging.getLogger("edr.analytics")


def load_fonds_df() -> pd.DataFrame:
    """Charge les données fonds depuis la DB."""
    query = "SELECT * FROM fonds WHERE nom_fonds IS NOT NULL"
    df = pd.read_sql(query, engine)
    for col in ("perf_ytd_pct", "perf_1y_pct", "perf_3y_pct", "perf_5y_pct", "ter_pct", "aum_meur"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def load_marche_df() -> pd.DataFrame:
    """Charge les données marché depuis la DB."""
    df = pd.read_sql("SELECT * FROM marche", engine)
    if "valeur" in df.columns:
        df["valeur"] = pd.to_numeric(df["valeur"], errors="coerce")
    return df


def load_sources_df() -> pd.DataFrame:
    """Charge les sources depuis la DB."""
    return pd.read_sql("SELECT * FROM sources", engine)


def load_scrape_log_df() -> pd.DataFrame:
    """Charge les logs de scraping."""
    return pd.read_sql("SELECT * FROM scrape_log", engine)


# ── Agrégations Fonds ────────────────────────────────────────────────────────

def top_fonds_by_performance(n: int = 20, period: str = "1y") -> pd.DataFrame:
    """Top N fonds par performance sur une période."""
    df = load_fonds_df()
    col = {"ytd": "perf_ytd_pct", "1y": "perf_1y_pct", "3y": "perf_3y_pct", "5y": "perf_5y_pct"}.get(period, "perf_1y_pct")
    return df.dropna(subset=[col]).nlargest(n, col)[
        ["nom_fonds", "isin", "societe_gestion", "categorie", col, "ter_pct", "aum_meur"]
    ]


def top_fonds_by_aum(n: int = 20) -> pd.DataFrame:
    """Top N fonds par AUM."""
    df = load_fonds_df()
    return df.dropna(subset=["aum_meur"]).nlargest(n, "aum_meur")[
        ["nom_fonds", "isin", "societe_gestion", "categorie", "aum_meur", "perf_1y_pct", "ter_pct"]
    ]


def performance_by_category() -> pd.DataFrame:
    """Performance moyenne par catégorie."""
    df = load_fonds_df()
    return df.groupby("categorie").agg(
        nb_fonds=("id", "count"),
        perf_1y_moy=("perf_1y_pct", "mean"),
        perf_3y_moy=("perf_3y_pct", "mean"),
        ter_moyen=("ter_pct", "mean"),
        aum_total=("aum_meur", "sum"),
    ).sort_values("aum_total", ascending=False).reset_index()


def performance_by_manager() -> pd.DataFrame:
    """Performance et AUM par société de gestion."""
    df = load_fonds_df()
    return df.groupby("societe_gestion").agg(
        nb_fonds=("id", "count"),
        perf_1y_moy=("perf_1y_pct", "mean"),
        ter_moyen=("ter_pct", "mean"),
        aum_total=("aum_meur", "sum"),
    ).sort_values("aum_total", ascending=False).reset_index()


def edram_vs_competitors() -> pd.DataFrame:
    """Compare EdRAM vs concurrents directs."""
    df = load_fonds_df()
    if df.empty or "societe_gestion" not in df.columns:
        return pd.DataFrame()
    competitors = [
        "edmond", "rothschild", "edram", "edr",
        "candriam", "pictet", "amundi", "dws", "flossbach",
        "lupus alpha", "berenberg", "pimco", "fidelity",
        "blackrock", "axa", "bnp paribas"
    ]

    def match_manager(name):
        if not name:
            return False
        return any(c in name.lower() for c in competitors)

    mask = df["societe_gestion"].apply(match_manager)
    comp_df = df[mask].copy()
    if comp_df.empty:
        return pd.DataFrame()

    return comp_df.groupby("societe_gestion").agg(
        nb_fonds=("id", "count"),
        perf_1y_moy=("perf_1y_pct", "mean"),
        perf_3y_moy=("perf_3y_pct", "mean"),
        ter_moyen=("ter_pct", "mean"),
        aum_total=("aum_meur", "sum"),
        rating_moyen=("rating_morningstar", "mean"),
    ).sort_values("aum_total", ascending=False).reset_index()


# ── Agrégations Marché ───────────────────────────────────────────────────────

def market_segmentation() -> pd.DataFrame:
    """Segmentation du marché par catégorie d'actifs."""
    df = load_marche_df()
    return df.groupby(["entite", "metrique", "categorie"]).agg(
        derniere_valeur=("valeur", "last"),
        date_recente=("date_donnees", "max"),
    ).reset_index()


def market_flows_by_month() -> pd.DataFrame:
    """Flux nets mensuels par catégorie."""
    df = load_marche_df()
    flux = df[df["metrique"].str.contains("flux", case=False, na=False)].copy()
    if flux.empty:
        return flux
    flux["mois"] = pd.to_datetime(flux["date_donnees"]).dt.to_period("M").astype(str)
    return flux.groupby(["mois", "categorie"]).agg(
        flux_total=("valeur", "sum"),
    ).reset_index().sort_values("mois")


# ── Scoring stratégique des sources ──────────────────────────────────────────

def score_sources() -> pd.DataFrame:
    """Scoring des sources par importance stratégique."""
    sources = load_sources_df()
    logs = load_scrape_log_df()

    # Taux de succès par source
    if not logs.empty and "source_id" in logs.columns:
        success_rate = logs.groupby("source_id").agg(
            total_scrapes=("id", "count"),
            successful=("success", "sum"),
        ).reset_index()
        success_rate["taux_succes"] = success_rate["successful"] / success_rate["total_scrapes"]
        sources = sources.merge(success_rate, left_on="id", right_on="source_id", how="left")
    else:
        sources["taux_succes"] = 0.0

    # Score composite
    priority_scores = {"high": 3, "medium": 2, "low": 1}
    type_scores = {"donnees_marche": 3, "plateforme": 3, "regulateur": 2, "asset_manager": 2, "presse": 1, "recherche": 1}

    sources["score_priorite"] = sources["priorite"].map(priority_scores).fillna(1)
    sources["score_type"] = sources["type_source"].map(type_scores).fillna(1)
    sources["score_strategique"] = (
        sources["score_priorite"] * 0.4 +
        sources["score_type"] * 0.3 +
        sources["taux_succes"].fillna(0) * 3 * 0.3
    )

    return sources[["url", "domain", "secteur_nom", "type_source", "priorite", "score_strategique"]
    ].sort_values("score_strategique", ascending=False)


# ── Export ───────────────────────────────────────────────────────────────────

def export_all_reports():
    """Exporte tous les rapports en CSV."""
    reports = {
        "top_fonds_performance": top_fonds_by_performance,
        "top_fonds_aum": top_fonds_by_aum,
        "performance_par_categorie": performance_by_category,
        "performance_par_manager": performance_by_manager,
        "edram_vs_competitors": edram_vs_competitors,
        "segmentation_marche": market_segmentation,
        "flux_mensuels": market_flows_by_month,
        "scoring_sources": score_sources,
    }

    output_dir = DATA_DIR / "reports"
    output_dir.mkdir(exist_ok=True)

    for name, func in reports.items():
        try:
            df = func()
            if not df.empty:
                path = output_dir / f"{name}.csv"
                df.to_csv(path, index=False, encoding="utf-8-sig")
                logger.info(f"Export: {path} ({len(df)} lignes)")
            else:
                logger.warning(f"Export vide: {name}")
        except Exception as e:
            logger.error(f"Erreur export {name}: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    export_all_reports()

-- ──────────────────────────────────────────────────────────────────────────────
-- EDR Scraping — Schéma PostgreSQL
-- ──────────────────────────────────────────────────────────────────────────────

-- Sources enregistrées
CREATE TABLE IF NOT EXISTS sources (
    id              SERIAL PRIMARY KEY,
    url             TEXT NOT NULL UNIQUE,
    domain          TEXT NOT NULL,
    secteur         INTEGER NOT NULL,
    secteur_nom     TEXT NOT NULL,
    sous_categorie  TEXT,
    type_source     TEXT NOT NULL,         -- regulateur, donnees_marche, plateforme, asset_manager, presse, recherche, institutionnel, autre
    nature_technique TEXT NOT NULL,        -- html_statique, dynamique_js, pdf
    methode_scraping TEXT NOT NULL,        -- requests_bs4, playwright, pdf_parser
    priorite        TEXT NOT NULL DEFAULT 'low',
    description     TEXT,
    dernier_scrape  TIMESTAMP,
    actif           BOOLEAN NOT NULL DEFAULT TRUE,
    score_strategique REAL DEFAULT 0.0,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sources_secteur ON sources(secteur);
CREATE INDEX IF NOT EXISTS idx_sources_priorite ON sources(priorite);
CREATE INDEX IF NOT EXISTS idx_sources_type ON sources(type_source);
CREATE INDEX IF NOT EXISTS idx_sources_domain ON sources(domain);

-- Données fonds (performance, AUM, TER, rating)
CREATE TABLE IF NOT EXISTS fonds (
    id              SERIAL PRIMARY KEY,
    source_id       INTEGER REFERENCES sources(id),
    isin            TEXT,
    nom_fonds       TEXT NOT NULL,
    societe_gestion TEXT,
    categorie       TEXT,
    sous_categorie  TEXT,
    devise          TEXT DEFAULT 'EUR',
    aum_meur        REAL,
    ter_pct         REAL,
    perf_ytd_pct    REAL,
    perf_1y_pct     REAL,
    perf_3y_pct     REAL,
    perf_5y_pct     REAL,
    rating_morningstar INTEGER,
    rating_scope    TEXT,
    article_sfdr    TEXT,
    date_donnees    DATE,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(isin, date_donnees)
);

CREATE INDEX IF NOT EXISTS idx_fonds_isin ON fonds(isin);
CREATE INDEX IF NOT EXISTS idx_fonds_societe ON fonds(societe_gestion);
CREATE INDEX IF NOT EXISTS idx_fonds_categorie ON fonds(categorie);

-- Données réglementaires
CREATE TABLE IF NOT EXISTS reglementation (
    id              SERIAL PRIMARY KEY,
    source_id       INTEGER REFERENCES sources(id),
    titre           TEXT NOT NULL,
    organisme       TEXT,
    type_texte      TEXT,                 -- directive, reglement, merkblatt, guideline, circulaire
    reference       TEXT,
    resume          TEXT,
    contraintes     TEXT,
    date_publication DATE,
    date_application DATE,
    url_document    TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_reglementation_organisme ON reglementation(organisme);
CREATE INDEX IF NOT EXISTS idx_reglementation_type ON reglementation(type_texte);

-- Données marché (flux, volumes, segmentation)
CREATE TABLE IF NOT EXISTS marche (
    id              SERIAL PRIMARY KEY,
    source_id       INTEGER REFERENCES sources(id),
    entite          TEXT NOT NULL,         -- BVI, Bundesbank, EFAMA...
    metrique        TEXT NOT NULL,         -- aum_total, flux_net, part_marche, nb_fonds...
    categorie       TEXT,                  -- actions, obligations, mixte, etf...
    segment         TEXT,                  -- retail, institutionnel, spezialfonds...
    valeur          REAL,
    unite           TEXT DEFAULT 'MEUR',
    date_donnees    DATE NOT NULL,
    created_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_marche_entite ON marche(entite);
CREATE INDEX IF NOT EXISTS idx_marche_metrique ON marche(metrique);
CREATE INDEX IF NOT EXISTS idx_marche_date ON marche(date_donnees);

-- Contenu scrappé brut
CREATE TABLE IF NOT EXISTS scrape_raw (
    id              SERIAL PRIMARY KEY,
    source_id       INTEGER REFERENCES sources(id),
    url             TEXT NOT NULL,
    status_code     INTEGER,
    content_type    TEXT,
    titre_page      TEXT,
    contenu_text    TEXT,
    contenu_html    TEXT,
    hash_contenu    TEXT,                 -- SHA-256 pour déduplication / scraping incrémental
    scrape_date     TIMESTAMP NOT NULL DEFAULT NOW(),
    duree_ms        INTEGER
);

CREATE INDEX IF NOT EXISTS idx_scrape_raw_source ON scrape_raw(source_id);
CREATE INDEX IF NOT EXISTS idx_scrape_raw_hash ON scrape_raw(hash_contenu);
CREATE INDEX IF NOT EXISTS idx_scrape_raw_date ON scrape_raw(scrape_date);

-- Log de scraping
CREATE TABLE IF NOT EXISTS scrape_log (
    id              SERIAL PRIMARY KEY,
    source_id       INTEGER REFERENCES sources(id),
    url             TEXT NOT NULL,
    success         BOOLEAN NOT NULL,
    status_code     INTEGER,
    error_message   TEXT,
    duree_ms        INTEGER,
    methode         TEXT,
    scrape_date     TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scrape_log_date ON scrape_log(scrape_date);
CREATE INDEX IF NOT EXISTS idx_scrape_log_success ON scrape_log(success);

-- Découverte de nouvelles URLs
CREATE TABLE IF NOT EXISTS discovered_urls (
    id              SERIAL PRIMARY KEY,
    parent_source_id INTEGER REFERENCES sources(id),
    url             TEXT NOT NULL UNIQUE,
    domain          TEXT,
    description     TEXT,
    discovered_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    reviewed        BOOLEAN NOT NULL DEFAULT FALSE,
    accepted        BOOLEAN DEFAULT NULL
);

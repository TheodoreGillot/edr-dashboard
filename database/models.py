# ──────────────────────────────────────────────────────────────────────────────
# EDR Scraping — ORM SQLAlchemy + init DB
# ──────────────────────────────────────────────────────────────────────────────
import hashlib
from datetime import datetime, date
from sqlalchemy import (
    create_engine, Column, Integer, Text, Float, Boolean, Date,
    DateTime, ForeignKey, UniqueConstraint, Index
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from config.settings import DB_DSN, SQLITE_PATH

Base = declarative_base()


class Source(Base):
    __tablename__ = "sources"
    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(Text, nullable=False, unique=True)
    domain = Column(Text, nullable=False)
    secteur = Column(Integer, nullable=False)
    secteur_nom = Column(Text, nullable=False)
    sous_categorie = Column(Text)
    type_source = Column(Text, nullable=False)
    nature_technique = Column(Text, nullable=False)
    methode_scraping = Column(Text, nullable=False)
    priorite = Column(Text, nullable=False, default="low")
    description = Column(Text)
    dernier_scrape = Column(DateTime)
    actif = Column(Boolean, nullable=False, default=True)
    score_strategique = Column(Float, default=0.0)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class Fonds(Base):
    __tablename__ = "fonds"
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_id = Column(Integer, ForeignKey("sources.id"))
    isin = Column(Text)
    nom_fonds = Column(Text, nullable=False)
    societe_gestion = Column(Text)
    categorie = Column(Text)
    sous_categorie = Column(Text)
    devise = Column(Text, default="EUR")
    aum_meur = Column(Float)
    ter_pct = Column(Float)
    perf_ytd_pct = Column(Float)
    perf_1y_pct = Column(Float)
    perf_3y_pct = Column(Float)
    perf_5y_pct = Column(Float)
    rating_morningstar = Column(Integer)
    rating_scope = Column(Text)
    article_sfdr = Column(Text)
    date_donnees = Column(Date)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    __table_args__ = (UniqueConstraint("isin", "date_donnees"),)


class Reglementation(Base):
    __tablename__ = "reglementation"
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_id = Column(Integer, ForeignKey("sources.id"))
    titre = Column(Text, nullable=False)
    organisme = Column(Text)
    type_texte = Column(Text)
    reference = Column(Text)
    resume = Column(Text)
    contraintes = Column(Text)
    date_publication = Column(Date)
    date_application = Column(Date)
    url_document = Column(Text)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class Marche(Base):
    __tablename__ = "marche"
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_id = Column(Integer, ForeignKey("sources.id"))
    entite = Column(Text, nullable=False)
    metrique = Column(Text, nullable=False)
    categorie = Column(Text)
    segment = Column(Text)
    valeur = Column(Float)
    unite = Column(Text, default="MEUR")
    date_donnees = Column(Date, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)


class ScrapeRaw(Base):
    __tablename__ = "scrape_raw"
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_id = Column(Integer, ForeignKey("sources.id"))
    url = Column(Text, nullable=False)
    status_code = Column(Integer)
    content_type = Column(Text)
    titre_page = Column(Text)
    contenu_text = Column(Text)
    contenu_html = Column(Text)
    hash_contenu = Column(Text)
    scrape_date = Column(DateTime, nullable=False, default=datetime.utcnow)
    duree_ms = Column(Integer)


class ScrapeLog(Base):
    __tablename__ = "scrape_log"
    id = Column(Integer, primary_key=True, autoincrement=True)
    source_id = Column(Integer, ForeignKey("sources.id"))
    url = Column(Text, nullable=False)
    success = Column(Boolean, nullable=False)
    status_code = Column(Integer)
    error_message = Column(Text)
    duree_ms = Column(Integer)
    methode = Column(Text)
    scrape_date = Column(DateTime, nullable=False, default=datetime.utcnow)


class DiscoveredUrl(Base):
    __tablename__ = "discovered_urls"
    id = Column(Integer, primary_key=True, autoincrement=True)
    parent_source_id = Column(Integer, ForeignKey("sources.id"))
    url = Column(Text, nullable=False, unique=True)
    domain = Column(Text)
    description = Column(Text)
    discovered_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    reviewed = Column(Boolean, nullable=False, default=False)
    accepted = Column(Boolean)


# ── Engine & Session ─────────────────────────────────────────────────────────
engine = create_engine(DB_DSN, echo=False, future=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)


def init_db():
    """Crée toutes les tables."""
    Base.metadata.create_all(engine)
    print(f"[OK] Base initialisée : {DB_DSN}")


def get_session():
    return SessionLocal()


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ── Insertion des sources depuis le parser ───────────────────────────────────
def load_sources_from_parsed(entries: list[dict]):
    """Insère les sources parsées dans la DB (upsert)."""
    session = get_session()
    inserted, skipped = 0, 0
    try:
        for e in entries:
            existing = session.query(Source).filter_by(url=e["url"]).first()
            if existing:
                skipped += 1
                continue
            source = Source(
                url=e["url"],
                domain=e["domain"],
                secteur=e["secteur"],
                secteur_nom=e["secteur_nom"],
                sous_categorie=e["sous_categorie"],
                type_source=e["type_source"],
                nature_technique=e["nature_technique"],
                methode_scraping=e["methode_scraping"],
                priorite=e["priorite"],
                description=e["description"],
            )
            session.add(source)
            inserted += 1
        session.commit()
        print(f"[OK] Sources : {inserted} insérées, {skipped} déjà existantes")
    except Exception as exc:
        session.rollback()
        raise exc
    finally:
        session.close()


if __name__ == "__main__":
    init_db()
    from config.parser import parse_links_file
    entries = parse_links_file()
    load_sources_from_parsed(entries)

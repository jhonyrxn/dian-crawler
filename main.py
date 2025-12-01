import os
import hashlib
from datetime import datetime
from typing import List, Optional

import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy import (Column, Integer, String, Text, DateTime, create_engine,
                        select)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

# -----------------------
# CONFIG
# -----------------------
# Cambia esta URL si prefieres otra sección de la DIAN
TARGET_URL = "https://www.dian.gov.co/notificaciones/Paginas/default.aspx"

DB_FILE = "documents.db"
DOWNLOAD_DIR = "pdfs"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Hora en la que quieres que corra diariamente (Formato HH:MM en zona Colombia)
# Aquí programamos a las 04:00 AM Colombia (-05:00)
SCHEDULE_HOUR = 4
SCHEDULE_MINUTE = 0
TIMEZONE = "America/Bogota"

# -----------------------
# DB (SQLAlchemy)
# -----------------------
Base = declarative_base()
engine = create_engine(f"sqlite:///{DB_FILE}", connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine)

class Document(Base):
    __tablename__ = "documents"
    id = Column(Integer, primary_key=True)
    title = Column(String(1000))
    url = Column(String(2000), unique=True)
    summary = Column(Text)
    hash = Column(String(64))
    discovered_at = Column(DateTime)

Base.metadata.create_all(engine)

# -----------------------
# FastAPI init
# -----------------------
app = FastAPI(title="DIAN Notificaciones Bot")

app.mount("/static", StaticFiles(directory="static"), name="static")

# -----------------------
# Pydantic models
# -----------------------
class DocumentOut(BaseModel):
    id: int
    title: str
    url: str
    summary: Optional[str]
    discovered_at: datetime

    class Config:
        orm_mode = True

# -----------------------
# CRAWLER
# -----------------------
def text_extract_from_html(soup):
    # Devuelve un texto corto para summary (primeros 300 caracteres de la sección principal)
    text = soup.get_text(separator=" ", strip=True)
    return (text[:600] + "...") if len(text) > 600 else text

def download_and_register(url: str, title: str, session: Session):
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        content = resp.content
        h = hashlib.md5(content).hexdigest()

        existing = session.query(Document).filter(Document.hash == h).first()
        if existing:
            return False  # ya existe

        # Intentar extraer un breve summary si el PDF no es HTML (si es HTML se usará text)
        summary = None
        try:
            if "text/html" in resp.headers.get("Content-Type", ""):
                soup = BeautifulSoup(resp.text, "html.parser")
                summary = text_extract_from_html(soup)
        except Exception:
            summary = None

        # guardar archivo si es pdf
        if resp.headers.get("Content-Type", "").lower().startswith("application/pdf"):
            filename = f"{h}.pdf"
            path = os.path.join(DOWNLOAD_DIR, filename)
            with open(path, "wb") as f:
                f.write(content)

        doc = Document(
            title=title[:1000],
            url=url,
            summary=summary,
            hash=h,
            discovered_at=datetime.utcnow()
        )
        session.add(doc)
        session.commit()
        return True
    except Exception as e:
        print("Error downloading:", url, e)
        return False

def crawl_notifications():
    print("Starting crawl:", datetime.utcnow().isoformat())
    try:
        resp = requests.get(TARGET_URL, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Estrategia general: buscar enlaces a PDF o a páginas que contengan "Notificación" / "Calendario"
        # Ajusta selectores según el HTML real de la página si quieres mayor precisión.
        links = soup.select("a[href]")
        new_count = 0
        with SessionLocal() as session:
            for a in links:
                href = a.get("href")
                title = a.get_text(strip=True) or "Documento DIAN"
                if not href:
                    continue
                if href.lower().endswith(".pdf") or "notific" in href.lower() or "calend" in href.lower():
                    # normaliza URL
                    if not href.startswith("http"):
                        href = requests.compat.urljoin(TARGET_URL, href)
                    added = download_and_register(href, title, session)
                    if added:
                        new_count += 1
        print(f"Crawl finished. New items: {new_count}")
    except Exception as e:
        print("Error in crawl_notifications:", e)

# Ejecutar un crawl inmediato al iniciar (una vez)
crawl_notifications()

# -----------------------
# SCHEDULER (APScheduler)
# -----------------------
scheduler = BackgroundScheduler()
trigger = CronTrigger(hour=SCHEDULE_HOUR, minute=SCHEDULE_MINUTE, timezone=pytz.timezone(TIMEZONE))
scheduler.add_job(crawl_notifications, trigger, id="daily-crawl")
scheduler.start()

# -----------------------
# ENDPOINTS
# -----------------------
@app.get("/api/documents", response_model=List[DocumentOut])
def list_documents(limit: int = 100):
    with SessionLocal() as session:
        stmt = select(Document).order_by(Document.discovered_at.desc()).limit(limit)
        docs = session.execute(stmt).scalars().all()
        return docs

@app.get("/api/documents/{doc_id}", response_model=DocumentOut)
def get_document(doc_id: int):
    with SessionLocal() as session:
        doc = session.get(Document, doc_id)
        if not doc:
            return {}
        return doc

@app.get("/")
def index():
    return FileResponse("static/index.html")

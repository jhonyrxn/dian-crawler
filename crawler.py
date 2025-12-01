import os
import hashlib
import sqlite3
from datetime import datetime
import requests
from bs4 import BeautifulSoup

TARGET_URL = "https://www.dian.gov.co/normatividad/normatividad/Resoluciones/Paginas/default.aspx"
DOWNLOAD_DIR = "pdfs"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

DB_FILE = "crawler.db"

conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS documentos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT,
    url TEXT UNIQUE,
    hash TEXT,
    downloaded_at TEXT
)
""")
conn.commit()

def download_pdf(url, title):
    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()

        file_hash = hashlib.md5(response.content).hexdigest()

        cursor.execute("SELECT id FROM documentos WHERE hash = ?", (file_hash,))
        if cursor.fetchone():
            print(f"⚠️ Ya existe: {title}")
            return

        filename = os.path.join(DOWNLOAD_DIR, f"{file_hash}.pdf")

        with open(filename, "wb") as f:
            f.write(response.content)

        cursor.execute(
            "INSERT INTO documentos (title, url, hash, downloaded_at) VALUES (?, ?, ?, ?)",
            (title, url, file_hash, datetime.now().isoformat())
        )
        conn.commit()
        print(f"📥 Descargar: {title}")

    except Exception as e:
        print(f"❌ Error {url}: {e}")

def crawl():
    print("🔎 Buscando resoluciones DIAN...")

    response = requests.get(TARGET_URL)
    soup = BeautifulSoup(response.text, "html.parser")

    for link in soup.select("a[href$='.pdf']"):
        url = link.get("href")
        title = link.get_text(strip=True)
        if not url.startswith("http"):
            url = "https://www.dian.gov.co" + url
        download_pdf(url, title)

    print("✔ Finalizado.")

if __name__ == "__main__":
    crawl()
    conn.close()

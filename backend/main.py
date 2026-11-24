import os
import json
import socket
import time
import redis
import psycopg2
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="SOLEIL Fine Art API")
cache = redis.Redis(host=os.getenv("REDIS_HOST", "redis"), port=6379, decode_responses=True)

ARTISTS_BIO = {
    "Mark Rothko": {
        "years": "1903–1970",
        "origin": "United States",
        "bio": "A towering figure of Abstract Expressionism and Color Field painting. Rothko’s luminous, large-scale canvases evoke profound emotional and spiritual resonance, ranking among the most coveted works in private collections worldwide."
    },
    "Berthe Morisot": {
        "years": "1841–1895",
        "origin": "France",
        "bio": "A foundational member of the Parisian Impressionist circle and the sole woman to exhibit in the landmark 1874 exhibition. Celebrated for her luminous brushwork and intimate, psychologically nuanced compositions."
    },
    "Tamara de Lempicka": {
        "years": "1898–1980",
        "origin": "Poland / France",
        "bio": "The undisputed icon of Art Deco portraiture and favorite painter of the European aristocracy in the 1920s and 1930s, renowned for sculptural volumes and cool, aristocratic elegance."
    },
    "Paul Signac": {
        "years": "1863–1935",
        "origin": "France",
        "bio": "Co-founder of Neo-Impressionism and master theorist of Pointillism, famed for radiant Mediterranean harbor scenes constructed from pure, unmixed strokes of color."
    },
    "Mary Cassatt": {
        "years": "1844–1926",
        "origin": "United States / France",
        "bio": "An eminent American painter who worked closely alongside Edgar Degas in Paris. Her original oils are held in the world’s foremost museum and private holdings."
    },
    "Paul Gauguin": {
        "years": "1848–1903",
        "origin": "France",
        "bio": "A pioneer of Post-Impressionism and Synthetism whose bold color harmonies and rare still lifes profoundly shaped twentieth-century modern art."
    },
    "Marc Chagall": {
        "years": "1887–1985",
        "origin": "France",
        "bio": "One of the foremost modernists of the twentieth century, uniting avant-garde structure with poetic symbolism and jewel-like chromatic depth."
    }
}

INITIAL_ARTWORKS = [
    {
        "id": 1,
        "section": "catalog",
        "title": "No. 61 (Rust and Blue) — Study",
        "artist": "Mark Rothko",
        "year": "1953",
        "era": "Abstract Expressionism",
        "medium": "Oil on canvas · 115 × 92 cm",
        "provenance": "Private Collection, New York; Acquired directly from the artist's estate",
        "exhibited": "Kunsthalle Basel, Retrospective Exhibition, 1971",
        "price": None,
        "price_label": "Price Upon Request",
        "status": "Original · Private Sale",
        "image": "/images/art1.jpg"
    },
    {
        "id": 2,
        "section": "catalog",
        "title": "Jeune fille au chien",
        "artist": "Berthe Morisot",
        "year": "1887",
        "era": "Impressionism",
        "medium": "Oil on canvas · 73 × 60 cm",
        "provenance": "Collection of Julie Manet, Paris; Private European Holding",
        "exhibited": "Galerie Durand-Ruel, Paris, 1896",
        "price": 840000,
        "price_label": "€ 840,000",
        "status": "Original · Available",
        "image": "/images/art2.jpg"
    },
    {
        "id": 3,
        "section": "catalog",
        "title": "Pilote de la Meuse",
        "artist": "Paul Signac",
        "year": "1896",
        "era": "Post-Impressionism",
        "medium": "Oil on canvas · 65 × 81 cm",
        "provenance": "Private Collection, Antwerp; Geneva Fine Art Trust",
        "exhibited": "Salon des Indépendants, Paris, 1897",
        "price": 1120000,
        "price_label": "€ 1,120,000",
        "status": "Original · Available",
        "image": "/images/art3.jpg"
    },
    {
        "id": 4,
        "section": "catalog",
        "title": "Adoration",
        "artist": "Mary Cassatt",
        "year": "1898",
        "era": "Impressionism",
        "medium": "Oil on canvas · 81.3 × 65.4 cm",
        "provenance": "Durand-Ruel Galleries, New York; Private Collection, Boston",
        "exhibited": "Art Institute of Chicago, Loan Exhibition, 1926",
        "price": 920000,
        "price_label": "€ 920,000",
        "status": "Original · Available",
        "image": "/images/art4.jpg"
    },
    {
        "id": 5,
        "section": "catalog",
        "title": "I and the Village (Chamber Variation)",
        "artist": "Marc Chagall",
        "year": "1911",
        "era": "Modernism",
        "medium": "Oil on canvas · 92 × 73 cm",
        "provenance": "Private Collection, Zurich; Acquired through Galerie Maeght",
        "exhibited": "Kunsthaus Zürich, Modern Masters, 1955",
        "price": None,
        "price_label": "Price Upon Request",
        "status": "Original · Private Sale",
        "image": "/images/art5.jpg"
    },
    {
        "id": 6,
        "section": "drop",
        "title": "Portrait of Mrs. Boucard — Lot 01",
        "artist": "Tamara de Lempicka",
        "year": "1931",
        "era": "Art Deco",
        "medium": "Oil on canvas · 135 × 75 cm",
        "provenance": "Estate of Mme Boucard, Paris; Private Collection, Geneva",
        "exhibited": "Palais des Beaux-Arts, Brussels, 1932",
        "price": 2450000,
        "price_label": "€ 2,450,000",
        "status": "Upcoming Drop · Lot 01",
        "image": "/images/art6.jpg"
    },
    {
        "id": 7,
        "section": "drop",
        "title": "Still Life with Teapot and Fruit — Lot 02",
        "artist": "Paul Gauguin",
        "year": "1896",
        "era": "Post-Impressionism",
        "medium": "Oil on canvas · 66 × 75 cm",
        "provenance": "Ambroise Vollard, Paris; Private Swiss Collection",
        "exhibited": "Galerie Vollard, Paris, 1898",
        "price": None,
        "price_label": "Price Upon Request",
        "status": "Upcoming Drop · Lot 02",
        "image": "/images/art7.jpg"
    }
]

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "db"),
        database=os.getenv("DB_NAME", "artgallery"),
        user=os.getenv("DB_USER", "curator"),
        password=os.getenv("DB_PASS", "secretpass")
    )

@app.on_event("startup")
def init_db():
    for _ in range(10):
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS inquiries (
                    id SERIAL PRIMARY KEY,
                    inquiry_type VARCHAR(50),
                    client_name VARCHAR(150),
                    client_email VARCHAR(150),
                    artwork_titles TEXT,
                    total_eur INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()
            cur.close()
            conn.close()
            return
        except Exception:
            time.sleep(2)

class InquiryRequest(BaseModel):
    inquiry_type: str
    client_name: str
    client_email: str
    items: list[dict]

@app.get("/artworks")
def get_artworks():
    start = time.time()
    cached = cache.get("soleil_catalog_final")
    if cached:
        payload = json.loads(cached)
        cache_status = "HIT"
    else:
        payload = {"items": INITIAL_ARTWORKS, "artists": ARTISTS_BIO}
        cache.setex("soleil_catalog_final", 60, json.dumps(payload))
        cache_status = "MISS"
    return {
        "node": socket.gethostname(),
        "cache": cache_status,
        "latency_ms": round((time.time() - start) * 1000, 2),
        "items": payload["items"],
        "artists": payload["artists"]
    }

@app.post("/inquire")
def create_inquiry(req: InquiryRequest):
    if not req.items:
        raise HTTPException(status_code=400, detail="No artworks selected")
    titles = ", ".join([i["title"] for i in req.items])
    total = sum([i["price"] or 0 for i in req.items])
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO inquiries (inquiry_type, client_name, client_email, artwork_titles, total_eur) VALUES (%s, %s, %s, %s, %s)",
        (req.inquiry_type, req.client_name, req.client_email, titles, total)
    )
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "registered", "handled_by_node": socket.gethostname()}

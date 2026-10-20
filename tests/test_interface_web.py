"""Testes da Interface Web (Opção A) servida pelo FastAPI."""
from fastapi.testclient import TestClient
from api.main import app


def test_raiz_servi_painel_html():
    with TestClient(app) as client:
        r = client.get("/")
        assert r.status_code == 200
        assert "text/html" in r.headers["content-type"]
        # elementos essenciais do painel
        corpo = r.text
        assert "arrastar" in corpo.lower() or "upload" in corpo.lower()
        assert "X-API-Key" in corpo or "apiKey" in corpo or "api-key" in corpo.lower()


def test_docs_disponivel():
    with TestClient(app) as client:
        assert client.get("/docs").status_code == 200


def test_headers_de_seguranca_presentes():
    with TestClient(app) as client:
        r = client.get("/")
        assert r.headers["X-Content-Type-Options"] == "nosniff"
        assert r.headers["X-Frame-Options"] == "DENY"
        assert r.headers["Referrer-Policy"] == "no-referrer"

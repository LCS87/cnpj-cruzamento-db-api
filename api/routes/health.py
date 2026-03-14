"""
Rota de health check.
"""
import sys
import os
from fastapi import APIRouter

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
from config import DB_CONFIG, API_CONFIG
from cnpj_cruzamento.db import get_db_connection
from api.models import HealthResponse

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
def health_check():
    """Verifica conectividade com o banco e presença do token da API."""
    conn = get_db_connection(DB_CONFIG)
    db_status = "ok" if conn else "erro"
    if conn:
        conn.close()

    api_status = "configurado" if API_CONFIG.get('token') else "token ausente"

    return HealthResponse(
        status="ok" if db_status == "ok" else "degradado",
        db=db_status,
        api_token=api_status,
    )

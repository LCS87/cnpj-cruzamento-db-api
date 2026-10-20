"""
Rota de health check.

Fase 2: inclui métricas do cache de API (entradas válidas/total) para
observabilidade do consumo de créditos.
"""
import sys
import os
from fastapi import APIRouter

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
from config import DB_CONFIG, API_CONFIG
import security
from cnpj_cruzamento.db import get_db_connection
from cnpj_cruzamento import cache as api_cache
from api.models import HealthResponse

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
def health_check():
    """Verifica conectividade com o banco e presença do token da API."""
    conn = get_db_connection(DB_CONFIG)
    db_status = "online" if conn else "offline"
    if conn:
        conn.close()

    api_status = "configurado" if API_CONFIG.get('token') else "token ausente"

    return HealthResponse(
        status="ok" if db_status == "online" else "degradado",
        db=db_status,
        api_token=api_status,
        auth="ativada" if security.AUTH_REQUIRED else "desativada",
    )


@router.get("/cache/stats")
def cache_stats():
    """Estatísticas do cache de consultas à API externa (público, sem segredos)."""
    return api_cache.estatisticas()

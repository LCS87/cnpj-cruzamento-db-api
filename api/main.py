"""
API REST - Sistema de Cruzamento de CNPJs
"""
import logging
import sys
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from utils import setup_logging
from config import SYSTEM_CONFIG, DB_CONFIG, API_CONFIG
import security
from .jobs import carregar_jobs_pendentes

# Logging com filtro de redação de segredos (token/password em mensagens são mascarados)
setup_logging(log_level=SYSTEM_CONFIG.get('log_level', 'INFO'))
logging.getLogger().addFilter(security.RedacaoSegredosFilter())

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    os.makedirs(SYSTEM_CONFIG['input_folder'], exist_ok=True)
    os.makedirs(SYSTEM_CONFIG['output_folder'], exist_ok=True)

    # Checagem de configuração insegura
    problemas = security.checar_configuracao_segura(
        DB_CONFIG.get('password', ''), API_CONFIG.get('token', '')
    )
    for p in problemas:
        logger.warning(f"[SEGURANÇA] {p}")
    if problemas and security.ENVIRONMENT == "production":
        raise RuntimeError(
            "Configuração insegura detectada em produção: " + " | ".join(problemas)
        )

    # Recupera jobs persistidos (sobrevivência a restart)
    carregar_jobs_pendentes()

    auth_status = (
        f"ativada ({len(security.API_KEYS_RAW)} chave(s))"
        if security.AUTH_REQUIRED else "DESATIVADA (AUTH_REQUIRED=false)"
    )
    logger.info(f"API de Cruzamento de CNPJs iniciada. Autenticação: {auth_status}.")
    yield
    logger.info("API encerrada.")


app = FastAPI(
    title="CNPJ Cruzamento API",
    description=(
        "Cruzamento de CNPJs entre banco de dados local e API Invertexto.\n\n"
        "**Autenticação:** envie o header `X-API-Key` (clique em *Authorize* acima). "
        "`/v1/health` é público."
    ),
    version="1.1.0",
    lifespan=lifespan,
)

# ---------------------------------------------------------------------------
# Middleware de segurança: headers anti-XSS/clickjacking/sniffing
# ---------------------------------------------------------------------------
@app.middleware("http")
async def headers_de_seguranca(request, call_next):
    resposta = await call_next(request)
    resposta.headers["X-Content-Type-Options"] = "nosniff"
    resposta.headers["X-Frame-Options"] = "DENY"
    resposta.headers["Referrer-Policy"] = "no-referrer"
    return resposta


_origens = [o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_origens,
    allow_methods=["GET", "POST"],
    allow_headers=["X-API-Key", "Content-Type"],
)

from .routes import cnpj, cruzamento, health
from .web import ui as web_ui

app.include_router(health.router, prefix="/v1", tags=["health"])
app.include_router(cnpj.router, prefix="/v1", tags=["cnpj"])
app.include_router(cruzamento.router, prefix="/v1", tags=["cruzamento"])

# Interface web (Opção A): painel estático servido na raiz
app.include_router(web_ui.router)

"""
API REST - Sistema de Cruzamento de CNPJs
"""
import logging
import sys
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
from utils import setup_logging
from config import SYSTEM_CONFIG, DB_CONFIG, API_CONFIG
import security
import observabilidade as obs
from .jobs import carregar_jobs_pendentes

# Logging com redação de segredos + correlation-id em cada linha (Fase 3)
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

    # Sentry opcional (Fase 3): só ativa se SENTRY_DSN estiver definido e o
    # pacote instalado; falha de observabilidade nunca derruba a aplicação.
    dsn = os.getenv("SENTRY_DSN", "").strip()
    if dsn:
        try:
            import sentry_sdk
            from observabilidade import REQUEST_ID as _rid

            def _tracing_before_send(event, hint):
                event.setdefault("extra", {})["request_id"] = _rid.get()
                return event

            sentry_sdk.init(
                dsn=dsn,
                environment=security.ENVIRONMENT,
                traces_sample_rate=float(os.getenv("SENTRY_TRACES_SAMPLE_RATE", "0.1")),
                before_send=_tracing_before_send,
            )
            logger.info("Sentry inicializado.")
        except ImportError:
            logger.warning("SENTRY_DSN definido mas sentry-sdk não instalado; ignorando.")

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
# + correlation-id (X-Request-ID) + métricas Prometheus por rota (Fase 3)
# ---------------------------------------------------------------------------
@app.middleware("http")
async def headers_de_seguranca(request, call_next):
    request_id = obs.novo_request_id(request.headers.get("X-Request-ID"))
    token_ctx = obs.REQUEST_ID.set(request_id)
    inicio = time.perf_counter()
    status = 500
    try:
        resposta = await call_next(request)
        status = resposta.status_code
    finally:
        duracao = time.perf_counter() - inicio
        # Normaliza a path para evitar cardinalidade alta de labels: ids de
        # job/CNPJs arbitrários não podem virar séries novas no Prometheus.
        path_label = _normalizar_path(request.url.path)
        obs._metrica_http(request.method, path_label, status, duracao)
        obs.REQUEST_ID.reset(token_ctx)
    resposta.headers["X-Content-Type-Options"] = "nosniff"
    resposta.headers["X-Frame-Options"] = "DENY"
    resposta.headers["Referrer-Policy"] = "no-referrer"
    resposta.headers["X-Request-ID"] = request_id
    return resposta


# Prefixos com identificadores dinâmicos ({job_id}, CNPJs etc.) são
# colapsados em um segmento fixo antes de virar label de métrica.
_PREFIXOS_PARAMETRICOS = (
    "/v1/cruzamento", "/v1/cnpj", "/v1/cache",
)


def _normalizar_path(path: str) -> str:
    for prefixo in _PREFIXOS_PARAMETRICOS:
        if path == prefixo:
            return path
        if path.startswith(prefixo + "/"):
            resto = path[len(prefixo) + 1:]
            partes = resto.split("/")
            # mantém sufixos estáticos conhecidos (status/download/jobs)
            if partes and partes[0] in ("jobs", "upload", "stats"):
                return f"{prefixo}/{partes[0]}"
            return f"{prefixo}/{{id}}"
    return path


_origens = [o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_origens,
    allow_methods=["GET", "POST"],
    allow_headers=["X-API-Key", "Content-Type"],
)

from .routes import cnpj, cruzamento, health, metrics
from .web import ui as web_ui

app.include_router(health.router, prefix="/v1", tags=["health"])
app.include_router(cnpj.router, prefix="/v1", tags=["cnpj"])
app.include_router(cruzamento.router, prefix="/v1", tags=["cruzamento"])
app.include_router(metrics.router, tags=["observabilidade"])

# Interface web (Opção A): painel estático servido na raiz
app.include_router(web_ui.router)

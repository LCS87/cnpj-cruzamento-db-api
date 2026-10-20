"""
Observabilidade (Fase 3): correlation-id + métricas Prometheus.

- `CorrelationIdMiddleware`: gera/propaga um ID por requisição (header
  `X-Request-ID`), injeta no contexto de logging e devolve no response.
- Métricas: contador de requisições HTTP, histograma de latência, contadores
  de consultas à API Invertexto (hit/miss de cache, erros) e gauge de jobs
  ativos. Exportadas em `/metrics` (formato padrão Prometheus).

Se o pacote `prometheus-client` não estiver instalado, tudo degrada para
no-op silencioso — a aplicação nunca quebra por falta de observabilidade.
"""
import logging
import time
import uuid
from contextvars import ContextVar

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Correlation ID
# ---------------------------------------------------------------------------
REQUEST_ID: ContextVar[str] = ContextVar("request_id", default="-")


class RequestIdLogFilter(logging.Filter):
    """Injeta `record.request_id` em todo registro de log."""

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "request_id"):
            record.request_id = REQUEST_ID.get()
        return True


def novo_request_id(externo: str | None = None) -> str:
    rid = (externo or "").strip()[:64]
    return rid if rid else uuid.uuid4().hex[:16]


# ---------------------------------------------------------------------------
# Métricas Prometheus (opcionais)
# ---------------------------------------------------------------------------
try:
    from prometheus_client import (
        CONTENT_TYPE_LATEST,
        Counter,
        Gauge,
        Histogram,
        generate_latest,
    )

    TEMPO_REQUISICOES = Histogram(
        "http_request_duration_seconds",
        "Latência das requisições HTTP da API",
        ["method", "path", "status"],
        buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60),
    )
    TOTAL_REQUISICOES = Counter(
        "http_requests_total", "Total de requisições HTTP", ["method", "path", "status"]
    )
    CONSULTAS_API = Counter(
        "invertexto_consultas_total",
        "Consultas à API Invertexto por resultado",
        ["resultado"],  # ok | erro | cache_hit | token_faltando
    )
    CACHE_STATS = Gauge("api_cache_registros", "Registros no cache SQLite de consultas")
    JOBS_ATIVOS = Gauge("cruzamento_jobs_ativos", "Jobs de cruzamento em processamento")

    def _metrica_http(method: str, path_template: str, status: int, duracao: float):
        TOTAL_REQUISICOES.labels(method, path_template, str(status)).inc()
        TEMPO_REQUISICOES.labels(method, path_template, str(status)).observe(duracao)

    def metrica_api(resultado: str):
        CONSULTAS_API.labels(resultado).inc()

    def atualizar_gauges(jobs_ativos: int, cache_registros: int | None = None):
        JOBS_ATIVOS.set(jobs_ativos)
        if cache_registros is not None:
            CACHE_STATS.set(cache_registros)

    def render_metrics() -> tuple[bytes, str]:
        return generate_latest(), CONTENT_TYPE_LATEST

    PROMETHEUS_DISPONIVEL = True

except ImportError:  # prometheus-client ausente → no-op

    def _metrica_http(*_a, **_k):
        pass

    def metrica_api(*_a, **_k):
        pass

    def atualizar_gauges(*_a, **_k):
        pass

    def render_metrics() -> tuple[bytes, str]:
        return (
            b"# prometheus-client nao instalado; metricas desativadas\n",
            "text/plain; charset=utf-8",
        )

    PROMETHEUS_DISPONIVEL = False

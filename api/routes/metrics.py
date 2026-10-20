"""
Endpoint /metrics no formato Prometheus (Fase 3 — Observabilidade).

Protegido por autenticação quando `METRICS_PUBLICA=false` (padrão em produção
é expor apenas na rede interna; aqui o default é exigir a API key, pois
/v1/health continua sendo o probe público de liveness/readiness).
"""
import os
import sys

from fastapi import APIRouter, Depends
from fastapi.responses import Response

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
import observabilidade as obs  # noqa: E402
import security  # noqa: E402

router = APIRouter()

METRICS_PUBLICA = os.getenv("METRICS_PUBLICA", "false").lower() in ("1", "true", "sim")


@router.get(
    "/metrics",
    response_class=Response,
    summary="Métricas Prometheus",
    description=(
        "Exporta métricas no texto plano do Prometheus: latência HTTP por rota, "
        "consultas à API Invertexto (ok/erro/cache), registros de cache e jobs ativos. "
        "`METRICS_PUBLICA=true` remove a exigência de API key."
    ),
    dependencies=[] if METRICS_PUBLICA else [Depends(security.verificar_api_key)],
)
async def metrics(_: None = Depends(lambda: None)):
    corpo, content_type = obs.render_metrics()
    return Response(content=corpo, media_type=content_type)

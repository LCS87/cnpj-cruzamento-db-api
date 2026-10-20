"""
Testes da Fase 3 — Observabilidade:
- correlation-id (X-Request-ID): gerado, propagado e presente nos logs
- /metrics: protegido por API key por padrão, texto Prometheus válido
- normalização de labels: ids dinâmicos não viram séries novas (cardinality)
- LOG_FORMAT=json: linhas parseáveis como JSON com request_id
"""
import io
import json
import logging

import pytest
from fastapi.testclient import TestClient

from api.main import app

CHAVE = {"X-API-Key": "chave-teste-1"}


@pytest.fixture(scope="module")
def client():
    """Cliente HTTP compartilhado pelos testes deste módulo."""
    with TestClient(app) as c:
        yield c


# alias explícito: mesma instância, nome usado nos testes de /metrics
client_auth = client


@pytest.fixture()
def headers_validos():
    return dict(CHAVE)


# ---------------------------------------------------------------------------
# Correlation ID
# ---------------------------------------------------------------------------
def test_health_retorna_x_request_id(client: TestClient):
    r = client.get("/v1/health")
    rid = r.headers.get("X-Request-ID")
    assert rid and len(rid) >= 8


def test_request_id_do_cliente_e_propagado(client: TestClient):
    r = client.get("/v1/health", headers={"X-Request-ID": "id-cliente-abc"})
    assert r.headers["X-Request-ID"] == "id-cliente-abc"


def test_request_ids_distintos_por_requisicao(client: TestClient):
    a = client.get("/v1/health").headers["X-Request-ID"]
    b = client.get("/v1/health").headers["X-Request-ID"]
    assert a != b


# ---------------------------------------------------------------------------
# /metrics
# ---------------------------------------------------------------------------
def test_metrics_exige_api_key(client_auth: TestClient):
    """Sem chave → 401 (padrão METRICS_PUBLICA=false)."""
    r = client_auth.get("/metrics")
    assert r.status_code == 401


def test_metrics_com_chave_retorna_prometheus(client_auth: TestClient, headers_validos):
    client_auth.get("/v1/health")  # gera tráfego para as métricas HTTP
    r = client_auth.get("/metrics", headers=headers_validos)
    assert r.status_code == 200
    assert "text/plain" in r.headers["content-type"] or "openmetrics" in r.headers["content-type"]
    corpo = r.text
    assert "http_requests_total" in corpo
    assert "http_request_duration_seconds" in corpo
    assert "invertexto_consultas_total" in corpo


def test_labels_normalizados_sem_vazamento_de_cardinalidade(
    client_auth: TestClient, headers_validos
):
    """job_ids/CNPJs arbitrários devem colapsar em {id}, nunca virar label nova."""
    client_auth.get("/v1/cruzamento/job-arbitrario-999/status", headers=headers_validos)
    client_auth.get("/v1/cnpj/11222333000181", headers=headers_validos)
    corpo = client_auth.get("/metrics", headers=headers_validos).text
    assert "job-arbitrario-999" not in corpo
    assert "{id}" in corpo  # forma normalizada presente
    assert 'path="/v1/cruzamento/{id}/status"' in corpo or 'path="/v1/cruzamento/{id}"' in corpo


# ---------------------------------------------------------------------------
# Métricas do pipeline de consulta (contadores ok/erro/cache_hit)
# ---------------------------------------------------------------------------
def test_metrica_api_noop_seguro():
    """metrica_api nunca lança exceção mesmo com rótulos inesperados."""
    import observabilidade as obs
    obs.metrica_api("cache_hit")
    obs.metrica_api("ok")
    obs.atualizar_gauges(3, 10)


# ---------------------------------------------------------------------------
# Logging estruturado (LOG_FORMAT=json)
# ---------------------------------------------------------------------------
def test_log_format_json_gera_linhas_parseaveis(monkeypatch, tmp_path):
    monkeypatch.setenv("LOG_FORMAT", "json")
    from utils import setup_logging

    log_file = str(tmp_path / "teste.json.log")
    logger = logging.getLogger("teste_fase3_json")
    # isola handlers para não afetar outros testes
    old_handlers = list(logging.getLogger().handlers)
    setup_logging(log_level="INFO", log_file=log_file)
    try:
        logger.info("mensagem de teste fase3")
        for h in logging.getLogger().handlers:
            h.flush()
        with open(log_file, encoding="utf-8") as f:
            linha = [l for l in f if "mensagem de teste fase3" in l][-1]
        payload = json.loads(linha)
        assert payload["message"] == "mensagem de teste fase3"
        assert "request_id" in payload
        assert payload["level"] == "INFO"
    finally:
        root = logging.getLogger()
        for h in list(root.handlers):
            root.removeHandler(h)
        for h in old_handlers:
            root.addHandler(h)


def test_log_format_texto_contem_request_id(monkeypatch, tmp_path):
    monkeypatch.setenv("LOG_FORMAT", "text")
    from utils import setup_logging

    log_file = str(tmp_path / "teste.txt.log")
    logger = logging.getLogger("teste_fase3_texto")
    old_handlers = list(logging.getLogger().handlers)
    setup_logging(log_level="INFO", log_file=log_file)
    try:
        import observabilidade as obs
        tok = obs.REQUEST_ID.set("rid-teste-123")
        try:
            logger.info("linha com correlation id")
        finally:
            obs.REQUEST_ID.reset(tok)
        for h in logging.getLogger().handlers:
            h.flush()
        conteudo = open(log_file, encoding="utf-8").read()
        assert "rid-teste-123" in conteudo
    finally:
        root = logging.getLogger()
        for h in list(root.handlers):
            root.removeHandler(h)
        for h in old_handlers:
            root.addHandler(h)

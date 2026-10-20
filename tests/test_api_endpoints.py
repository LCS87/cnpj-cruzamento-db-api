"""
Testes de integração dos endpoints HTTP com TestClient.

Cobertura: autenticação real por middleware, path traversal no upload,
extensões/tamanho/limite rejeitados, download seguro e health público.
As funções de banco/API externa são mockadas — nenhum MySQL ou rede é usado.
"""
import io
import os

import pytest
from fastapi.testclient import TestClient

import api.routes.cruzamento as rotas_cruzamento
import api.routes.cnpj as rotas_cnpj
import api.routes.health as rotas_health
from api.main import app
from api.models import JobStatus

CHAVE = {"X-API-Key": "chave-teste-1"}


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        yield c


CSV_VALIDO = b"cnpj\n11222333000181\n34028316000103\n"


# ---------------------------------------------------------------------------
# Autenticação na API real
# ---------------------------------------------------------------------------
def test_upload_sem_chave_e_401(client):
    r = client.post("/v1/cruzamento/upload", files={"file": ("a.csv", io.BytesIO(CSV_VALIDO))})
    assert r.status_code == 401
    assert "X-API-Key" in r.json()["detail"]


def test_upload_chave_invalida_e_401(client):
    r = client.post(
        "/v1/cruzamento/upload",
        headers={"X-API-Key": "chave-errada"},
        files={"file": ("a.csv", io.BytesIO(CSV_VALIDO))},
    )
    assert r.status_code == 401


def test_jobs_sem_chave_e_401(client):
    assert client.get("/v1/cruzamento/jobs").status_code == 401


def test_health_e_publico(client, monkeypatch):
    monkeypatch.setattr(rotas_health, "get_db_connection", lambda cfg: None)
    r = client.get("/v1/health")
    assert r.status_code == 200
    dados = r.json()
    assert dados["db"] == "offline"
    assert dados["auth"] == "ativada"


# ---------------------------------------------------------------------------
# Upload — validações e anti path traversal
# ---------------------------------------------------------------------------
def test_upload_rejeita_extensao_proibida(client):
    r = client.post(
        "/v1/cruzamento/upload", headers=CHAVE,
        files={"file": ("malware.txt", io.BytesIO(b"x"), "text/plain")},
    )
    assert r.status_code == 422


def test_upload_rejeita_arquivo_vazio(client):
    r = client.post(
        "/v1/cruzamento/upload", headers=CHAVE,
        files={"file": ("vazio.csv", io.BytesIO(b""), "text/csv")},
    )
    assert r.status_code == 422


def test_upload_respeita_limite_de_tamanho(client):
    limite_mb = int(os.environ["MAX_UPLOAD_SIZE_MB"])
    grande = b"x" * (limite_mb * 1024 * 1024 + 1024)  # 1 KB acima do limite
    r = client.post(
        "/v1/cruzamento/upload", headers=CHAVE,
        files={"file": ("grande.csv", io.BytesIO(grande), "text/csv")},
    )
    assert r.status_code == 413


def test_upload_com_traversal_salva_apenas_na_pasta_input(client, monkeypatch):
    """Nome malicioso '../../etc/passwd.csv' jamais pode escapar do INPUT_FOLDER."""
    chamado = {}

    def _fake_executar(job_id, caminho):
        chamado["caminho"] = caminho

    monkeypatch.setattr(rotas_cruzamento, "_executar_job", _fake_executar)

    r = client.post(
        "/v1/cruzamento/upload", headers=CHAVE,
        files={"file": ("../../etc/passwd.csv", io.BytesIO(CSV_VALIDO), "text/csv")},
    )
    assert r.status_code == 202
    destino = chamado["caminho"]
    pasta_input = os.path.realpath(os.environ["INPUT_FOLDER"])
    assert os.path.dirname(destino) == pasta_input, f"arquivo salvo fora de input/: {destino}"
    assert os.path.basename(destino) == "passwd.csv"
    assert not os.path.exists("/etc/passwd.csv")


def test_upload_ok_retorna_job(client, monkeypatch):
    monkeypatch.setattr(rotas_cruzamento, "_executar_job", lambda jid, caminho: None)
    r = client.post(
        "/v1/cruzamento/upload", headers=CHAVE,
        files={"file": ("clientes.csv", io.BytesIO(CSV_VALIDO), "text/csv")},
    )
    assert r.status_code == 202
    corpo = r.json()
    assert corpo["status"] == "pending"
    assert corpo["arquivo"] == "clientes.csv"


# ---------------------------------------------------------------------------
# Status / listagem / download
# ---------------------------------------------------------------------------
def test_status_job_inexistente_e_404(client):
    r = client.get("/v1/cruzamento/não-existe/status", headers=CHAVE)
    assert r.status_code == 404


def test_download_antes_de_concluir_e_409(client, monkeypatch):
    monkeypatch.setattr(rotas_cruzamento, "_executar_job", lambda jid, caminho: None)
    up = client.post(
        "/v1/cruzamento/upload", headers=CHAVE,
        files={"file": ("pendente.csv", io.BytesIO(CSV_VALIDO), "text/csv")},
    ).json()
    r = client.get(f"/v1/cruzamento/{up['job_id']}/download", headers=CHAVE)
    assert r.status_code == 409


def test_download_aplica_validacao_de_caminho(client, monkeypatch):
    """Mesmo com job 'done', arquivo fora de OUTPUT_FOLDER nunca é servido."""
    import api.jobs as jobs_mod
    from api.models import JobResponse

    job = JobResponse(job_id="dl-test", status=JobStatus.done, arquivo="x.csv")
    jobs_mod._cache["dl-test"] = job

    # SYSTEM_CONFIG é um dict comum — substituímos a cópia do módulo da rota
    config_falso = dict(rotas_cruzamento.SYSTEM_CONFIG)
    config_falso["output_folder"] = os.environ["OUTPUT_FOLDER"]
    monkeypatch.setattr(rotas_cruzamento, "SYSTEM_CONFIG", config_falso)

    r = client.get("/v1/cruzamento/dl-test/download", headers=CHAVE)
    assert r.status_code == 404  # não encontrado, mas jamais leitura arbitrária


def test_listagem_de_jobs(client, monkeypatch):
    monkeypatch.setattr(rotas_cruzamento, "_executar_job", lambda jid, caminho: None)
    client.post(
        "/v1/cruzamento/upload", headers=CHAVE,
        files={"file": ("listado.csv", io.BytesIO(CSV_VALIDO), "text/csv")},
    )
    r = client.get("/v1/cruzamento/jobs", headers=CHAVE)
    assert r.status_code == 200
    assert any(j["arquivo"] == "listado.csv" for j in r.json())


# ---------------------------------------------------------------------------
# Rota unitária de CNPJ — validação oficial em ação
# ---------------------------------------------------------------------------
def test_cnpj_malformado_e_422(client):
    r = client.get("/v1/cnpj/123", headers=CHAVE)
    assert r.status_code == 422


def test_cnpj_dv_invalido_e_422(client):
    # formato correto (14 dígitos), mas dígitos verificadores não conferem
    r = client.get("/v1/cnpj/11222333000182", headers=CHAVE)
    assert r.status_code == 422
    assert "verificadores" in r.json()["detail"]


def test_cnpj_valido_sem_db_e_503(client, monkeypatch):
    monkeypatch.setattr(rotas_cnpj, "get_db_connection", lambda cfg: None)
    r = client.get("/v1/cnpj/11222333000181", headers=CHAVE)
    assert r.status_code == 503

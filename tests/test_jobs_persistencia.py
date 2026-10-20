"""Testes da persistência de jobs em SQLite (api/jobs.py)."""
import api.jobs as jobs
from api.models import JobStatus


def _reset_cache():
    jobs._cache.clear()
    jobs._inicializado = False


def test_criar_e_obter_job(tmp_path, monkeypatch):
    monkeypatch.setattr(jobs, "_DB_PATH", str(tmp_path / "jobs.db"))
    _reset_cache()

    job_id = jobs.criar_job("clientes.csv")
    job = jobs.obter_job(job_id)
    assert job is not None
    assert job.status == JobStatus.pending
    assert job.arquivo == "clientes.csv"


def test_atualizar_job_grava_campos(tmp_path, monkeypatch):
    monkeypatch.setattr(jobs, "_DB_PATH", str(tmp_path / "jobs.db"))
    _reset_cache()

    job_id = jobs.criar_job("a.csv")
    jobs.atualizar_job(job_id, status=JobStatus.done, coincide=10, divergente=2, erros=1)
    job = jobs.obter_job(job_id)
    assert job.status == JobStatus.done
    assert (job.coincide, job.divergente, job.erros) == (10, 2, 1)


def test_jobs_sobrevivem_a_restart(tmp_path, monkeypatch):
    """Simula restart: limpa o cache e recarrega exclusivamente do SQLite."""
    db = str(tmp_path / "jobs.db")
    monkeypatch.setattr(jobs, "_DB_PATH", db)
    _reset_cache()

    id1 = jobs.criar_job("um.csv")
    id2 = jobs.criar_job("dois.csv")
    jobs.atualizar_job(id1, status=JobStatus.done, coincide=5)
    jobs.atualizar_job(id2, status=JobStatus.processing, processados=3, total=10)

    # "restart": processo novo -> cache vazio
    _reset_cache()
    recuperados = jobs.carregar_jobs_pendentes()

    assert jobs.obter_job(id1).status == JobStatus.done
    assert jobs.obter_job(id1).coincide == 5
    # job que estava em processing não pode ficar preso para sempre
    assert recuperados == 1
    assert jobs.obter_job(id2).status == JobStatus.error
    assert "reinicializacao" in jobs.obter_job(id2).mensagem.lower()


def test_listagem_retorna_todos_e_respeita_limite(tmp_path, monkeypatch):
    monkeypatch.setattr(jobs, "_DB_PATH", str(tmp_path / "jobs.db"))
    _reset_cache()

    ids = {jobs.criar_job(f"v{t}.csv") for t in range(4)}
    lista = jobs.listar_jobs(limite=10)
    assert {j.job_id for j in lista} == ids

    # limite é aplicado
    assert len(jobs.listar_jobs(limite=2)) == 2


def test_obter_job_inexistente_retorna_none(tmp_path, monkeypatch):
    monkeypatch.setattr(jobs, "_DB_PATH", str(tmp_path / "jobs.db"))
    _reset_cache()
    assert jobs.obter_job("nao-existe") is None


def test_gravacao_de_progresso_throttle_nao_perde_estado_final(tmp_path, monkeypatch):
    """Mesmo com throttle de escrita, o estado final (done) é gravado no disco."""
    db = str(tmp_path / "jobs.db")
    monkeypatch.setattr(jobs, "_DB_PATH", db)
    _reset_cache()

    job_id = jobs.criar_job("throttle.csv")
    for i in range(50):  # rajada de callbacks — só ~1x/s é persistido
        jobs.atualizar_job(job_id, processados=i, total=50, progresso_pct=i * 2.0)
    jobs.atualizar_job(job_id, status=JobStatus.done, progresso_pct=100.0)

    _reset_cache()
    jobs.carregar_jobs_pendentes()
    recuperado = jobs.obter_job(job_id)
    assert recuperado.status == JobStatus.done
    assert recuperado.progresso_pct == 100.0


def test_cnpjs_sinteticos_passam_na_validacao_oficial(cnpjs_validos):
    """200 CNPJs gerados por script independente validam no algoritmo da API."""
    import security
    assert len(cnpjs_validos) == 200
    assert all(security.validar_cnpj_oficial(c) for c in cnpjs_validos)

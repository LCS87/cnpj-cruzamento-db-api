"""
Gerenciamento de jobs com persistência em SQLite (WAL).

Os jobs sobrevivem a reinicializações da API. O estado em memória é apenas
um cache de leitura; toda mutação é gravada no arquivo `jobs.db` ao lado do
bank de dados configurado (pasta raiz, ou JOB_DB_PATH via env).
"""
import json
import logging
import os
import sqlite3
import threading
import uuid
from typing import Optional

from api.models import JobResponse, JobStatus

logger = logging.getLogger(__name__)

_DB_PATH = os.getenv("JOB_DB_PATH", os.path.join(os.path.dirname(os.path.dirname(__file__)), "jobs.db"))

_lock = threading.Lock()
_cache: dict[str, JobResponse] = {}
_inicializado = False

_CAMPOS = [
    "job_id", "status", "arquivo", "total", "processados", "progresso_pct",
    "coincide", "divergente", "erros", "tempo_segundos", "download_url", "mensagem",
    "criado_em", "atualizado_em",
]


def _conectar() -> sqlite3.Connection:
    conn = sqlite3.connect(_DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def _garantir_schema(conn: sqlite3.Connection):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            arquivo TEXT,
            status TEXT NOT NULL,
            dados TEXT NOT NULL,
            criado_em TEXT NOT NULL,
            atualizado_em TEXT NOT NULL
        )
        """
    )
    conn.commit()


def _row_para_job(dados_json: str, criado_em: str, atualizado_em: str) -> JobResponse:
    dados = json.loads(dados_json)
    dados["criado_em"] = criado_em
    dados["atualizado_em"] = atualizado_em
    return JobResponse(**dados)


_ultima_gravacao_progresso: dict[str, float] = {}


def _deve_persistir_agora(job: JobResponse) -> bool:
    """
    Evita escrever no disco a cada callback de progresso: jobs em andamento
    são gravados no máximo 1x por segundo; mudanças de status gravam sempre.
    """
    import time as _t
    if job.status in (JobStatus.pending, JobStatus.processing):
        agora_mono = _t.monotonic()
        ultimo = _ultima_gravacao_progresso.get(job.job_id, 0.0)
        if agora_mono - ultimo < 1.0:
            return False
        _ultima_gravacao_progresso[job.job_id] = agora_mono
    else:
        _ultima_gravacao_progresso.pop(job.job_id, None)
    return True


def _persistir(job: JobResponse):
    from datetime import datetime, timezone
    agora = datetime.now(timezone.utc).isoformat(timespec="seconds")
    payload = job.model_dump(exclude={"criado_em", "atualizado_em"})
    with _lock, _conectar() as conn:
        _garantir_schema(conn)
        conn.execute(
            """
            INSERT INTO jobs (job_id, arquivo, status, dados, criado_em, atualizado_em)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(job_id) DO UPDATE SET
                arquivo = excluded.arquivo,
                status = excluded.status,
                dados = excluded.dados,
                atualizado_em = excluded.atualizado_em
            """,
            (job.job_id, job.arquivo, job.status.value, json.dumps(payload),
             job.criado_em or agora, agora),
        )
        conn.commit()


def carregar_jobs_pendentes():
    """
    Carrega o histórico do SQLite para a memória e recupera jobs que ficaram
    presos em pending/processing por causa de um restart (marcados como error).
    Retorna quantos jobs foram recuperados.
    """
    global _inicializado
    with _lock:
        _cache.clear()
        try:
            with _conectar() as conn:
                _garantir_schema(conn)
                rows = conn.execute(
                    "SELECT dados, criado_em, atualizado_em FROM jobs ORDER BY criado_em DESC LIMIT 500"
                ).fetchall()
        except sqlite3.Error as e:
            logger.error(f"Falha ao carregar jobs persistidos: {e}")
            _inicializado = True
            return 0

        recuperados = 0
        for dados_json, criado_em, atualizado_em in rows:
            job = _row_para_job(dados_json, criado_em, atualizado_em)
            if job.status in (JobStatus.pending, JobStatus.processing):
                # O processo que executava o job não sobreviveu ao restart
                job = job.model_copy(update={
                    "status": JobStatus.error,
                    "mensagem": "Job interrompido por reinicializacao da API. Reenvie o arquivo.",
                })
                payload = job.model_dump(exclude={"criado_em", "atualizado_em"})
                conn2 = _conectar()
                conn2.execute(
                    "UPDATE jobs SET status=?, dados=?, atualizado_em=? WHERE job_id=?",
                    (job.status.value, json.dumps(payload), atualizado_em, job.job_id),
                )
                conn2.commit()
                conn2.close()
                recuperados += 1
            _cache[job.job_id] = job
        _inicializado = True
        if rows:
            logger.info(f"{len(rows)} jobs carregados de {os.path.basename(_DB_PATH)}; {recuperados} recuperados como error.")
        return recuperados


def criar_job(arquivo: str) -> str:
    if not _inicializado:
        carregar_jobs_pendentes()
    job_id = str(uuid.uuid4())
    job = JobResponse(job_id=job_id, status=JobStatus.pending, arquivo=arquivo)
    _persistir(job)
    _cache[job_id] = job
    return job_id


def atualizar_job(job_id: str, **kwargs):
    if job_id not in _cache:
        return
    dados = _cache[job_id].model_dump()
    dados.update(kwargs)
    job = JobResponse(**dados)
    _cache[job_id] = job
    if not _deve_persistir_agora(job):
        return
    try:
        _persistir(job)
    except Exception as e:  # persistência não deve derrubar o processamento
        logger.warning(f"Falha ao persistir job {job_id}: {e}")


def obter_job(job_id: str) -> Optional[JobResponse]:
    if not _inicializado:
        carregar_jobs_pendentes()
    return _cache.get(job_id)


def listar_jobs(limite: int = 50) -> list[JobResponse]:
    if not _inicializado:
        carregar_jobs_pendentes()
    return sorted(_cache.values(), key=lambda j: j.atualizado_em or "", reverse=True)[:limite]

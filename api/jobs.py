"""
Gerenciamento simples de jobs em memória para processamento assíncrono.
"""
import uuid
from typing import Dict
from api.models import JobStatus, JobResponse


_jobs: Dict[str, JobResponse] = {}


def criar_job(arquivo: str) -> str:
    job_id = str(uuid.uuid4())
    _jobs[job_id] = JobResponse(job_id=job_id, status=JobStatus.pending, arquivo=arquivo)
    return job_id


def atualizar_job(job_id: str, **kwargs):
    if job_id in _jobs:
        dados = _jobs[job_id].model_dump()
        dados.update(kwargs)
        _jobs[job_id] = JobResponse(**dados)


def obter_job(job_id: str) -> JobResponse | None:
    return _jobs.get(job_id)

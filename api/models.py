"""
Modelos Pydantic para request/response da API.
"""
from pydantic import BaseModel
from typing import Optional
from enum import Enum


class JobStatus(str, Enum):
    pending = "pending"
    processing = "processing"
    done = "done"
    error = "error"


class CnpjResultado(BaseModel):
    cnpj: str
    status_db: Optional[str] = None
    motivo_db: Optional[str] = None
    status_api: Optional[str] = None
    razao_social_api: Optional[str] = None
    nome_fantasia_api: Optional[str] = None
    status_final: Optional[str] = None
    erro_api: Optional[str] = None


class JobResponse(BaseModel):
    job_id: str
    status: JobStatus
    arquivo: Optional[str] = None
    total: Optional[int] = None
    coincide: Optional[int] = None
    divergente: Optional[int] = None
    erros: Optional[int] = None
    tempo_segundos: Optional[float] = None
    download_url: Optional[str] = None
    mensagem: Optional[str] = None


class HealthResponse(BaseModel):
    status: str
    db: str
    api_token: str

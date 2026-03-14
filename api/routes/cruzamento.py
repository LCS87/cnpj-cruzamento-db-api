"""
Rotas de cruzamento em lote: upload, status e download.
"""
import os
import sys
import threading
import logging

from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
from config import DB_CONFIG, API_CONFIG, SYSTEM_CONFIG, PROCESSING_CONFIG
from config import SITUACAO_MAP, MOTIVO_MAP, COLUMN_CONFIG, YAML_CONFIG
from cnpj_cruzamento.db import get_db_connection
from cnpj_cruzamento.processor import processar_arquivo
from api.jobs import criar_job, atualizar_job, obter_job
from api.models import JobResponse, JobStatus

router = APIRouter()
logger = logging.getLogger(__name__)

EXTENSOES_PERMITIDAS = {'.csv', '.xlsx', '.xls'}


def _config_completa() -> dict:
    return {
        'db_config': DB_CONFIG,
        'api_config': API_CONFIG,
        'system_config': SYSTEM_CONFIG,
        'processing': PROCESSING_CONFIG,
        'situacao_map': SITUACAO_MAP,
        'motivo_map': MOTIVO_MAP,
        'column_config': COLUMN_CONFIG,
        'yaml_config': YAML_CONFIG,
    }


def _executar_job(job_id: str, caminho_arquivo: str):
    """Executa o processamento em background e atualiza o job."""
    atualizar_job(job_id, status=JobStatus.processing)
    config = _config_completa()

    conn = get_db_connection(config['db_config'])
    if not conn:
        atualizar_job(job_id, status=JobStatus.error, mensagem="Banco de dados indisponível.")
        return

    try:
        stats = processar_arquivo(caminho_arquivo, conn, config)
    finally:
        conn.close()

    if 'erro' in stats:
        atualizar_job(job_id, status=JobStatus.error, mensagem=stats['erro'])
        return

    atualizar_job(
        job_id,
        status=JobStatus.done,
        total=stats.get('total'),
        coincide=stats.get('coincide_status'),
        divergente=stats.get('divergente_status'),
        erros=stats.get('erro_consulta'),
        tempo_segundos=stats.get('tempo_segundos'),
        download_url=f"/v1/cruzamento/{job_id}/download",
    )


@router.post("/cruzamento/upload", response_model=JobResponse, status_code=202)
async def upload_arquivo(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    """
    Recebe um arquivo CSV ou XLSX, inicia o cruzamento em background e retorna um job_id.
    """
    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in EXTENSOES_PERMITIDAS:
        raise HTTPException(status_code=422, detail=f"Extensão '{ext}' não suportada. Use CSV ou XLSX.")

    pasta_entrada = SYSTEM_CONFIG['input_folder']
    os.makedirs(pasta_entrada, exist_ok=True)
    caminho = os.path.join(pasta_entrada, file.filename)

    conteudo = await file.read()
    with open(caminho, 'wb') as f:
        f.write(conteudo)

    job_id = criar_job(file.filename)
    background_tasks.add_task(_executar_job, job_id, caminho)

    logger.info(f"Job {job_id} criado para arquivo: {file.filename}")
    return obter_job(job_id)


@router.get("/cruzamento/{job_id}/status", response_model=JobResponse)
def status_job(job_id: str):
    """Retorna o status atual de um job de cruzamento."""
    job = obter_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job não encontrado.")
    return job


@router.get("/cruzamento/{job_id}/download")
def download_resultado(job_id: str):
    """Faz o download do arquivo de resultado quando o job estiver concluído."""
    job = obter_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job não encontrado.")
    if job.status != JobStatus.done:
        raise HTTPException(status_code=409, detail=f"Job ainda não concluído. Status: {job.status}")

    pasta_saida = SYSTEM_CONFIG['output_folder']
    nome_base = os.path.splitext(job.arquivo)[0]
    caminho_saida = os.path.join(pasta_saida, f"{nome_base}_CRUZAMENTO.csv")

    if not os.path.exists(caminho_saida):
        raise HTTPException(status_code=404, detail="Arquivo de resultado não encontrado.")

    return FileResponse(
        path=caminho_saida,
        media_type='text/csv',
        filename=os.path.basename(caminho_saida),
    )

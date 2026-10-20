"""
Rotas de cruzamento em lote: upload, status, listagem e download.

Proteções aplicadas (Fase 1 — Segurança):
- Autenticação via header X-API-Key + rate limiting (security.verificar_api_key)
- Sanitização do nome do arquivo enviado (anti path traversal)
- Limite de tamanho de upload (MAX_UPLOAD_SIZE_MB) com leitura em chunks
- Validação do caminho de download dentro de OUTPUT_FOLDER
"""
import os
import sys
import logging

from fastapi import APIRouter, UploadFile, File, HTTPException, BackgroundTasks, Depends
from fastapi.responses import FileResponse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
from config import DB_CONFIG, API_CONFIG, SYSTEM_CONFIG, PROCESSING_CONFIG
from config import SITUACAO_MAP, MOTIVO_MAP, COLUMN_CONFIG, YAML_CONFIG
import security
from cnpj_cruzamento.db import get_db_connection
from cnpj_cruzamento.processor import processar_arquivo
from api.jobs import criar_job, atualizar_job, obter_job, listar_jobs
from api.models import JobResponse, JobStatus

router = APIRouter(dependencies=[Depends(security.verificar_api_key)])
logger = logging.getLogger(__name__)


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

    def _on_progresso(p: dict):
        atualizar_job(
            job_id,
            total=p.get('total'),
            processados=p.get('processados', 0),
            progresso_pct=p.get('progresso_pct', 0.0),
        )

    conn = get_db_connection(config['db_config'])
    if not conn:
        atualizar_job(job_id, status=JobStatus.error, mensagem="Banco de dados indisponível.")
        return

    try:
        stats = processar_arquivo(caminho_arquivo, conn, config, callback_progresso=_on_progresso)
    except Exception as e:
        logger.exception(f"Erro inesperado no job {job_id}")
        atualizar_job(job_id, status=JobStatus.error, mensagem=f"Erro inesperado: {e}")
        return
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

    O nome do arquivo é sanitizado (sem componentes de caminho) e o tamanho é
    limitado por `MAX_UPLOAD_SIZE_MB` (padrão 25 MB).
    """
    nome_seguro = security.sanitize_filename(file.filename or "arquivo.csv")

    pasta_entrada = SYSTEM_CONFIG['input_folder']
    os.makedirs(pasta_entrada, exist_ok=True)
    caminho = security.validar_caminho_dentro_da_pasta(
        os.path.join(pasta_entrada, nome_seguro), pasta_entrada
    )

    # Leitura em chunks com limite rígido de tamanho
    limite_bytes = security.MAX_UPLOAD_SIZE_MB * 1024 * 1024
    recebido = 0
    with open(caminho, 'wb') as f:
        while chunk := await file.read(1024 * 1024):
            recebido += len(chunk)
            if recebido > limite_bytes:
                f.close()
                try:
                    os.remove(caminho)
                except OSError:
                    pass
                raise HTTPException(
                    status_code=413,
                    detail=f"Arquivo excede o limite de {security.MAX_UPLOAD_SIZE_MB} MB.",
                )
            f.write(chunk)

    if recebido == 0:
        os.remove(caminho)
        raise HTTPException(status_code=422, detail="Arquivo vazio.")

    job_id = criar_job(nome_seguro)
    background_tasks.add_task(_executar_job, job_id, caminho)

    logger.info(f"Job {job_id} criado para arquivo: {nome_seguro} ({_formatar_tamanho(recebido)})")
    return obter_job(job_id)


def _formatar_tamanho(n: int) -> str:
    for unidade in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unidade}"
        n /= 1024
    return f"{n:.1f} TB"


@router.get("/cruzamento/jobs", response_model=list[JobResponse])
def listar_historico_jobs(limite: int = 50):
    """Lista os últimos jobs (persistidos — sobrevivem a reinicializações)."""
    return listar_jobs(limite=min(max(limite, 1), 200))


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
    caminho_saida = security.validar_caminho_dentro_da_pasta(
        os.path.join(pasta_saida, f"{nome_base}_CRUZAMENTO.csv"), pasta_saida
    )

    if not os.path.exists(caminho_saida):
        raise HTTPException(status_code=404, detail="Arquivo de resultado não encontrado.")

    return FileResponse(
        path=caminho_saida,
        media_type='text/csv',
        filename=os.path.basename(caminho_saida),
    )

"""
Orquestração do processamento de todos os arquivos da pasta de entrada.
"""
import logging
import os
import sys
import time
from glob import glob

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from config import (
    DB_CONFIG, API_CONFIG, SYSTEM_CONFIG, PROCESSING_CONFIG,
    SITUACAO_MAP, MOTIVO_MAP, COLUMN_CONFIG, YAML_CONFIG
)
from .db import get_db_connection
from .processor import processar_arquivo

logger = logging.getLogger(__name__)


def _montar_config() -> dict:
    """Consolida todas as configurações em um único dict para o processor."""
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


def processar_todos_arquivos_cruzamento():
    """Ponto de entrada principal: processa todos os arquivos em input/."""
    config = _montar_config()
    pasta_entrada = config['system_config']['input_folder']
    pasta_saida = config['system_config']['output_folder']

    os.makedirs(pasta_entrada, exist_ok=True)
    os.makedirs(pasta_saida, exist_ok=True)

    logger.info(f"Pasta de entrada: {pasta_entrada}")
    logger.info(f"Pasta de saída: {pasta_saida}")

    conn = get_db_connection(config['db_config'])
    if not conn:
        logger.error("Não foi possível conectar ao banco de dados. Abortando.")
        return

    padroes = [
        os.path.join(pasta_entrada, '*.csv'),
        os.path.join(pasta_entrada, '*.xlsx'),
        os.path.join(pasta_entrada, '*.xls'),
    ]
    arquivos = []
    for p in padroes:
        arquivos.extend(glob(p))

    if not arquivos:
        logger.warning(f"Nenhum arquivo encontrado em '{pasta_entrada}'.")
        conn.close()
        return

    logger.info(f"Encontrados {len(arquivos)} arquivo(s) para processar.")
    inicio_global = time.time()
    resultados = []

    for i, arquivo in enumerate(arquivos, 1):
        logger.info(f"--- Arquivo {i}/{len(arquivos)}: {os.path.basename(arquivo)} ---")
        stats = processar_arquivo(arquivo, conn, config)
        resultados.append(stats)

    conn.close()

    tempo_total = time.time() - inicio_global
    logger.info(f"Processamento concluído em {tempo_total:.1f}s | {len(arquivos)} arquivo(s).")

    # Resumo final
    for r in resultados:
        if 'erro' in r:
            logger.warning(f"  {r['arquivo']}: ERRO — {r['erro']}")
        else:
            logger.info(
                f"  {r['arquivo']}: {r.get('total', 0)} registros | "
                f"coincide={r.get('coincide_status', 0)} | "
                f"divergente={r.get('divergente_status', 0)} | "
                f"erro={r.get('erro_consulta', 0)} | "
                f"{r.get('tempo_segundos', 0)}s"
            )

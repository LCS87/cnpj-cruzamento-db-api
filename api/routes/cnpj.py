"""
Rota de consulta unitária de CNPJ.
"""
import re
import sys
import os
from fastapi import APIRouter, HTTPException

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))
from config import DB_CONFIG, API_CONFIG, COLUMN_CONFIG, SITUACAO_MAP, MOTIVO_MAP, YAML_CONFIG
from cnpj_cruzamento.db import get_db_connection, consultar_situacao_db
from cnpj_cruzamento.api import consultar_cnpj_api
from api.models import CnpjResultado

router = APIRouter()

TABELA = (YAML_CONFIG or {}).get('database', {}).get('table', 'estabelecimentos')
DB_COLS = COLUMN_CONFIG['db']
OUT_COLS = COLUMN_CONFIG['output']


def _limpar_cnpj(cnpj: str) -> str | None:
    limpo = re.sub(r'\D', '', cnpj)
    return limpo.zfill(14) if len(limpo) == 14 else None


@router.get("/cnpj/{cnpj}", response_model=CnpjResultado)
def consultar_cnpj(cnpj: str):
    """Consulta um único CNPJ no DB local e na API Invertexto e retorna o cruzamento."""
    cnpj_limpo = _limpar_cnpj(cnpj)
    if not cnpj_limpo:
        raise HTTPException(status_code=422, detail="CNPJ inválido. Informe 14 dígitos numéricos.")

    conn = get_db_connection(DB_CONFIG)
    if not conn:
        raise HTTPException(status_code=503, detail="Banco de dados indisponível.")

    try:
        status_db, motivo_db = consultar_situacao_db(
            conn, cnpj_limpo, TABELA,
            DB_COLS['situacao'], DB_COLS['motivo'],
            SITUACAO_MAP, MOTIVO_MAP,
        )
    finally:
        conn.close()

    resultado_api = consultar_cnpj_api(cnpj_limpo, API_CONFIG, COLUMN_CONFIG)

    status_api = resultado_api.get(OUT_COLS['api_status'], '')
    erro_api = resultado_api.get(OUT_COLS['api_error'], '')

    # Comparação de status
    if status_db.upper() == status_api.upper():
        status_final = "COINCIDE"
    elif any(k in status_db.upper() for k in ("ERRO", "NÃO ENCONTRADO")) or "ERRO" in status_api.upper():
        status_final = "ERRO NA CONSULTA"
    else:
        status_final = "DIVERGENTE"

    return CnpjResultado(
        cnpj=cnpj_limpo,
        status_db=status_db,
        motivo_db=motivo_db,
        status_api=status_api,
        razao_social_api=resultado_api.get(OUT_COLS['api_razao'], ''),
        nome_fantasia_api=resultado_api.get(OUT_COLS['api_fantasia'], ''),
        status_final=status_final,
        erro_api=erro_api if erro_api != "OK" else None,
    )

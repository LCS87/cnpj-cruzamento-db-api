"""
Lógica de cruzamento e comparação de dados de CNPJs.
"""
import re
import csv
import logging
import time
import os

import pandas as pd

from .db import consultar_situacao_db
from .api import consultar_cnpj_api

logger = logging.getLogger(__name__)


def _ler_arquivo(caminho: str) -> pd.DataFrame | None:
    """Lê CSV ou XLSX e retorna DataFrame. Retorna None em caso de falha."""
    ext = os.path.splitext(caminho)[1].lower()
    try:
        if ext == '.csv':
            for params in [{'sep': ';', 'encoding': 'utf-8'}, {'sep': ',', 'encoding': 'utf-8'},
                           {'sep': ';', 'encoding': 'latin-1'}]:
                try:
                    df = pd.read_csv(caminho, dtype=str, **params)
                    if len(df.columns) > 1:
                        return df
                except Exception:
                    continue
            raise ValueError("Não foi possível ler o CSV com os separadores testados.")
        elif ext in ('.xlsx', '.xls'):
            return pd.read_excel(caminho, dtype=str, engine='openpyxl')
        else:
            logger.warning(f"Extensão '{ext}' não suportada: {caminho}")
            return None
    except Exception as e:
        logger.error(f"Erro ao ler arquivo {caminho}: {e}")
        return None


def _identificar_coluna_cnpj(df: pd.DataFrame, aliases: list) -> str | None:
    """Detecta a coluna de CNPJ no DataFrame."""
    for col in df.columns:
        col_norm = col.lower().replace('_', '').strip()
        for alias in aliases:
            if alias.lower().replace('_', '') == col_norm:
                return col
    for col in df.columns:
        if 'cnpj' in col.lower():
            return col
    return None


def _formatar_cnpj(valor) -> str | None:
    """Limpa e formata o CNPJ para 14 dígitos."""
    if pd.isna(valor):
        return None
    limpo = re.sub(r'\D', '', str(valor))
    return limpo.zfill(14) if len(limpo) == 14 else None


def processar_arquivo(caminho_arquivo: str, conn, config: dict) -> dict:
    """
    Processa um arquivo CSV/XLSX: consulta DB + API, cruza resultados e salva saída.
    Retorna dict com estatísticas do processamento.
    """
    nome = os.path.basename(caminho_arquivo)
    logger.info(f"Iniciando processamento: {nome}")

    df = _ler_arquivo(caminho_arquivo)
    if df is None:
        return {'arquivo': nome, 'erro': 'Falha na leitura'}

    aliases = config['processing'].get('cnpj_aliases',
                                       ['cnpj', 'cnpj_completo', 'cnpj_base', 'cnpj_num', 'documento'])
    col_cnpj = _identificar_coluna_cnpj(df, aliases)
    if not col_cnpj:
        logger.error(f"Coluna CNPJ não encontrada em: {nome}")
        return {'arquivo': nome, 'erro': 'Coluna CNPJ não encontrada'}

    logger.info(f"Coluna CNPJ: '{col_cnpj}' | Registros: {len(df):,}")

    # Configurações de colunas de saída
    cols = config['column_config']['output']
    col_status_db = cols['db_status']
    col_motivo_db = cols['db_motivo']
    col_status_api = cols['api_status']
    col_razao_api = cols['api_razao']
    col_fantasia_api = cols['api_fantasia']
    col_erro_api = cols['api_error']
    col_status_final = cols['comparison_status']
    col_razao_compara = cols['comparison_razao']

    colunas_resultado = [
        col_status_db, col_motivo_db,
        col_status_api, col_razao_api, col_fantasia_api,
        col_status_final, col_razao_compara, col_erro_api
    ]
    for c in colunas_resultado:
        if c not in df.columns:
            df[c] = ''

    df['_cnpj_limpo'] = df[col_cnpj].apply(_formatar_cnpj)

    total = len(df)
    stats = {
        'arquivo': nome,
        'total': total,
        'coincide_status': 0,
        'divergente_status': 0,
        'erro_consulta': 0,
        'nao_encontrado_db': 0,
        'sucesso_api': 0,
        'erro_api': 0,
    }

    db_cfg = config['column_config']['db']
    tabela = config.get('yaml_config', {}).get('database', {}).get('table', 'estabelecimentos')
    situacao_map = config['situacao_map']
    motivo_map = config['motivo_map']
    api_config = config['api_config']

    inicio = time.time()
    ultimo_log = inicio

    for index, row in df.iterrows():
        cnpj = row['_cnpj_limpo']

        # Log de progresso a cada 2 segundos
        agora = time.time()
        if agora - ultimo_log >= 2 or (index + 1) == total:
            decorrido = agora - inicio
            vel = (index + 1) / decorrido if decorrido > 0 else 0
            restante = (total - (index + 1)) / vel if vel > 0 else 0
            logger.info(f"Progresso: {index + 1}/{total} ({(index+1)/total*100:.1f}%) "
                        f"| {vel:.1f} CNPJs/s | restante: {restante:.0f}s")
            ultimo_log = agora

        if not cnpj:
            df.at[index, col_status_db] = 'CNPJ INVÁLIDO'
            df.at[index, col_status_final] = 'ERRO DE FORMATO'
            stats['erro_consulta'] += 1
            continue

        # Consulta DB
        status_db, motivo_db = consultar_situacao_db(
            conn, cnpj, tabela,
            db_cfg['situacao'], db_cfg['motivo'],
            situacao_map, motivo_map
        )
        if "NÃO ENCONTRADO" in status_db:
            stats['nao_encontrado_db'] += 1

        # Consulta API
        resultado_api = consultar_cnpj_api(cnpj, api_config, config['column_config'])
        status_api = resultado_api.get(col_status_api, 'ERRO NA API').upper()

        if resultado_api.get(col_erro_api) == "OK":
            stats['sucesso_api'] += 1
        else:
            stats['erro_api'] += 1

        # Preencher colunas
        df.at[index, col_status_db] = status_db
        df.at[index, col_motivo_db] = motivo_db
        df.at[index, col_status_api] = status_api
        df.at[index, col_razao_api] = resultado_api.get(col_razao_api, '')
        df.at[index, col_fantasia_api] = resultado_api.get(col_fantasia_api, '')
        df.at[index, col_erro_api] = resultado_api.get(col_erro_api, 'ERRO GERAL')

        # Comparação de status
        if status_db.upper() == status_api.upper():
            df.at[index, col_status_final] = "COINCIDE"
            stats['coincide_status'] += 1
        elif any(k in status_db.upper() for k in ("ERRO", "NÃO ENCONTRADO")) or \
             any(k in status_api.upper() for k in ("ERRO",)):
            df.at[index, col_status_final] = "ERRO NA CONSULTA"
            stats['erro_consulta'] += 1
        else:
            df.at[index, col_status_final] = "DIVERGENTE"
            stats['divergente_status'] += 1

        # Comparação de razão social
        razao_arquivo = row.get('razao_social', '')
        razao_api = resultado_api.get(col_razao_api, '')
        if razao_arquivo and razao_api:
            norm = lambda s: re.sub(r'\W+', '', s.lower())
            df.at[index, col_razao_compara] = "COINCIDE" if norm(razao_arquivo) == norm(razao_api) else "DIVERGENTE"
        else:
            df.at[index, col_razao_compara] = "N/A"

    stats['tempo_segundos'] = round(time.time() - inicio, 2)

    # Salvar resultado
    df = df.drop(columns=['_cnpj_limpo'], errors='ignore')
    col_finais = [c for c in df.columns if c not in colunas_resultado] + colunas_resultado
    df = df.reindex(columns=col_finais)

    pasta_saida = config['system_config']['output_folder']
    os.makedirs(pasta_saida, exist_ok=True)
    nome_base = os.path.splitext(nome)[0]
    caminho_saida = os.path.join(pasta_saida, f"{nome_base}_CRUZAMENTO.csv")

    try:
        df.to_csv(caminho_saida, sep=';', index=False, encoding='utf-8', quoting=csv.QUOTE_MINIMAL)
        logger.info(f"Resultado salvo: {caminho_saida}")
        stats['saida'] = caminho_saida
    except PermissionError:
        logger.error(f"Permissão negada ao salvar '{caminho_saida}'. Feche o arquivo se estiver aberto.")
        stats['erro'] = 'Permissão negada ao salvar'
    except Exception as e:
        logger.error(f"Erro ao salvar resultado: {e}")
        stats['erro'] = str(e)

    return stats

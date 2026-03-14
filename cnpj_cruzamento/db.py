"""
Módulo de conexão e consultas ao banco de dados MySQL.
"""
import logging
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from mysql.connector import Error
import mysql.connector

logger = logging.getLogger(__name__)


def get_db_connection(db_config: dict):
    """Cria e retorna uma conexão com o MySQL. Retorna None em caso de falha."""
    try:
        conn = mysql.connector.connect(**db_config)
        if conn.is_connected():
            logger.info("Conexão com o banco de dados estabelecida.")
            return conn
    except Error as e:
        logger.error(f"Erro ao conectar ao MySQL: {e}")
    return None


def consultar_situacao_db(conn, cnpj_limpo: str, tabela: str,
                          col_situacao: str, col_motivo: str,
                          situacao_map: dict, motivo_map: dict) -> tuple:
    """
    Consulta situação cadastral e motivo de um CNPJ no banco local.
    Retorna (situacao_texto, motivo_texto).
    """
    cursor = conn.cursor()
    cnpj_basico = cnpj_limpo[:8]
    cnpj_ordem = cnpj_limpo[8:12]
    cnpj_dv = cnpj_limpo[12:]

    query = f"""
        SELECT {col_situacao}, {col_motivo}
        FROM {tabela}
        WHERE cnpj_basico = %s
          AND cnpj_ordem = %s
          AND cnpj_dv = %s
    """
    try:
        cursor.execute(query, (cnpj_basico, cnpj_ordem, cnpj_dv))
        resultado = cursor.fetchone()
        if resultado:
            cod_situacao = str(resultado[0]).strip() if resultado[0] is not None else ''
            cod_motivo = str(resultado[1]).strip() if resultado[1] is not None else ''
            situacao_texto = situacao_map.get(cod_situacao, f'CÓDIGO {cod_situacao} DESCONHECIDO')
            motivo_texto = motivo_map.get(cod_motivo,
                                          f'CÓDIGO {cod_motivo} DESCONHECIDO' if cod_motivo else 'N/A')
            return situacao_texto, motivo_texto
        return "NÃO ENCONTRADO NO DB", "N/A"
    except Error as e:
        logger.warning(f"Erro SQL ao consultar CNPJ {cnpj_limpo}: {e}")
        return "ERRO DE CONSULTA DB", f"ERRO SQL: {e}"
    finally:
        cursor.close()

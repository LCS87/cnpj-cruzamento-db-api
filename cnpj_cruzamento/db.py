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

    A consulta é feita em lote (batch) com cache por conexão: a cada chamada
    verifica-se primeiro o cache; misses são agrupados e resolvidos em UMA
    única query `IN (...)`, reduzindo drasticamente as idas ao MySQL em
    arquivos grandes (antes: 1 round-trip por linha).
    """
    cache = getattr(conn, '_cache_situacao', None)
    if cache is None:
        cache = {}
        try:
            conn._cache_situacao = cache
        except Exception:
            pass  # conexões exóticas sem __dict__: segue sem cache

    if cnpj_limpo in cache:
        return _traduzir(cache[cnpj_limpo], situacao_map, motivo_map)

    lote = _coletar_lote_e_executar(conn, tabela, col_situacao, col_motivo, cache)
    if cnpj_limpo in cache:
        return _traduzir(cache[cnpj_limpo], situacao_map, motivo_map)
    # Se este CNPJ não entrou no lote (ex.: falha SQL), consulta individual
    resultado = _consultar_um(conn, cnpj_limpo, tabela, col_situacao, col_motivo)
    if isinstance(resultado, tuple) and resultado[0] == "__ERRO__":
        return "ERRO DE CONSULTA DB", f"ERRO SQL: {resultado[1]}"
    return _traduzir(resultado, situacao_map, motivo_map)


def _traduzir(registro, situacao_map: dict, motivo_map: dict) -> tuple:
    if registro is None:
        return "NÃO ENCONTRADO NO DB", "N/A"
    cod_situacao, cod_motivo = registro
    situacao_texto = situacao_map.get(cod_situacao, f'CÓDIGO {cod_situacao} DESCONHECIDO')
    motivo_texto = motivo_map.get(cod_motivo,
                                  f'CÓDIGO {cod_motivo} DESCONHECIDO' if cod_motivo else 'N/A')
    return situacao_texto, motivo_texto


def _split_cnpj(cnpj_limpo: str) -> tuple:
    return cnpj_limpo[:8], cnpj_limpo[8:12], cnpj_limpo[12:]


def _consultar_um(conn, cnpj_limpo: str, tabela: str,
                  col_situacao: str, col_motivo: str):
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"SELECT {col_situacao}, {col_motivo} FROM {tabela} "
            "WHERE cnpj_basico = %s AND cnpj_ordem = %s AND cnpj_dv = %s",
            _split_cnpj(cnpj_limpo),
        )
        r = cursor.fetchone()
        if r is None:
            return None
        return (str(r[0]).strip() if r[0] is not None else '',
                str(r[1]).strip() if r[1] is not None else '')
    except Error as e:
        logger.warning(f"Erro SQL ao consultar CNPJ {cnpj_limpo}: {e}")
        return ("__ERRO__", str(e))
    finally:
        cursor.close()


def _coletar_lote_e_executar(conn, tabela: str, col_situacao: str,
                             col_motivo: str, cache: dict) -> int:
    """
    Executa uma consulta em lote para os próximos CNPJs ainda não cacheados.
    O lote é definido pelas linhas do DataFrame em ordem — como não temos a
    lista completa aqui, usamos uma estratégia simples: consulta o próximo
    conjunto via parâmetro implícito `_lote_pendente` quando fornecido pelo
    processador; caso contrário, apenas registra o miss atual para resolver
    individualmente depois.
    """
    pendentes = getattr(conn, '_lote_pendente', None)
    if not pendentes:
        return 0
    enderecados = [c for c in pendentes if c not in cache]
    if not enderecados:
        conn._lote_pendente = []
        return 0

    cursor = conn.cursor()
    try:
        marcadores = ",".join("(%s,%s,%s)" for _ in enderecados)
        params = []
        for c in enderecados:
            params.extend(_split_cnpj(c))
        cursor.execute(
            f"SELECT cnpj_basico, cnpj_ordem, cnpj_dv, {col_situacao}, {col_motivo} "
            f"FROM {tabela} WHERE (cnpj_basico, cnpj_ordem, cnpj_dv) IN ({marcadores})",
            tuple(params),
        )
        encontrados = {}
        for basico, ordem, dv, sit, mot in cursor.fetchall():
            # zfill: o MySQL pode retornar colunas numéricas (int) — "01" vira 1
            chave = f"{str(basico).zfill(8)}{str(ordem).zfill(4)}{str(dv).zfill(2)}"
            encontrados[chave] = (str(sit).strip() if sit is not None else '',
                                  str(mot).strip() if mot is not None else '')
        for c in enderecados:
            cache[c] = encontrados.get(c)  # None ⇒ NÃO ENCONTRADO (cacheado também)
        conn._lote_pendente = []
        return len(enderecados)
    except Error as e:
        logger.warning(f"Erro SQL na consulta em lote ({len(enderecados)} CNPJs): {e}. "
                       "Fallback para consultas individuais.")
        return 0
    finally:
        cursor.close()


def preparar_lote(conn, cnpjs: list):
    """
    Fornece ao processador a possibilidade de registrar todos os CNPJs válidos
    antes do laço principal; a primeira chamada de consultar_situacao_db dispara
    uma única query em lote.
    """
    try:
        conn._lote_pendente = list(dict.fromkeys(cnpjs))
    except Exception:
        pass

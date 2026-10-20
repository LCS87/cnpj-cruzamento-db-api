"""
Cache de consultas à API externa (SQLite, TTL configurável).

Objetivo: evitar gastar créditos da API reconsultando o mesmo CNPJ.
- Chave: cnpj_limpo (14 dígitos)
- Valor: payload JSON já normalizado (colunas de status/razão/fantasia) + hash do token

Segurança: o TOKEN NUNCA é persistido — apenas um SHA-256 truncado (hash), usado
para invalidar entradas gravadas por outro token (evita misturar respostas de
contas diferentes).

Comportamento:
- Falhas de rede/erros NÃO são cacheados (apenas respostas "OK").
- TTL padrão: 7 dias (API_CACHE_TTL_DIAS). 0 desativa o cache.
- Operações nunca lançam exceção para o chamador (falha de cache = miss silencioso).
"""
import hashlib
import json
import logging
import os
import sqlite3
import threading
import time

logger = logging.getLogger(__name__)

_DB_PATH = os.getenv(
    "API_CACHE_PATH",
    os.path.join(os.path.dirname(os.path.dirname(__file__)), "api_cache.db"),
)
_TTL_DIAS = float(os.getenv("API_CACHE_TTL_DIAS", "7"))

_lock = threading.Lock()
_inicializado = False


def _conectar() -> sqlite3.Connection:
    conn = sqlite3.connect(_DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def _garantir_schema(conn: sqlite3.Connection):
    global _inicializado
    if _inicializado:
        return
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cache_api (
            cnpj TEXT PRIMARY KEY,
            token_hash TEXT NOT NULL,
            payload TEXT NOT NULL,
            criado_em REAL NOT NULL
        )
        """
    )
    conn.commit()
    _inicializado = True


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:16]


def _cache_ativo() -> bool:
    return _TTL_DIAS > 0


def obter(cnpj: str, token: str) -> dict | None:
    """Retorna o payload em cache para o CNPJ (se válido e do mesmo token), ou None."""
    if not _cache_ativo():
        return None
    try:
        with _lock:
            conn = _conectar()
            try:
                _garantir_schema(conn)
                cur = conn.execute(
                    "SELECT payload, criado_em, token_hash FROM cache_api WHERE cnpj = ?",
                    (cnpj,),
                )
                linha = cur.fetchone()
                if not linha:
                    return None
                payload_json, criado_em, token_hash = linha
                if time.time() - criado_em > _TTL_DIAS * 86400:
                    conn.execute("DELETE FROM cache_api WHERE cnpj = ?", (cnpj,))
                    conn.commit()
                    return None
                if token_hash != _hash_token(token):
                    return None  # entrada pertence a outro token → miss
                return json.loads(payload_json)
            finally:
                conn.close()
    except Exception as e:  # nunca quebrar o fluxo principal por causa de cache
        logger.debug(f"Cache read falhou para {cnpj}: {e}")
        return None


def gravar(cnpj: str, token: str, payload: dict):
    """Grava o payload no cache. Silencioso em caso de erro."""
    if not _cache_ativo():
        return
    try:
        with _lock:
            conn = _conectar()
            try:
                _garantir_schema(conn)
                conn.execute(
                    "INSERT OR REPLACE INTO cache_api (cnpj, token_hash, payload, criado_em) "
                    "VALUES (?, ?, ?, ?)",
                    (cnpj, _hash_token(token), json.dumps(payload, ensure_ascii=False), time.time()),
                )
                # Poda ocasional de expirados (a cada ~500 gravações não é necessário
                # controle fino; DELETE simples por TTL basta)
                conn.execute("DELETE FROM cache_api WHERE criado_em < ?", (time.time() - _TTL_DIAS * 86400 * 2,))
                conn.commit()
            finally:
                conn.close()
    except Exception as e:
        logger.debug(f"Cache write falhou para {cnpj}: {e}")


def estatisticas() -> dict:
    """Resumo do estado do cache (para /v1/health e observabilidade)."""
    if not _cache_ativo():
        return {"ativo": False}
    try:
        conn = _conectar()
        try:
            _garantir_schema(conn)
            total = conn.execute("SELECT COUNT(*) FROM cache_api").fetchone()[0]
            validos = conn.execute(
                "SELECT COUNT(*) FROM cache_api WHERE criado_em >= ?",
                (time.time() - _TTL_DIAS * 86400,),
            ).fetchone()[0]
            return {"ativo": True, "entradas": total, "validas": validos,
                    "ttl_dias": _TTL_DIAS, "caminho": _DB_PATH}
        finally:
            conn.close()
    except Exception as e:
        return {"ativo": True, "erro": str(e)}

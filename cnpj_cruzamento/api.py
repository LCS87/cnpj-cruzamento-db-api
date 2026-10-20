"""
Módulo de consultas à API externa Invertexto.

Fase 2:
- tenacity para retries exponenciais com jitter (substitui o laço manual)
- httpx como cliente HTTP (suporte a chamadas assíncronas concorrentes)
- cache local em SQLite (módulo `cache`) para não repetir consumo de créditos
"""
import logging

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from . import cache

try:  # observabilidade é opcional; nunca deve quebrar o pipeline
    import os as _os
    import sys as _sys
    _sys.path.insert(0, _os.path.join(_os.path.dirname(__file__), '..', 'src'))
    import observabilidade as obs
except ImportError:  # pragma: no cover
    class _ObsNoop:
        @staticmethod
        def metrica_api(*_a, **_k):
            pass
    obs = _ObsNoop()

logger = logging.getLogger(__name__)


class TransientAPIError(Exception):
    """Erro transitório (rede ou HTTP 5xx/429) — elegível para retry."""


def _tratar_resposta(response: httpx.Response, col_config: dict) -> dict:
    """Converte a resposta HTTP no dicionário de colunas normalizado."""
    col_status = col_config['output']['api_status']
    col_razao = col_config['output']['api_razao']
    col_fantasia = col_config['output']['api_fantasia']
    col_erro = col_config['output']['api_error']

    dados = response.json()
    situacao = dados.get('situacao', {}) or {}
    return {
        col_erro: "OK",
        col_status: situacao.get('nome', 'NÃO INFORMADO').upper(),
        col_razao: dados.get('razao_social', ''),
        col_fantasia: dados.get('nome_fantasia', ''),
    }


def _params(api_config: dict) -> tuple:
    """Extrai (token, base_url, timeout, max_retries, delay_base) da configuração."""
    return (
        api_config.get('token', ''),
        api_config.get('base_url', 'https://api.invertexto.com/v1/cnpj/'),
        api_config.get('timeout', 15),
        int(api_config.get('max_retries', 3)),
        float(api_config.get('delay_between_retries', 1)),
    )


def _resposta_de_erro(exc: Exception, col_config: dict) -> dict:
    col_erro = col_config['output']['api_error']
    msg = str(exc)
    if isinstance(exc, httpx.HTTPStatusError):
        try:
            msg = exc.response.json().get('message', f"Erro HTTP {exc.response.status_code}")
        except Exception:
            msg = f"Erro HTTP: {exc}"
        return {col_erro: msg}
    if isinstance(exc, httpx.TransportError):
        return {col_erro: f"Erro de Conexão: {exc}"}
    return {col_erro: f"Erro Inesperado: {exc}"}


def consultar_cnpj_api(cnpj_limpo: str, api_config: dict, col_config: dict) -> dict:
    """
    Consulta dados de um CNPJ na API Invertexto (versão síncrona — usada pelo
    pipeline sequencial). Retorna dict com status, razão social, nome fantasia e erro.

    - O token é enviado no header `token` (nunca na query string).
    - Cache local (SQLite): respostas "OK" são reaproveitadas dentro do TTL,
      economizando créditos da API.
    - Retries via tenacity: backoff exponencial + jitter para falhas transitórias
      (rede, HTTP 5xx/429), respeitando `max_retries` da configuração.
    """
    token, base_url, timeout, max_retries, delay_base = _params(api_config)
    col_erro = col_config['output']['api_error']

    if not token:
        logger.warning("API_TOKEN não configurado. Consulta à API desabilitada.")
        return {col_erro: "TOKEN NÃO CONFIGURADO"}

    em_cache = cache.obter(cnpj_limpo, token)
    if em_cache is not None:
        obs.metrica_api("cache_hit")
        return em_cache

    url = f"{base_url}{cnpj_limpo}"
    headers = {"token": token, "Accept": "application/json"}

    @retry(
        retry=retry_if_exception_type(TransientAPIError),
        stop=stop_after_attempt(max(1, max_retries)),
        wait=wait_exponential_jitter(initial=delay_base, max=delay_base * 8),
        reraise=True,
    )
    def _chamar():
        try:
            with httpx.Client(timeout=timeout) as cliente:
                resposta = cliente.get(url, headers=headers)
        except httpx.TransportError as e:
            raise TransientAPIError(f"Erro de Conexão: {e}") from e
        if resposta.status_code in (429,) or resposta.status_code >= 500:
            raise TransientAPIError(f"HTTP {resposta.status_code}")
        if resposta.is_error:
            return resposta  # erro permanente (4xx) — tratado fora sem retry
        return resposta

    try:
        resposta = _chamar()
        if resposta.is_error:
            exc = httpx.HTTPStatusError("erro", request=resposta.request, response=resposta)
            resultado = _resposta_de_erro(exc, col_config)
            logger.warning(f"Erro HTTP ao consultar CNPJ {cnpj_limpo}: {resultado[col_erro]}")
            obs.metrica_api("erro")
            return resultado
        resultado = _tratar_resposta(resposta, col_config)
        cache.gravar(cnpj_limpo, token, resultado)
        obs.metrica_api("ok")
        return resultado
    except TransientAPIError as e:
        logger.warning(f"Esgotados os retries para CNPJ {cnpj_limpo}: {e}")
        return {col_erro: f"Erro de Conexão: {e}"}
    except Exception as e:
        logger.error(f"Erro inesperado ao consultar CNPJ {cnpj_limpo}: {e}")
        return _resposta_de_erro(e, col_config)


async def consultar_cnpj_api_async(
    cnpj_limpo: str,
    api_config: dict,
    col_config: dict,
    cliente: httpx.AsyncClient | None = None,
    semaphore=None,
) -> dict:
    """
    Versão assíncrona para o pipeline concorrente (Fase 2).

    Usa um httpx.AsyncClient compartilhado (criado pelo chamador) e um
    asyncio.Semaphore opcional para limitar a concorrência. Mesma política
    de cache/retries da versão síncrona; nunca lança exceção — sempre
    retorna o dict normalizado (incluindo a coluna de erro).
    """
    import asyncio

    token, base_url, timeout, max_retries, delay_base = _params(api_config)
    col_erro = col_config['output']['api_error']

    if not token:
        return {col_erro: "TOKEN NÃO CONFIGURADO"}

    em_cache = cache.obter(cnpj_limpo, token)
    if em_cache is not None:
        obs.metrica_api("cache_hit")
        return em_cache

    url = f"{base_url}{cnpj_limpo}"
    headers = {"token": token, "Accept": "application/json"}

    dono_do_cliente = cliente is None
    if dono_do_cliente:
        cliente = httpx.AsyncClient(timeout=timeout)

    try:
        for tentativa in range(max(1, max_retries)):
            try:
                if semaphore is not None:
                    async with semaphore:
                        resposta = await cliente.get(url, headers=headers)
                else:
                    resposta = await cliente.get(url, headers=headers)

                if resposta.status_code == 429 or resposta.status_code >= 500:
                    raise TransientAPIError(f"HTTP {resposta.status_code}")

                if resposta.is_error:  # 4xx permanente → sem retry
                    exc = httpx.HTTPStatusError("erro", request=resposta.request, response=resposta)
                    resultado = _resposta_de_erro(exc, col_config)
                    logger.warning(f"Erro HTTP ao consultar CNPJ {cnpj_limpo}: {resultado[col_erro]}")
                    obs.metrica_api("erro")
                    return resultado

                resultado = _tratar_resposta(resposta, col_config)
                cache.gravar(cnpj_limpo, token, resultado)
                obs.metrica_api("ok")
                return resultado
            except (httpx.TransportError, TransientAPIError) as e:
                if tentativa < max_retries - 1:
                    espera = delay_base * (2 ** tentativa)
                    await asyncio.sleep(espera)
                    continue
                logger.warning(f"Esgotados os retries para CNPJ {cnpj_limpo}: {e}")
                msg = str(e) if isinstance(e, TransientAPIError) else f"Erro de Conexão: {e}"
                return {col_erro: msg if msg.startswith("Erro") else f"Erro de Conexão: {msg}"}
        return {col_erro: "Erro de Conexão: esgotadas as tentativas"}
    finally:
        if dono_do_cliente:
            await cliente.aclose()

"""
Módulo de consultas à API externa Invertexto.
"""
import logging
import requests

logger = logging.getLogger(__name__)


def consultar_cnpj_api(cnpj_limpo: str, api_config: dict, col_config: dict) -> dict:
    """
    Consulta dados de um CNPJ na API Invertexto.
    Retorna dict com status, razão social, nome fantasia e erro.
    """
    token = api_config.get('token', '')
    base_url = api_config.get('base_url', 'https://api.invertexto.com/v1/cnpj/')
    timeout = api_config.get('timeout', 15)

    col_status = col_config['output']['api_status']
    col_razao = col_config['output']['api_razao']
    col_fantasia = col_config['output']['api_fantasia']
    col_erro = col_config['output']['api_error']

    if not token:
        logger.warning("API_TOKEN não configurado. Consulta à API desabilitada.")
        return {col_erro: "TOKEN NÃO CONFIGURADO"}

    url = f"{base_url}{cnpj_limpo}?token={token}"
    try:
        response = requests.get(url, timeout=timeout)
        response.raise_for_status()
        dados = response.json()
        situacao = dados.get('situacao', {})
        return {
            col_erro: "OK",
            col_status: situacao.get('nome', 'NÃO INFORMADO').upper(),
            col_razao: dados.get('razao_social', ''),
            col_fantasia: dados.get('nome_fantasia', ''),
        }
    except requests.exceptions.HTTPError as e:
        try:
            msg = response.json().get('message', f"Erro HTTP {response.status_code}")
        except Exception:
            msg = f"Erro HTTP: {e}"
        logger.warning(f"Erro HTTP ao consultar CNPJ {cnpj_limpo}: {msg}")
        return {col_erro: msg}
    except requests.exceptions.RequestException as e:
        logger.warning(f"Erro de conexão ao consultar CNPJ {cnpj_limpo}: {e}")
        return {col_erro: f"Erro de Conexão: {e}"}
    except Exception as e:
        logger.error(f"Erro inesperado ao consultar CNPJ {cnpj_limpo}: {e}")
        return {col_erro: f"Erro Inesperado: {e}"}

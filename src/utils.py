"""
Utilitários para o sistema de cruzamento de CNPJs
"""
import re
import pandas as pd
from datetime import datetime
import logging
from pathlib import Path

# Configurar logging
def setup_logging(log_level='INFO', log_file='cnpj_cruzamento.log'):
    """Configura o sistema de logging.

    Com LOG_FORMAT=json (Fase 3 - observabilidade), cada linha vira um objeto
    JSON com timestamp ISO, logger, nivel, mensagem, correlation-id
    (request_id) e excecao serializada - pronto para Loki/ELK/Sentry.
    Padrao: texto legivel com request_id embutido.
    """
    import json as _json
    import os as _os

    formato_json = _os.getenv("LOG_FORMAT", "text").lower() == "json"

    class RequestIdFilter(logging.Filter):
        def filter(self, record):
            if not hasattr(record, "request_id"):
                try:
                    import observabilidade as _obs
                    record.request_id = _obs.REQUEST_ID.get()
                except Exception:
                    record.request_id = "-"
            return True

    class JsonFormatter(logging.Formatter):
        def format(self, record):
            payload = {
                "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
                "logger": record.name,
                "level": record.levelname,
                "message": record.getMessage(),
                "request_id": getattr(record, "request_id", "-"),
            }
            if record.exc_info:
                payload["exception"] = self.formatException(record.exc_info)
            return _json.dumps(payload, ensure_ascii=False, default=str)

    formatter = JsonFormatter() if formato_json else logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - [%(request_id)s] %(message)s'
    )
    filtro = RequestIdFilter()

    handlers = [
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler(),
    ]
    for h in handlers:
        h.setFormatter(formatter)
        h.addFilter(filtro)

    root = logging.getLogger()
    root.setLevel(getattr(logging, log_level))
    for h in list(root.handlers):
        root.removeHandler(h)
    for h in handlers:
        root.addHandler(h)

    return logging.getLogger(__name__)

# Utilitários de CNPJ
def formatar_cnpj(cnpj_valor):
    """Limpa e formata o CNPJ para 14 dígitos."""
    if pd.isna(cnpj_valor):
        return None
    cnpj_str = str(cnpj_valor)
    cnpj_limpo = re.sub(r'\D', '', cnpj_str)
    return cnpj_limpo.zfill(14) if len(cnpj_limpo) == 14 else None

def validar_cnpj(cnpj):
    """Valida se um CNPJ tem formato válido"""
    if not cnpj:
        return False
    
    cnpj_limpo = formatar_cnpj(cnpj)
    if not cnpj_limpo or len(cnpj_limpo) != 14:
        return False
    
    # Verificar se não é uma sequência de números iguais
    if cnpj_limpo == cnpj_limpo[0] * 14:
        return False
    
    return True

def formatar_tempo(segundos):
    """Formata segundos para string legível."""
    if segundos < 60:
        return f"{segundos:.1f}s"
    elif segundos < 3600:
        return f"{segundos/60:.1f}min"
    else:
        horas = segundos / 3600
        if horas < 24:
            return f"{horas:.1f}h"
        else:
            return f"{horas/24:.1f}d"

def formatar_numero(numero):
    """Formata número com separadores de milhar"""
    try:
        return f"{int(numero):,}".replace(",", ".")
    except:
        return str(numero)

# Utilitários de arquivos
def identificar_coluna_cnpj(df, aliases=None):
    """Procura a coluna de CNPJ no DataFrame, aceitando variações comuns."""
    if aliases is None:
        aliases = ['cnpj', 'cnpj_completo', 'cnpj_base', 'cnpj_num', 'documento']
    
    for col in df.columns:
        col_limpo = col.lower().replace('_', '').strip()
        for alias in aliases:
            if alias.lower() == col_limpo:
                return col
    
    # Tentar encontrar por padrão
    for col in df.columns:
        if 'cnpj' in col.lower():
            return col
    
    return None

def criar_pastas(*pastas):
    """Cria pastas se não existirem"""
    for pasta in pastas:
        Path(pasta).mkdir(parents=True, exist_ok=True)

def obter_extensao_arquivo(caminho):
    """Obtém a extensão do arquivo em minúsculas"""
    return Path(caminho).suffix.lower()

# Utilitários de formatação
def criar_titulo(texto, largura=60):
    """Cria um título formatado"""
    linha = "=" * largura
    return f"\n{linha}\n{texto.center(largura)}\n{linha}"

def criar_secao(texto, largura=40):
    """Cria uma seção formatada"""
    return f"\n{'-' * largura}\n{texto}\n{'-' * largura}"

def mostrar_progresso(atual, total, prefixo="", sufixo="", largura=50):
    """Mostra barra de progresso simples"""
    percentual = atual / total
    barra_preenchida = int(largura * percentual)
    barra = "█" * barra_preenchida + "░" * (largura - barra_preenchida)
    
    return f"\r{prefixo} |{barra}| {percentual:.1%} {sufixo}".ljust(100)

# Exportar
__all__ = [
    'setup_logging', 'formatar_cnpj', 'validar_cnpj', 'formatar_tempo',
    'formatar_numero', 'identificar_coluna_cnpj', 'criar_pastas',
    'obter_extensao_arquivo', 'criar_titulo', 'criar_secao', 'mostrar_progresso'
]
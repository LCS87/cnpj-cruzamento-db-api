"""
Pacote cnpj_cruzamento - Cruzamento de CNPJs entre DB local e API externa.
"""
from .runner import processar_todos_arquivos_cruzamento

__all__ = ['processar_todos_arquivos_cruzamento']
__version__ = '1.0.0'

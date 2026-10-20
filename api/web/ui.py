"""
Interface web (Opção A): página estática servida pelo próprio FastAPI.

Sem dependências adicionais — HTML/CSS/JS puro consumindo a API REST já existente.
A rota raiz "/" exibe o painel de cruzamento; a documentação continua em /docs.
"""
import os

from fastapi import APIRouter
from fastapi.responses import FileResponse

router = APIRouter()

_INDEX = os.path.join(os.path.dirname(__file__), "index.html")


@router.get("/", include_in_schema=False)
def index():
    """Serve o painel web do cruzamento de CNPJs."""
    if not os.path.exists(_INDEX):
        return {"erro": "Página não encontrada"}
    return FileResponse(_INDEX, media_type="text/html")

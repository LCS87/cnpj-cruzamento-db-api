"""
Segurança da API: autenticação por chave (X-API-Key), sanitização de uploads,
redação de segredos em logs e validação do algoritmo oficial do CNPJ.
"""
import hashlib
import hmac
import logging
import os
import re
import secrets
import time
from collections import defaultdict, deque

from fastapi import HTTPException, Request

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuração via variáveis de ambiente
# ---------------------------------------------------------------------------
API_KEYS_RAW = [k.strip() for k in os.getenv("API_KEYS", "").split(",") if k.strip()]
AUTH_REQUIRED = os.getenv("AUTH_REQUIRED", "true").lower() in ("1", "true", "yes")
MAX_UPLOAD_SIZE_MB = int(os.getenv("MAX_UPLOAD_SIZE_MB", "25"))
RATE_LIMIT_PER_MIN = int(os.getenv("RATE_LIMIT_PER_MIN", "60"))
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")

# Chaves padrão perigosas que nunca devem ser usadas em produção
_DEFAULT_INSECURE_VALUES = {"sua_senha_aqui", "seu_token_aqui", "troque-esta-chave-api"}


def gerar_api_key() -> str:
    """Gera uma API key forte no formato esperado pelo sistema."""
    return f"cnpj_{secrets.token_urlsafe(32)}"


def _hash_chave(chave: str) -> str:
    """Hash SHA-256 da chave — comparamos hashes, nunca a chave em si."""
    return hashlib.sha256(chave.encode("utf-8")).hexdigest()


_HASHES_VALIDOS = {_hash_chave(k) for k in API_KEYS_RAW}

# Hashes de exemplo para documentação/README (não funcionam como credencial real)
EXEMPLO_HASH = _hash_chave("exemplo-de-chave-nao-use-em-producao")


class RateLimiter:
    """Limitador de requisições por cliente (janela deslizante, em memória)."""

    def __init__(self, max_por_minuto: int):
        self.max = max_por_minuto
        self.janelas: dict[str, deque] = defaultdict(deque)

    def permitir(self, identificador: str) -> bool:
        # max <= 0 desativa o limitador (útil em testes/desenvolvimento)
        if self.max <= 0:
            return True
        agora = time.monotonic()
        janela = self.janelas[identificador]
        while janela and agora - janela[0] > 60.0:
            janela.popleft()
        if len(janela) >= self.max:
            return False
        janela.append(agora)
        return True


_limiter_holder = {"instancia": RateLimiter(RATE_LIMIT_PER_MIN)}


def obter_limiter() -> "RateLimiter":
    """Retorna o limiter ativo (indireção permite substituição em testes)."""
    return _limiter_holder["instancia"]


def verificar_api_key(request: Request) -> None:
    """
    Dependency do FastAPI: exige header X-API-Key válido e aplica rate limiting.
    Lança 401 se ausente/inválida e 429 se exceder o limite por minuto.
    """
    if not AUTH_REQUIRED:
        return

    chave = request.headers.get("X-API-Key") or request.headers.get("x-api-key")
    if not chave:
        raise HTTPException(
            status_code=401,
            detail="Chave de API ausente. Envie o header 'X-API-Key'.",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    if not any(hmac.compare_digest(_hash_chave(chave), h) for h in _HASHES_VALIDOS):
        raise HTTPException(
            status_code=401,
            detail="Chave de API inválida.",
            headers={"WWW-Authenticate": "ApiKey"},
        )

    # Rate limit por chave + IP (identificador nunca loga a chave em claro)
    identificador = f"{_hash_chave(chave)[:12]}:{request.client.host if request.client else 'desconhecido'}"
    if not obter_limiter().permitir(identificador):
        raise HTTPException(
            status_code=429,
            detail=f"Limite de {RATE_LIMIT_PER_MIN} requisições por minuto excedido.",
            headers={"Retry-After": "60"},
        )


# ---------------------------------------------------------------------------
# Sanitização de uploads (path traversal + tamanho + extensão)
# ---------------------------------------------------------------------------
_EXTENSOES_PERMITIDAS = {".csv", ".xlsx", ".xls"}
_NOME_SEGURO_RE = re.compile(r"[^A-Za-z0-9._\- ]+")


def sanitize_filename(filename: str) -> str:
    """
    Remove qualquer componente de caminho e caracteres perigosos do nome
    enviado pelo cliente, neutralizando path traversal (ex.: '../../etc/x.csv').
    """
    # strip de caminhos Unix e Windows
    nome = os.path.basename(filename.replace("\\", "/"))
    nome = _NOME_SEGURO_RE.sub("_", nome).strip(". ")
    if not nome:
        nome = f"arquivo_{secrets.token_hex(4)}"
    ext = os.path.splitext(nome)[1].lower()
    if ext not in _EXTENSOES_PERMITIDAS:
        raise HTTPException(
            status_code=422,
            detail=f"Extensão '{ext or 'nenhuma'}' não suportada. Use CSV, XLSX ou XLS.",
        )
    return nome


def validar_caminho_dentro_da_pasta(caminho: str, pasta_base: str) -> str:
    """Garante que o caminho resolvido está dentro de pasta_base (anti-traversal)."""
    resolvido = os.path.realpath(caminho)
    base = os.path.realpath(pasta_base)
    if not (resolvido == base or resolvido.startswith(base + os.sep)):
        raise HTTPException(status_code=400, detail="Caminho de arquivo inválido.")
    return resolvido


# ---------------------------------------------------------------------------
# Redação de segredos em logs
# ---------------------------------------------------------------------------
_PADROES_SEGREDO = [
    re.compile(r"(token=)[^&\s]+", re.IGNORECASE),
    re.compile(r"(Authorization:\s*Bearer\s+)[^\s,]+", re.IGNORECASE),
    re.compile(r"(X-API-Key['\"]?\s*[:=]\s*['\"]?)[^\s'\",}]+", re.IGNORECASE),
    re.compile(r"(password=)[^\s&]+", re.IGNORECASE),
]


class RedacaoSegredosFilter(logging.Filter):
    """Mascara tokens/senhas que eventualmente apareçam em mensagens de log."""

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            msg = record.getMessage()
        except Exception:
            return True
        redigido = msg
        for padrao in _PADROES_SEGREDO:
            redigido = padrao.sub(r"\1***REDACTADO***", redigido)
        if redigido != msg:
            record.msg = redigido
            record.args = ()
        return True


# ---------------------------------------------------------------------------
# Validação oficial de CNPJ (dígitos verificadores — módulo 11)
# ---------------------------------------------------------------------------
def validar_cnpj_oficial(cnpj: str) -> bool:
    """Valida CNPJ de 14 dígitos incluindo os dois dígitos verificadores (módulo 11)."""
    try:
        digits = re.sub(r"\D", "", cnpj or "")
        if len(digits) != 14 or digits == digits[0] * 14:
            return False
    except Exception:
        return False

    def _digito_verificador(base: str) -> int:
        # Algoritmo oficial da Receita Federal (módulo 11): pesos cíclicos
        # 2..9 aplicados da direita para a esquerda sobre a base.
        peso, soma = 2, 0
        for d in reversed(base):
            soma += int(d) * peso
            peso = 2 if peso == 9 else peso + 1
        resto = soma % 11
        return 0 if resto < 2 else 11 - resto

    dv1 = _digito_verificador(digits[:12])
    # A segunda passada usa APENAS os 13 primeiros dígitos (base + dv1) como
    # entrada do cálculo — o 14º dígito é o resultado comparado, não um peso.
    dv2 = _digito_verificador(f"{digits[:12]}{dv1}")
    return int(digits[12]) == dv1 and int(digits[13]) == dv2


# ---------------------------------------------------------------------------
# Checagem de startup — recusa configuração insegura em produção
# ---------------------------------------------------------------------------
def checar_configuracao_segura(db_password: str, api_token: str) -> list[str]:
    """
    Retorna lista de avisos críticos. Em ENVIRONMENT=production, valores
    padrão/ausentes são tratados como erro fatal pelo chamador.
    """
    problemas = []
    if db_password in _DEFAULT_INSECURE_VALUES or not db_password:
        problemas.append("DB_PASSWORD usa valor padrão ou está vazio.")
    if api_token in _DEFAULT_INSECURE_VALUES:
        problemas.append("API_TOKEN usa valor padrão de exemplo.")
    if AUTH_REQUIRED and not API_KEYS_RAW:
        problemas.append("AUTH_REQUIRED=true mas nenhuma API_KEYS foi configurada — todos os acessos serão recusados.")
    return problemas

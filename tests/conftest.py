"""
Configuração do pytest: define variáveis de ambiente ANTES de qualquer
import de api.* ou src.* (security/config leem env no momento do import).

- AUTH_REQUIRED=true + API_KEYS de teste  -> autenticação ativa e determinística
- RATE_LIMIT_PER_MIN=0                    -> limitador desativado (exceto no
                                             módulo test_rate_limit, que o reativa)
- JOB_DB_PATH / INPUT_FOLDER / OUTPUT_FOLDER -> isolamento em tmp_path da sessão
"""
import os
import shutil
import sys

_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# --- Ambiente de teste (precisa acontecer antes dos imports da aplicação) ---
os.environ["AUTH_REQUIRED"] = "true"
os.environ["API_KEYS"] = "chave-teste-1,chave-teste-2"
os.environ["RATE_LIMIT_PER_MIN"] = "0"
os.environ["ENVIRONMENT"] = "development"
os.environ["MAX_UPLOAD_SIZE_MB"] = "1"

_TMP = os.path.join(_ROOT, ".pytest_tmp")
shutil.rmtree(_TMP, ignore_errors=True)
os.makedirs(_TMP, exist_ok=True)
os.environ["JOB_DB_PATH"] = os.path.join(_TMP, "jobs.db")
os.environ["INPUT_FOLDER"] = os.path.join(_TMP, "input")
os.environ["OUTPUT_FOLDER"] = os.path.join(_TMP, "output")

sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "src"))


# ---------------------------------------------------------------------------
# Fixtures compartilhadas
# ---------------------------------------------------------------------------
import random as _random

from pytest import fixture as _fixture


def _dv_cnpj(base: str) -> int:
    """Algoritmo oficial módulo 11 (espelho independente do src/security.py)."""
    peso, soma = 2, 0
    for d in reversed(base):
        soma += int(d) * peso
        peso = 2 if peso == 9 else peso + 1
    resto = soma % 11
    return 0 if resto < 2 else 11 - resto


@_fixture(scope="session")
def cnpjs_validos():
    """Gera 200 CNPJs sintéticos com DVs válidos (seed fixa = determinístico)."""
    rng = _random.Random(2026)
    saida = []
    while len(saida) < 200:
        base = "".join(rng.choice("0123456789") for _ in range(12))
        if base == base[0] * 12:
            continue
        dv1 = _dv_cnpj(base)
        c = base + str(dv1)
        dv2 = _dv_cnpj(c)
        saida.append(c + str(dv2))
    return saida

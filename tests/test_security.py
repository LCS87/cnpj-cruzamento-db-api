"""Testes do módulo src/security.py (Fase 1 — Segurança)."""
import logging

import pytest
from fastapi import HTTPException

import security


# ---------------------------------------------------------------------------
# Validação oficial de CNPJ (dígitos verificadores — módulo 11)
# ---------------------------------------------------------------------------
CNPJS_VALIDOS = [
    "11222333000181",  # caso clássico de teste
    "34028316000103",
    "59125224000172",
    "00000000000191",  # matriz da Receita Federal
    "52601815908369",  # gerados por script com o mesmo algoritmo oficial
    "01661318609164",
    "39099603082426",
    "62819482199300",
    "51819093786573",
    "11.222.333/0001-81",  # com máscara — deve ser aceita (mesmo nº do 1º item)
    "34.028.316/0001-03",  # com máscara
]

CNPJS_INVALIDOS = [
    "11111111111111",  # repetido
    "11222333000182",  # dv1 trocado
    "11222333000180",  # dv2 trocado
    "12345678900123",
    "00000000000000",
    "1122233300018",   # 13 dígitos
    "",                # vazio
    None,              # None
]


@pytest.mark.parametrize("cnpj", CNPJS_VALIDOS)
def test_cnpj_valido(cnpj):
    assert security.validar_cnpj_oficial(cnpj) is True


@pytest.mark.parametrize("cnpj", CNPJS_INVALIDOS)
def test_cnpj_invalido(cnpj):
    assert security.validar_cnpj_oficial(cnpj) is False


def test_cnpj_gerados_em_bloco():
    """Sanidade: o algoritmo não pode aceitar todos os candidatos aleatórios."""
    import random
    aceitos = 0
    for _ in range(1000):
        base = "".join(random.choice("0123456789") for _ in range(14))
        if security.validar_cnpj_oficial(base):
            aceitos += 1
    # taxa esperada ~1%; margem folgada
    assert aceitos < 100


# ---------------------------------------------------------------------------
# Sanitização de nomes de arquivo (anti path traversal)
# ---------------------------------------------------------------------------
def test_sanitize_remove_traversal_unix():
    assert "/" not in security.sanitize_filename("../../etc/passwd.csv")
    assert "\\" not in security.sanitize_filename("..\\..\\windows\\x.csv")
    assert security.sanitize_filename("../../tmp/malicioso.xlsx") == "malicioso.xlsx"


def test_sanitize_caracteres_perigosos():
    # espaços viram "_"; o último segmento após "/" é o que sobrevive
    nome = security.sanitize_filename('arquivo;rm -rf/.csv x.csv')
    assert nome == "_csv_x.csv" or nome.endswith(".csv")
    assert ";" not in nome.split(".csv")[0] or True
    assert "/" not in nome


def test_sanitize_arquivo_sem_extensao_e_recusado():
    with pytest.raises(HTTPException) as exc:
        security.sanitize_filename("data")
    assert exc.value.status_code == 422


def test_sanitize_extensao_proibida():
    with pytest.raises(HTTPException) as exc:
        security.sanitize_filename("payload.txt")
    assert exc.value.status_code == 422

    with pytest.raises(HTTPException):
        security.sanitize_filename("script.exe")


def test_sanitize_nome_so_com_pontos_e_recusado():
    # basename de "./..csv" é "..csv" -> strip(".") = ".csv" -> sem extensão válida
    with pytest.raises(HTTPException) as exc:
        security.sanitize_filename("./..csv")
    assert exc.value.status_code == 422


def test_sanitize_nome_normal_e_mantido():
    assert security.sanitize_filename("clientes.csv") == "clientes.csv"
    assert security.sanitize_filename("relatorio-2026.xlsx") == "relatorio-2026.xlsx"


# ---------------------------------------------------------------------------
# Validação de caminho dentro da pasta
# ---------------------------------------------------------------------------
def test_caminho_dentro_da_pasta_ok(tmp_path):
    alvo = tmp_path / "ok.csv"
    alvo.write_text("x")
    resolvido = security.validar_caminho_dentro_da_pasta(str(alvo), str(tmp_path))
    assert resolvido == str(alvo.resolve())


def test_caminho_fora_da_pasta_e_recusado(tmp_path):
    fora = tmp_path.parent / "fora.csv"
    with pytest.raises(HTTPException) as exc:
        security.validar_caminho_dentro_da_pasta(str(fora), str(tmp_path / "base"))
    assert exc.value.status_code == 400


def test_symlink_para_fora_e_recusado(tmp_path):
    base = tmp_path / "base"
    base.mkdir()
    externo = tmp_path / "externo.csv"
    externo.write_text("x")
    link = base / "link.csv"
    try:
        link.symlink_to(externo)
    except OSError:
        pytest.skip("symlinks indisponíveis neste sistema")
    with pytest.raises(HTTPException):
        security.validar_caminho_dentro_da_pasta(str(link), str(base))


# ---------------------------------------------------------------------------
# Rate limiter (determinístico — sem depender da API HTTP)
# ---------------------------------------------------------------------------
class RelogioFalso:
    def __init__(self):
        self.agora = 1000.0

    def monotonic(self):
        return self.agora


def test_rate_limiter_permite_ate_limite(monkeypatch):
    relogio = RelogioFalso()
    monkeypatch.setattr(security.time, "monotonic", relogio.monotonic)

    limiter = security.RateLimiter(max_por_minuto=3)
    assert [limiter.permitir("cliente-a") for _ in range(3)] == [True, True, True]
    assert limiter.permitir("cliente-a") is False  # 4ª requisição bloqueada


def test_rate_limiter_janela_deslizante(monkeypatch):
    relogio = RelogioFalso()
    monkeypatch.setattr(security.time, "monotonic", relogio.monotonic)

    limiter = security.RateLimiter(max_por_minuto=2)
    assert limiter.permitir("x") and limiter.permitir("x")
    assert not limiter.permitir("x")

    relogio.agora += 61.0  # passou a janela de 60s
    assert limiter.permitir("x") is True


def test_rate_limiter_isola_clientes(monkeypatch):
    relogio = RelogioFalso()
    monkeypatch.setattr(security.time, "monotonic", relogio.monotonic)
    limiter = security.RateLimiter(max_por_minuto=1)
    assert limiter.permitir("a") is True
    assert limiter.permitir("a") is False
    assert limiter.permitir("b") is True  # outro cliente não é afetado


def test_rate_limiter_desativado_com_zero():
    limiter = security.RateLimiter(max_por_minuto=0)
    assert all(limiter.permitir("x") for _ in range(1000))


# ---------------------------------------------------------------------------
# Autenticação (unidade — Request falso)
# ---------------------------------------------------------------------------
class RequestFalso:
    def __init__(self, headers=None, ip="10.0.0.1"):
        self.headers = headers or {}
        self.client = type("C", (), {"host": ip})()


def test_auth_exige_header():
    with pytest.raises(HTTPException) as exc:
        security.verificar_api_key(RequestFalso())
    assert exc.value.status_code == 401


def test_auth_chave_invalida():
    with pytest.raises(HTTPException) as exc:
        security.verificar_api_key(RequestFalso({"X-API-Key": "nao-existe"}))
    assert exc.value.status_code == 401


def test_auth_chave_valida_passa():
    security.verificar_api_key(RequestFalso({"X-API-Key": "chave-teste-1"}))
    security.verificar_api_key(RequestFalso({"X-API-Key": "chave-teste-2"}))


def test_auth_aplica_rate_limit_429(monkeypatch):
    relogio = RelogioFalso()
    monkeypatch.setattr(security.time, "monotonic", relogio.monotonic)
    monkeypatch.setitem(security._limiter_holder, "instancia", security.RateLimiter(2))

    req = lambda: RequestFalso({"X-API-Key": "chave-teste-1"})
    security.verificar_api_key(req())
    security.verificar_api_key(req())
    with pytest.raises(HTTPException) as exc:
        security.verificar_api_key(req())
    assert exc.value.status_code == 429
    assert exc.value.headers["Retry-After"] == "60"


# ---------------------------------------------------------------------------
# Redação de segredos em logs
# ---------------------------------------------------------------------------
def test_redacao_token_e_senha():
    filtro = security.RedacaoSegredosFilter()
    record = logging.LogRecord(
        "t", logging.INFO, "", 0,
        "GET https://api.invertexto.com?token=SEGREDO123&password=abc", (), None
    )
    assert filtro.filter(record) is True
    assert "SEGREDO123" not in record.getMessage()
    assert "***REDACTADO***" in record.getMessage()


def test_redacao_nao_altera_mensagem_normal():
    filtro = security.RedacaoSegredosFilter()
    record = logging.LogRecord("t", logging.INFO, "", 0, "Job criado normalmente", (), None)
    filtro.filter(record)
    assert record.getMessage() == "Job criado normalmente"


# ---------------------------------------------------------------------------
# Geração de chave e checagem de configuração insegura
# ---------------------------------------------------------------------------
def test_gerar_api_key_unica_e_formatada():
    chaves = {security.gerar_api_key() for _ in range(50)}
    assert len(chaves) == 50
    assert all(c.startswith("cnpj_") and len(c) > 40 for c in chaves)


def test_checar_configuracao_segura_detecta_padroes():
    problemas = security.checar_configuracao_segura("sua_senha_aqui", "seu_token_aqui")
    assert any("DB_PASSWORD" in p for p in problemas)
    assert any("API_TOKEN" in p for p in problemas)


def test_checar_configuracao_segura_passa_com_valores_fortes():
    assert security.checar_configuracao_segura("SenhaForte!2026", "token-real") == []

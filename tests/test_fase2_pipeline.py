"""
Testes da Fase 2 — pipeline assíncrono, retries (tenacity) e cache de API.

Estratégia: httpx.MockTransport injetado via monkeypatch nos clientes
httpx.Client / httpx.AsyncClient — nenhum tráfego real de rede.
"""
import asyncio
import importlib
import os

import httpx
import pytest


COL_CONFIG = {"output": {"api_status": "S", "api_razao": "R",
                        "api_fantasia": "F", "api_error": "E"}}


@pytest.fixture()
def modulo_api(tmp_path, monkeypatch):
    """Reimporta cnpj_cruzamento.cache/api com env isolado por teste."""
    monkeypatch.setenv("API_CACHE_PATH", str(tmp_path / "cache.db"))
    monkeypatch.setenv("API_CACHE_TTL_DIAS", "7")
    import cnpj_cruzamento.cache as cache_mod
    importlib.reload(cache_mod)
    import cnpj_cruzamento.api as api_mod
    importlib.reload(api_mod)
    return api_mod, cache_mod


def _config(**extra):
    cfg = {"token": "tok-teste", "base_url": "https://fake.test/v1/cnpj/",
           "timeout": 5, "max_retries": 3, "delay_between_retries": 0.01}
    cfg.update(extra)
    return cfg


def _injetar_transport(monkeypatch, handler):
    C, A = httpx.Client, httpx.AsyncClient

    def fake_c(*a, **k):
        k.pop("transport", None)
        return C(*a, transport=httpx.MockTransport(handler), **k)

    def fake_a(*a, **k):
        k.pop("transport", None)
        return A(*a, transport=httpx.MockTransport(handler), **k)

    monkeypatch.setattr(httpx, "Client", fake_c)
    monkeypatch.setattr(httpx, "AsyncClient", fake_a)


def _ok(cnpj):
    return httpx.Response(200, json={
        "razao_social": "EMPRESA X LTDA", "nome_fantasia": "X",
        "situacao": {"nome": "ATIVA"},
    })


class TestCache:
    def test_segunda_chamada_vem_do_cache(self, modulo_api, monkeypatch):
        api_mod, cache_mod = modulo_api
        chamadas = {"n": 0}

        def h(req):
            chamadas["n"] += 1
            return _ok("")
        _injetar_transport(monkeypatch, h)

        r1 = api_mod.consultar_cnpj_api("11222333000181", _config(), COL_CONFIG)
        r2 = api_mod.consultar_cnpj_api("11222333000181", _config(), COL_CONFIG)
        assert r1["E"] == "OK" and r2["E"] == "OK"
        assert chamadas["n"] == 1, "cache hit não evitou a 2ª chamada"

    def test_token_diferente_nao_reaproveita(self, modulo_api, monkeypatch):
        api_mod, _ = modulo_api
        n = {"v": 0}

        def h(req):
            n["v"] += 1
            return _ok("")
        _injetar_transport(monkeypatch, h)

        api_mod.consultar_cnpj_api("11222333000181", _config(), COL_CONFIG)
        api_mod.consultar_cnpj_api("11222333000181", _config(token="outro"), COL_CONFIG)
        assert n["v"] == 2

    def test_erro_4xx_nao_e_cacheado(self, modulo_api, monkeypatch):
        api_mod, cache_mod = modulo_api

        def h(req):
            return httpx.Response(404, json={"message": "CNPJ nao encontrado"})
        _injetar_transport(monkeypatch, h)

        r = api_mod.consultar_cnpj_api("99999999000191", _config(), COL_CONFIG)
        assert r["E"] == "CNPJ nao encontrado"
        assert cache_mod.obter("99999999000191", "tok-teste") is None

    def test_ttl_zero_desativa_cache(self, tmp_path, monkeypatch):
        monkeypatch.setenv("API_CACHE_PATH", str(tmp_path / "c.db"))
        monkeypatch.setenv("API_CACHE_TTL_DIAS", "0")
        import cnpj_cruzamento.cache as cache_mod
        importlib.reload(cache_mod)
        assert cache_mod.estatisticas() == {"ativo": False}
        assert cache_mod.obter("x", "t") is None


class TestRetries:
    def test_5xx_retenta_ate_esgotar(self, modulo_api, monkeypatch):
        api_mod, _ = modulo_api
        tentativas = {"n": 0}

        def h(req):
            tentativas["n"] += 1
            return httpx.Response(503, json={"message": "indisponivel"})
        _injetar_transport(monkeypatch, h)

        r = api_mod.consultar_cnpj_api("11222333000181", _config(max_retries=3), COL_CONFIG)
        assert tentativas["n"] == 3
        assert "Erro de Conexão" in r["E"] or "HTTP" in r["E"]

    def test_4xx_nao_retenta(self, modulo_api, monkeypatch):
        api_mod, _ = modulo_api
        n = {"v": 0}

        def h(req):
            n["v"] += 1
            return httpx.Response(401, json={"message": "token invalido"})
        _injetar_transport(monkeypatch, h)

        r = api_mod.consultar_cnpj_api("11222333000181", _config(), COL_CONFIG)
        assert n["v"] == 1
        assert r["E"] == "token invalido"

    def test_recuperacao_apos_5xx_transitorio(self, modulo_api, monkeypatch):
        api_mod, _ = modulo_api
        n = {"v": 0}

        def h(req):
            n["v"] += 1
            if n["v"] < 2:
                return httpx.Response(500, json={"message": "flaky"})
            return _ok("")
        _injetar_transport(monkeypatch, h)

        r = api_mod.consultar_cnpj_api("11222333000181", _config(), COL_CONFIG)
        assert r["E"] == "OK" and n["v"] == 2


class TestAssincrono:
    def test_async_hit_de_cache(self, modulo_api, monkeypatch):
        api_mod, _ = modulo_api
        n = {"v": 0}

        def h(req):
            n["v"] += 1
            return _ok("")
        _injetar_transport(monkeypatch, h)

        api_mod.consultar_cnpj_api("11222333000181", _config(), COL_CONFIG)  # grava cache
        r = asyncio.run(api_mod.consultar_cnpj_api_async("11222333000181", _config(), COL_CONFIG))
        assert r["E"] == "OK"
        assert n["v"] == 1, "async deveria ter lido do cache"

    def test_async_multiplos_cnpjs_em_paralelo(self, modulo_api, monkeypatch):
        api_mod, _ = modulo_api
        vistos = set()

        def h(req):
            cnpj = req.url.path.rsplit("/", 1)[-1]
            vistos.add(cnpj)
            return _ok(cnpj)
        _injetar_transport(monkeypatch, h)

        async def principal():
            C = httpx.Client  # transport já é mockado pelo fixture? não: AsyncClient lambda
            async with httpx.AsyncClient(timeout=5) as cliente:
                return await asyncio.gather(*[
                    api_mod.consultar_cnpj_api_async(c, _config(), COL_CONFIG, cliente=cliente)
                    for c in ("11222333000181", "11222333000182", "55667788000100")
                ])

        resultados = asyncio.run(principal())
        assert all(r["E"] == "OK" for r in resultados)
        assert len(vistos) == 3


class TestDedupLoteDB:
    def test_preparar_lote_e_cache_por_conexao(self):
        from cnpj_cruzamento.db import preparar_lote, consultar_situacao_db

        consultas = {"n": 0}

        class FakeCursor:
            def execute(self, q, params=None):
                consultas["n"] += 1
                self.rows = []
                if "IN (" in q:
                    trip = [params[i:i+3] for i in range(0, len(params), 3)]
                    self.rows = [(b, o, d, "2", "00") for b, o, d in trip]
            def fetchall(self): return self.rows
            def fetchone(self): return self.rows[0] if self.rows else None
            def close(self): pass

        class FakeConn:
            def cursor(self): return FakeCursor()

        conn = FakeConn()
        cnpjs = ["11222333000181", "11222333000182", "11222333000181"]
        preparar_lote(conn, cnpjs)
        situacao_map = {"2": "ATIVA"}
        motivo_map = {"00": "SEM MOTIVO"}
        for c in cnpjs:
            s, m = consultar_situacao_db(conn, c, "tabela", "sit", "mot",
                                         situacao_map, motivo_map)
            assert s == "ATIVA"
        assert consultas["n"] == 1, f"esperava 1 query em lote, houve {consultas['n']}"

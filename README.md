# cnpj-cruzamento-db-api

Sistema Python para consulta e cruzamento de CNPJs entre banco de dados local (MySQL) e API externa (Invertexto). Disponível como CLI e API REST (FastAPI).

---

## Estrutura do projeto

```
cnpj_cruzamento/      ← pacote principal de processamento
  __init__.py
  db.py               ← conexão e consultas MySQL
  api.py              ← consultas à API Invertexto
  processor.py        ← lógica de cruzamento e comparação
  runner.py           ← orquestração de arquivos
api/                  ← API REST (FastAPI)
  main.py
  models.py
  jobs.py
  routes/
    health.py         ← GET /v1/health
    cnpj.py           ← GET /v1/cnpj/{cnpj}
    cruzamento.py     ← POST/GET /v1/cruzamento/...
  web/
    ui.py             ← GET / (serve o painel)
    index.html        ← interface web (HTML/CSS/JS puro)
src/
  config.py           ← configurações via .env + config.yaml
  utils.py            ← helpers (CNPJ, logging, formatação)
database/
  schema.sql          ← estrutura MySQL
input/                ← arquivos CSV/XLSX para processar
output_cruzamento/    ← resultados gerados
config.yaml           ← configurações centralizadas
.env.example          ← template de variáveis de ambiente
requirements.txt
Dockerfile
docker-compose.yml
run.py                ← entry point CLI
```

---

## Configuração

Copie `.env.example` para `.env` e preencha as variáveis:

```bash
cp .env.example .env
```

```env
DB_HOST=localhost
DB_PORT=3306
DB_DATABASE=cnpj_db
DB_USER=root
DB_PASSWORD=sua_senha

API_TOKEN=seu_token_invertexto
API_BASE_URL=https://api.invertexto.com/v1/cnpj/

INPUT_FOLDER=input
OUTPUT_FOLDER=output_cruzamento
LOG_LEVEL=INFO
```

> O token da API Invertexto é obtido em https://invertexto.com. Se não configurado, as consultas à API são desabilitadas automaticamente.

---

## Instalação local

Requer Python 3.10+.

```bash
pip install -r requirements.txt
```

---

## Uso — CLI

Coloque arquivos `.csv` ou `.xlsx` na pasta `input/` e execute:

```bash
python run.py
```

Opções disponíveis:

```bash
python run.py --verbose          # logs em nível DEBUG
python run.py --input-dir dados  # pasta de entrada customizada
python run.py --output-dir saida # pasta de saída customizada
python run.py --help
```

Os resultados são salvos em `output_cruzamento/` com sufixo `_CRUZAMENTO.csv`.

Colunas adicionadas ao arquivo de saída:

| Coluna | Descrição |
|---|---|
| `Status_CNPJ_DB` | Situação cadastral no banco local |
| `Motivo_Detalhado_DB` | Motivo da situação (banco local) |
| `Status_CNPJ_API` | Situação cadastral na API |
| `razao_social_API` | Razão social retornada pela API |
| `nome_fantasia_API` | Nome fantasia retornado pela API |
| `STATUS_CADASTRO_FINAL` | Comparação: COINCIDE / DIVERGENTE / ERRO |
| `RAZAO_SOCIAL_COMPARA` | Comparação da razão social |
| `Erro_Consulta_API` | Mensagem de erro da API (se houver) |

---

## Uso — Interface Web (painel)

A aplicação FastAPI serve um **painel web** na raiz (`http://localhost:8000/`), sem dependências adicionais (HTML/CSS/JS puro):

- **Upload com arrastar e soltar** de CSV/XLSX/XLS
- **Barra de progresso em tempo real** do cruzamento (polling a cada 2 s no endpoint de status)
- **Estatísticas do resultado**: total, coincide, divergente e erros
- **Download do CSV** processado com um clique
- **Consulta rápida de um único CNPJ**
- **Indicadores de saúde** (conexão com o banco e token da API) no cabeçalho

O painel consome exclusivamente os endpoints REST existentes — nenhuma mudança de arquitetura foi necessária.

---

## Uso — API REST

### Iniciar o servidor

```bash
uvicorn api.main:app --reload
```

- Painel web: `http://localhost:8000/`
- Documentação interativa: `http://localhost:8000/docs`

### Autenticação (Fase 1 — Segurança)

Todos os endpoints de negócio exigem o header **`X-API-Key`** (apenas `/v1/health` é público).
Gere uma chave forte com:

```bash
python -m api.keygen          # imprime uma chave nova no formato cnpj_xxx
```

Configure via variáveis de ambiente:

| Variável | Padrão | Descrição |
|---|---|---|
| `API_KEYS` | *(vazio)* | Lista separada por vírgula das chaves aceitas |
| `AUTH_REQUIRED` | `true` | Desative apenas em desenvolvimento local |
| `RATE_LIMIT_PER_MIN` | `60` | Requisições/minuto por chave+IP (`0` desativa) |
| `MAX_UPLOAD_SIZE_MB` | `25` | Limite rígido de tamanho de upload |
| `ENVIRONMENT` | `development` | Em `production`, config insegura aborta o startup |

### Endpoints

| Método | Rota | Auth | Descrição |
|---|---|---|---|
| `GET` | `/` | — | Interface web (painel de cruzamento) |
| `GET` | `/v1/health` | público | Status do sistema (DB + token API + auth) |
| `GET` | `/v1/cnpj/{cnpj}` | 🔑 | Consulta e cruzamento de um CNPJ (valida dígitos verificadores) |
| `POST` | `/v1/cruzamento/upload` | 🔑 | Upload de CSV/XLSX para processamento em background |
| `GET` | `/v1/cruzamento/jobs` | 🔑 | Histórico de jobs (persistido — sobrevive a restart) |
| `GET` | `/v1/cruzamento/{job_id}/status` | 🔑 | Status e progresso do job (`progresso_pct`, `processados`) |
| `GET` | `/v1/cruzamento/{job_id}/download` | 🔑 | Download do resultado quando concluído |

Proteções ativas na Fase 1: sanitização anti *path traversal* no nome do arquivo,
leitura em chunks com limite de tamanho (HTTP 413), validação de extensão
(`.csv/.xlsx/.xls`), rate limiting (HTTP 429 + `Retry-After`), verificação oficial
de dígitos verificadores do CNPJ (módulo 11), redação de segredos em logs e
headers de segurança (`X-Content-Type-Options`, `X-Frame-Options`, `Referrer-Policy`).

### Exemplo — consulta unitária

```bash
curl -H "X-API-Key: $API_KEY" http://localhost:8000/v1/cnpj/00000000000191
```

### Exemplo — upload de arquivo

```bash
# 1. Enviar arquivo
curl -X POST http://localhost:8000/v1/cruzamento/upload \
  -H "X-API-Key: $API_KEY" \
  -F "file=@input/meus_cnpjs.csv"
# Retorna: { "job_id": "abc-123", "status": "pending", ... }

# 2. Verificar status
curl -H "X-API-Key: $API_KEY" http://localhost:8000/v1/cruzamento/abc-123/status

# 3. Baixar resultado
curl -O -H "X-API-Key: $API_KEY" http://localhost:8000/v1/cruzamento/abc-123/download
```

---

## Testes automatizados

Suíte permanente em `tests/` (**pytest + FastAPI TestClient**), sem necessidade de
MySQL ou acesso à rede — banco e API externa são mockados:

```bash
pip install -r requirements.txt
python -m pytest            # suíte completa
python -m pytest -v tests/test_security.py   # módulo específico
```

| Arquivo | O que cobre |
|---|---|
| `tests/test_security.py` | Validação oficial de CNPJ (módulo 11), sanitização anti path traversal, rate limiter com relógio fake (determinístico), autenticação, redação de segredos em logs, checagem de config insegura |
| `tests/test_jobs_persistencia.py` | Persistência SQLite, recuperação pós-restart (jobs `processing` viram `error`), throttle de gravação de progresso, listagem |
| `tests/test_api_endpoints.py` | Endpoints reais: 401 sem/chave errada, 422 extensão vazia/DV inválido, 413 tamanho, traversal salvo apenas em `input/`, 404/409 em status/download, health público |
| `tests/test_interface_web.py` | Painel servido em `/`, `/docs`, headers de segurança |
| `tests/test_fase2_pipeline.py` | Pipeline assíncrono (httpx MockTransport): cache de API com TTL e isolamento por token, retries tenacity em 5xx/429 (e ausência de retry em 4xx), deduplicação de CNPJs, consulta DB em lote (1 round-trip) |

### Fase 2 — desempenho no cruzamento em lote

- **Consultas paralelas à API**: o processador agrupa todas as consultas HTTP
  num pipeline `asyncio` com concorrência limitada (`API_CONCURRENCY`, padrão 5)
  usando `httpx.AsyncClient` + `asyncio.Semaphore`.
- **Deduplicação**: um mesmo CNPJ aparece uma única vez por arquivo → 1 chamada.
- **Cache persistente** (`api_cache.db`, SQLite): respostas OK são reaproveitadas
  pelo período de `API_CACHE_TTL_DIAS` (padrão 7; `0` desativa). O token nunca é
  gravado — apenas um hash para invalidar entradas de outra conta. Métricas em
  `GET /v1/cache/stats`.
- **Retries reais**: `max_retries` da configuração agora é aplicado via
  `tenacity` (backoff exponencial + jitter) para falhas de rede/5xx/429; erros
  4xx (token inválido, CNPJ não encontrado) não são repetidos nem cacheados.
- **Batch no MySQL**: a situação cadastral é buscada em UMA query `IN (...)`
  por arquivo (antes: 1 round-trip por linha), com cache por conexão e fallback
  automático para consultas individuais se o lote falhar.

Configuração em `pytest.ini` + `tests/conftest.py` (variáveis de ambiente de teste
isoladas em `.pytest_tmp/`; fixture `cnpjs_validos` gera 200 CNPJs sintéticos com
DVs válidos usando implementação independente do algoritmo).

---

## Uso — Docker

```bash
# Subir app + banco MySQL
docker compose up --build

# Apenas o banco (para usar CLI local)
docker compose up db
```

O banco MySQL é inicializado automaticamente com o schema de `database/schema.sql`.

> Atenção: `docker compose down -v` apaga o volume do banco. Faça backup antes.

---

## Banco de dados

O schema em `database/schema.sql` cria:

- `empresas` — dados básicos da empresa (CNPJ base, razão social, porte)
- `estabelecimentos` — dados do estabelecimento (CNPJ completo, endereço, situação)
- Tabelas de lookup: `natureza_juridica_descricoes`, `cnae_descricoes`, `municipios`
- View `vw_cnpj_completo` — consulta unificada com descrições decodificadas

Os dados do CNPJ (Receita Federal) podem ser obtidos em: https://dados.gov.br/dados/conjuntos-dados/cadastro-nacional-da-pessoa-juridica---cnpj

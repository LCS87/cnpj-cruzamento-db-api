# 📋 Plano de Melhorias — cnpj-cruzamento-db-api

> **Status:** Planejamento (nenhum código alterado ainda)
> **Data:** 21/10/2026
> **Base da análise:** commit `0bd1a8e` — inspeção completa de `api/`, `cnpj_cruzamento/`, `src/`, `database/`, `Dockerfile`, `docker-compose.yml`, `config.yaml`, `.env.example` e `requirements.txt`.

---

## 1. Diagnóstico do estado atual

O projeto está bem organizado em camadas (CLI, API REST, núcleo de processamento, config centralizada), mas apresenta lacunas importantes em **segurança**, **escalabilidade** e **robustez operacional**:

| Área | Situação atual | Risco |
|---|---|---|
| Segurança | Nenhum endpoint exige autenticação | 🔴 Crítico |
| Segurança | Token da API enviado via query string (`?token=`) | 🔴 Alto |
| Segurança | CORS liberado (`allow_origins=["*"]`) | 🟠 Médio |
| Segurança | Upload sem validação de tamanho/conteúdo + path traversal | 🔴 Alto |
| Config | `DB_PASSWORD` usado como root e compartilhado com o app | 🟠 Médio |
| Escalabilidade | Jobs em memória (`dict`) — perdidos no restart, sem persistência | 🔴 Alto |
| Escalabilidade | Chamadas à API externa sequenciais (1 por CNPJ) | 🟠 Médio |
| Confiabilidade | Retry configurado mas não implementado; sem circuit breaker | 🟠 Médio |
| Qualidade | Zero testes automatizados, sem CI/CD, sem versionamento de deps | 🔴 Alto |
| Interface | Sem frontend; docs da API sem guia de autenticação | 🟡 Baixo |

---

## 2. 🔐 Segurança (prioridade máxima)

### 2.1 Autenticação na API
- **API Keys** para acesso aos endpoints `/v1/*`: header `X-API-Key`, armazenadas com hash (bcrypt/SHA-256) em tabela `api_keys` no MySQL ou em config.
- Implementar com dependency do FastAPI (`Depends(verify_api_key)`) + middleware para rate limiting por chave.
- **Alternativa futura:** OAuth2/JWT se houver múltiplos usuários com perfis.
- Proteger rotas sensíveis: upload, download e consulta unitária. `/health` pode ficar público (sem dados sensíveis).

### 2.2 Tratamento de segredos
- **Token Invertexto no header** (`Authorization: Bearer <token>`) se a API suportar; caso contrário, mover para header custom e nunca logar URLs com token. Hoje o token vai na query string e pode vazar em logs/proxies.
- Redação de segredos no logging (filtro que mascara `token=...`, senhas).
- Adicionar `APP_SECRET_KEY` obrigatória ao `.env.example`; validar no startup se valores padrão ("sua_senha_aqui") estão em uso → recusar subir em produção (`ENVIRONMENT=production`).
- Criar usuário MySQL dedicado (não root) apenas com `SELECT` nas tabelas necessárias; separar `MYSQL_ROOT_PASSWORD` de `DB_PASSWORD` no compose.

### 2.3 Proteção de uploads
- Validar extensão **e** conteúdo (magic bytes / leitura real com pandas).
- Sanitizar nome do arquivo (`werkzeug.secure_filename` ou regex) — hoje `file.filename` é concatenado direto no caminho → **path traversal** possível (ex.: `../../etc/cron.d/x.csv`).
- Limitar tamanho (ex.: 25 MB) via `MAX_UPLOAD_SIZE` e streaming em chunks.
- Servir download com `Content-Disposition` correto e validar que o caminho resolvido está dentro de `OUTPUT_FOLDER` (`Path.resolve()` + verificação de prefixo).

### 2.4 CORS e headers
- CORS com lista de origens configurável via env (`CORS_ORIGINS=https://app.minhaempresa.com`).
- Headers de segurança via middleware: `X-Content-Type-Options`, `X-Frame-Options`, `Referrer-Policy`, HSTS quando atrás de TLS.

### 2.5 Rate limiting e custo de API
- Rate limiter no backend (slowapi/redis): ex. 60 req/min por API key.
- **Cache de resultados de API** (tabela `api_cache` com TTL, ex. 7 dias) — evita gastar créditos Invertexto reconsultando o mesmo CNPJ e reduz tempo de cruzamento.
- Quota diária configurável por chave + métrica de chamadas externas por job.

### 2.6 Infraestrutura
- Dockerfile: user não-root (`USER appuser`), imagem `slim` atualizada, healthcheck, sem `COPY . .` de arquivos sensíveis (garantir `.dockerignore` com `.env`, `.git`, `input/`, `output_cruzamento/`).
- Expor MySQL apenas internamente (remover `ports: 3306:3306` do compose em produção ou vincular a `127.0.0.1`).
- TLS/reverse proxy (nginx/Caddy/Traefik) documentado.
- Validação de entrada reforçada: Pydantic em todos os parâmetros, validação de **dígitos verificadores do CNPJ** (hoje `validar_cnpj` só checa 14 dígitos e repetição — implementar algoritmo oficial módulo 11).

---

## 3. ⚙️ Tecnologias a acrescentar

### 3.1 Robustez do processamento
| Tecnologia | Para quê | Impacto |
|---|---|---|
| **Celery + Redis** | Fila real de jobs (substitui `BackgroundTasks` + dict em memória) | Jobs sobrevivem a restart, múltiplos workers, retries |
| **PostgreSQL** (opção) ou pool no MySQL (`SQLAlchemy` + `connection pooling`) | Conexões eficientes; hoje abre/fecha conexão por request/job | Performance e estabilidade |
| **httpx** (async) + `asyncio.gather` com concorrência limitada | Consultas paralelas à API (ex. 5–10 simultâneas) | Cruzamento de 10k CNPJs: horas → minutos |
| **tenacity** | Retries exponenciais com backoff/jitter (a config `max_retries` existe mas não é usada) | Resiliência a falhas transitórias |
| **Redis** | Cache de consultas, rate limiting, estado de jobs | Reduz custo de API e habilita progresso em tempo real |
| **Arrow/Polars** (opcional) | Leitura de arquivos grandes (CSV da Receita tem GBs) | Processamento de datasets completos |

### 3.2 Observabilidade
- **Prometheus + Grafana**: métricas (`/metrics` com `prometheus-fastapi-instrumentator`), contadores por status final, histograma de latência da API externa.
- **Sentry** (ou similar) para captura de erros em produção.
- **Log estruturado JSON** (`structlog`) com correlation-id por requisição/job.
- Progresso de jobs em tempo real: campo `progresso` (% concluído) no status do job (persistido em Redis/BD).

### 3.3 Qualidade e DevEx
- **pytest + pytest-asyncio + respx** (mock de HTTP) + cobertura (`pytest-cov`).
- **ruff** (lint+format) e **mypy** (type checking) pré-commit.
- **pre-commit** hooks.
- **GitHub Actions**: lint → type check → testes → build Docker → scan de imagens (Trivy) → publish.
- **Dependabot/Renovate** para atualização de dependências.
- `uv` ou `pip-tools` com lockfile (`requirements.lock` / `uv.lock`) — deps atuais usam `>=`, o que quebra reprodutibilidade.
- **Alembic/Flyway** para migrações versionadas do schema (hoje só `schema.sql` inicial).

### 3.4 Interface (frontend)
- **Fase 1 (rápida):** UI web leve servida pelo próprio FastAPI — HTML + Tailwind/Alpine.js ou **Gradio**: upload de arquivo, barra de progresso do job, dashboard de resultados (tabela filtrável COINCIDE/DIVERGENTE/ERRO) e botão de download.
- **Fase 2:** SPA em **React + Vite + TypeScript** com:
  - Login por API key
  - Upload drag-and-drop com validação de colunas antes de enviar
  - Tela de jobs (histórico persistente em BD)
  - Dashboard com gráficos (Chart.js/ECharts): distribuição de situações, divergências por UF, consumo de créditos
  - Consulta unitária de CNPJ com card de resultado formatado
- **Extras:** versão OpenAPI exportada + Postman collection; CLI moderna com **Typer/Rich** (tabelas, spinner de progresso).

---

## 4. 🧩 Correções e melhorias de código identificadas na análise

1. **`jobs.py`**: estado em memória → migrar para Redis/MySQL; adicionar timestamps (`created_at`, `finished_at`) e histórico.
2. **`api/routes/cruzamento.py`**: `_executar_job` não trata exceção de `processar_arquivo` → job fica preso em `processing` se lançar erro fora do esperado (envolver em try/except amplo).
3. **`db.py`**: `tabela`/colunas interpoladas em f-string no SQL — embora venham de config, validar contra whitelist de identificadores.
4. **`utils.validar_cnpj`**: implementar cálculo real dos dígitos verificadores.
5. **`config.py`**: duplicação de padrões entre `.env` e `config.yaml` (timeout, retries definidos nos dois) — definir hierarquia clara (env > yaml > default) e documentar.
6. **`processor.py`**: deduplicar CNPJs antes de consultar API (hoje repete chamadas p/ linhas duplicadas); aproveitar e consolidar lógica de comparação que está duplicada em `routes/cnpj.py`.
7. **Dockerfile**: instalar deps uma única vez (fastapi/uvicorn aparecem duas vezes: `requirements.txt` + `RUN pip install` separado).
8. **README**: atualizar conforme seção 6.

---

## 5. 🗺️ Roadmap sugerido

### Fase 1 — Segurança & Quick wins (1–2 semanas)
- [x] API Key + rate limiting + CORS configurável — `src/security.py` (X-API-Key com hash SHA-256 + comparação constant-time; limiter de janela deslizante; `CORS_ORIGINS` por env)
- [x] Fix path traversal/upload validation/tamanho máximo — `sanitize_filename` + `validar_caminho_dentro_da_pasta` + leitura em chunks com HTTP 413
- [x] Redação de segredos em logs (`RedacaoSegredosFilter`) + token nunca exposto em respostas (health mostra apenas "configurado/ausente")
- [ ] Usuário MySQL não-root + `.dockerignore` + container não-root
- [x] Validação de dígitos CNPJ (módulo 11 oficial) + try/except no worker de job (jobs não ficam presos em "processing")
- [x] Deduplicação de CNPJs + cache em SQLite (`cnpj_cruzamento/cache.py`, TTL configurável, isolamento por hash de token) — Fase 2
- [ ] Lockfile de dependências

### Fase 2 — Arquitetura & Escala (2–4 semanas)
- [ ] Celery + Redis (jobs persistentes, workers, retries)
- [x] httpx async com concorrência limitada (`API_CONCURRENCY`) + tenacity (backoff+jitter; 4xx sem retry) — Fase 2
- [x] Batch de consultas MySQL (query `IN` única por arquivo + cache por conexão) — Fase 2
- [ ] Connection pooling (SQLAlchemy)
- [ ] Migrações (Alembic) + tabela de histórico de jobs
- [x] Testes (unit + integração) — suíte pytest permanente em `tests/` (68 casos: segurança, persistência, endpoints, interface web); [ ] ruff/mypy/pre-commit
- [ ] CI/CD GitHub Actions + Trivy

### Fase 3 — Observabilidade & Interface (4–6 semanas)
- [x] `/metrics` Prometheus protegido por API key (`api/routes/metrics.py`) + middleware de latência/contadores com labels normalizados — Fase 3
- [ ] Dashboards Grafana (infra externa; documentação de scrape no README)
- [x] Logs estruturados (`LOG_FORMAT=json`), correlation-id `X-Request-ID` em toda requisição/log, Sentry opcional via `SENTRY_DSN` — Fase 3
- [x] Progresso de job em tempo real (polling) — implementado via callback no processor + campos `processados`/`progresso_pct` no `JobResponse`
- [x] UI web — **Opção A entregue**: painel HTML/CSS/JS servido pelo FastAPI em `/` (`api/web/`), com upload drag&drop, barra de progresso, estatísticas, download e consulta rápida de CNPJ
- [ ] UI web Fase 2 (React SPA) — futuro
- [ ] Export de relatórios (XLSX formatado / PDF resumido)

### Fase 4 — Futuro
- [ ] Multi-fontes de API (Invertexto + BrasilAPI + Receita WS) com fallback
- [ ] Webhooks ao concluir jobs
- [ ] Multi-tenant com cotas por cliente
- [ ] Carga dos microdados da Receita (datasets oficiais completos) com Polars

---

## 6. 📖 Atualização do README (escopo aprovado)

Expandir o README para um documento completo, adicionando:

1. **Badges** (licença, Python, FastAPI, Docker, CI)
2. **Sumário navegável**
3. **Descrição estendida + arquitetura** (diagrama Mermaid do fluxo CLI/API → DB → Invertexto)
4. **Tabela de tecnologias** com justificativa de cada escolha
5. **Pré-requisitos** detalhados
6. **Instalação**: local, Docker, e "somente banco" — passo a passo
7. **Configuração completa**: todas as variáveis de `.env` e `config.yaml` em tabelas explicadas + hierarquia de precedência
8. **Guia de primeiros passos** (quick start de 3 comandos)
9. **Uso CLI** expandido + exemplos de formatos de entrada aceitos
10. **Uso da API**: autenticação (nova), todos os endpoints com exemplos de request/response em JSON, códigos de erro
11. **Upload e carga dos dados da Receita Federal** no MySQL (scripts/instruções)
12. **Modelo de dados**: diagrama das tabelas + view
13. **Segurança**: como gerar API keys, boas práticas de deploy (TLS, backups, volumes)
14. **Testes, lint e CI**: como rodar localmente
15. **Troubleshooting** (FAQ de erros comuns: conexão DB, token inválido, coluna não encontrada, encoding)
16. **Contributing** + padrão de branches + Gitmoji/conventional commits
17. **Changelog** + versionamento (SemVer)
18. **Licença**

---

## 7. Perguntas para decidir antes de executar

1. Deploy alvo: servidor próprio, cloud (AWS/GCP/Azure) ou on-premise corporativo? (define TLS, secrets manager e observabilidade)
2. Volume típico de arquivos: dezenas, milhares ou centenas de milhares de CNPJs por lote? (define urgência de Celery/paralelismo)
3. A API Invertexto aceita o token via header, ou somente query string? (define mitigação de vazamento)
4. Frontend: prioridade já, ou primeiro segurança/robustez?
5. Posso manter MySQL ou há margem para migrar para PostgreSQL?

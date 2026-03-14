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
    health.py         ← GET /health
    cnpj.py           ← GET /v1/cnpj/{cnpj}
    cruzamento.py     ← POST/GET /v1/cruzamento/...
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

## Uso — API REST

### Iniciar o servidor

```bash
uvicorn api.main:app --reload
```

Acesse a documentação interativa em: `http://localhost:8000/docs`

### Endpoints

| Método | Rota | Descrição |
|---|---|---|
| `GET` | `/health` | Status do sistema (DB + token API) |
| `GET` | `/v1/cnpj/{cnpj}` | Consulta e cruzamento de um CNPJ |
| `POST` | `/v1/cruzamento/upload` | Upload de CSV/XLSX para processamento em background |
| `GET` | `/v1/cruzamento/{job_id}/status` | Status do job de processamento |
| `GET` | `/v1/cruzamento/{job_id}/download` | Download do resultado quando concluído |

### Exemplo — consulta unitária

```bash
curl http://localhost:8000/v1/cnpj/00000000000191
```

### Exemplo — upload de arquivo

```bash
# 1. Enviar arquivo
curl -X POST http://localhost:8000/v1/cruzamento/upload \
  -F "file=@input/meus_cnpjs.csv"
# Retorna: { "job_id": "abc-123", "status": "pending", ... }

# 2. Verificar status
curl http://localhost:8000/v1/cruzamento/abc-123/status

# 3. Baixar resultado
curl -O http://localhost:8000/v1/cruzamento/abc-123/download
```

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

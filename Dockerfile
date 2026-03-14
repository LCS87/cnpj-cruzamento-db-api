FROM python:3.12-slim

WORKDIR /app

# Instalar dependências do sistema para mysql-connector
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-libmysqlclient-dev gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Instalar uvicorn para servir a API
RUN pip install --no-cache-dir "fastapi>=0.110.0" "uvicorn[standard]>=0.29.0" "python-multipart>=0.0.9"

COPY . .

# Criar pastas de entrada/saída
RUN mkdir -p input output_cruzamento

EXPOSE 8000

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]

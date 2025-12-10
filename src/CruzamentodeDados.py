import os
from glob import glob
import pandas as pd
import mysql.connector
from mysql.connector import Error
import csv
import requests
import re
import time  # <<< Import necessário para a medição de tempo

# ---------------- CONFIGURAÇÃO DE AMBIENTE ----------------
DB_CONFIG = {
    'host': 'localhost',
    'database': 'cnpj_db',
    'user': 'root',
    'password': ''
}

# ---------------- CONFIGURAÇÃO DA API ----------------
TOKEN_API = ""
BASE_URL = "https://api.invertexto.com/v1/cnpj/"

# ---------------- CONFIGURAÇÃO DE PASTAS E COLUNAS ----------------
PASTA_ENTRADA = 'input'
PASTA_SAIDA = 'output_cruzamento'
TABELA_CONSULTA = 'estabelecimentos'

# Colunas de Resultados (DB Local)
COLUNA_SITUACAO = 'situacao_cadastral'
COLUNA_MOTIVO = 'motivo_situacao_cadastral'
COLUNA_STATUS_DB = 'Status_CNPJ_DB'
COLUNA_MOTIVO_DB = 'Motivo_Detalhado_DB'

# Colunas de Resultados (API Externa)
COLUNA_STATUS_API = 'Status_CNPJ_API'
COLUNA_RAZAO_API = 'razao_social_API'
COLUNA_FANTASIA_API = 'nome_fantasia_API'
COLUNA_ERRO_API = 'Erro_Consulta_API'

# Colunas de Comparação (Cruzamento)
COLUNA_STATUS_COMPARA = 'STATUS_CADASTRO_FINAL'
COLUNA_RAZAO_COMPARA = 'RAZAO_SOCIAL_COMPARA'

# Mapeamentos para decodificação do DB (Mantenha o MOTIVO_MAP completo aqui)
SITUACAO_MAP = {
    '1': 'NULA', '2': 'ATIVA', '3': 'SUSPENSA', '4': 'INAPTA', '8': 'BAIXADA'
}
MOTIVO_MAP = {
    '00': "SEM MOTIVO", '01': "EXTINCAO POR ENCERRAMENTO LIQUIDACAO VOLUNTARIA",
    # ... (restante do MOTIVO_MAP) ...
}


# ---------------- UTILITÁRIOS (Sem Alterações) ----------------
def formatar_cnpj(cnpj_valor):
    """Limpa e formata o CNPJ para 14 dígitos."""
    if pd.isna(cnpj_valor): return None
    cnpj_str = str(cnpj_valor)
    cnpj_limpo = re.sub(r'\D', '', cnpj_str)
    return cnpj_limpo.zfill(14) if len(cnpj_limpo) == 14 else None


def get_db_connection():
    """Tenta conectar ao banco de dados MySQL."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        if conn.is_connected():
            print("Conexão com o banco de dados estabelecida com sucesso.")
            return conn
    except Error as e:
        print(f"❌ Erro ao conectar ao MySQL: {e}")
        return None
    return None


def identificar_coluna_cnpj(df):
    """Procura a coluna de CNPJ no DataFrame, aceitando variações comuns."""
    variaveis_cnpj = ['cnpj', 'cnpj_completo', 'cnpj_base', 'cnpj_num', 'documento']
    for col in df.columns:
        col_limpo = col.lower().replace('_', '').strip()
        if col_limpo in variaveis_cnpj:
            return col
    if 'cnpj_completo' in df.columns:
        return 'cnpj_completo'
    return None


# ---------------- CONSULTAS (Sem Alterações) ----------------
def consultar_situacao_db(conn, cnpj_limpo):
    # Lógica de consulta ao DB (mantida)
    cursor = conn.cursor()
    cnpj_basico = cnpj_limpo[:8]
    cnpj_ordem = cnpj_limpo[8:12]
    cnpj_dv = cnpj_limpo[12:]
    query = f"""
    SELECT {COLUNA_SITUACAO}, {COLUNA_MOTIVO}
    FROM {TABELA_CONSULTA}
    WHERE cnpj_basico = %s
      AND cnpj_ordem = %s
      AND cnpj_dv = %s
    """
    try:
        cursor.execute(query, (cnpj_basico, cnpj_ordem, cnpj_dv))
        resultado = cursor.fetchone()
        if resultado:
            codigo_situacao = str(resultado[0]).strip() if resultado[0] is not None else ''
            codigo_motivo = str(resultado[1]).strip() if resultado[1] is not None else ''
            situacao_texto = SITUACAO_MAP.get(codigo_situacao, f'CÓDIGO {codigo_situacao} DESCONHECIDO')
            motivo_texto = MOTIVO_MAP.get(codigo_motivo,
                                          f'CÓDIGO {codigo_motivo} DESCONHECIDO' if codigo_motivo else 'N/A')
            return situacao_texto, motivo_texto
        else:
            return "NÃO ENCONTRADO NO DB", "N/A"
    except Error as e:
        return "ERRO DE CONSULTA DB", f"VERIFICAR ERRO SQL: {e}"
    finally:
        cursor.close()


def consultar_cnpj_api(cnpj_limpo: str) -> dict:
    # Lógica de consulta à API (mantida)
    url_completa = f"{BASE_URL}{cnpj_limpo}?token={TOKEN_API}"
    try:
        response = requests.get(url_completa, timeout=15)
        response.raise_for_status()
        dados = response.json()
        situacao = dados.get('situacao', {})
        resultado = {
            COLUNA_ERRO_API: "OK",
            COLUNA_STATUS_API: situacao.get('nome', 'NÃO INFORMADO').upper(),
            COLUNA_RAZAO_API: dados.get('razao_social', ''),
            COLUNA_FANTASIA_API: dados.get('nome_fantasia', ''),
        }
        return resultado
    except requests.exceptions.HTTPError as errh:
        try:
            erro_json = response.json()
            msg = erro_json.get('message', f"Erro HTTP {response.status_code}")
        except:
            msg = f"Erro HTTP: {errh}. Status: {response.status_code}"
        return {COLUNA_ERRO_API: msg}
    except requests.exceptions.RequestException as err:
        return {COLUNA_ERRO_API: f"Erro de Conexão: {err}"}
    except Exception as e:
        return {COLUNA_ERRO_API: f"Erro Inesperado: {e}"}


# ---------------- PROCESSAMENTO E CRUZAMENTO (ATUALIZADO) ----------------
def processar_e_cruzar_arquivo(caminho_arquivo, conn):
    nome_arquivo = os.path.basename(caminho_arquivo)
    print(f"\n--- Processando e Cruzando: {nome_arquivo} ---")

    extensao = nome_arquivo.split('.')[-1].lower()
    # Leitura do arquivo (mantida)
    try:
        if extensao == 'csv':
            testes = [{'sep': ';', 'encoding': 'utf-8'}, {'sep': ',', 'encoding': 'utf-8'}]
            df = None
            for params in testes:
                try:
                    df = pd.read_csv(caminho_arquivo, dtype=str, **params)
                    if len(df) > 0: break
                except:
                    continue
            if df is None or len(df.columns) <= 1:
                raise Exception("Não foi possível ler o CSV.")
        elif extensao in ['xlsx', 'xls']:
            df = pd.read_excel(caminho_arquivo, dtype=str, engine='openpyxl')
        else:
            print(f"⚠️ A extensão '{extensao}' não é suportada. Pulando.")
            return
    except Exception as e:
        print(f"❌ ERRO ao ler o arquivo {nome_arquivo}: {e}")
        return

    coluna_cnpj_real = identificar_coluna_cnpj(df)
    if not coluna_cnpj_real:
        print(f"❌ ERRO: Nenhuma coluna de CNPJ foi identificada no arquivo.")
        return

    print(f"  > Coluna CNPJ identificada como: '{coluna_cnpj_real}'")
    total_cnpjs = len(df)
    print(f"Total de {total_cnpjs} registros para processar.")

    # Inicializar Colunas de Resultado (mantida)
    colunas_resultado = [
        COLUNA_STATUS_DB, COLUNA_MOTIVO_DB,
        COLUNA_STATUS_API, COLUNA_RAZAO_API, COLUNA_FANTASIA_API,
        COLUNA_STATUS_COMPARA, COLUNA_RAZAO_COMPARA, COLUNA_ERRO_API
    ]
    for c in colunas_resultado:
        if c not in df.columns:
            df[c] = ''

    df['_cnpj_limpo'] = df[coluna_cnpj_real].apply(formatar_cnpj)

    # --- INÍCIO DA MEDIÇÃO DE TEMPO E CONTADORES ---
    inicio_arquivo = time.time()
    total_por_status = {
        'coincide': 0,
        'divergente': 0,
        'erro': 0
    }

    # ---------------- Lógica de Cruzamento ----------------
    for index, row in df.iterrows():
        cnpj_limpo = row['_cnpj_limpo']

        # --- PROGRESOS E ESTIMATIVA DE TEMPO ---
        if (index + 1) % 10 == 0 or (index + 1) == total_cnpjs:  # Reduzindo para 10 para feedback mais rápido
            tempo_decorrido = time.time() - inicio_arquivo
            velocidade = (index + 1) / tempo_decorrido if tempo_decorrido > 0 else 0
            tempo_estimado = (total_cnpjs - (index + 1)) / velocidade if velocidade > 0 else 0

            # Exibe o progresso
            print(f"  > Progresso: {index + 1}/{total_cnpjs} ({((index + 1) / total_cnpjs) * 100:.1f}%) | "
                  f"Velocidade: {velocidade:.1f} CNPJs/s | "
                  f"Tempo restante: {tempo_estimado / 60:.1f} min", end='\r')

        if not cnpj_limpo:
            df.at[index, COLUNA_STATUS_DB] = 'CNPJ INVÁLIDO'
            df.at[index, COLUNA_STATUS_COMPARA] = 'ERRO DE FORMATO'
            total_por_status['erro'] += 1
            continue

        # A. Consulta DB Local
        status_db, motivo_db = consultar_situacao_db(conn, cnpj_limpo)

        # B. Consulta API Externa
        resultado_api = consultar_cnpj_api(cnpj_limpo)
        status_api = resultado_api.get(COLUNA_STATUS_API, 'ERRO NA API').upper()
        razao_social_api = resultado_api.get(COLUNA_RAZAO_API, '')

        # C. Preencher Resultados
        df.at[index, COLUNA_STATUS_DB] = status_db
        df.at[index, COLUNA_MOTIVO_DB] = motivo_db
        df.at[index, COLUNA_STATUS_API] = status_api
        df.at[index, COLUNA_RAZAO_API] = razao_social_api
        df.at[index, COLUNA_FANTASIA_API] = resultado_api.get(COLUNA_FANTASIA_API, '')
        df.at[index, COLUNA_ERRO_API] = resultado_api.get(COLUNA_ERRO_API, 'ERRO GERAL')

        # D. Lógica de Comparação

        # 1. Comparação de Status
        if status_db.upper() == status_api.upper():
            status_comparacao = "COINCIDE"
            total_por_status['coincide'] += 1
        elif "ERRO" in status_db.upper() or "ERRO" in status_api.upper() or "NÃO ENCONTRADO" in status_db.upper() or "ERRO" in resultado_api.get(
                COLUNA_ERRO_API, '').upper():
            status_comparacao = "ERRO NA CONSULTA"
            total_por_status['erro'] += 1
        else:
            status_comparacao = "**DIVERGENTE**"
            total_por_status['divergente'] += 1

        df.at[index, COLUNA_STATUS_COMPARA] = status_comparacao

        # 2. Comparação de Razão Social
        razao_social_arquivo = row.get('razao_social')
        if razao_social_arquivo and razao_social_api:
            if re.sub(r'\W+', '', razao_social_arquivo.lower()) == re.sub(r'\W+', '', razao_social_api.lower()):
                razao_comparacao = "COINCIDE"
            else:
                razao_comparacao = "DIVERGENTE"
        else:
            razao_comparacao = "N/A OU DADO FALTANDO"

        df.at[index, COLUNA_RAZAO_COMPARA] = razao_comparacao

    # --- FIM DA MEDIÇÃO DE TEMPO E EXIBIÇÃO DE ESTATÍSTICAS ---
    tempo_total = time.time() - inicio_arquivo

    print("\n  > Cruzamento de todas as consultas concluído.")

    print(f"\n  📊 Estatísticas do Arquivo:")
    print(
        f"    - Coincidências: {total_por_status['coincide']} ({total_por_status['coincide'] / total_cnpjs * 100:.1f}%)")
    print(
        f"    - Divergências: {total_por_status['divergente']} ({total_por_status['divergente'] / total_cnpjs * 100:.1f}%)")
    print(
        f"    - Erros de Consulta/Formato: {total_por_status['erro']} ({total_por_status['erro'] / total_cnpjs * 100:.1f}%)")
    print(f"  ⏱️ Tempo total de processamento: {tempo_total:.2f} segundos ({tempo_total / 60:.1f} minutos)")

    # ---------------- Salvamento (Mantido) ----------------
    df = df.drop(columns=['_cnpj_limpo'], errors='ignore')

    col_finais = [c for c in df.columns if c not in colunas_resultado] + colunas_resultado
    df = df.reindex(columns=col_finais)

    os.makedirs(PASTA_SAIDA, exist_ok=True)
    nome_base = os.path.splitext(nome_arquivo)[0]
    caminho_saida = os.path.join(PASTA_SAIDA, f"{nome_base}_CRUZAMENTO.csv")

    try:
        df.to_csv(caminho_saida, sep=';', index=False, encoding='utf-8', quoting=csv.QUOTE_MINIMAL)
        print(f"✅ Arquivo de resultados salvo em: {caminho_saida}")
    except PermissionError:
        print(f"❌ ERRO: Permissão negada ao salvar '{caminho_saida}'. Feche o arquivo.")
    except Exception as e:
        print(f"❌ ERRO ao salvar o arquivo de saída: {e}")


# ---------------- ORQUESTRAÇÃO GERAL (Mantida) ----------------
def processar_todos_arquivos_cruzamento():
    os.makedirs(PASTA_ENTRADA, exist_ok=True)
    os.makedirs(PASTA_SAIDA, exist_ok=True)

    conn = get_db_connection()
    if not conn:
        print("Não foi possível prosseguir sem conexão com o banco de dados.")
        return

    padroes_busca = [
        os.path.join(PASTA_ENTRADA, '*.csv'),
        os.path.join(PASTA_ENTRADA, '*.xlsx'),
        os.path.join(PASTA_ENTRADA, '*.xls')
    ]

    lista_arquivos = []
    for padrao in padroes_busca:
        lista_arquivos.extend(glob(padrao))

    if not lista_arquivos:
        print(f"⚠️ Nenhum arquivo encontrado na pasta '{PASTA_ENTRADA}'.")
    else:
        print(f"✨ Encontrados {len(lista_arquivos)} arquivos para cruzamento.")
        for arquivo in lista_arquivos:
            processar_e_cruzar_arquivo(arquivo, conn)

    conn.close()
    print("\n--- FIM DO PROCESSO DE CRUZAMENTO ---")


# ---------------- EXECUÇÃO ----------------
if __name__ == "__main__":
    processar_todos_arquivos_cruzamento()
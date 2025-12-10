import os
from glob import glob
import pandas as pd
import mysql.connector
from mysql.connector import Error
import csv
import requests
import re
import time
from datetime import timedelta

# ---------------- CONFIGURAÇÃO DE AMBIENTE ----------------
DB_CONFIG = {
    'host': 'localhost',
    'database': 'cnpj_db',
    'user': 'root',
    'password': ''  # <<< VERIFIQUE SUA SENHA
}

# ---------------- CONFIGURAÇÃO DA API ----------------
TOKEN_API = ""  # <<< SUBSTITUA PELO SEU TOKEN REAL AQUI!
BASE_URL = "https://api.invertexto.com/v1/cnpj/"

# ---------------- CONFIGURAÇÃO DE PASTAS E COLUNAS ----------------
PASTA_ENTRADA = 'input'
PASTA_SAIDA = 'output_cruzamento'
TABELA_CONSULTA = 'estabelecimentos'  # Tabela principal do CNPJ no seu DB

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

# Mapeamentos para decodificação do DB (mantidos do seu código original)
SITUACAO_MAP = {
    '1': 'NULA', '2': 'ATIVA', '3': 'SUSPENSA', '4': 'INAPTA', '8': 'BAIXADA'
}
MOTIVO_MAP = {
    '00': "SEM MOTIVO", '01': "EXTINCAO POR ENCERRAMENTO LIQUIDACAO VOLUNTARIA",
    # ... (Mantenha o MOTIVO_MAP completo aqui, omitido por brevidade) ...
}


# ---------------- UTILITÁRIOS ----------------
def formatar_cnpj(cnpj_valor):
    """Limpa e formata o CNPJ para 14 dígitos."""
    if pd.isna(cnpj_valor):
        return None
    cnpj_str = str(cnpj_valor)
    cnpj_limpo = re.sub(r'\D', '', cnpj_str)
    return cnpj_limpo.zfill(14) if len(cnpj_limpo) == 14 else None


def get_db_connection():
    """Tenta conectar ao banco de dados MySQL."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        if conn.is_connected():
            print("✅ Conexão com o banco de dados estabelecida com sucesso.")
            return conn
    except Error as e:
        print(f"❌ Erro ao conectar ao MySQL: {e}")
        return None
    return None


def identificar_coluna_cnpj(df):
    """Procura a coluna de CNPJ no DataFrame, aceitando variações comuns."""
    variaveis_cnpj = [
        'cnpj', 'cnpj_completo', 'cnpj_base', 'cnpj_num', 'documento'
    ]
    for col in df.columns:
        # Verifica se o nome da coluna bate com as variações
        col_limpo = col.lower().replace('_', '').strip()
        if col_limpo in variaveis_cnpj:
            return col
    # Se não encontrar, tenta a coluna original esperada
    if 'cnpj_completo' in df.columns:
        return 'cnpj_completo'
    return None


def formatar_tempo(segundos):
    """Formata segundos para string legível."""
    if segundos < 60:
        return f"{segundos:.1f}s"
    elif segundos < 3600:
        return f"{segundos / 60:.1f}min"
    else:
        return f"{segundos / 3600:.1f}h"


# ---------------- CONSULTAS ----------------
def consultar_situacao_db(conn, cnpj_limpo):
    """Consulta situação e motivo no DB local."""
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
    """Consulta status e dados na API Invertexto."""
    url_completa = f"{BASE_URL}{cnpj_limpo}?token={TOKEN_API}"

    try:
        response = requests.get(url_completa, timeout=15)
        response.raise_for_status()
        dados = response.json()

        # Mapeamento dos Dados da API
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


# ---------------- PROCESSAMENTO E CRUZAMENTO ----------------
def processar_e_cruzar_arquivo(caminho_arquivo, conn):
    nome_arquivo = os.path.basename(caminho_arquivo)
    print(f"\n{'=' * 60}")
    print(f"📁 PROCESSANDO E CRUZANDO: {nome_arquivo}")
    print(f"{'=' * 60}")

    # Iniciar contador de tempo total
    inicio_total = time.time()

    extensao = nome_arquivo.split('.')[-1].lower()
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

    # Identificar a coluna CNPJ
    coluna_cnpj_real = identificar_coluna_cnpj(df)
    if not coluna_cnpj_real:
        print(f"❌ ERRO: Nenhuma coluna de CNPJ foi identificada no arquivo. Verifique o cabeçalho.")
        return

    print(f"📊 DADOS DO ARQUIVO:")
    print(f"  • Coluna CNPJ identificada: '{coluna_cnpj_real}'")
    print(f"  • Total de registros: {len(df):,}")
    print(f"  • Colunas encontradas: {len(df.columns)}")

    # Inicializar Colunas de Resultado
    colunas_resultado = [
        COLUNA_STATUS_DB, COLUNA_MOTIVO_DB,
        COLUNA_STATUS_API, COLUNA_RAZAO_API, COLUNA_FANTASIA_API,
        COLUNA_STATUS_COMPARA, COLUNA_RAZAO_COMPARA, COLUNA_ERRO_API
    ]
    for c in colunas_resultado:
        if c not in df.columns:
            df[c] = ''

    # Limpeza/normalização do CNPJ
    print(f"\n🔧 ETAPA 1: Normalizando CNPJs...")
    inicio_normalizacao = time.time()
    df['_cnpj_limpo'] = df[coluna_cnpj_real].apply(formatar_cnpj)

    # Verificar CNPJs válidos
    cnpjs_validos = df['_cnpj_limpo'].notna().sum()
    cnpjs_invalidos = len(df) - cnpjs_validos
    tempo_normalizacao = time.time() - inicio_normalizacao
    print(f"  • CNPJs válidos: {cnpjs_validos:,}")
    print(f"  • CNPJs inválidos: {cnpjs_invalidos:,}")
    print(f"  • Tempo de normalização: {formatar_tempo(tempo_normalizacao)}")

    # Inicializar contadores para estatísticas
    estatisticas = {
        'coincide_status': 0,
        'divergente_status': 0,
        'erro_consulta': 0,
        'nao_encontrado_db': 0,
        'coincide_razao': 0,
        'divergente_razao': 0,
        'erro_api': 0,
        'sucesso_api': 0,
        'sucesso_db': 0
    }

    # ---------------- Lógica de Cruzamento ----------------
    total_cnpjs = len(df)
    print(f"\n🚀 ETAPA 2: Iniciando consultas cruzadas...")
    print(f"{'-' * 40}")
    print(f"Fonte 1: Banco de Dados Local")
    print(f"Fonte 2: API Externa (Invertexto)")
    print(f"Processo: Comparação de Status e Razão Social")
    print(f"{'-' * 40}")

    inicio_consultas = time.time()
    ultimo_tempo = inicio_consultas

    for index, row in df.iterrows():
        cnpj_limpo = row['_cnpj_limpo']

        # Atualizar progresso a cada 10 registros ou a cada 2 segundos
        tempo_atual = time.time()
        if (index + 1) % 10 == 0 or (tempo_atual - ultimo_tempo) >= 2 or (index + 1) == total_cnpjs:
            percentual = (index + 1) / total_cnpjs * 100
            tempo_decorrido = tempo_atual - inicio_consultas
            velocidade = (index + 1) / tempo_decorrido if tempo_decorrido > 0 else 0
            tempo_restante = (total_cnpjs - (index + 1)) / velocidade if velocidade > 0 else 0

            # Mostrar progresso detalhado
            print(f"  📈 Progresso: {index + 1:,}/{total_cnpjs:,} "
                  f"({percentual:.1f}%) | "
                  f"Velocidade: {velocidade:.1f} CNPJs/s | "
                  f"Tempo restante: {formatar_tempo(tempo_restante)} | "
                  f"Coincidências: {estatisticas['coincide_status']}", end='\r')
            ultimo_tempo = tempo_atual

        if not cnpj_limpo:
            df.at[index, COLUNA_STATUS_DB] = 'CNPJ INVÁLIDO'
            df.at[index, COLUNA_STATUS_COMPARA] = 'ERRO DE FORMATO'
            estatisticas['erro_consulta'] += 1
            continue

        # A. Consulta DB Local
        inicio_db = time.time()
        status_db, motivo_db = consultar_situacao_db(conn, cnpj_limpo)
        tempo_db = time.time() - inicio_db

        if "ERRO" not in status_db and "NÃO ENCONTRADO" not in status_db:
            estatisticas['sucesso_db'] += 1
        elif "NÃO ENCONTRADO" in status_db:
            estatisticas['nao_encontrado_db'] += 1

        # B. Consulta API Externa
        inicio_api = time.time()
        resultado_api = consultar_cnpj_api(cnpj_limpo)
        tempo_api = time.time() - inicio_api

        status_api = resultado_api.get(COLUNA_STATUS_API, 'ERRO NA API').upper()
        razao_social_api = resultado_api.get(COLUNA_RAZAO_API, '')

        if resultado_api.get(COLUNA_ERRO_API) == "OK":
            estatisticas['sucesso_api'] += 1
        else:
            estatisticas['erro_api'] += 1

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
            estatisticas['coincide_status'] += 1
        elif "ERRO" in status_db.upper() or "ERRO" in status_api.upper() or "NÃO ENCONTRADO" in status_db.upper():
            status_comparacao = "ERRO NA CONSULTA"
            estatisticas['erro_consulta'] += 1
        else:
            status_comparacao = "**DIVERGENTE**"
            estatisticas['divergente_status'] += 1

        df.at[index, COLUNA_STATUS_COMPARA] = status_comparacao

        # 2. Comparação de Razão Social (Assume que a coluna 'razao_social' existe no arquivo original)
        razao_social_arquivo = row.get('razao_social')
        if razao_social_arquivo and razao_social_api:
            # Compara se a Razão Social da API é similar à do arquivo (removendo espaços e case-sensitive)
            if re.sub(r'\W+', '', razao_social_arquivo.lower()) == re.sub(r'\W+', '', razao_social_api.lower()):
                razao_comparacao = "COINCIDE"
                estatisticas['coincide_razao'] += 1
            else:
                razao_comparacao = "DIVERGENTE"
                estatisticas['divergente_razao'] += 1
        else:
            razao_comparacao = "N/A OU DADO FALTANDO"

        df.at[index, COLUNA_RAZAO_COMPARA] = razao_comparacao

    tempo_total_consultas = time.time() - inicio_consultas
    print("\n" + "=" * 60)
    print(f"✅ CONSULTAS CRUZADAS CONCLUÍDAS")
    print(f"{'=' * 60}")

    # ---------------- EXIBIR ESTATÍSTICAS DETALHADAS ----------------
    print(f"\n📊 ESTATÍSTICAS DETALHADAS:")
    print(f"{'-' * 40}")
    print(f"1. CONSULTAS AO BANCO DE DADOS:")
    print(f"   • Sucesso: {estatisticas['sucesso_db']:,}")
    print(f"   • Não encontrado: {estatisticas['nao_encontrado_db']:,}")
    print(
        f"   • Erros: {estatisticas['total_cnpjs'] - estatisticas['sucesso_db'] - estatisticas['nao_encontrado_db']:,}")

    print(f"\n2. CONSULTAS À API EXTERNA:")
    print(f"   • Sucesso: {estatisticas['sucesso_api']:,}")
    print(f"   • Erros: {estatisticas['erro_api']:,}")

    print(f"\n3. COMPARAÇÃO DE STATUS:")
    print(
        f"   • ✅ Coincidem: {estatisticas['coincide_status']:,} ({(estatisticas['coincide_status'] / total_cnpjs * 100):.1f}%)")
    print(
        f"   • ❌ Divergentes: {estatisticas['divergente_status']:,} ({(estatisticas['divergente_status'] / total_cnpjs * 100):.1f}%)")
    print(
        f"   • ⚠️  Erros na consulta: {estatisticas['erro_consulta']:,} ({(estatisticas['erro_consulta'] / total_cnpjs * 100):.1f}%)")

    print(f"\n4. COMPARAÇÃO DE RAZÃO SOCIAL:")
    print(f"   • ✅ Coincidem: {estatisticas['coincide_razao']:,}")
    print(f"   • ❌ Divergentes: {estatisticas['divergente_razao']:,}")

    print(f"\n⏱️  TEMPOS DE PROCESSAMENTO:")
    print(f"   • Total de consultas: {formatar_tempo(tempo_total_consultas)}")
    print(f"   • Média por CNPJ: {tempo_total_consultas / total_cnpjs * 1000:.1f}ms")
    print(f"   • Velocidade média: {total_cnpjs / tempo_total_consultas:.1f} CNPJs/s")

    # ---------------- SALVAMENTO ----------------

    # Remover coluna temporária
    df = df.drop(columns=['_cnpj_limpo'], errors='ignore')

    # Garantir que as colunas de comparação e resultado sejam as últimas
    col_finais = [c for c in df.columns if c not in colunas_resultado] + colunas_resultado
    df = df.reindex(columns=col_finais)

    os.makedirs(PASTA_SAIDA, exist_ok=True)
    nome_base = os.path.splitext(nome_arquivo)[0]
    # Usar ';' como separador de saída
    caminho_saida = os.path.join(PASTA_SAIDA, f"{nome_base}_CRUZAMENTO.csv")

    print(f"\n💾 ETAPA 3: Salvando resultados...")
    inicio_salvamento = time.time()

    try:
        df.to_csv(caminho_saida, sep=';', index=False, encoding='utf-8', quoting=csv.QUOTE_MINIMAL)
        tempo_salvamento = time.time() - inicio_salvamento
        tempo_total_processo = time.time() - inicio_total

        print(f"✅ ARQUIVO SALVO COM SUCESSO!")
        print(f"{'-' * 40}")
        print(f"📁 Local: {caminho_saida}")
        print(f"📄 Registros salvos: {len(df):,}")
        print(f"📊 Tamanho estimado: {df.memory_usage(deep=True).sum() / 1024 / 1024:.1f} MB")
        print(f"⏱️  Tempo de salvamento: {formatar_tempo(tempo_salvamento)}")
        print(f"⏱️  Tempo total do processo: {formatar_tempo(tempo_total_processo)}")
        print(f"{'=' * 60}\n")

    except PermissionError:
        print(f"❌ ERRO: Permissão negada ao salvar '{caminho_saida}'. Feche o arquivo.")
    except Exception as e:
        print(f"❌ ERRO ao salvar o arquivo de saída: {e}")


# ---------------- ORQUESTRAÇÃO GERAL ----------------
def processar_todos_arquivos_cruzamento():
    print(f"{'*' * 70}")
    print(f"🚀 INICIANDO PROCESSO DE CRUZAMENTO DE CNPJs")
    print(f"📁 Pasta de entrada: {PASTA_ENTRADA}")
    print(f"📁 Pasta de saída: {PASTA_SAIDA}")
    print(f"{'*' * 70}")

    inicio_global = time.time()

    os.makedirs(PASTA_ENTRADA, exist_ok=True)
    os.makedirs(PASTA_SAIDA, exist_ok=True)

    conn = get_db_connection()
    if not conn:
        print("❌ Não foi possível prosseguir sem conexão com o banco de dados.")
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
        print(f"\n✨ ENCONTRADOS {len(lista_arquivos)} ARQUIVOS PARA PROCESSAMENTO:")
        for i, arquivo in enumerate(lista_arquivos, 1):
            print(f"  {i}. {os.path.basename(arquivo)}")

        print(f"\n{'=' * 70}")

        for i, arquivo in enumerate(lista_arquivos, 1):
            print(f"\n📋 ARQUIVO {i} de {len(lista_arquivos)}")
            processar_e_cruzar_arquivo(arquivo, conn)

            # Adicionar separador entre arquivos, exceto no último
            if i < len(lista_arquivos):
                print(f"\n{'-' * 70}")
                print(f"⏳ PREPARANDO PRÓXIMO ARQUIVO...")
                time.sleep(1)  # Pequena pausa entre arquivos

    conn.close()

    tempo_total_global = time.time() - inicio_global
    print(f"\n{'*' * 70}")
    print(f"🏁 PROCESSO DE CRUZAMENTO CONCLUÍDO!")
    print(f"{'*' * 70}")
    print(f"⏱️  Tempo total de execução: {formatar_tempo(tempo_total_global)}")
    print(f"📁 Arquivos processados: {len(lista_arquivos)}")
    print(f"📁 Resultados salvos em: {PASTA_SAIDA}")
    print(f"{'*' * 70}")


# ---------------- EXECUÇÃO ----------------
if __name__ == "__main__":
    try:
        processar_todos_arquivos_cruzamento()
    except KeyboardInterrupt:
        print(f"\n\n⚠️  Processo interrompido pelo usuário.")
    except Exception as e:
        print(f"\n❌ ERRO GLOBAL NO PROCESSO: {e}")
        import traceback

        traceback.print_exc()
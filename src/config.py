"""
Configurações do sistema de cruzamento de CNPJs
"""
import os
from dotenv import load_dotenv
import yaml
from pathlib import Path

# Carregar variáveis de ambiente
load_dotenv()

# Caminhos base
BASE_DIR = Path(__file__).parent.parent

# Configurações do Banco de Dados
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', 3306)),
    'database': os.getenv('DB_DATABASE', 'cnpj_db'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', ''),
    'charset': 'utf8mb4',
    'use_unicode': True,
    'collation': 'utf8mb4_unicode_ci'
}

# Configurações da API
API_CONFIG = {
    'token': os.getenv('API_TOKEN', ''),
    'base_url': os.getenv('API_BASE_URL', 'https://api.invertexto.com/v1/cnpj/'),
    'timeout': int(os.getenv('TIMEOUT_API', 30)),
    'max_retries': int(os.getenv('MAX_RETRIES', 3))
}

# Configurações do Sistema
SYSTEM_CONFIG = {
    'input_folder': os.getenv('INPUT_FOLDER', 'input'),
    'output_folder': os.getenv('OUTPUT_FOLDER', 'output_cruzamento'),
    'log_level': os.getenv('LOG_LEVEL', 'INFO'),
    'batch_size': 100,
    'show_progress_every': 10,
    'update_interval': 2  # segundos
}

# Configurações de Processamento
PROCESSING_CONFIG = {
    'cnpj_aliases': ['cnpj', 'cnpj_completo', 'cnpj_base', 'cnpj_num', 'documento'],
    'output_separator': ';',
    'output_encoding': 'utf-8',
    'quote_minimal': True
}

# Carregar configurações do YAML
def load_yaml_config():
    config_path = BASE_DIR / 'config.yaml'
    if config_path.exists():
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    return {}

# Carregar configurações
YAML_CONFIG = load_yaml_config()

# Atualizar configurações com YAML se disponível
if YAML_CONFIG:
    # Atualizar mapeamentos
    if 'mappings' in YAML_CONFIG:
        SITUACAO_MAP = YAML_CONFIG['mappings'].get('situacao', {})
        MOTIVO_MAP = YAML_CONFIG['mappings'].get('motivo', {})
    
    # Atualizar configurações de processamento
    if 'processing' in YAML_CONFIG:
        SYSTEM_CONFIG.update({
            'batch_size': YAML_CONFIG['processing'].get('batch_size', 100),
            'show_progress_every': YAML_CONFIG['processing'].get('show_progress_every', 10),
            'update_interval': YAML_CONFIG['processing'].get('update_interval_seconds', 2)
        })
    
    # Atualizar configurações de colunas
    if 'columns' in YAML_CONFIG:
        PROCESSING_CONFIG['cnpj_aliases'] = YAML_CONFIG['columns'].get('cnpj_aliases', PROCESSING_CONFIG['cnpj_aliases'])
else:
    # Mapeamentos padrão
    SITUACAO_MAP = {
        '1': 'NULA', '2': 'ATIVA', '3': 'SUSPENSA', '4': 'INAPTA', '8': 'BAIXADA'
    }
    
    MOTIVO_MAP = {
        '00': "SEM MOTIVO", '01': "EXTINCAO POR ENCERRAMENTO LIQUIDACAO VOLUNTARIA",
        '02': "EXTINCAO POR ENCERRAMENTO FORCADA",
        '03': "EXTINCAO PELO ENCERRAMENTO DA LIQUIDACAO JUDICIAL",
        '04': "EXTINCAO POR INCORPORACAO",
        '05': "EXTINCAO POR FUSAO",
        '06': "EXTINCAO POR CISÃO",
        '07': "ENCERRAMENTO POR ENCERRAMENTO DE CUMPRIMENTO DE SENTENÇA JUDICIAL",
        '08': "EXTINCAO PELA CANCELAMENTO",
        '09': "OMISSA DE CONTRIBUICAO",
        '10': "BAIXA INICIADA EM PROCESSO ADMINISTRATIVO",
        '11': "BAIXA INICIADA POR DETERMINACAO DO RESPONSAVEL",
        '12': "BAIXA INICIADA POR SOLICITACAO DO EMPRESARIO"
    }

# Configurações de colunas
COLUMN_CONFIG = {
    # Colunas do banco de dados
    'db': {
        'situacao': 'situacao_cadastral',
        'motivo': 'motivo_situacao_cadastral'
    },
    
    # Colunas de resultado
    'output': {
        'db_status': 'Status_CNPJ_DB',
        'db_motivo': 'Motivo_Detalhado_DB',
        'api_status': 'Status_CNPJ_API',
        'api_razao': 'razao_social_API',
        'api_fantasia': 'nome_fantasia_API',
        'api_error': 'Erro_Consulta_API',
        'comparison_status': 'STATUS_CADASTRO_FINAL',
        'comparison_razao': 'RAZAO_SOCIAL_COMPARA'
    }
}

# Atualizar com YAML se disponível
if YAML_CONFIG and 'columns' in YAML_CONFIG:
    if 'output_columns' in YAML_CONFIG['columns']:
        COLUMN_CONFIG['output'].update(YAML_CONFIG['columns']['output_columns'])

# Exportar configurações
__all__ = [
    'DB_CONFIG', 'API_CONFIG', 'SYSTEM_CONFIG', 'PROCESSING_CONFIG',
    'SITUACAO_MAP', 'MOTIVO_MAP', 'COLUMN_CONFIG', 'YAML_CONFIG', 'BASE_DIR'
]
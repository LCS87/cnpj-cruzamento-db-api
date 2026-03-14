#!/usr/bin/env python3
"""
Ponto de entrada CLI do sistema de cruzamento de CNPJs.
"""
import sys
import argparse
import logging
from pathlib import Path

# Adicionar src ao path para importar config.py
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from src.utils import setup_logging
from cnpj_cruzamento import processar_todos_arquivos_cruzamento


def main():
    parser = argparse.ArgumentParser(
        description='Sistema de Cruzamento de CNPJs - DB Local + API Externa',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python run.py                    # Processa todos os arquivos em input/
  python run.py --verbose          # Modo detalhado (DEBUG)
  python run.py --input-dir dados  # Pasta de entrada customizada
        """
    )
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Modo detalhado (nível DEBUG)')
    parser.add_argument('--input-dir', type=str, default=None,
                        help='Diretório de entrada (sobrescreve config)')
    parser.add_argument('--output-dir', type=str, default=None,
                        help='Diretório de saída (sobrescreve config)')
    parser.add_argument('--version', action='version', version='%(prog)s 1.0.0')

    args = parser.parse_args()

    log_level = 'DEBUG' if args.verbose else 'INFO'
    setup_logging(log_level=log_level)

    # Sobrescrever pastas via ENV se passadas por argumento
    if args.input_dir:
        import os
        os.environ['INPUT_FOLDER'] = args.input_dir
    if args.output_dir:
        import os
        os.environ['OUTPUT_FOLDER'] = args.output_dir

    logger = logging.getLogger(__name__)
    logger.info("Sistema de Cruzamento de CNPJs iniciado.")

    try:
        processar_todos_arquivos_cruzamento()
        logger.info("Processamento concluído com sucesso.")
    except KeyboardInterrupt:
        logger.warning("Processo interrompido pelo usuário.")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Erro durante o processamento: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

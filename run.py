#!/usr/bin/env python3
"""
Ponto de entrada principal do sistema de cruzamento de CNPJs
"""
import sys
from pathlib import Path

# Adicionar src ao path
src_path = Path(__file__).parent / 'src'
sys.path.insert(0, str(src_path))

from cnpj_cruzamento import processar_todos_arquivos_cruzamento
import argparse

def main():
    """Função principal"""
    parser = argparse.ArgumentParser(
        description='Sistema de Cruzamento de CNPJs - DB Local + API Externa',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos de uso:
  %(prog)s                    # Processa todos os arquivos na pasta input
  %(prog)s --verbose         # Modo detalhado
  %(prog)s --batch-size 50   # Processa em lotes de 50
  %(prog)s --help            # Mostra esta ajuda
        """
    )
    
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Modo detalhado com mais informações')
    
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Tamanho do lote para processamento (padrão: 100)')
    
    parser.add_argument('--output-dir', type=str, default='output_cruzamento',
                       help='Diretório de saída (padrão: output_cruzamento)')
    
    parser.add_argument('--input-dir', type=str, default='input',
                       help='Diretório de entrada (padrão: input)')
    
    parser.add_argument('--version', action='version',
                       version='%(prog)s 1.0.0')
    
    args = parser.parse_args()
    
    print("🚀 Sistema de Cruzamento de CNPJs")
    print("📊 DB Local + API Externa")
    print("=" * 50)
    
    if args.verbose:
        print("🔍 Modo detalhado ativado")
        print(f"📁 Entrada: {args.input_dir}")
        print(f"📁 Saída: {args.output_dir}")
        print(f"📦 Tamanho do lote: {args.batch_size}")
        print("-" * 50)
    
    try:
        processar_todos_arquivos_cruzamento()
        print("\n✅ Processamento concluído com sucesso!")
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Processo interrompido pelo usuário.")
        sys.exit(1)
        
    except Exception as e:
        print(f"\n❌ Erro durante o processamento: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
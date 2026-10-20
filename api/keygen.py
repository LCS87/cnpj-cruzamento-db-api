"""
Gerador de API keys para o sistema de cruzamento de CNPJs.

Uso:
    python -m api.keygen            # gera 1 chave
    python -m api.keygen 3          # gera 3 chaves

Adicione a chave gerada na variável de ambiente API_KEYS (separadas por vírgula).
"""
import sys

sys.path.insert(0, "src")
from security import gerar_api_key  # noqa: E402


def main():
    quantidade = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    for _ in range(quantidade):
        print(gerar_api_key())


if __name__ == "__main__":
    main()

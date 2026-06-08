"""Pipeline de processamento do Relatório Focus do BC.

Executa:
1. Download do PDF mais recente
2. Extração de dados
3. Transformação para nível gold
4. Deleção do PDF
"""

from pathlib import Path
from relat_focus_functions import processar_relatorio_focus


DIRETORIO_TEMPORARIO = Path("tmp_focus")
DIRETORIO_GOLD = Path("data/gold")


def main() -> None:
    """Executa o pipeline completo."""
    print("=" * 60)
    print("Pipeline - Relatório Focus do BC")
    print("=" * 60)
    
    sucesso = processar_relatorio_focus(
        diretorio_temporario=DIRETORIO_TEMPORARIO,
        diretorio_saida=DIRETORIO_GOLD,
        limpar_pdf=True
    )
    
    if sucesso:
        print("\n✓ Pipeline executado com sucesso!")
    else:
        print("\n✗ Pipeline falhou!")
        exit(1)


if __name__ == "__main__":
    main()

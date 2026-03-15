from __future__ import annotations

from pathlib import Path

from data_cleaning_functions import executar_data_cleaning


RAIZ_PROJETO = Path(__file__).resolve().parents[2]
DIR_ENTRADA = RAIZ_PROJETO / "dados_cvm"
DIR_SAIDA = RAIZ_PROJETO / "dados_tgt"


def main() -> None:
    print(f"Origem dos dados: {DIR_ENTRADA}")
    print(f"Destino dos dados: {DIR_SAIDA}")

    if not DIR_ENTRADA.exists():
        print(f"Diretório de entrada não encontrado: {DIR_ENTRADA}")
        print("Execute a ingestão antes da limpeza.")
        return

    executar_data_cleaning(
        diretorio_entrada=DIR_ENTRADA,
        diretorio_saida=DIR_SAIDA,
    )


if __name__ == "__main__":
    main()
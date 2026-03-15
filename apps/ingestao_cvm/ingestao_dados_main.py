from __future__ import annotations

from pathlib import Path

from ingestao_dados_functions import (
    executar_download,
)


DIRETORIO_SAIDA = Path("dados_cvm")
TIMEOUT_SEGUNDOS = 600


def main() -> None:
    """Executa a ingestão com carga completa e configuração fixa.

    Args:
        Nenhum.

    Returns:
        None. A função inicia o download completo da base.
    """
    executar_download(
        diretorio_saida=DIRETORIO_SAIDA,
        timeout=TIMEOUT_SEGUNDOS,
    )


if __name__ == "__main__":
    main()
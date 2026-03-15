from __future__ import annotations

import importlib
import importlib.util
import re
import shutil
import ssl
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen


USER_AGENT = "Mozilla/5.0 (compatible; CVMDownloader/1.0)"
BASE_URL = "https://dados.cvm.gov.br/dados/"


def log(nivel: str, mensagem: str) -> None:
    """Imprime mensagem de execução com nível e timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{nivel}] {mensagem}")


def criar_contexto_ssl() -> ssl.SSLContext:
    """Cria contexto SSL para HTTPS com cadeia de certificados confiáveis.

    Args:
        Nenhum.

    Returns:
        Contexto SSL configurado. Usa `certifi` quando disponível e, caso
        contrário, usa o store padrão do sistema operacional.
    """
    if importlib.util.find_spec("certifi") is not None:
        certifi = importlib.import_module("certifi")
        return ssl.create_default_context(cafile=certifi.where())
    return ssl.create_default_context()


SSL_CONTEXT = criar_contexto_ssl()


@dataclass(frozen=True)
class FonteTipo:
    tipo: str
    urls: tuple[str, ...]
    descricao: str


@dataclass(frozen=True)
class ArquivoCVM:
    url: str
    nome: str
    data_modificacao: datetime | None


FONTE_CVM = FonteTipo(
    tipo="FI",
    urls=(
        urljoin(BASE_URL, "FI/CAD/DADOS/"),
        urljoin(BASE_URL, "FI/DOC/INF_DIARIO/DADOS/"),
    ),
    descricao="Fundos de Investimento (registro de classe e informe diário)",
)


def arquivo_relevante(arquivo: ArquivoCVM) -> bool:
    """Define se o arquivo pertence ao escopo da ingestão.

    Regras:
    - cadastro: apenas o arquivo zip `registro_fundo_classe`;
    - informe diário: arquivos cujo nome contém `inf_diario`.
    """
    nome = arquivo.nome.lower()
    if nome.endswith(".zip") and "registro_fundo_classe" in nome:
        return True
    return "inf_diario" in nome


def baixar_html(url: str, timeout: int) -> str:
    """Baixa o conteúdo HTML de uma URL.

    Args:
        url: Endereço da página a ser consultada.
        timeout: Tempo máximo de espera da requisição, em segundos.

    Returns:
        Conteúdo HTML decodificado em formato texto.
    """
    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req, timeout=timeout, context=SSL_CONTEXT) as response:
        return response.read().decode("utf-8", errors="ignore")


def extrair_arquivos_cvm(html: str, base_url: str) -> list[ArquivoCVM]:
    """Extrai arquivos válidos da listagem HTML da CVM.

    Args:
        html: Conteúdo HTML retornado pela listagem da CVM.
        base_url: URL base usada para transformar links relativos em absolutos.

    Returns:
        Lista deduplicada de arquivos com URL, nome e data de modificação
        (quando disponível).
    """
    padrao = re.compile(
        r'<a\s+href="([^"]+)">[^<]*</a>\s+(\d{2}-[A-Za-z]{3}-\d{4}\s+\d{2}:\d{2})',
        flags=re.IGNORECASE,
    )

    arquivos: list[ArquivoCVM] = []
    for href, data_str in padrao.findall(html):
        if href in {"../", "./"} or href.endswith("/"):
            continue

        url = urljoin(base_url, href)
        path = urlparse(url).path.lower()
        if not path.endswith((".zip", ".csv", ".txt", ".xml")):
            continue

        data_modificacao: datetime | None = None
        try:
            data_modificacao = datetime.strptime(data_str, "%d-%b-%Y %H:%M")
        except ValueError:
            data_modificacao = None

        nome = Path(urlparse(url).path).name
        arquivos.append(ArquivoCVM(url=url, nome=nome, data_modificacao=data_modificacao))

    vistos: set[str] = set()
    resultado: list[ArquivoCVM] = []
    for arquivo in arquivos:
        if arquivo.url in vistos:
            continue
        vistos.add(arquivo.url)
        resultado.append(arquivo)

    return sorted(resultado, key=lambda a: a.nome)


def baixar_arquivo(url: str, destino: Path, timeout: int) -> tuple[bool, str]:
    """Realiza o download de um arquivo e salva localmente.

    Args:
        url: URL do arquivo de origem.
        destino: Caminho de destino no disco.
        timeout: Tempo máximo de espera da requisição, em segundos.

    Returns:
        Tupla com:
        - bool indicando se houve download efetivo;
        - mensagem de status para log.
    """
    destino.parent.mkdir(parents=True, exist_ok=True)

    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req, timeout=timeout, context=SSL_CONTEXT) as response:
        conteudo = response.read()
        destino.write_bytes(conteudo)

    tamanho_mb = destino.stat().st_size / (1024 * 1024)
    return True, f"Baixado: {destino.name} ({tamanho_mb:.2f} MB)"


def coletar_arquivos_fi(timeout: int) -> list[ArquivoCVM]:
    """Coleta e consolida apenas os arquivos FI relevantes na CVM.

    Args:
        timeout: Tempo máximo de espera da requisição, em segundos.

    Returns:
        Lista ordenada e sem duplicidade dos arquivos relevantes para ingestão.
    """
    arquivos: list[ArquivoCVM] = []

    for base in FONTE_CVM.urls:
        html = baixar_html(base, timeout=timeout)
        arquivos.extend(extrair_arquivos_cvm(html, base_url=base))

    vistos: set[str] = set()
    resultado: list[ArquivoCVM] = []
    for arquivo in arquivos:
        if arquivo.url in vistos:
            continue
        vistos.add(arquivo.url)
        resultado.append(arquivo)

    filtrados = [arquivo for arquivo in resultado if arquivo_relevante(arquivo)]
    return sorted(filtrados, key=lambda a: a.nome)


def limpar_diretorio_destino(diretorio: Path) -> None:
    """Remove completamente o conteúdo anterior do diretório de saída."""
    if diretorio.exists():
        shutil.rmtree(diretorio)
    diretorio.mkdir(parents=True, exist_ok=True)


def descompactar_zips_e_manter_csv(diretorio: Path) -> tuple[int, int, int]:
    """Descompacta arquivos ZIP, remove os ZIPs e mantém apenas CSVs.

    Args:
        diretorio: Diretório base com os arquivos baixados.

    Returns:
        Tupla com:
        - quantidade de ZIPs descompactados com sucesso;
        - quantidade de ZIPs removidos;
        - quantidade de arquivos não-CSV removidos.

    Observação:
        Ao final do processo, o diretório mantém apenas arquivos `.csv`.
    """
    zips_descompactados = 0
    zips_removidos = 0
    nao_csv_removidos = 0

    arquivos_zip = sorted(diretorio.rglob("*.zip"))
    for arquivo_zip in arquivos_zip:
        try:
            with zipfile.ZipFile(arquivo_zip, "r") as arquivo:
                arquivo.extractall(path=arquivo_zip.parent)
            zips_descompactados += 1
        except zipfile.BadZipFile as erro:
            log("WARN", f"ZIP inválido, não foi possível descompactar {arquivo_zip.name}: {erro}")
            continue
        except Exception as erro:
            log("WARN", f"Erro ao descompactar {arquivo_zip.name}: {erro}")
            continue

        try:
            arquivo_zip.unlink()
            zips_removidos += 1
        except Exception as erro:
            log("WARN", f"Erro ao remover ZIP {arquivo_zip.name}: {erro}")

    for arquivo in diretorio.rglob("*"):
        if not arquivo.is_file():
            continue
        if arquivo.suffix.lower() == ".csv":
            continue
        try:
            arquivo.unlink()
            nao_csv_removidos += 1
        except Exception as erro:
            log("WARN", f"Erro ao remover arquivo não-CSV {arquivo.name}: {erro}")

    return zips_descompactados, zips_removidos, nao_csv_removidos


def executar_download(
    diretorio_saida: Path,
    timeout: int,
) -> None:
    """Executa o fluxo completo de download (carga completa).

    Args:
        diretorio_saida: Diretório raiz para salvar os arquivos baixados.
        timeout: Tempo máximo de espera das requisições HTTP, em segundos.

    Returns:
        None. A função realiza os downloads e registra logs de execução.
    """
    log("INFO", "Tipo de carga: completa (limpa destino e baixa todos os arquivos).")

    log("INFO", f"Fonte: {FONTE_CVM.descricao}")
    log("INFO", "Limpando diretório de saída antes de iniciar os downloads...")
    limpar_diretorio_destino(diretorio_saida)

    try:
        log("INFO", "Coletando links de arquivos na CVM...")
        arquivos = coletar_arquivos_fi(timeout=timeout)
    except Exception as erro:
        log("ERROR", f"Erro ao coletar links FI: {erro}")
        return

    if not arquivos:
        log("WARN", "Nenhum arquivo relevante encontrado para download.")
        return

    total = len(arquivos)
    baixados = 0

    for idx, arquivo in enumerate(arquivos, start=1):
        destino = diretorio_saida / arquivo.nome

        try:
            fez_download, msg = baixar_arquivo(
                arquivo.url,
                destino,
                timeout=timeout,
            )
            if fez_download:
                baixados += 1
            log("INFO", f"[{idx}/{total}] {msg}")
        except Exception as erro:
            log("ERROR", f"[{idx}/{total}] Erro ao baixar {arquivo.nome}: {erro}")

    log("INFO", "Processando arquivos compactados e mantendo apenas CSV...")
    zips_descompactados, zips_removidos, nao_csv_removidos = descompactar_zips_e_manter_csv(diretorio_saida)
    log(
        "INFO",
        "Resumo pós-processamento: "
        f"{zips_descompactados} ZIP(s) descompactados, "
        f"{zips_removidos} ZIP(s) removidos, "
        f"{nao_csv_removidos} arquivo(s) não-CSV removidos."
    )

    log("INFO", f"Resumo: {baixados} downloads de {total} arquivos listados.")
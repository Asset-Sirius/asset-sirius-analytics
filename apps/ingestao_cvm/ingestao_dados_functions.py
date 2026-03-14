from __future__ import annotations

import importlib
import importlib.util
import re
import shutil
import ssl
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen


USER_AGENT = "Mozilla/5.0 (compatible; CVMDownloader/1.0)"
BASE_URL = "https://dados.cvm.gov.br/dados/"


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
    """Valida se o arquivo está entre os artefatos necessários.

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
        Lista de arquivos com URL, nome e data de modificação (quando disponível).
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


def filtrar_por_carga(arquivos: Iterable[ArquivoCVM], tipo_carga: str, data_execucao: datetime) -> list[ArquivoCVM]:
    """Filtra arquivos para carga incremental ou histórica.

    Regras aplicadas:
    - incremental: arquivos atualizados na data da execução;
    - histórica: arquivos atualizados nos últimos 90 dias.

    Args:
        arquivos: Coleção de arquivos já coletados da CVM.
        tipo_carga: Tipo de carga desejado ("incremental" ou "historica").
        data_execucao: Data/hora de referência para aplicação do filtro.

    Returns:
        Lista de arquivos elegíveis para o modo de carga informado.
    """
    if tipo_carga == "incremental":
        hoje = data_execucao.date()
        filtrados: list[ArquivoCVM] = []
        for arquivo in arquivos:
            if arquivo.data_modificacao and arquivo.data_modificacao.date() == hoje:
                filtrados.append(arquivo)
                continue

            if not arquivo.data_modificacao:
                token_data = data_execucao.strftime("%Y%m%d")
                if token_data in arquivo.nome:
                    filtrados.append(arquivo)

        return filtrados

    limite = data_execucao - timedelta(days=90)
    return [
        arquivo
        for arquivo in arquivos
        if arquivo.data_modificacao is None or arquivo.data_modificacao >= limite
    ]


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
        Lista ordenada e sem duplicidade dos arquivos necessários.
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
    """Remove completamente os arquivos existentes no diretório de saída."""
    if diretorio.exists():
        shutil.rmtree(diretorio)
    diretorio.mkdir(parents=True, exist_ok=True)


def diretorio_tem_arquivos(diretorio: Path) -> bool:
    """Verifica se o diretório possui ao menos um arquivo.

    Essa função é usada para decidir o modo de carga automática:
    - sem arquivos: carga histórica;
    - com arquivos: carga incremental.

    Args:
        diretorio: Caminho do diretório de saída da ingestão.

    Returns:
        True se existir pelo menos um arquivo, False caso contrário.
    """
    if not diretorio.exists():
        return False

    return any(caminho.is_file() for caminho in diretorio.rglob("*"))


def executar_download(
    diretorio_saida: Path,
    timeout: int,
    tipo_carga: str,
) -> None:
    """Executa o fluxo completo de download para os tipos selecionados.

    Args:
        diretorio_saida: Diretório raiz para salvar os arquivos baixados.
        timeout: Tempo máximo de espera das requisições HTTP, em segundos.
        tipo_carga: Modo de carga ("incremental" ou "historica").

    Returns:
        None. A função realiza os downloads e imprime logs de execução.
    """
    data_execucao = datetime.now()
    print(f"Tipo de carga: {tipo_carga}")
    if tipo_carga == "historica":
        print("Regra histórica: últimos 90 dias de atualização na base CVM.")
    else:
        print("Regra incremental: arquivos atualizados no dia da execução.")

    print(f"\nFonte: {FONTE_CVM.descricao}")
    print("Limpando diretório de saída antes de iniciar os downloads...")
    limpar_diretorio_destino(diretorio_saida)

    try:
        arquivos = coletar_arquivos_fi(timeout=timeout)
    except Exception as erro:
        print(f"Erro ao coletar links FI: {erro}")
        return

    arquivos = filtrar_por_carga(arquivos, tipo_carga=tipo_carga, data_execucao=data_execucao)
    if not arquivos:
        print("Nenhum arquivo encontrado para o modo de carga informado.")
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
            print(f"[{idx}/{total}] {msg}")
        except Exception as erro:
            print(f"[{idx}/{total}] Erro ao baixar {arquivo.nome}: {erro}")

    print(f"Resumo: {baixados} downloads de {total} arquivos listados.")
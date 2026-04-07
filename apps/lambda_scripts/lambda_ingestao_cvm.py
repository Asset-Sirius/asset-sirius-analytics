from __future__ import annotations

import importlib
import importlib.util
import json
import re
import ssl
import zipfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

import boto3


USER_AGENT = "Mozilla/5.0 (compatible; CVMDownloader/1.0)"
BASE_URL = "https://dados.cvm.gov.br/dados/"
DEFAULT_BRONZE_BUCKET = "s3-asset-sirius-bucket-bronze"
TMP_DIR = Path("/tmp/asset_sirius_ingestao")


def log(nivel: str, mensagem: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{nivel}] {mensagem}")


def criar_contexto_ssl() -> ssl.SSLContext:
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


FONTE_CVM = FonteTipo(
    tipo="FI",
    urls=(
        urljoin(BASE_URL, "FI/CAD/DADOS/"),
        urljoin(BASE_URL, "FI/DOC/INF_DIARIO/DADOS/"),
    ),
    descricao="Fundos de Investimento (registro de classe e informe diário)",
)


def arquivo_relevante(nome: str) -> bool:
    nome_low = nome.lower()
    if nome_low.endswith(".zip") and "registro_fundo_classe" in nome_low:
        return True
    return "inf_diario" in nome_low


def baixar_html(url: str, timeout: int) -> str:
    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req, timeout=timeout, context=SSL_CONTEXT) as response:
        return response.read().decode("utf-8", errors="ignore")


def extrair_arquivos_cvm(html: str, base_url: str) -> list[ArquivoCVM]:
    padrao = re.compile(
        r'<a\s+href="([^"]+)">[^<]*</a>\s+(\d{2}-[A-Za-z]{3}-\d{4}\s+\d{2}:\d{2})',
        flags=re.IGNORECASE,
    )

    arquivos: list[ArquivoCVM] = []
    for href, _ in padrao.findall(html):
        if href in {"../", "./"} or href.endswith("/"):
            continue

        url = urljoin(base_url, href)
        path = urlparse(url).path.lower()
        if not path.endswith((".zip", ".csv", ".txt", ".xml")):
            continue

        nome = Path(urlparse(url).path).name
        arquivos.append(ArquivoCVM(url=url, nome=nome))

    vistos: set[str] = set()
    resultado: list[ArquivoCVM] = []
    for arquivo in arquivos:
        if arquivo.url in vistos:
            continue
        vistos.add(arquivo.url)
        resultado.append(arquivo)

    return sorted(resultado, key=lambda a: a.nome)


def coletar_arquivos_fi(timeout: int) -> list[ArquivoCVM]:
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

    filtrados = [arquivo for arquivo in resultado if arquivo_relevante(arquivo.nome)]
    return sorted(filtrados, key=lambda a: a.nome)


def baixar_arquivo(url: str, destino: Path, timeout: int) -> None:
    destino.parent.mkdir(parents=True, exist_ok=True)
    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req, timeout=timeout, context=SSL_CONTEXT) as response:
        destino.write_bytes(response.read())


def limpar_tmp() -> None:
    if TMP_DIR.exists():
        for arquivo in sorted(TMP_DIR.rglob("*"), reverse=True):
            if arquivo.is_file():
                arquivo.unlink(missing_ok=True)
            elif arquivo.is_dir():
                arquivo.rmdir()
    TMP_DIR.mkdir(parents=True, exist_ok=True)


def descompactar_zip(zip_path: Path) -> None:
    with zipfile.ZipFile(zip_path, "r") as arquivo:
        arquivo.extractall(path=zip_path.parent)


def upload_para_s3(s3_client, bucket: str, key: str, caminho_local: Path) -> None:
    s3_client.upload_file(str(caminho_local), bucket, key)


def lambda_handler(event, context):
    timeout = int(event.get("timeout", 600)) if isinstance(event, dict) else 600
    bucket_bronze = (
        (event.get("bucket_bronze") if isinstance(event, dict) else None)
        or DEFAULT_BRONZE_BUCKET
    )
    prefixo_bronze = (
        (event.get("prefixo_bronze") if isinstance(event, dict) else None)
        or "cvm/raw"
    ).strip("/")

    s3_client = boto3.client("s3")
    limpar_tmp()

    total_arquivos = 0
    total_csv_upload = 0
    arquivos_erro: list[str] = []

    try:
        arquivos = coletar_arquivos_fi(timeout=timeout)
        total_arquivos = len(arquivos)
        log("INFO", f"Arquivos encontrados na CVM: {total_arquivos}")

        for idx, arquivo in enumerate(arquivos, start=1):
            try:
                destino = TMP_DIR / arquivo.nome
                baixar_arquivo(arquivo.url, destino, timeout=timeout)
                log("INFO", f"[{idx}/{total_arquivos}] Baixado {arquivo.nome}")

                if destino.suffix.lower() == ".zip":
                    descompactar_zip(destino)
                    destino.unlink(missing_ok=True)

                # Upload apenas de CSVs para o bronze.
                for csv_file in sorted(TMP_DIR.rglob("*.csv")):
                    key = f"{prefixo_bronze}/{csv_file.name}"
                    upload_para_s3(s3_client, bucket_bronze, key, csv_file)
                    total_csv_upload += 1
                    csv_file.unlink(missing_ok=True)
            except Exception as erro:
                msg = f"Falha em {arquivo.nome}: {erro}"
                arquivos_erro.append(msg)
                log("ERROR", msg)

        status_code = 207 if arquivos_erro else 200
        body = {
            "bucket_bronze": bucket_bronze,
            "prefixo_bronze": prefixo_bronze,
            "arquivos_listados": total_arquivos,
            "csvs_enviados": total_csv_upload,
            "erros": arquivos_erro,
        }
        return {"statusCode": status_code, "body": json.dumps(body, ensure_ascii=False)}

    except Exception as erro:
        log("ERROR", f"Erro fatal da ingestão: {erro}")
        body = {
            "mensagem": "Erro fatal na ingestão CVM",
            "erro": str(erro),
            "bucket_bronze": bucket_bronze,
            "prefixo_bronze": prefixo_bronze,
        }
        return {"statusCode": 500, "body": json.dumps(body, ensure_ascii=False)}
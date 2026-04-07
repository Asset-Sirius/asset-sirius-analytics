from __future__ import annotations

import io
import json
from dataclasses import dataclass

import boto3
import pandas as pd


DEFAULT_BRONZE_BUCKET = "s3-asset-sirius-bucket-bronze"
DEFAULT_SILVER_BUCKET = "s3-asset-sirius-bucket-silver"


@dataclass(frozen=True)
class ArquivoProcessado:
    nome: str
    quantidade_registros: int


def limpar_texto(df: pd.DataFrame) -> pd.DataFrame:
    for coluna in df.select_dtypes(include=["object", "string"]).columns:
        serie = df[coluna].astype("string")
        df[coluna] = serie.str.strip()
    return df


def filtrar_registro_fundo(df: pd.DataFrame) -> pd.DataFrame:
    if "Data_Adaptacao_RCVM175" in df.columns:
        df = df[df["Data_Adaptacao_RCVM175"].notna()]

    tipos_permitidos = ["FIP", "FIF", "FACFIF", "FI"]
    if "Tipo_Fundo" in df.columns:
        df = df[df["Tipo_Fundo"].isin(tipos_permitidos)]

    if "Situacao" in df.columns:
        df = df[df["Situacao"] == "Em Funcionamento Normal"]

    if "Forma_Condominio" in df.columns:
        df = df[df["Forma_Condominio"] == "Aberto"]

    if "Exclusivo" in df.columns:
        df = df[df["Exclusivo"] == "N"]

    return df


def filtrar_registro_classe(df: pd.DataFrame) -> pd.DataFrame:
    if "Forma_Condominio" in df.columns:
        df = df[df["Forma_Condominio"] == "Aberto"]

    if "Classificacao" in df.columns:
        df = df[df["Classificacao"].isin(["Renda Fixa", "Ações", "Multimercado"])]

    if "Exclusivo" in df.columns:
        df = df[df["Exclusivo"] == "N"]

    if "Situacao" in df.columns:
        df = df[df["Situacao"] == "Em Funcionamento Normal"]

    return df


def filtrar_registro_subclasse(df: pd.DataFrame) -> pd.DataFrame:
    if "Forma_Condominio" in df.columns:
        df = df[df["Forma_Condominio"] == "Aberto"]

    if "Exclusivo" in df.columns:
        df = df[df["Exclusivo"] == "N"]

    if "Exclusivo_INR" in df.columns:
        df = df[df["Exclusivo_INR"] == "N"]

    if "Situacao" in df.columns:
        df = df[df["Situacao"] == "Em Funcionamento Normal"]

    return df


def tratar_informe_diario(df: pd.DataFrame) -> pd.DataFrame:
    if "DT_COMPTC" in df.columns:
        df["DT_COMPTC"] = pd.to_datetime(df["DT_COMPTC"], errors="coerce")

    colunas_numericas = ["VL_TOTAL", "VL_QUOTA", "VL_PATRIM_LIQ", "CAPTC_DIA", "RESG_DIA", "NR_COTST"]
    for coluna in colunas_numericas:
        if coluna in df.columns:
            df[coluna] = pd.to_numeric(df[coluna], errors="coerce")

    return df


def limpeza_basica(df: pd.DataFrame) -> pd.DataFrame:
    df = limpar_texto(df)
    df = df.drop_duplicates()
    df = df.dropna(how="all")
    return df


def ler_csv_s3(s3_client, bucket: str, key: str) -> pd.DataFrame:
    objeto = s3_client.get_object(Bucket=bucket, Key=key)
    conteudo = objeto["Body"].read()
    return pd.read_csv(io.BytesIO(conteudo), sep=";", encoding="latin1", low_memory=False)


def salvar_csv_s3(s3_client, df: pd.DataFrame, bucket: str, key: str) -> None:
    buffer = io.StringIO()
    df.to_csv(buffer, sep=";", index=False)
    s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue().encode("utf-8"))


def listar_objetos_csv(s3_client, bucket: str, prefixo: str) -> list[dict]:
    objetos: list[dict] = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefixo):
        for item in page.get("Contents", []):
            key = item["Key"]
            if key.lower().endswith(".csv"):
                objetos.append(item)
    return objetos


def selecionar_mais_recente(objetos: list[dict], contem: str) -> str | None:
    candidatos = [obj for obj in objetos if contem in obj["Key"].lower()]
    if not candidatos:
        return None
    candidatos.sort(key=lambda obj: obj.get("LastModified"), reverse=True)
    return candidatos[0]["Key"]


def processar_cadastral(df: pd.DataFrame, tipo: str) -> pd.DataFrame:
    df = limpeza_basica(df)
    if tipo == "fundo":
        df = filtrar_registro_fundo(df)
    elif tipo == "classe":
        df = filtrar_registro_classe(df)
    elif tipo == "subclasse":
        df = filtrar_registro_subclasse(df)
    df = limpeza_basica(df)
    return df


def lambda_handler(event, context):
    bucket_bronze = (
        (event.get("bucket_bronze") if isinstance(event, dict) else None)
        or DEFAULT_BRONZE_BUCKET
    )
    bucket_silver = (
        (event.get("bucket_silver") if isinstance(event, dict) else None)
        or DEFAULT_SILVER_BUCKET
    )

    prefixo_bronze = (
        (event.get("prefixo_bronze") if isinstance(event, dict) else None)
        or "cvm/raw"
    ).strip("/")
    prefixo_silver = (
        (event.get("prefixo_silver") if isinstance(event, dict) else None)
        or "cvm/clean"
    ).strip("/")

    s3_client = boto3.client("s3")
    resultados: list[ArquivoProcessado] = []
    erros: list[str] = []

    try:
        objetos_csv = listar_objetos_csv(s3_client, bucket_bronze, prefixo_bronze)
        if not objetos_csv:
            body = {
                "mensagem": "Nenhum CSV encontrado no bronze para limpeza.",
                "bucket_bronze": bucket_bronze,
                "prefixo_bronze": prefixo_bronze,
            }
            return {"statusCode": 404, "body": json.dumps(body, ensure_ascii=False)}

        # Cadastrais: processa sempre o arquivo mais recente de cada tipo.
        mapas = {
            "fundo": ("registro_fundo", "registro_fundo_clean.csv"),
            "classe": ("registro_classe", "registro_classe_clean.csv"),
            "subclasse": ("registro_subclasse", "registro_subclasse_clean.csv"),
        }

        for tipo, (termo, nome_saida) in mapas.items():
            key_origem = selecionar_mais_recente(objetos_csv, termo)
            if not key_origem:
                continue
            try:
                df = ler_csv_s3(s3_client, bucket_bronze, key_origem)
                df = processar_cadastral(df, tipo)
                key_destino = f"{prefixo_silver}/{nome_saida}"
                salvar_csv_s3(s3_client, df, bucket_silver, key_destino)
                resultados.append(ArquivoProcessado(nome=nome_saida, quantidade_registros=len(df)))
            except Exception as erro:
                erros.append(f"Erro no arquivo {key_origem}: {erro}")

        # Informes diarios: processa todos os arquivos encontrados no bronze.
        informes = sorted(
            [obj["Key"] for obj in objetos_csv if "inf_diario_fi" in obj["Key"].lower()]
        )
        for key_origem in informes:
            try:
                df = ler_csv_s3(s3_client, bucket_bronze, key_origem)
                df = limpeza_basica(df)
                df = tratar_informe_diario(df)
                df = limpeza_basica(df)

                nome_origem = key_origem.split("/")[-1]
                nome_saida = nome_origem.replace(".csv", "_clean.csv")
                key_destino = f"{prefixo_silver}/{nome_saida}"
                salvar_csv_s3(s3_client, df, bucket_silver, key_destino)
                resultados.append(ArquivoProcessado(nome=nome_saida, quantidade_registros=len(df)))
            except Exception as erro:
                erros.append(f"Erro no arquivo {key_origem}: {erro}")

        status_code = 207 if erros else 200
        body = {
            "bucket_bronze": bucket_bronze,
            "bucket_silver": bucket_silver,
            "prefixo_bronze": prefixo_bronze,
            "prefixo_silver": prefixo_silver,
            "arquivos_processados": [
                {"nome": item.nome, "quantidade_registros": item.quantidade_registros}
                for item in resultados
            ],
            "total_arquivos": len(resultados),
            "erros": erros,
        }
        return {"statusCode": status_code, "body": json.dumps(body, ensure_ascii=False)}

    except Exception as erro:
        body = {
            "mensagem": "Erro fatal no data cleaning",
            "erro": str(erro),
            "bucket_bronze": bucket_bronze,
            "bucket_silver": bucket_silver,
        }
        return {"statusCode": 500, "body": json.dumps(body, ensure_ascii=False)}

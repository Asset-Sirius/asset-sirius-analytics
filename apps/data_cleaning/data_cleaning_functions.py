from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import glob
import os

import pandas as pd


@dataclass(frozen=True)
class ArquivoProcessado:
    nome: str
    quantidade_registros: int


def limpar_texto(df: pd.DataFrame) -> pd.DataFrame:
    for coluna in df.select_dtypes(include=["object", "string"]).columns:
        serie = df[coluna].astype("string")
        df[coluna] = serie.str.strip()
    return df


def carregar_csv(caminho_arquivo: Path) -> pd.DataFrame:
    return pd.read_csv(caminho_arquivo, sep=";", encoding="latin1", low_memory=False)


def salvar_csv(df: pd.DataFrame, caminho_arquivo: Path) -> None:
    caminho_arquivo.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(caminho_arquivo, sep=";", index=False)


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


def encontrar_arquivo_unico(diretorio_entrada: Path, padrao: str) -> Path | None:
    arquivos = glob.glob(os.path.join(str(diretorio_entrada), padrao))
    if not arquivos:
        return None
    return Path(arquivos[0])


def processar_arquivo_cadastral(
    diretorio_entrada: Path,
    diretorio_saida: Path,
    padrao_entrada: str,
    nome_saida: str,
    filtro,
) -> ArquivoProcessado | None:
    caminho_entrada = encontrar_arquivo_unico(diretorio_entrada, padrao_entrada)
    if caminho_entrada is None:
        print(f"✗ {padrao_entrada}: arquivo não encontrado em {diretorio_entrada}")
        return None

    df = carregar_csv(caminho_entrada)
    print(f"\nProcessando {caminho_entrada.name}: {len(df)} registros")

    df = limpeza_basica(df)
    df = filtro(df)
    df = limpeza_basica(df)

    caminho_saida = diretorio_saida / nome_saida
    salvar_csv(df, caminho_saida)
    print(f"✓ {nome_saida}: {len(df)} registros")

    return ArquivoProcessado(nome=nome_saida, quantidade_registros=len(df))


def processar_informes_diarios(
    diretorio_entrada: Path,
    diretorio_saida: Path,
) -> list[ArquivoProcessado]:
    resultados: list[ArquivoProcessado] = []
    arquivos_informe = sorted(glob.glob(os.path.join(str(diretorio_entrada), "inf_diario_fi*.csv")))

    if not arquivos_informe:
        print(f"✗ inf_diario_fi*.csv: nenhum arquivo encontrado em {diretorio_entrada}")
        return resultados

    for caminho in arquivos_informe:
        caminho_arquivo = Path(caminho)
        print(f"\nProcessando informe: {caminho_arquivo.name}")

        df = carregar_csv(caminho_arquivo)
        df = limpeza_basica(df)
        df = tratar_informe_diario(df)
        df = limpeza_basica(df)

        nome_saida = caminho_arquivo.name.replace(".csv", "_clean.csv")
        caminho_saida = diretorio_saida / nome_saida
        salvar_csv(df, caminho_saida)

        print(f"✓ {nome_saida}: {len(df)} registros")
        resultados.append(ArquivoProcessado(nome=nome_saida, quantidade_registros=len(df)))

    return resultados


def executar_data_cleaning(diretorio_entrada: Path, diretorio_saida: Path) -> list[ArquivoProcessado]:
    diretorio_saida.mkdir(parents=True, exist_ok=True)

    resultados: list[ArquivoProcessado] = []

    arquivo_fundo = processar_arquivo_cadastral(
        diretorio_entrada=diretorio_entrada,
        diretorio_saida=diretorio_saida,
        padrao_entrada="*registro_fundo*.csv",
        nome_saida="registro_fundo_clean.csv",
        filtro=filtrar_registro_fundo,
    )
    if arquivo_fundo:
        resultados.append(arquivo_fundo)

    arquivo_classe = processar_arquivo_cadastral(
        diretorio_entrada=diretorio_entrada,
        diretorio_saida=diretorio_saida,
        padrao_entrada="*registro_classe*.csv",
        nome_saida="registro_classe_clean.csv",
        filtro=filtrar_registro_classe,
    )
    if arquivo_classe:
        resultados.append(arquivo_classe)

    arquivo_subclasse = processar_arquivo_cadastral(
        diretorio_entrada=diretorio_entrada,
        diretorio_saida=diretorio_saida,
        padrao_entrada="*registro_subclasse*.csv",
        nome_saida="registro_subclasse_clean.csv",
        filtro=filtrar_registro_subclasse,
    )
    if arquivo_subclasse:
        resultados.append(arquivo_subclasse)

    resultados.extend(processar_informes_diarios(diretorio_entrada, diretorio_saida))

    print("\n" + "=" * 60)
    print("PROCESSAMENTO FINALIZADO")
    print("=" * 60)
    for resultado in resultados:
        print(f"✓ {resultado.nome}: {resultado.quantidade_registros} registros")
    print(f"\nTotal de arquivos gerados: {len(resultados)}")
    print("=" * 60)

    return resultados

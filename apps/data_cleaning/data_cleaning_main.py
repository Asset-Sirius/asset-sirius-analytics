import pandas as pd
import os
import glob

DIR_ENTRADA = ""
DIR_SAIDA = ""

os.makedirs(DIR_SAIDA, exist_ok=True)

def limpar_texto(df):
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].astype(str).str.strip()
    return df

arquivo_fundo = glob.glob(os.path.join(DIR_ENTRADA, "*registro_fundo*.csv"))

if len(arquivo_fundo) > 0:
    arquivo_fundo = arquivo_fundo[0]
    print(f"\nProcessando fundos: {len(pd.read_csv(arquivo_fundo, sep=';', encoding='latin1'))} registros")
    
    fundos = pd.read_csv(arquivo_fundo, sep=";", encoding="latin1")
    fundos = limpar_texto(fundos)
    
    if "Data_Adaptacao_RCVM175" in fundos.columns:
        fundos = fundos[fundos["Data_Adaptacao_RCVM175"].notna()]
    
    tipos_permitidos = ["FIP", "FIF", "FACFIF", "FI"]
    if "Tipo_Fundo" in fundos.columns:
        fundos = fundos[fundos["Tipo_Fundo"].isin(tipos_permitidos)]

    if "Situacao" in fundos.columns:
        fundos = fundos[fundos["Situacao"] == "Em Funcionamento Normal"]

    if "Forma_Condominio" in fundos.columns:
        fundos = fundos[fundos["Forma_Condominio"] == "Aberto"]
    
    if "Exclusivo" in fundos.columns:
        fundos = fundos[fundos["Exclusivo"] == "N"]

    fundos = fundos.drop_duplicates()
    fundos = fundos.dropna(how="all")

    caminho_saida = os.path.join(DIR_SAIDA, "registro_fundo_clean.csv")
    fundos.to_csv(caminho_saida, sep=";", index=False)
    print(f"Fundos filtrados: {len(fundos)} registros")

arquivo_classe = glob.glob(os.path.join(DIR_ENTRADA, "*registro_classe*.csv"))

if len(arquivo_classe) > 0:
    arquivo_classe = arquivo_classe[0]
    print(f"\nProcessando classes: {len(pd.read_csv(arquivo_classe, sep=';', encoding='latin1'))} registros")
    
    classes = pd.read_csv(arquivo_classe, sep=";", encoding="latin1")
    classes = limpar_texto(classes)
    
    if "Forma_Condominio" in classes.columns:
        classes = classes[classes["Forma_Condominio"] == "Aberto"]

    if "Classificacao" in classes.columns:
        classes = classes[classes["Classificacao"].isin(["Renda Fixa", "Ações", "Multimercado"])]
    
    if "Exclusivo" in classes.columns:
        classes = classes[classes["Exclusivo"] == "N"]
    
    if "Situacao" in classes.columns:
        classes = classes[classes["Situacao"] == "Em Funcionamento Normal"]

    classes = classes.drop_duplicates()
    classes = classes.dropna(how="all")

    caminho_saida = os.path.join(DIR_SAIDA, "registro_classe_clean.csv")
    classes.to_csv(caminho_saida, sep=";", index=False)
    print(f"Classes filtradas: {len(classes)} registros")

arquivo_subclasse = glob.glob(os.path.join(DIR_ENTRADA, "*registro_subclasse*.csv"))

if len(arquivo_subclasse) > 0:
    arquivo_subclasse = arquivo_subclasse[0]
    print(f"\nProcessando subclasses: {len(pd.read_csv(arquivo_subclasse, sep=';', encoding='latin1'))} registros")
    
    subclasses = pd.read_csv(arquivo_subclasse, sep=";", encoding="latin1")
    subclasses = limpar_texto(subclasses)
    
    if "Forma_Condominio" in subclasses.columns:
        subclasses = subclasses[subclasses["Forma_Condominio"] == "Aberto"]

    if "Exclusivo" in subclasses.columns:
        subclasses = subclasses[subclasses["Exclusivo"] == "N"]

    if "Exclusivo_INR" in subclasses.columns:
        subclasses = subclasses[subclasses["Exclusivo_INR"] == "N"]
    
    if "Situacao" in subclasses.columns:
        subclasses = subclasses[subclasses["Situacao"] == "Em Funcionamento Normal"]

    subclasses = subclasses.drop_duplicates()
    subclasses = subclasses.dropna(how="all")

    caminho_saida = os.path.join(DIR_SAIDA, "registro_subclasse_clean.csv")
    subclasses.to_csv(caminho_saida, sep=";", index=False)
    print(f"Subclasses filtradas: {len(subclasses)} registros")

arquivos_informe = glob.glob(os.path.join(DIR_ENTRADA, "inf_diario_fi*.csv"))

for arquivo in arquivos_informe:
    print(f"\nProcessando informe: {os.path.basename(arquivo)}")
    
    informe = pd.read_csv(arquivo, sep=";", encoding="latin1")
    informe = limpar_texto(informe)

    if "DT_COMPTC" in informe.columns:
        informe["DT_COMPTC"] = pd.to_datetime(informe["DT_COMPTC"], errors="coerce")

    colunas_numericas = ["VL_TOTAL", "VL_QUOTA", "VL_PATRIM_LIQ", "CAPTC_DIA", "RESG_DIA", "NR_COTST"]
    for col in colunas_numericas:
        if col in informe.columns:
            informe[col] = pd.to_numeric(informe[col], errors="coerce")

    informe = informe.drop_duplicates()
    informe = informe.dropna(how="all")

    nome_saida = os.path.basename(arquivo).replace(".csv", "_clean.csv")
    caminho_saida = os.path.join(DIR_SAIDA, nome_saida)
    informe.to_csv(caminho_saida, sep=";", index=False)
    print(f"Informe processado: {len(informe)} registros")

print("\n" + "="*60)
print("PROCESSAMENTO FINALIZADO")
print("="*60)

arquivos_gerados = []
for arquivo in ["registro_fundo_clean.csv", "registro_classe_clean.csv", "registro_subclasse_clean.csv"]:
    caminho = os.path.join(DIR_SAIDA, arquivo)
    if os.path.exists(caminho):
        df_temp = pd.read_csv(caminho, sep=";")
        arquivos_gerados.append((arquivo, len(df_temp)))
        print(f"✓ {arquivo}: {len(df_temp)} registros")
    else:
        print(f"✗ {arquivo}: ERRO - não encontrado")

arquivos_informe_limpos = glob.glob(os.path.join(DIR_SAIDA, "inf_diario_fi*_clean.csv"))
if arquivos_informe_limpos:
    for arquivo in arquivos_informe_limpos:
        nome = os.path.basename(arquivo)
        df_temp = pd.read_csv(arquivo, sep=";")
        print(f"✓ {nome}: {len(df_temp)} registros")

print(f"\nTotal de arquivos gerados: {len(arquivos_gerados) + len(arquivos_informe_limpos)}")
print("="*60)
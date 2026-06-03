"""
Lambda para criacao da camada Gold (Asset Sirius Analytics)
Baseado em: planejamento_gold_simulacao.md
Padrão: lambda_ingestao_cvm.py + lambda_data_cleaning_cvm.py

Fase 1: Criacao de tabelas reais (CVM)
  - dim_tempo, dim_fundo, fct_fundo_diario, agg_fundo_periodo

Fase 2 (Futuro): Simulador de clientes
  - dim_cliente_simulado, fatos simulados

Execução:
  - Local: python -c "from lambda_gold_layer import lambda_handler; lambda_handler({}, None)"
  - AWS Lambda: configurar como function handler

Payload opcional:
  {
    "bucket_silver": "s3-asset-sirius-bucket-silver",
    "bucket_gold": "s3-asset-sirius-bucket-gold",
    "prefixo_silver": "cvm/clean",
    "prefixo_gold": "cvm/gold",
    "data_inicio": "2023-01-01",
    "data_fim": "2024-12-31",
    "periodos_agregacao": [7, 30, 60, 90],
    "modo": "local"  # "local" ou "s3"
  }
"""

from __future__ import annotations

import gc
import io
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import boto3
import pandas as pd
import numpy as np


# ========== LOGGING ==========

def log(nivel: str, mensagem: str) -> None:
    """Logger estruturado com timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{nivel}] {mensagem}")


# ========== DATACLASSES ==========

@dataclass(frozen=True)
class TabelaGoldProcessada:
    """Representa uma tabela gerada com sucesso"""
    nome: str
    quantidade_registros: int
    tamanho_mb: float


# ========== CONFIGURACOES PADRAO ==========

DEFAULT_SILVER_BUCKET = "s3-asset-sirius-bucket-silver"
DEFAULT_GOLD_BUCKET = "s3-asset-sirius-bucket-gold"
DEFAULT_PREFIXO_SILVER = "cvm/clean"
DEFAULT_PREFIXO_GOLD = "cvm/gold"
DEFAULT_DATA_INICIO = "2025-01-01"
DEFAULT_DATA_FIM = "2026-05-30"
DEFAULT_PERIODOS_AGREGACAO = [7, 30, 60, 90]


# ========== UTILITARIOS S3 ==========

def ler_csv_s3(s3_client, bucket: str, key: str) -> pd.DataFrame:
    """Le CSV do S3 (separador `;`, encoding `utf-8`)"""
    try:
        objeto = s3_client.get_object(Bucket=bucket, Key=key)
        conteudo = objeto["Body"].read()
        return pd.read_csv(
            io.BytesIO(conteudo),
            sep=";",
            encoding="utf-8",
            low_memory=False
        )
    except Exception as e:
        log("ERROR", f"Erro ao ler S3 {bucket}/{key}: {e}")
        raise


def salvar_parquet_s3(s3_client, df: pd.DataFrame, bucket: str, key: str) -> float:
    """Salva DataFrame como Parquet no S3, retorna tamanho em MB"""
    try:
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, compression="snappy")
        tamanho_mb = len(buffer.getvalue()) / (1024 * 1024)
        
        s3_client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
        log("INFO", f"Salvo S3: {key} ({tamanho_mb:.2f} MB)")
        return tamanho_mb
    except Exception as e:
        log("ERROR", f"Erro ao salvar S3 {bucket}/{key}: {e}")
        raise


def listar_csvs_s3(s3_client, bucket: str, prefixo: str) -> list[dict]:
    """Lista todos os CSVs em um prefixo S3"""
    objetos: list[dict] = []
    try:
        paginator = s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefixo):
            for item in page.get("Contents", []):
                key = item["Key"]
                if key.lower().endswith(".csv"):
                    objetos.append(item)
    except Exception as e:
        log("ERROR", f"Erro ao listar S3 {bucket}/{prefixo}: {e}")
    return objetos


def selecionar_mais_recente(objetos: list[dict], contem: str) -> Optional[str]:
    """Seleciona o arquivo mais recente que contém a string especificada"""
    candidatos = [obj for obj in objetos if contem in obj["Key"].lower()]
    if not candidatos:
        return None
    candidatos.sort(key=lambda obj: obj.get("LastModified"), reverse=True)
    return candidatos[0]["Key"]


# ========== UTILITARIOS LOCAIS ==========

def ler_csv_local(caminho: Path) -> pd.DataFrame:
    """Le CSV local com deteccao automatica de separador"""
    try:
        # Tenta detectar separador
        with open(caminho, 'r', encoding='utf-8') as f:
            primeira_linha = f.readline()
        
        virgulas = primeira_linha.count(',')
        ponto_virgulas = primeira_linha.count(';')
        sep = ';' if ponto_virgulas > virgulas else ','
        
        return pd.read_csv(caminho, sep=sep, encoding='utf-8', low_memory=False)
    except Exception as e:
        log("ERROR", f"Erro ao ler local {caminho}: {e}")
        raise


def salvar_parquet_local(df: pd.DataFrame, caminho: Path) -> float:
    """Salva DataFrame como Parquet localmente, retorna tamanho em MB"""
    try:
        caminho.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(caminho, index=False, compression="snappy")
        tamanho_mb = caminho.stat().st_size / (1024 * 1024)
        log("INFO", f"Salvo local: {caminho} ({tamanho_mb:.2f} MB)")
        return tamanho_mb
    except Exception as e:
        log("ERROR", f"Erro ao salvar local {caminho}: {e}")
        raise


def _salvar_particionado_local(df: pd.DataFrame, col_data: str,
                                nome_tabela: str, path_output: Path) -> float:
    """Salva tabela particionada por ano_mes (YYYYMM) localmente"""
    try:
        df_temp = df.copy()
        # Extrai ano_mes: YYYYMMDD // 100 = YYYYMM
        df_temp['ano_mes'] = (df_temp[col_data] // 100).astype(str)
        
        path_tabela = path_output / nome_tabela
        path_tabela.mkdir(parents=True, exist_ok=True)
        
        tamanho_total = 0
        for ano_mes, grupo in df_temp.groupby('ano_mes', sort=True):
            grupo_clean = grupo.drop(columns=['ano_mes'])
            
            path_mes = path_tabela / f"ano_mes={ano_mes}"
            path_mes.mkdir(parents=True, exist_ok=True)
            
            grupo_clean.to_parquet(f"{path_mes}/data.parquet", index=False, compression="snappy")
            grupo_clean.to_csv(f"{path_mes}/data.csv", index=False, sep=';', encoding='utf-8')
            tamanho_total += (path_mes / "data.parquet").stat().st_size / (1024 * 1024)
        
        return tamanho_total
    except Exception as e:
        log("ERROR", f"Erro ao salvar particionado local {nome_tabela}: {e}")
        raise


def _salvar_particionado_s3(s3_client, df: pd.DataFrame, col_data: str,
                             nome_tabela: str, bucket: str, prefixo: str) -> float:
    """Salva tabela particionada por ano_mes (YYYYMM) no S3"""
    try:
        df_temp = df.copy()
        # Extrai ano_mes: YYYYMMDD // 100 = YYYYMM
        df_temp['ano_mes'] = (df_temp[col_data] // 100).astype(str)
        
        log("INFO", f"  Particionando {nome_tabela}...")
        log("INFO", f"    Total de registros: {len(df_temp)}")
        log("INFO", f"    Coluna de data usada: {col_data}")
        log("INFO", f"    Primeiras datas: {df_temp[col_data].head(3).tolist()}")
        log("INFO", f"    Últimas datas: {df_temp[col_data].tail(3).tolist()}")
        log("INFO", f"    Data mín/máx: {df_temp[col_data].min()} / {df_temp[col_data].max()}")
        
        # Mostra distribuição por mês ANTES de salvar
        anos_meses_unicos = df_temp['ano_mes'].unique()
        log("INFO", f"    Partições encontradas: {len(anos_meses_unicos)}")
        log("INFO", f"    Meses: {sorted(anos_meses_unicos)}")
        for ano_mes in sorted(anos_meses_unicos):
            qtd = len(df_temp[df_temp['ano_mes'] == ano_mes])
            log("INFO", f"      - {ano_mes}: {qtd} registros")
        
        tamanho_total = 0
        meses_salvos = 0
        
        for ano_mes, grupo in df_temp.groupby('ano_mes', sort=True):
            grupo_clean = grupo.drop(columns=['ano_mes'])

            # Salva apenas CSV para tabelas particionadas
            buffer_csv = io.StringIO()
            grupo_clean.to_csv(buffer_csv, sep=';', index=False, encoding='utf-8')
            key_csv = f"{prefixo}/{nome_tabela}/ano_mes={ano_mes}/data.csv"
            conteudo_csv = buffer_csv.getvalue().encode('utf-8')
            
            try:
                s3_client.put_object(Bucket=bucket, Key=key_csv, Body=conteudo_csv)
                tamanho_mb = len(conteudo_csv) / (1024 * 1024)
                tamanho_total += tamanho_mb
                meses_salvos += 1
                log("INFO", f"    ✓ Salvo: {key_csv} ({tamanho_mb:.4f} MB, {len(grupo_clean)} registros)")
            except Exception as e_s3:
                log("ERROR", f"    ✗ Erro ao salvar {key_csv}: {e_s3}")
                raise
        
        log("INFO", f"  ✓ {nome_tabela}: {meses_salvos} partições, {tamanho_total:.2f} MB total")
        return tamanho_total
    except Exception as e:
        log("ERROR", f"Erro ao salvar particionado S3 {nome_tabela}: {e}")
        raise


# ========== FUNCOES DE CRIACAO GOLD ==========

def _encontrar_coluna(df: pd.DataFrame, variacoes: list) -> str:
    """Encontra coluna com case-insensitive + alias"""
    colunas_lower = {col.lower(): col for col in df.columns}
    for var in variacoes:
        if var.lower() in colunas_lower:
            return colunas_lower[var.lower()]
    raise KeyError(
        f"Coluna não encontrada. Testadas: {variacoes} | "
        f"Disponíveis: {list(df.columns)}"
    )


def _encontrar_coluna_opcional(df: pd.DataFrame, variacoes: list) -> Optional[str]:
    """Encontra coluna opcional (retorna None se não encontrada)"""
    try:
        return _encontrar_coluna(df, variacoes)
    except KeyError:
        return None


def criar_dim_tempo(data_inicio: Optional[str] = None, data_fim: Optional[str] = None) -> pd.DataFrame:
    """
    Cria dimensão de tempo com o intervalo de datas especificado
    
    Args:
        data_inicio: data de início (formato: YYYY-MM-DD) ou None para usar DEFAULT_DATA_INICIO
        data_fim: data de fim (formato: YYYY-MM-DD) ou None para usar DEFAULT_DATA_FIM
    
    Returns:
        DataFrame com dim_tempo
    """
    try:
        # Usa valores passados ou defaults
        inicio = data_inicio or DEFAULT_DATA_INICIO
        fim = data_fim or DEFAULT_DATA_FIM
        
        datas = pd.date_range(start=inicio, end=fim, freq='D')
        
        dim_tempo = pd.DataFrame({
            'sk_data': datas.strftime('%Y%m%d').astype(int),
            'data': datas,
            'ano': datas.year,
            'mes': datas.month,
            'dia': datas.day,
            'trimestre': datas.quarter,
            'dia_semana': datas.dayofweek,
            'eh_fim_de_mes': datas.is_month_end.astype(int)
        })
        
        log("INFO", f"dim_tempo criada: {len(dim_tempo)} registros ({inicio} a {fim})")
        return dim_tempo
    except Exception as e:
        log("ERROR", f"Erro ao criar dim_tempo: {e}")
        raise


def criar_dim_fundo(df_registro_classe: pd.DataFrame, 
                    df_registro_fundo: Optional[pd.DataFrame] = None) -> pd.DataFrame:
    """
    Cria dimensão de fundo
    
    Args:
        df_registro_classe: registro_classe_clean.csv
        df_registro_fundo: registro_fundo_clean.csv (opcional)
    
    Returns:
        DataFrame com dim_fundo
    """
    try:
        import hashlib
        
        # Encontra coluna CNPJ
        col_cnpj_classe = _encontrar_coluna(df_registro_classe,
            ['CNPJ_FUNDO_CLASSE', 'cnpj_fundo_classe', 'CNPJ_Classe'])
        
        # Encontra nome do fundo
        col_denom = _encontrar_coluna_opcional(df_registro_classe,
            ['DENOM_SOCIAL', 'denom_social', 'Denominacao_Social', 'nome'])
        
        dim_fundo = df_registro_classe.copy()
        
        # Normaliza CNPJ
        dim_fundo['cnpj_fundo_classe_norm'] = (
            dim_fundo[col_cnpj_classe]
            .astype(str)
            .str.replace(r'\D', '', regex=True)
        )
        
        # Cria surrogate key
        dim_fundo['sk_fundo'] = dim_fundo['cnpj_fundo_classe_norm'].apply(
            lambda x: int(hashlib.md5(str(x).encode()).hexdigest(), 16) % 1000000
        )
        
        # Seleciona colunas principais
        colunas_base = [
            'sk_fundo',
            'cnpj_fundo_classe_norm',
            col_cnpj_classe
        ]
        
        if col_denom:
            colunas_base.append(col_denom)
        
        # Adiciona colunas opcionais
        for variacoes in [
            ['Tipo_Classe', 'TIPO_FUNDO', 'tipo_fundo'],
            ['Classificacao', 'CLASSIFICACAO', 'classificacao'],
            ['Situacao', 'SITUACAO', 'situacao'],
            ['Publico_Alvo', 'PUBLICO_ALVO', 'publico_alvo'],
        ]:
            col = _encontrar_coluna_opcional(dim_fundo, variacoes)
            if col and col not in colunas_base:
                colunas_base.append(col)
        
        dim_fundo = dim_fundo[[c for c in colunas_base if c in dim_fundo.columns]]
        dim_fundo = dim_fundo.drop_duplicates(subset=['sk_fundo'])
        
        log("INFO", f"dim_fundo criada: {len(dim_fundo)} fundos")
        return dim_fundo
    except Exception as e:
        log("ERROR", f"Erro ao criar dim_fundo: {e}")
        raise


def criar_fct_fundo_diario(df_inf_diario: pd.DataFrame,
                           dim_tempo: pd.DataFrame,
                           dim_fundo: pd.DataFrame) -> pd.DataFrame:
    """
    Cria fato fundo diário
    
    Args:
        df_inf_diario: informe diário da CVM
        dim_tempo: dimensão tempo
        dim_fundo: dimensão fundo
    
    Returns:
        DataFrame com fct_fundo_diario
    """
    try:
        # Encontra colunas obrigatórias
        col_cnpj = _encontrar_coluna(df_inf_diario,
            ['CNPJ_FUNDO_CLASSE', 'cnpj_fundo_classe', 'CNPJ_Classe'])
        col_data = _encontrar_coluna(df_inf_diario,
            ['DT_COMPTC', 'dt_comptc', 'Data'])
        col_pl = _encontrar_coluna(df_inf_diario,
            ['VL_PATRIM_LIQ', 'vl_patrim_liq', 'Patrimonio'])
        col_captc = _encontrar_coluna(df_inf_diario,
            ['CAPTC_DIA', 'captc_dia', 'Captacao'])
        col_resg = _encontrar_coluna(df_inf_diario,
            ['RESG_DIA', 'resg_dia', 'Resgate'])
        col_cotst = _encontrar_coluna(df_inf_diario,
            ['NR_COTST', 'nr_cotst', 'Cotistas'])
        
        fct = df_inf_diario.copy()
        
        # Normaliza CNPJ
        fct['cnpj_fundo_classe_norm'] = (
            fct[col_cnpj].astype(str).str.replace(r'\D', '', regex=True)
        )
        
        # Converte data
        fct['data'] = pd.to_datetime(fct[col_data], errors='coerce')
        fct['sk_data'] = fct['data'].dt.strftime('%Y%m%d').astype(int)
        
        # Converte numéricas
        for col in [col_pl, col_captc, col_resg, col_cotst]:
            fct[col] = pd.to_numeric(fct[col], errors='coerce')
        
        # Join com dim_fundo
        fct = fct.merge(
            dim_fundo[['sk_fundo', 'cnpj_fundo_classe_norm']],
            on='cnpj_fundo_classe_norm',
            how='left'
        )
        
        # Calcula métricas
        fct['captacao_dia'] = fct[col_captc]
        fct['resgate_dia'] = fct[col_resg]
        fct['fluxo_liquido_dia'] = fct['captacao_dia'] - fct['resgate_dia']
        fct['patrimonio_liquido_dia'] = fct[col_pl]
        fct['cotistas_dia'] = fct[col_cotst]
        fct['flag_base_real'] = 1
        
        # Calcula variações
        fct = fct.sort_values(['sk_fundo', 'sk_data'])
        fct['variacao_pl_dia_pct'] = fct.groupby('sk_fundo')[col_pl].pct_change() * 100
        fct['variacao_cotistas_dia_pct'] = fct.groupby('sk_fundo')[col_cotst].pct_change() * 100
        
        # Seleciona colunas
        colunas_fct = [
            'sk_data', 'sk_fundo', 'cnpj_fundo_classe_norm',
            'captacao_dia', 'resgate_dia', 'fluxo_liquido_dia',
            'patrimonio_liquido_dia', 'cotistas_dia',
            'variacao_pl_dia_pct', 'variacao_cotistas_dia_pct',
            'flag_base_real'
        ]
        
        fct_final = fct[colunas_fct].dropna(subset=['sk_fundo'])
        
        # Diagnóstico: datas únicas em fct_fundo_diario
        datas_unicas_fct = fct_final['sk_data'].unique()
        log("INFO", f"fct_fundo_diario criada: {len(fct_final)} registros, {len(datas_unicas_fct)} datas únicas")
        log("DEBUG", f"  Data mín/máx: {fct_final['sk_data'].min()} / {fct_final['sk_data'].max()}")
        log("DEBUG", f"  Fundos únicos: {fct_final['sk_fundo'].nunique()}")
        return fct_final
    except Exception as e:
        log("ERROR", f"Erro ao criar fct_fundo_diario: {e}")
        raise


def criar_agg_fundo_periodo(fct_fundo_diario: pd.DataFrame,
                            periodos: list = None) -> pd.DataFrame:
    """
    Cria agregação por período usando janelas móveis (otimizado com groupby)
    
    Args:
        fct_fundo_diario: fato fundo diário
        periodos: lista de períodos em dias (ex: [7, 30, 60, 90])
    
    Returns:
        DataFrame com agg_fundo_periodo
    """
    try:
        if periodos is None:
            periodos = [7, 30, 60, 90]
        
        fct = fct_fundo_diario.copy()
        fct['data'] = pd.to_datetime(fct['sk_data'], format='%Y%m%d')
        
        # Diagnóstico: mostra datas únicas em fct_fundo_diario
        datas_unicas = fct['sk_data'].unique()
        log("DEBUG", f"  agg_fundo_periodo - Datas únicas em fct_fundo_diario: {len(datas_unicas)}")
        log("DEBUG", f"    Data mín: {fct['sk_data'].min()}, Data máx: {fct['sk_data'].max()}")
        
        # Ordena por fundo e data (CRÍTICO para rolling windows)
        fct = fct.sort_values(['sk_fundo', 'data']).reset_index(drop=True)
        
        agregados = []
        
        for periodo in periodos:
            log("DEBUG", f"  Calculando período {periodo} dias...")
            
            # Função para aplicar rolling em cada grupo de fundo
            def calcular_rolling(grupo):
                grupo = grupo.sort_values('data').reset_index(drop=True)
                
                grupo['captacao_periodo'] = grupo['captacao_dia'].rolling(window=periodo, min_periods=1).sum()
                grupo['resgate_periodo'] = grupo['resgate_dia'].rolling(window=periodo, min_periods=1).sum()
                grupo['fluxo_liquido_periodo'] = grupo['fluxo_liquido_dia'].rolling(window=periodo, min_periods=1).sum()
                grupo['var_patrimonio_periodo_pct'] = grupo['variacao_pl_dia_pct'].rolling(window=periodo, min_periods=1).mean()
                grupo['var_cotistas_periodo_pct'] = grupo['variacao_cotistas_dia_pct'].rolling(window=periodo, min_periods=1).mean()
                
                return grupo
            
            # Aplica rolling para cada fundo de uma vez
            agg_periodo = fct.groupby('sk_fundo', sort=False).apply(calcular_rolling)
            # Reconverte sk_fundo do índice para coluna (MultiIndex após apply)
            agg_periodo = agg_periodo.reset_index(level=1, drop=True).reset_index()
            
            # Preenche NaNs
            agg_periodo['var_patrimonio_periodo_pct'] = agg_periodo['var_patrimonio_periodo_pct'].fillna(0)
            agg_periodo['var_cotistas_periodo_pct'] = agg_periodo['var_cotistas_periodo_pct'].fillna(0)
            
            # Score de risco
            agg_periodo['score_risco_fundo'] = (
                50 + agg_periodo['var_patrimonio_periodo_pct'].abs() * 10
            ).clip(0, 100)
            
            # Classificação de risco
            agg_periodo['nivel_risco_fundo'] = pd.cut(
                agg_periodo['score_risco_fundo'],
                bins=[0, 45, 70, 100],
                labels=['baixo', 'medio', 'alto']
            )
            
            # Mantém apenas colunas necessárias
            agg_periodo['periodo'] = periodo
            agg_periodo['data_referencia'] = agg_periodo['sk_data']
            
            agregados.append(agg_periodo[[
                'periodo', 'data_referencia', 'sk_fundo',
                'captacao_periodo', 'resgate_periodo', 'fluxo_liquido_periodo',
                'var_patrimonio_periodo_pct', 'var_cotistas_periodo_pct',
                'score_risco_fundo', 'nivel_risco_fundo'
            ]])
            
            gc.collect()
        
        agg_final = pd.concat(agregados, ignore_index=True)
        del agregados
        gc.collect()
        
        # Diagnóstico: datas únicas em agg_fundo_periodo
        datas_unicas_agg = agg_final['data_referencia'].unique()
        log("INFO", f"agg_fundo_periodo criada: {len(agg_final)} registros ({len(periodos)} períodos)")
        log("DEBUG", f"  Datas únicas: {len(datas_unicas_agg)}")
        log("DEBUG", f"  Data mín/máx: {agg_final['data_referencia'].min()} / {agg_final['data_referencia'].max()}")
        log("DEBUG", f"  Distribuição por período:")
        for periodo in periodos:
            qtd = len(agg_final[agg_final['periodo'] == periodo])
            log("DEBUG", f"    Período {periodo} dias: {qtd} registros")
        
        return agg_final
    except Exception as e:
        log("ERROR", f"Erro ao criar agg_fundo_periodo: {e}")
        raise


# ========== LAMBDA HANDLER ==========

def lambda_handler(event, context):
    """
    Handler principal da Lambda Gold Layer
    
    Args:
        event: dict com configurações (opcional)
        context: contexto Lambda
    
    Returns:
        dict com statusCode e body JSON
    """
    
    # ========== PARSE PARAMETROS ==========
    
    modo = (event.get("modo") if isinstance(event, dict) else None) or "s3"
    bucket_silver = (
        (event.get("bucket_silver") if isinstance(event, dict) else None)
        or DEFAULT_SILVER_BUCKET
    )
    bucket_gold = (
        (event.get("bucket_gold") if isinstance(event, dict) else None)
        or DEFAULT_GOLD_BUCKET
    )
    prefixo_silver = (
        (event.get("prefixo_silver") if isinstance(event, dict) else None)
        or DEFAULT_PREFIXO_SILVER
    ).strip("/")
    prefixo_gold = (
        (event.get("prefixo_gold") if isinstance(event, dict) else None)
        or DEFAULT_PREFIXO_GOLD
    ).strip("/")
    data_inicio = (
        (event.get("data_inicio") if isinstance(event, dict) else None)
        or DEFAULT_DATA_INICIO
    )
    data_fim = (
        (event.get("data_fim") if isinstance(event, dict) else None)
        or DEFAULT_DATA_FIM
    )
    periodos_agregacao = (
        (event.get("periodos_agregacao") if isinstance(event, dict) else None)
        or DEFAULT_PERIODOS_AGREGACAO
    )
    
    # ========== INICIALIZA ==========
    
    log("INFO", "=" * 70)
    log("INFO", "LAMBDA GOLD LAYER - Asset Sirius Analytics")
    log("INFO", f"Modo: {modo} | Data: {data_inicio} a {data_fim}")
    log("INFO", "=" * 70)
    
    tabelas_processadas: list[TabelaGoldProcessada] = []
    erros: list[str] = []
    
    try:
        # ========== CARREGA DADOS SILVER ==========
        
        log("INFO", "[1/4] Carregando dados Silver...")
        
        if modo == "local":
            path_silver = Path("dados_tgt")
            
            # Carrega todos os arquivos de informe diário no intervalo de datas
            arquivos_inf = sorted(path_silver.glob("inf_diario_fi_*_clean.csv"))
            log("INFO", f"  Procurando por: inf_diario_fi_*_clean.csv em {path_silver}")
            log("INFO", f"  Arquivos encontrados: {len(arquivos_inf)}")
            
            dfs_inf = []
            for arquivo in arquivos_inf:
                try:
                    df_temp = ler_csv_local(arquivo)
                    dfs_inf.append(df_temp)
                    log("INFO", f"  ✓ Carregado: {arquivo.name} ({len(df_temp)} registros)")
                except Exception as e:
                    log("WARN", f"  ✗ Erro ao carregar {arquivo.name}: {e}")
            
            if not dfs_inf:
                raise ValueError(f"Nenhum arquivo inf_diario_fi_*_clean.csv encontrado em {path_silver}")
            
            df_inf_diario = pd.concat(dfs_inf, ignore_index=True)
            del dfs_inf  # Libera memória dos arquivos carregados
            gc.collect()
            
            # Diagnóstico: datas carregadas
            col_data_carregada = None
            for variacoes in [['DT_COMPTC', 'dt_comptc', 'Data'], ['Data_Competencia', 'data_competencia']]:
                for var in variacoes:
                    if var.lower() in [c.lower() for c in df_inf_diario.columns]:
                        col_data_carregada = var
                        break
                if col_data_carregada:
                    break
            
            if col_data_carregada:
                datas_carregadas = pd.to_datetime(df_inf_diario[col_data_carregada], errors='coerce')
                log("INFO", f"  ✓ Informe diário: {len(df_inf_diario)} registros, datas de {datas_carregadas.min().date()} a {datas_carregadas.max().date()}")
            else:
                log("INFO", f"  ✓ Informe diário: {len(df_inf_diario)} registros")
            
            df_registro_classe = ler_csv_local(path_silver / "registro_classe_clean.csv")
            df_registro_fundo = ler_csv_local(path_silver / "registro_fundo_clean.csv")
        else:
            s3_client = boto3.client("s3")
            objetos = listar_csvs_s3(s3_client, bucket_silver, prefixo_silver)
            
            # Extrai ano_mes dos parâmetros para filtrar arquivos
            data_ini = datetime.strptime(data_inicio, "%Y-%m-%d")
            data_fim_dt = datetime.strptime(data_fim, "%Y-%m-%d")
            ano_mes_ini = int(data_ini.strftime("%Y%m"))
            ano_mes_fim = int(data_fim_dt.strftime("%Y%m"))
            
            log("INFO", f"  Filtro de período: {ano_mes_ini} a {ano_mes_fim}")
            
            # Carrega APENAS arquivos de informe diário no intervalo de datas
            keys_inf_candidatos = [obj["Key"] for obj in objetos if "inf_diario_fi" in obj["Key"].lower()]
            keys_inf = []
            
            for key in keys_inf_candidatos:
                # Extrai YYYYMM do nome do arquivo (ex: inf_diario_fi_202605_clean.csv)
                try:
                    partes = key.split('_')
                    for i, parte in enumerate(partes):
                        if len(parte) == 6 and parte.isdigit():
                            ano_mes_arquivo = int(parte)
                            if ano_mes_ini <= ano_mes_arquivo <= ano_mes_fim:
                                keys_inf.append(key)
                            break
                except Exception as e:
                    log("WARN", f"  Não foi possível extrair período de {key}: {e}")
            
            key_classe = selecionar_mais_recente(objetos, "registro_classe")
            key_fundo = selecionar_mais_recente(objetos, "registro_fundo")
            
            if not (keys_inf and key_classe and key_fundo):
                raise ValueError("Arquivos Silver obrigatórios não encontrados")
            
            log("INFO", f"  Arquivos inf_diario no período [{ano_mes_ini}-{ano_mes_fim}]: {len(keys_inf)}")
            
            # Carrega arquivos de informe diário do intervalo
            dfs_inf = []
            for key in sorted(keys_inf):
                try:
                    df_temp = ler_csv_s3(s3_client, bucket_silver, key)
                    
                    # Encontra coluna de data e filtra pelo intervalo
                    col_data_temp = None
                    for variacoes in [['DT_COMPTC', 'dt_comptc', 'Data'], ['Data_Competencia', 'data_competencia']]:
                        for var in variacoes:
                            if var.lower() in [c.lower() for c in df_temp.columns]:
                                col_data_temp = var
                                break
                        if col_data_temp:
                            break
                    
                    if col_data_temp:
                        df_temp['_data_temp'] = pd.to_datetime(df_temp[col_data_temp], errors='coerce')
                        df_temp = df_temp[(df_temp['_data_temp'] >= pd.Timestamp(data_inicio)) & 
                                         (df_temp['_data_temp'] <= pd.Timestamp(data_fim))]
                        df_temp = df_temp.drop(columns=['_data_temp'])
                    
                    dfs_inf.append(df_temp)
                    arquivo_nome = key.split('/')[-1]
                    log("INFO", f"  ✓ Carregado do S3: {arquivo_nome} ({len(df_temp)} registros após filtro)")
                except Exception as e:
                    arquivo_nome = key.split('/')[-1]
                    log("WARN", f"  ✗ Erro ao carregar {arquivo_nome}: {e}")
            
            if not dfs_inf:
                raise ValueError("Nenhum arquivo inf_diario_fi encontrado no S3 para o período")
            
            df_inf_diario = pd.concat(dfs_inf, ignore_index=True)
            del dfs_inf  # Libera memória dos arquivos carregados
            gc.collect()
            
            # Diagnóstico: datas carregadas do S3
            col_data_carregada = None
            for variacoes in [['DT_COMPTC', 'dt_comptc', 'Data'], ['Data_Competencia', 'data_competencia']]:
                for var in variacoes:
                    if var.lower() in [c.lower() for c in df_inf_diario.columns]:
                        col_data_carregada = var
                        break
                if col_data_carregada:
                    break
            
            if col_data_carregada:
                datas_carregadas = pd.to_datetime(df_inf_diario[col_data_carregada], errors='coerce')
                log("INFO", f"  ✓ Informe diário (total): {len(df_inf_diario)} registros, datas de {datas_carregadas.min().date()} a {datas_carregadas.max().date()}")
            else:
                log("INFO", f"  ✓ Informe diário (total): {len(df_inf_diario)} registros")
            
            df_registro_classe = ler_csv_s3(s3_client, bucket_silver, key_classe)
            df_registro_fundo = ler_csv_s3(s3_client, bucket_silver, key_fundo)
        
        log("INFO", f"  ✓ Informe diário: {len(df_inf_diario)} registros")
        log("INFO", f"  ✓ Registro classe: {len(df_registro_classe)} registros")
        log("INFO", f"  ✓ Registro fundo: {len(df_registro_fundo)} registros")
        
        # ========== FASE 1: TABELAS REAIS ==========
        
        log("INFO", "[2/4] Criando tabelas reais (CVM)...")
        
        dim_tempo = criar_dim_tempo(data_inicio, data_fim)
        dim_fundo = criar_dim_fundo(df_registro_classe, df_registro_fundo)
        fct_fundo_diario = criar_fct_fundo_diario(df_inf_diario, dim_tempo, dim_fundo)
        del df_inf_diario, df_registro_classe, df_registro_fundo  # Libera dados de input
        gc.collect()
        
        log("INFO", f"  ✓ fct_fundo_diario: {len(fct_fundo_diario)} registros")
        agg_fundo_periodo = criar_agg_fundo_periodo(fct_fundo_diario, periodos_agregacao)
        gc.collect()
        log("INFO", f"  ✓ agg_fundo_periodo: {len(agg_fundo_periodo)} registros")
        
        # ========== SALVA GOLD ==========
        
        log("INFO", "[3/4] Salvando tabelas Gold...")
        gc.collect()  # Limpa memória antes de salvar
        
        if modo == "local":
            path_gold = Path("data/gold")
            
            # Dimensões (sem particionamento)
            path_dim_tempo = path_gold / "dim_tempo"
            path_dim_tempo.mkdir(parents=True, exist_ok=True)
            dim_tempo.to_parquet(f"{path_dim_tempo}/data.parquet", index=False, compression="snappy")
            dim_tempo.to_csv(f"{path_dim_tempo}/data.csv", index=False, sep=';', encoding='utf-8')
            tabelas_processadas.append(TabelaGoldProcessada("dim_tempo", len(dim_tempo), 0.1))
            
            path_dim_fundo = path_gold / "dim_fundo"
            path_dim_fundo.mkdir(parents=True, exist_ok=True)
            dim_fundo.to_parquet(f"{path_dim_fundo}/data.parquet", index=False, compression="snappy")
            dim_fundo.to_csv(f"{path_dim_fundo}/data.csv", index=False, sep=';', encoding='utf-8')
            tabelas_processadas.append(TabelaGoldProcessada("dim_fundo", len(dim_fundo), 0.1))
            
            # Fatos (com particionamento por mês)
            tamanho = _salvar_particionado_local(fct_fundo_diario, "sk_data", "fct_fundo_diario", path_gold)
            tabelas_processadas.append(TabelaGoldProcessada("fct_fundo_diario", len(fct_fundo_diario), tamanho))
            
            tamanho = _salvar_particionado_local(agg_fundo_periodo, "data_referencia", "agg_fundo_periodo", path_gold)
            tabelas_processadas.append(TabelaGoldProcessada("agg_fundo_periodo", len(agg_fundo_periodo), tamanho))
        else:
            s3_client = boto3.client("s3")
            
            log("INFO", f"  Salvando em S3: s3://{bucket_gold}/{prefixo_gold}")
            
            # Dimensões (sem particionamento)
            log("INFO", "  Salvando dimensões...")
            salvar_parquet_s3(s3_client, dim_tempo, bucket_gold, f"{prefixo_gold}/dim_tempo/data.parquet")
            tabelas_processadas.append(TabelaGoldProcessada("dim_tempo", len(dim_tempo), 0.1))
            
            salvar_parquet_s3(s3_client, dim_fundo, bucket_gold, f"{prefixo_gold}/dim_fundo/data.parquet")
            tabelas_processadas.append(TabelaGoldProcessada("dim_fundo", len(dim_fundo), 0.1))
            
            # Fatos (com particionamento por mês)
            log("INFO", "  Salvando fatos com particionamento...")
            tamanho = _salvar_particionado_s3(s3_client, fct_fundo_diario, "sk_data", "fct_fundo_diario", bucket_gold, prefixo_gold)
            tabelas_processadas.append(TabelaGoldProcessada("fct_fundo_diario", len(fct_fundo_diario), tamanho))
            
            tamanho = _salvar_particionado_s3(s3_client, agg_fundo_periodo, "data_referencia", "agg_fundo_periodo", bucket_gold, prefixo_gold)
            tabelas_processadas.append(TabelaGoldProcessada("agg_fundo_periodo", len(agg_fundo_periodo), tamanho))
        
        # ========== VALIDACOES BASICAS ==========
        
        log("INFO", "[4/4] Validando qualidade...")
        
        # Reconciliação
        fluxo_reconciliado = (
            fct_fundo_diario['fluxo_liquido_dia'] ==
            fct_fundo_diario['captacao_dia'] - fct_fundo_diario['resgate_dia']
        ).all()
        
        if not fluxo_reconciliado:
            erros.append("Falha na reconciliação: fluxo != captação - resgate")
            log("ERROR", "  ✗ Reconciliação falhou")
        else:
            log("INFO", "  ✓ Reconciliação: fluxo = captação - resgate")
        
        # Valores negativos
        if (fct_fundo_diario['patrimonio_liquido_dia'] < 0).any():
            erros.append("Patrimônio negativo detectado")
            log("ERROR", "  ✗ Patrimônio negativo")
        else:
            log("INFO", "  ✓ Sem valores negativos")
        
        # Score de risco
        if not (agg_fundo_periodo['score_risco_fundo'].between(0, 100)).all():
            erros.append("Score de risco fora do intervalo 0-100")
            log("ERROR", "  ✗ Score de risco inválido")
        else:
            log("INFO", "  ✓ Score de risco válido (0-100)")
        
        log("INFO", "=" * 70)
        log("INFO", "✓ LAMBDA COMPLETADA COM SUCESSO")
        log("INFO", "=" * 70)
        
        # ========== RESPOSTA ==========
        
        status_code = 207 if erros else 200
        
        body = {
            "status": "success" if not erros else "partial",
            "modo": modo,
            "data_inicio": data_inicio,
            "data_fim": data_fim,
            "periodos_agregacao": periodos_agregacao,
            "tabelas_processadas": [
                {
                    "nome": t.nome,
                    "quantidade_registros": t.quantidade_registros,
                    "tamanho_mb": round(t.tamanho_mb, 2)
                }
                for t in tabelas_processadas
            ],
            "erros": erros
        }
        
        return {
            "statusCode": status_code,
            "body": json.dumps(body, ensure_ascii=False, indent=2)
        }
    
    except Exception as erro:
        log("ERROR", f"Erro não tratado: {erro}")
        body = {
            "status": "error",
            "mensagem": str(erro),
            "erros": [str(erro)]
        }
        return {
            "statusCode": 500,
            "body": json.dumps(body, ensure_ascii=False, indent=2)
        }


# ========== TESTE LOCAL ==========

if __name__ == "__main__":
    """Teste local da lambda"""
    
    # Teste modo local
    print("\n🧪 TESTE LOCAL (modo=local)")
    resultado = lambda_handler({
        "modo": "local",
        "data_inicio": "2023-01-01",
        "data_fim": "2024-12-31",
        "periodos_agregacao": [7, 30, 60, 90]
    }, None)
    
    print(f"\nStatus: {resultado['statusCode']}")
    print(resultado['body'])

"""
Modulo de funcoes para criacao da camada Gold
Implementa dimensoes e fatos conforme planejamento_gold_simulacao.md
TOTALMENTE LOCAL - Sem dependencia de S3
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Tuple, Optional
import hashlib
import glob
from pathlib import Path


class CamadaGold:
    """Classe responsavel pela criacao das tabelas da camada Gold"""
    
    def __init__(self, path_silver: str):
        """
        Inicializa a camada Gold
        
        Args:
            path_silver: caminho para os dados silver limpo
        """
        self.path_silver = path_silver
        self.df_inf_diario = None
        self.df_registro_fundo = None
        self.df_registro_classe = None
        self.dim_tempo = None
        self.dim_fundo = None
        self.fct_fundo_diario = None
        self.agg_fundo_periodo = None
    
    def _detectar_separador(self, filepath: str) -> str:
        """
        Detecta automaticamente o separador do CSV
        (`,` vs `;` comum em dados brasileiros)
        
        Args:
            filepath: caminho do arquivo CSV
        
        Returns:
            Separador detectado (`,` ou `;`)
        """
        try:
            with open(filepath, 'r', encoding='latin-1') as f:
                primeira_linha = f.readline()
            
            # Testa qual separador tem mais colunas
            virgulas = primeira_linha.count(',')
            ponto_virgulas = primeira_linha.count(';')
            
            sep = ';' if ponto_virgulas > virgulas else ','
            print(f"    Separador detectado: '{sep}' (encontrados {max(virgulas, ponto_virgulas)} campos)")
            return sep
            
        except Exception as e:
            print(f"    ⚠️ Erro ao detectar separador, usando ','")
            return ','
        
    def carregar_dados_silver(self) -> None:
        """Carrega arquivos silver limpiados (LOCAL) com tratamento de erros"""
        try:
            # Carrega informe diario com wildcard (LOCAL)
            arquivos_inf = glob.glob(f"{self.path_silver}/inf_diario_fi_*_clean.csv")
            if not arquivos_inf:
                raise FileNotFoundError(f"Nenhum arquivo 'inf_diario_fi_*_clean.csv' encontrado em {self.path_silver}")
            
            print(f"✓ Encontrados {len(arquivos_inf)} arquivo(s) de informe diário")
            
            # Lê múltiplos arquivos com tratamento robusto
            dfs_inf = []
            for arquivo in arquivos_inf:
                try:
                    print(f"  Lendo: {Path(arquivo).name}...", end=" ")
                    
                    # Detecta separador
                    sep = self._detectar_separador(arquivo)
                    
                    df_temp = pd.read_csv(
                        arquivo,
                        sep=sep,                # ✅ Detecta `,` ou `;`
                        encoding='latin-1',
                        on_bad_lines='warn',
                        engine='python'
                    )
                    dfs_inf.append(df_temp)
                    print(f"✓ ({len(df_temp)} linhas, {len(df_temp.columns)} colunas)")
                except Exception as e:
                    print(f"⚠️ Erro ao ler {arquivo}: {e}")
                    continue
            
            if not dfs_inf:
                raise ValueError("Nenhum arquivo de informe diário pôde ser lido com sucesso")
            
            self.df_inf_diario = pd.concat(dfs_inf, ignore_index=True)
            
            # Carrega registro fundo (LOCAL) com tratamento robusto
            arquivo_fundo = f"{self.path_silver}/registro_fundo_clean.csv"
            if not Path(arquivo_fundo).exists():
                raise FileNotFoundError(f"Arquivo não encontrado: {arquivo_fundo}")
            print(f"  Lendo: registro_fundo_clean.csv...", end=" ")
            
            sep_fundo = self._detectar_separador(arquivo_fundo)
            
            self.df_registro_fundo = pd.read_csv(
                arquivo_fundo,
                sep=sep_fundo,
                encoding='latin-1',
                on_bad_lines='warn',
                engine='python'
            )
            print(f"✓ ({len(self.df_registro_fundo)} linhas, {len(self.df_registro_fundo.columns)} colunas)")
            
            # Carrega registro classe (LOCAL) com tratamento robusto
            arquivo_classe = f"{self.path_silver}/registro_classe_clean.csv"
            if not Path(arquivo_classe).exists():
                raise FileNotFoundError(f"Arquivo não encontrado: {arquivo_classe}")
            print(f"  Lendo: registro_classe_clean.csv...", end=" ")
            
            sep_classe = self._detectar_separador(arquivo_classe)
            
            self.df_registro_classe = pd.read_csv(
                arquivo_classe,
                sep=sep_classe,
                encoding='latin-1',
                on_bad_lines='warn',
                engine='python'
            )
            print(f"✓ ({len(self.df_registro_classe)} linhas, {len(self.df_registro_classe.columns)} colunas)")
            
            print("✓ Todos os dados silver carregados com sucesso (100% LOCAL)")
            print(f"  - Informe diário (total): {len(self.df_inf_diario)} registros")
            print(f"  - Registro fundo: {len(self.df_registro_fundo)} registros")
            print(f"  - Registro classe: {len(self.df_registro_classe)} registros")
            
            # Inspeciona e lista as colunas disponíveis
            self._inspecionar_estrutura()
            
        except Exception as e:
            print(f"\n✗ Erro ao carregar dados silver: {e}")
            print(f"  Verifique se os arquivos existem em: {self.path_silver}")
            print(f"  Arquivos esperados:")
            print(f"    - inf_diario_fi_*_clean.csv (um ou múltiplos)")
            print(f"    - registro_fundo_clean.csv")
            print(f"    - registro_classe_clean.csv")
            raise
    
    def _inspecionar_estrutura(self) -> None:
        """Inspeciona e exibe a estrutura dos dados carregados"""
        print("\n" + "=" * 70)
        print("INSPEÇÃO DE ESTRUTURA DOS DADOS SILVER")
        print("=" * 70)
        
        if self.df_inf_diario is not None:
            print(f"\n📋 Informe Diário:")
            print(f"  Colunas ({len(self.df_inf_diario.columns)}): {list(self.df_inf_diario.columns)}")
            
        if self.df_registro_fundo is not None:
            print(f"\n📋 Registro Fundo:")
            print(f"  Colunas ({len(self.df_registro_fundo.columns)}): {list(self.df_registro_fundo.columns)}")
            
        if self.df_registro_classe is not None:
            print(f"\n📋 Registro Classe:")
            print(f"  Colunas ({len(self.df_registro_classe.columns)}): {list(self.df_registro_classe.columns)}")
        
        print("=" * 70 + "\n")
    
    def _encontrar_coluna(self, df: pd.DataFrame, variacoes: list) -> str:
        """
        Encontra uma coluna no DataFrame testando múltiplas variações
        Case-insensitive e com alias
        
        Args:
            df: DataFrame onde procurar
            variacoes: lista de possíveis nomes da coluna
        
        Returns:
            Nome da coluna encontrada (exato, como está no DataFrame)
        
        Raises:
            KeyError se nenhuma variação for encontrada
        """
        colunas_lower = {col.lower(): col for col in df.columns}
        
        for var in variacoes:
            if var.lower() in colunas_lower:
                return colunas_lower[var.lower()]
        
        raise KeyError(
            f"Coluna não encontrada. Testadas: {variacoes} | "
            f"Disponíveis: {list(df.columns)}"
        )
    
    def _encontrar_coluna_opcional(self, df: pd.DataFrame, variacoes: list) -> Optional[str]:
        """
        Encontra uma coluna opcional no DataFrame
        Retorna None se não encontrada (em vez de erro)
        
        Args:
            df: DataFrame onde procurar
            variacoes: lista de possíveis nomes da coluna
        
        Returns:
            Nome da coluna encontrada ou None
        """
        try:
            return self._encontrar_coluna(df, variacoes)
        except KeyError:
            return None
    
    # ========== DIM_TEMPO ==========
    def criar_dim_tempo(self, data_inicio: datetime = None, data_fim: datetime = None) -> pd.DataFrame:
        """
        Cria dimensao de tempo usando apenas D-1 (ontem)
        
        Args:
            data_inicio: ignorado (mantido por compatibilidade)
            data_fim: ignorado (mantido por compatibilidade)
        
        Returns:
            DataFrame com dim_tempo
        """
        # Determina o primeiro dia do mês atual até hoje
        hoje = datetime.now().date()
        primeiro_dia_mes_atual = hoje.replace(day=1)
        datas = pd.date_range(start=primeiro_dia_mes_atual, end=hoje, freq='D')
        
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
        
        self.dim_tempo = dim_tempo
        print(f"✓ dim_tempo criada: {len(dim_tempo)} registros ({primeiro_dia_mes_atual} a {hoje})")
        return dim_tempo
    
    # ========== DIM_FUNDO ==========
    def criar_dim_fundo(self) -> pd.DataFrame:
        """
        Cria dimensao de fundo de forma adaptativa
        Se adapta às colunas disponíveis no registro_classe
        Tenta fazer join com registro_fundo se disponível
        
        Returns:
            DataFrame com dim_fundo
        """
        try:
            print(f"  Detectando colunas...")
            
            # Encontra coluna CNPJ (obrigatória)
            col_cnpj_classe = self._encontrar_coluna(self.df_registro_classe, 
                                                      ['CNPJ_FUNDO_CLASSE', 'cnpj_fundo_classe', 'CNPJ_Classe', 'cnpj_classe', 'CNPJ_Fundo_Classe'])
            
            # Encontra nome do fundo (pode ter várias variações)
            col_denom = self._encontrar_coluna_opcional(self.df_registro_classe, 
                                               ['DENOM_SOCIAL', 'denom_social', 'Denominacao_Social', 'denominacao_social', 'nome', 'Nome'])
            
            print(f"    ✓ Mapeamento detectado:")
            print(f"      - CNPJ_FUNDO_CLASSE: {col_cnpj_classe}")
            if col_denom:
                print(f"      - DENOM_SOCIAL: {col_denom}")
            
            # Cria cópia para não modificar original
            dim_fundo = self.df_registro_classe.copy()
            
            # Normaliza CNPJ (apenas digitos)
            dim_fundo['cnpj_fundo_classe_norm'] = (
                dim_fundo[col_cnpj_classe]
                .astype(str)
                .str.replace(r'\D', '', regex=True)
            )
            
            # Cria surrogate key (hash do CNPJ)
            dim_fundo['sk_fundo'] = dim_fundo['cnpj_fundo_classe_norm'].apply(
                lambda x: int(hashlib.md5(str(x).encode()).hexdigest(), 16) % 1000000
            )
            
            # Seleciona colunas principais (mínimo viável)
            colunas_desejadas = [
                'sk_fundo',
                'cnpj_fundo_classe_norm',
                col_cnpj_classe
            ]
            
            if col_denom:
                colunas_desejadas.append(col_denom)
            
            # Adiciona outras colunas opcionais se existirem
            colunas_opcionais = [
                ['Tipo_Classe', 'TIPO_FUNDO', 'tipo_fundo'],
                ['Classificacao', 'CLASSIFICACAO', 'classificacao'],
                ['Situacao', 'SITUACAO', 'situacao'],
                ['Publico_Alvo', 'PUBLICO_ALVO', 'publico_alvo'],
            ]
            
            for variacoes in colunas_opcionais:
                col_opt = self._encontrar_coluna_opcional(dim_fundo, variacoes)
                if col_opt:
                    colunas_desejadas.append(col_opt)
            
            dim_fundo = dim_fundo[colunas_desejadas].drop_duplicates(subset=['sk_fundo'])
            
            # Renomeia colunas para padrão
            nomes_novo = {
                'sk_fundo': 'sk_fundo',
                'cnpj_fundo_classe_norm': 'cnpj_fundo_classe_norm',
                col_cnpj_classe: 'cnpj_fundo_classe',
            }
            if col_denom:
                nomes_novo[col_denom] = 'nome_fundo'
            
            dim_fundo = dim_fundo.rename(columns=nomes_novo)
            
            self.dim_fundo = dim_fundo
            print(f"✓ dim_fundo criada: {len(dim_fundo)} fundos (adaptativo)")
            return dim_fundo
            
        except Exception as e:
            print(f"✗ Erro ao criar dim_fundo: {e}")
            print(f"  Colunas disponíveis: {list(self.df_registro_classe.columns)}")
            raise
    
    # ========== FCT_FUNDO_DIARIO ==========
    def criar_fct_fundo_diario(self) -> pd.DataFrame:
        """
        Cria fato de fundo diário de forma adaptativa
        Se adapta às colunas disponíveis no informe diário
        
        Returns:
            DataFrame com fct_fundo_diario
        """
        try:
            df = self.df_inf_diario.copy()
            
            # Encontra colunas dinâmicamente (com fallbacks)
            print(f"  Detectando colunas do informe diário...")
            
            col_cnpj_classe = self._encontrar_coluna(df, ['CNPJ_FUNDO_CLASSE', 'cnpj_fundo_classe'])
            col_data = self._encontrar_coluna(df, ['DT_COMPTC', 'dt_comptc', 'data'])
            col_pl = self._encontrar_coluna(df, ['VL_PATRIM_LIQ', 'vl_patrim_liq', 'patrimonio'])
            col_cotistas = self._encontrar_coluna(df, ['NR_COTST', 'nr_cotst', 'cotistas'])
            
            # Colunas opcionais
            try:
                col_captacao = self._encontrar_coluna(df, ['CAPTC_DIA', 'captc_dia', 'captacao'])
            except KeyError:
                col_captacao = None
                print(f"    ⚠️ Captação não encontrada")
            
            try:
                col_resgate = self._encontrar_coluna(df, ['RESG_DIA', 'resg_dia', 'resgate'])
            except KeyError:
                col_resgate = None
                print(f"    ⚠️ Resgate não encontrado")
            
            print(f"    ✓ Mapeamento detectado:")
            print(f"      - CNPJ_FUNDO_CLASSE: {col_cnpj_classe}")
            print(f"      - Data: {col_data}")
            print(f"      - PL: {col_pl}")
            print(f"      - Cotistas: {col_cotistas}")
            if col_captacao:
                print(f"      - Captação: {col_captacao}")
            if col_resgate:
                print(f"      - Resgate: {col_resgate}")
            
            # Remove linhas com valores críticos nulos
            df = df.dropna(subset=[col_cnpj_classe, col_data])
            
            # Normaliza CNPJ
            df['cnpj_fundo_classe_norm'] = (
                df[col_cnpj_classe].astype(str).str.replace(r'\D', '', regex=True)
            )
            
            # Converte data com flexibilidade
            df['DT_COMPTC'] = pd.to_datetime(df[col_data], format='%Y-%m-%d', errors='coerce')
            df = df.dropna(subset=['DT_COMPTC'])
            df['sk_data'] = df['DT_COMPTC'].dt.strftime('%Y%m%d').astype(int)
            
            # Join com dim_fundo
            fct = df.merge(
                self.dim_fundo[['sk_fundo', 'cnpj_fundo_classe_norm']],
                on='cnpj_fundo_classe_norm',
                how='left'
            )
            
            # Remove registros sem fundo válido
            fct = fct.dropna(subset=['sk_fundo'])
            
            # Calcula variações
            fct = fct.sort_values(['sk_fundo', 'DT_COMPTC'])
            fct['variacao_pl_dia_pct'] = fct.groupby('sk_fundo')[col_pl].pct_change() * 100
            fct['variacao_cotistas_dia_pct'] = fct.groupby('sk_fundo')[col_cotistas].pct_change() * 100
            
            # Calcula fluxo liquido se disponível
            if col_captacao and col_resgate:
                fct['fluxo_liquido_dia'] = fct[col_captacao] - fct[col_resgate]
            else:
                fct['fluxo_liquido_dia'] = np.nan
            
            # Seleciona colunas principais
            colunas_fct = [
                'sk_data',
                'sk_fundo',
                'cnpj_fundo_classe_norm',
                col_pl,
                col_cotistas,
                'fluxo_liquido_dia',
                'variacao_pl_dia_pct',
                'variacao_cotistas_dia_pct'
            ]
            
            # Adiciona colunas opcionais se existem
            if col_captacao:
                colunas_fct.insert(4, col_captacao)
            if col_resgate:
                colunas_fct.insert(5, col_resgate)
            
            fct_fundo_diario = fct[colunas_fct].copy()
            
            # Renomeia para padrão
            novo_nome = {
                col_pl: 'patrimonio_liquido_dia',
                col_cotistas: 'cotistas_dia'
            }
            if col_captacao:
                novo_nome[col_captacao] = 'captacao_dia'
            if col_resgate:
                novo_nome[col_resgate] = 'resgate_dia'
            
            fct_fundo_diario = fct_fundo_diario.rename(columns=novo_nome)
            
            # Flag de base real
            fct_fundo_diario['flag_base_real'] = True
            
            self.fct_fundo_diario = fct_fundo_diario
            print(f"✓ fct_fundo_diario criada: {len(fct_fundo_diario)} registros (adaptativo)")
            return fct_fundo_diario
            
        except Exception as e:
            print(f"✗ Erro ao criar fct_fundo_diario: {e}")
            raise
        
        # Join com dim_fundo
        fct = df.merge(
            self.dim_fundo[['sk_fundo', 'cnpj_fundo_classe_norm']],
            on='cnpj_fundo_classe_norm',
            how='left'
        )
        
        # Calcula variacoes
        fct = fct.sort_values(['sk_fundo', 'DT_COMPTC'])
        fct['variacao_pl_dia_pct'] = fct.groupby('sk_fundo')['VL_PATRIM_LIQ'].pct_change() * 100
        fct['variacao_cotistas_dia_pct'] = fct.groupby('sk_fundo')['NR_COTST'].pct_change() * 100
        
        # Calcula fluxo liquido
        fct['fluxo_liquido_dia'] = fct['CAPTC_DIA'] - fct['RESG_DIA']
        
        # Seleciona colunas principais
        fct_fundo_diario = fct[[
            'sk_data',
            'sk_fundo',
            'cnpj_fundo_classe_norm',
            'CAPTC_DIA',
            'RESG_DIA',
            'fluxo_liquido_dia',
            'VL_PATRIM_LIQ',
            'NR_COTST',
            'variacao_pl_dia_pct',
            'variacao_cotistas_dia_pct'
        ]].dropna(subset=['sk_fundo'])
        
        fct_fundo_diario.columns = [
            'sk_data',
            'sk_fundo',
            'cnpj_fundo_classe_norm',
            'captacao_dia',
            'resgate_dia',
            'fluxo_liquido_dia',
            'patrimonio_liquido_dia',
            'cotistas_dia',
            'variacao_pl_dia_pct',
            'variacao_cotistas_dia_pct'
        ]
        
        # Flag de base real
        fct_fundo_diario['flag_base_real'] = True
        
        self.fct_fundo_diario = fct_fundo_diario
        print(f"✓ fct_fundo_diario criada: {len(fct_fundo_diario)} registros")
        return fct_fundo_diario
    
    # ========== AGG_FUNDO_PERIODO ==========
    def criar_agg_fundo_periodo(self, periodos: list = [7, 30, 60, 90]) -> pd.DataFrame:
        """
        Cria agregacao de fundo por peri​odo
        
        Args:
            periodos: lista de periodos em dias (default: 7, 30, 60, 90)
        
        Returns:
            DataFrame com agg_fundo_periodo
        """
        agg_lista = []
        
        for periodo in periodos:
            agg = self.fct_fundo_diario.copy()
            agg['periodo'] = periodo
            # Converte sk_data para datetime antes de agregar
            agg['data_temp'] = pd.to_datetime(agg['sk_data'], format='%Y%m%d')
            
            # Agrupa por sk_fundo em janelas moveis
            agg_grouped = agg.groupby(['sk_fundo', 'periodo']).agg({
                'captacao_dia': 'sum',
                'resgate_dia': 'sum',
                'fluxo_liquido_dia': 'sum',
                'variacao_pl_dia_pct': 'mean',
                'variacao_cotistas_dia_pct': 'mean',
                'data_temp': 'max'
            }).reset_index()
            
            agg_grouped.columns = [
                'sk_fundo',
                'periodo',
                'captacao_periodo',
                'resgate_periodo',
                'fluxo_liquido_periodo',
                'var_patrimonio_periodo_pct',
                'var_cotistas_periodo_pct',
                'data_temp'
            ]
            
            # Converte data_temp (datetime) para data_referencia (inteiro YYYYMMDD)
            agg_grouped['data_referencia'] = agg_grouped['data_temp'].dt.strftime('%Y%m%d').astype(int)
            agg_grouped = agg_grouped.drop(columns=['data_temp'])
            
            # Calcula score de risco (simplificado v1)
            agg_grouped['score_risco_fundo'] = (
                (agg_grouped['var_patrimonio_periodo_pct'].abs() * 0.5) +
                (agg_grouped['var_cotistas_periodo_pct'].abs() * 0.5)
            ).clip(0, 100)
            
            # Classifica risco
            agg_grouped['nivel_risco_fundo'] = pd.cut(
                agg_grouped['score_risco_fundo'],
                bins=[0, 25, 50, 100],
                labels=['baixo', 'medio', 'alto']
            )
            
            agg_lista.append(agg_grouped)
        
        agg_fundo_periodo = pd.concat(agg_lista, ignore_index=True)
        self.agg_fundo_periodo = agg_fundo_periodo
        print(f"✓ agg_fundo_periodo criada: {len(agg_fundo_periodo)} registros")
        return agg_fundo_periodo
    
    def exportar_gold(self, path_output: str) -> None:
        """
        Exporta todas as tabelas gold com particionamento por mês para fatos
        Formatos: Parquet (binário, comprimido) + CSV (texto, legível)
        
        Estrutura de particionamento:
        - dim_tempo/ -> arquivo único
        - dim_fundo/ -> arquivo único
        - fct_fundo_diario/ -> particionado por ano_mes (YYYYMM)
        - agg_fundo_periodo/ -> particionado por ano_mes (YYYYMM)
        
        Args:
            path_output: caminho local para salvar arquivos
        """
        try:
            # Cria diretório se não existir
            Path(path_output).mkdir(parents=True, exist_ok=True)
            
            print(f"✓ Exportando tabelas Gold com particionamento por mês:")
            print(f"  - Caminho: {path_output}")
            
            # ========== DIMENSOES (SEM PARTICIONAMENTO) ==========
            
            # dim_tempo
            path_dim_tempo = Path(path_output) / "dim_tempo"
            path_dim_tempo.mkdir(parents=True, exist_ok=True)
            self.dim_tempo.to_parquet(f"{path_dim_tempo}/data.parquet", index=False)
            self.dim_tempo.to_csv(f"{path_dim_tempo}/data.csv", index=False, sep=';', encoding='latin-1')
            print(f"  ✓ dim_tempo/ (arquivo único)")
            
            # dim_fundo
            path_dim_fundo = Path(path_output) / "dim_fundo"
            path_dim_fundo.mkdir(parents=True, exist_ok=True)
            self.dim_fundo.to_parquet(f"{path_dim_fundo}/data.parquet", index=False)
            self.dim_fundo.to_csv(f"{path_dim_fundo}/data.csv", index=False, sep=';', encoding='latin-1')
            print(f"  ✓ dim_fundo/ (arquivo único)")
            
            # ========== FATOS (COM PARTICIONAMENTO POR MÊS) ==========
            
            # fct_fundo_diario particionado por ano_mes
            self._exportar_particionado(
                self.fct_fundo_diario,
                "sk_data",
                "fct_fundo_diario",
                path_output
            )
            print(f"  ✓ fct_fundo_diario/ (particionado por YYYYMM)")
            
            # agg_fundo_periodo particionado por ano_mes (data_referencia)
            self._exportar_particionado(
                self.agg_fundo_periodo,
                "data_referencia",
                "agg_fundo_periodo",
                path_output
            )
            print(f"  ✓ agg_fundo_periodo/ (particionado por YYYYMM)")
            
        except Exception as e:
            print(f"✗ Erro ao exportar gold: {e}")
            raise
    
    def _exportar_particionado(self, df: pd.DataFrame, col_data: str, 
                               nome_tabela: str, path_output: str) -> None:
        """
        Exporta tabela particionada por ano_mes (YYYYMM)
        
        Args:
            df: DataFrame a exportar
            col_data: coluna com a data (formato: YYYYMMDD como inteiro)
            nome_tabela: nome da tabela
            path_output: caminho base para saída
        """
        try:
            # Cria coluna de ano_mes (YYYYMM) a partir da data
            df_temp = df.copy()
            
            # Extrai ano_mes: YYYYMMDD // 100 = YYYYMM
            df_temp['ano_mes'] = (df_temp[col_data] // 100).astype(str)
            
            # Cria diretório da tabela
            path_tabela = Path(path_output) / nome_tabela
            path_tabela.mkdir(parents=True, exist_ok=True)
            
            # Agrupa por ano_mes e salva cada mês em uma pasta
            for ano_mes, grupo in df_temp.groupby('ano_mes', sort=True):
                # Remove a coluna de ano_mes antes de salvar
                grupo_clean = grupo.drop(columns=['ano_mes'])
                
                # Cria diretório do mês
                path_mes = path_tabela / f"ano_mes={ano_mes}"
                path_mes.mkdir(parents=True, exist_ok=True)
                
                # Salva parquet e csv
                grupo_clean.to_parquet(f"{path_mes}/data.parquet", index=False)
                grupo_clean.to_csv(f"{path_mes}/data.csv", index=False, sep=';', encoding='latin-1')
        
        except Exception as e:
            print(f"✗ Erro ao exportar particionado {nome_tabela}: {e}")
            raise


class SimuladorClientes:
    """Classe para simular dados de clientes (dim_cliente_simulado e fatos simulados)"""
    
    def __init__(self, seed: int = 42):
        """
        Inicializa simulador
        
        Args:
            seed: seed para reproducibilidade
        """
        np.random.seed(seed)
        self.seed = seed
        self.dim_cliente = None
        self.fct_cliente_posicao = None
        self.fct_cliente_risco = None
    
    def gerar_dim_cliente_simulado(self, n_clientes: int = 10000) -> pd.DataFrame:
        """
        Gera dimensao de cliente simulada
        Baseado no padrão do simulador_aplicacao_resgate
        
        Args:
            n_clientes: quantidade de clientes a gerar
        
        Returns:
            DataFrame com dim_cliente_simulado
        """
        segmentos = ['varejo', 'private', 'institucional']
        dist_segmentos = [0.70, 0.20, 0.10]  # 70% varejo, 20% private, 10% institucional
        
        perfis_risco = ['conservador', 'moderado', 'arrojado', 'agressivo']
        
        # Distribuição por perfil (mais conservadores)
        dist_perfil = {
            'varejo': [0.40, 0.35, 0.20, 0.05],
            'private': [0.15, 0.30, 0.35, 0.20],
            'institucional': [0.05, 0.15, 0.30, 0.50]
        }
        
        ufs = ['SP', 'RJ', 'MG', 'BA', 'RS', 'PR', 'SC', 'ES', 'DF', 'PE']
        cidades_por_uf = {
            'SP': ['São Paulo', 'Campinas', 'Santos', 'Ribeirão Preto'],
            'RJ': ['Rio de Janeiro', 'Niterói', 'Duque de Caxias'],
            'MG': ['Belo Horizonte', 'Uberlândia', 'Contagem'],
            'BA': ['Salvador', 'Feira de Santana', 'Vitória da Conquista'],
            'RS': ['Porto Alegre', 'Caxias do Sul', 'Pelotas'],
            'PR': ['Curitiba', 'Londrina', 'Maringá'],
            'SC': ['Florianópolis', 'Joinville', 'Blumenau'],
            'ES': ['Vitória', 'Vila Velha', 'Cariacica'],
            'DF': ['Brasília'],
            'PE': ['Recife', 'Jaboatão', 'Olinda']
        }
        
        canais = ['digital', 'agencia', 'indicacao', 'outro']
        data_inicio_min = datetime(2018, 1, 1)
        data_fim_max = datetime(2026, 4, 19)
        
        # Gera segmentos
        segmentos_list = np.random.choice(segmentos, n_clientes, p=dist_segmentos)
        
        # Gera perfil de risco por segmento
        perfis_list = []
        for seg in segmentos_list:
            perfis_list.append(np.random.choice(perfis_risco, p=dist_perfil[seg]))
        
        # Gera UFs
        ufs_list = np.random.choice(ufs, n_clientes)
        
        # Gera cidades baseado em UF
        cidades_list = [
            np.random.choice(cidades_por_uf[uf])
            for uf in ufs_list
        ]
        
        # Gera datas de nascimento (clientes entre 21 e 75 anos)
        hoje = datetime(2026, 4, 19)
        datas_nasc = [
            hoje - timedelta(days=int(np.random.normal(loc=45*365, scale=15*365)))
            for _ in range(n_clientes)
        ]
        
        # Gera renda por segmento (exponencial com média realista)
        renda_list = []
        for seg in segmentos_list:
            if seg == 'varejo':
                renda = np.random.lognormal(mean=10.5, sigma=0.8)  # Média ~R$50k
            elif seg == 'private':
                renda = np.random.lognormal(mean=11.5, sigma=0.9)  # Média ~R$100k
            else:  # institucional
                renda = np.random.lognormal(mean=12.5, sigma=1.0)  # Média ~R$250k
            renda_list.append(max(20000, renda))  # Mínimo R$20k
        
        # Gera patrimônio (correlacionado com renda e segmento)
        patrimonio_list = []
        for renda, seg in zip(renda_list, segmentos_list):
            if seg == 'varejo':
                patrimonio = renda * np.random.uniform(2, 5)
            elif seg == 'private':
                patrimonio = renda * np.random.uniform(5, 15)
            else:  # institucional
                patrimonio = renda * np.random.uniform(10, 50)
            patrimonio_list.append(patrimonio)
        
        # Gera data de início do relacionamento
        datas_relacionamento = [
            data_inicio_min + timedelta(days=int(np.random.rand() * (data_fim_max - data_inicio_min).days))
            for _ in range(n_clientes)
        ]
        
        # Gera nomes realistas (simulado)
        primeiros_nomes = ['José', 'Maria', 'João', 'Ana', 'Carlos', 'Paula', 'Francisco', 'Lucia', 'Antonio', 'Fernanda']
        sobrenomes = ['Silva', 'Santos', 'Oliveira', 'Souza', 'Costa', 'Ferreira', 'Gomes', 'Martins', 'Alves', 'Pereira']
        nomes_list = [f"{np.random.choice(primeiros_nomes)} {np.random.choice(sobrenomes)}" for _ in range(n_clientes)]
        
        dim_cliente = pd.DataFrame({
            'sk_cliente': range(1, n_clientes + 1),
            'id_cliente': [f"CLI_{i:07d}" for i in range(1, n_clientes + 1)],
            'nome_cliente': nomes_list,
            'data_nascimento': datas_nasc,
            'faixa_etaria': pd.cut(
                [(hoje - d).days / 365.25 for d in datas_nasc],
                bins=[20, 30, 40, 50, 60, 100],
                labels=['21-30', '31-40', '41-50', '51-60', '60+']
            ),
            'perfil_risco': perfis_list,
            'segmento': segmentos_list,
            'uf': ufs_list,
            'cidade': cidades_list,
            'renda_estimada': renda_list,
            'patrimonio_estimado': patrimonio_list,
            'data_inicio_relacionamento': datas_relacionamento,
            'canal_origem': np.random.choice(canais, n_clientes, p=[0.40, 0.30, 0.20, 0.10]),
            'status_cliente': np.random.choice(['ativo', 'inativo'], n_clientes, p=[0.88, 0.12])
        })
        
        self.dim_cliente = dim_cliente
        print(f"✓ dim_cliente_simulado criada: {n_clientes} clientes (realista)")
        return dim_cliente
    
    def exportar_simulador(self, path_output: str) -> None:
        """
        Exporta dados simulados com particionamento por mês
        Formatos: Parquet (binário, comprimido) + CSV (texto, legível)
        
        Args:
            path_output: caminho local para salvar arquivos
        """
        try:
            # Cria diretório se não existir
            Path(path_output).mkdir(parents=True, exist_ok=True)
            
            # Exporta tabelas (LOCAL)
            print(f"✓ Exportando dados simulados (Parquet + CSV):")
            print(f"  - Caminho: {path_output}")
            
            if self.dim_cliente is not None:
                # dim_cliente sem partição (é dimensão)
                path_dim_cliente = Path(path_output) / "dim_cliente_simulado"
                path_dim_cliente.mkdir(parents=True, exist_ok=True)
                self.dim_cliente.to_parquet(f"{path_dim_cliente}/data.parquet", index=False)
                self.dim_cliente.to_csv(f"{path_dim_cliente}/data.csv", index=False, sep=';', encoding='latin-1')
                print(f"  ✓ dim_cliente_simulado/ (arquivo único)")
            
            if self.fct_cliente_posicao is not None:
                # Particionado por ano_mes
                self._exportar_fato_particionado(
                    self.fct_cliente_posicao,
                    "sk_data",
                    "fct_cliente_posicao_diaria_simulada",
                    path_output
                )
                print(f"  ✓ fct_cliente_posicao_diaria_simulada/ (particionado por mês)")
            
            if self.fct_cliente_risco is not None:
                # Particionado por ano_mes
                self._exportar_fato_particionado(
                    self.fct_cliente_risco,
                    "sk_data",
                    "fct_cliente_risco_diario_simulada",
                    path_output
                )
                print(f"  ✓ fct_cliente_risco_diario_simulada/ (particionado por mês)")
            
        except Exception as e:
            print(f"✗ Erro ao exportar simulador: {e}")
            raise
    
    def _exportar_fato_particionado(self, df: pd.DataFrame, col_data: str,
                                     nome_tabela: str, path_output: str) -> None:
        """Exporta fato particionado por ano_mes"""
        try:
            df_temp = df.copy()
            df_temp['ano_mes'] = (df_temp[col_data] // 100).astype(str)
            
            path_tabela = Path(path_output) / nome_tabela
            path_tabela.mkdir(parents=True, exist_ok=True)
            
            for ano_mes, grupo in df_temp.groupby('ano_mes', sort=True):
                grupo_clean = grupo.drop(columns=['ano_mes'])
                
                path_mes = path_tabela / f"ano_mes={ano_mes}"
                path_mes.mkdir(parents=True, exist_ok=True)
                
                grupo_clean.to_parquet(f"{path_mes}/data.parquet", index=False)
                grupo_clean.to_csv(f"{path_mes}/data.csv", index=False, sep=';', encoding='latin-1')
        
        except Exception as e:
            print(f"✗ Erro ao exportar particionado {nome_tabela}: {e}")
            raise

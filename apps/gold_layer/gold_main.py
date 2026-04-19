"""
Script principal para orquestrar criacao da camada Gold
Baseado em: planejamento_gold_simulacao.md
"""

import sys
import os
from datetime import datetime
from pathlib import Path
import glob

from gold_functions import CamadaGold, SimuladorClientes

# ========== CONFIGURACOES ==========

PROJECT_ROOT = Path(__file__).parent.parent.parent

PATH_DATA_SILVER = PROJECT_ROOT / "dados_tgt"
PATH_OUTPUT_GOLD = PROJECT_ROOT / "data" / "gold"

# Período apenas para teste/desenvolvimento (últimos 2 dias baseado em data atual)
DATA_INICIO_GOLD = "2026-04-18"  # Dia anterior (ontem)
DATA_FIM_GOLD = "2026-04-19"     # Dia atual (hoje)

PERIODOS_AGREGACAO = [7, 30, 60, 90]

N_CLIENTES_SIMULADOS = 10000


def main():
    """
    Funcao principal - orquestra criacao de todas as tabelas gold
    
    Fases:
    1. Carrega dados silver limpiados
    2. Cria dimensoes (tempo, fundo)
    3. Cria fatos reais (fundo_diario, agg_fundo_periodo)
    4. Cria simulacoes (dim_cliente, fatos simulados)
    5. Exporta arquivos em parquet
    """
    
    print("=" * 70)
    print("CAMADA GOLD - ASSET SIRIUS ANALYTICS")
    print("Planejamento: planejamento_gold_simulacao.md")
    print(f"Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    # ========== CONFIG ==========
    # Configuracoes importadas de config.py (LOCAL - Sem S3)
    print(f"\nConfiguração:")
    print(f"  Origem Silver: {PATH_DATA_SILVER}")
    print(f"  Saída Gold: {PATH_OUTPUT_GOLD}")
    print(f"  Período: {DATA_INICIO_GOLD} a {DATA_FIM_GOLD}")
    print(f"  Clientes a simular: {N_CLIENTES_SIMULADOS}")
    print(f"  Agregações: {PERIODOS_AGREGACAO} dias")
    
    # Cria diretorios se nao existem (LOCAL)
    Path(PATH_DATA_SILVER).mkdir(parents=True, exist_ok=True)
    Path(PATH_OUTPUT_GOLD).mkdir(parents=True, exist_ok=True)
    
    # ========== FASE 1: GOLD DE FUNDOS (DADOS REAIS CVM) ==========
    print("\n[FASE 1] Criando Gold de Fundos (dados reais CVM)...")
    print("-" * 70)
    
    try:
        # Inicializa camada gold (LOCAL - Sem S3)
        gold = CamadaGold(path_silver=str(PATH_DATA_SILVER))
        
        # Carrega dados silver (LOCAL)
        gold.carregar_dados_silver()
        
        # Cria dimensao tempo
        dim_tempo = gold.criar_dim_tempo(
            data_inicio=DATA_INICIO_GOLD,
            data_fim=DATA_FIM_GOLD
        )
        
        # Cria dimensao fundo
        dim_fundo = gold.criar_dim_fundo()
        
        # Cria fato fundo diario
        fct_fundo_diario = gold.criar_fct_fundo_diario()
        
        # Cria agregacao por periodo
        agg_fundo_periodo = gold.criar_agg_fundo_periodo(periodos=PERIODOS_AGREGACAO)
        
        print("\n✓ FASE 1 concluida com sucesso")
        print(f"  - dim_tempo: {len(dim_tempo)} dias")
        print(f"  - dim_fundo: {len(dim_fundo)} fundos")
        print(f"  - fct_fundo_diario: {len(fct_fundo_diario)} registros")
        print(f"  - agg_fundo_periodo: {len(agg_fundo_periodo)} registros")
        
    except Exception as e:
        print(f"\n✗ Erro na FASE 1: {e}")
        print("Verifique os caminhos dos dados silver")
        return
    
    # ========== FASE 2: SIMULADOR DE CLIENTES ==========
    print("\n[FASE 2] Criando Simulador de Clientes...")
    print("-" * 70)
    
    try:
        # Inicializa simulador
        simulador = SimuladorClientes(seed=42)
        
        # Gera dimensao cliente simulado (LOCAL)
        dim_cliente = simulador.gerar_dim_cliente_simulado(n_clientes=N_CLIENTES_SIMULADOS)
        
        # TODO: Implementar fct_cliente_posicao_diaria_simulada
        # TODO: Implementar fct_cliente_risco_diario_simulada
        # TODO: Implementar agg_relacionamento_periodo_simulada
        
        print("\n✓ FASE 2 concluida (parcialmente)")
        print(f"  - dim_cliente_simulado: {len(dim_cliente)} clientes")
        print("  - fct_cliente_posicao_diaria: EM DESENVOLVIMENTO")
        print("  - fct_cliente_risco_diario: EM DESENVOLVIMENTO")
        print("  - agg_relacionamento_periodo: EM DESENVOLVIMENTO")
        
    except Exception as e:
        print(f"\n✗ Erro na FASE 2: {e}")
        return
    
    # ========== FASE 3: EXPORTACAO ==========
    print("\n[FASE 3] Exportando arquivos Gold...")
    print("-" * 70)
    
    try:
        # Exporta tabelas gold (LOCAL - Sem S3)
        gold.exportar_gold(path_output=str(PATH_OUTPUT_GOLD))
        
        # Exporta simulacoes (LOCAL)
        simulador.exportar_simulador(path_output=str(PATH_OUTPUT_GOLD))
        
        print("\n✓ FASE 3 concluida com sucesso")
        
    except Exception as e:
        print(f"\n✗ Erro na FASE 3: {e}")
        return
    
    # ========== RESUMO FINAL ==========
    print("\n" + "=" * 70)
    print("RESUMO DA EXECUCAO")
    print("=" * 70)
    print(f"Status: ✓ SUCESSO (100% LOCAL - Sem S3)")
    print(f"Fim: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Output: {PATH_OUTPUT_GOLD}")
    print(f"\nArquivos gerados (LOCAL - Parquet + CSV):")
    try:
        for arquivo in sorted(Path(PATH_OUTPUT_GOLD).glob('*')):
            if arquivo.is_file():
                tamanho = arquivo.stat().st_size / 1024 / 1024  # MB
                print(f"  ✓ {arquivo.name} ({tamanho:.2f} MB)")
    except:
        pass
    print("\nProximos passos (Fase 3 - Qualidade e Operacao):")
    print("  1. Validar reconciliacao de metricas")
    print("  2. Testar desempenho de queries nos filtros 7D/30D/60D/90D")
    print("  3. Implementar testes de qualidade de dados")
    print("  4. Configurar reprocessamento incremental diario")
    print("=" * 70)


if __name__ == "__main__":
    main()

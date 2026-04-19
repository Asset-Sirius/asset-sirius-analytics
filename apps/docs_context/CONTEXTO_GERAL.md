# 📊 Contexto Geral - Asset Sirius Analytics Apps

**Data de Criação**: 19 de Abril de 2026  
**Projeto**: Asset Sirius Analytics  
**Localização**: `d:\SPTech\asset-sirius-analytics\apps`

---

## 🎯 Visão Geral do Projeto

Este projeto implementa um **pipeline de dados** (ETL) para análise financeira de fundos de investimento utilizando dados da **CVM (Comissão de Valores Mobiliários)**. A arquitetura segue o padrão **Medallion** com camadas: Bronze (raw), Silver (cleaned) e Gold (aggregated).

### Estrutura de Dados - Medallion Architecture

```
Bronze (Raw)       →  Silver (Cleaned)  →  Gold (Aggregated)
    ↓                      ↓                      ↓
Raw CSV            Data Cleaning          Dimensões & Fatos
Do CVM             Validações             Analytics
                   Transformações         Dashboards
```

---

## 📁 Estrutura de Pastas e Apps

### 1. 🔽 **`ingestao_cvm/`** - Ingestão de Dados
**Propósito**: Baixar dados brutos do CVM e armazená-los na camada Bronze

#### Arquivos:
- `ingestao_dados_main.py` - Ponto de entrada para execução local
- `ingestao_dados_functions.py` - Lógica de download do CVM

#### Características:
- ✅ Download de dados brutos da CVM
- ✅ Armazenamento em estrutura local (`dados_cvm/`) ou S3 (Bronze bucket)
- ✅ Suporte para timeout configurável (padrão: 600s)
- ✅ Compatível com AWS Lambda

#### Fluxo:
```
CVM API/Website  →  Download  →  dados_cvm/  ou  S3 Bronze Bucket
```

#### Configurações Padrão Lambda:
- **Bucket Bronze**: `s3-asset-sirius-bucket-bronze`
- **Prefixo Bronze**: `cvm/raw`
- **Permissões IAM**: `s3:PutObject`, `logs:*`

---

### 2. 🧹 **`data_cleaning/`** - Limpeza e Transformação
**Propósito**: Processar e limpar dados brutos para nível Silver

#### Arquivos:
- `data_cleaning_main.py` - Orquestração da limpeza
- `data_cleaning_functions.py` - Lógica de transformação

#### Características:
- ✅ Validação de dados
- ✅ Remoção de duplicatas
- ✅ Tratamento de valores nulos
- ✅ Padronização de formatos
- ✅ Compatível com AWS Lambda

#### Fluxo:
```
dados_cvm/  →  Limpeza & Validação  →  dados_tgt/  ou  S3 Silver Bucket
```

#### Direitos Padrão:
- **Entrada**: `dados_cvm/` (local) ou S3 Bronze
- **Saída**: `dados_tgt/` (local) ou S3 Silver
- **Bucket Bronze**: `s3-asset-sirius-bucket-bronze`
- **Bucket Silver**: `s3-asset-sirius-bucket-silver`
- **Permissões IAM**: `s3:GetObject`, `s3:ListBucket`, `s3:PutObject`

---

### 3. ✨ **`gold_layer/`** - Camada Gold (Analytics)
**Propósito**: Criar tabelas agregadas e dimensionais para alimentar dashboards

#### Arquivos:
- `gold_main.py` - Script principal de orquestração
- `gold_functions.py` - Classes para criar dimensões e fatos
- `config.py` - Configurações centralizadas
- `validador.py` - Validação de dados Gold
- `requirements.txt` - Dependências Python
- `README.md` - Documentação detalhada

#### Características:
- ✅ Criação de Dimensões (tempo, fundos)
- ✅ Criação de Fatos (métricas diárias)
- ✅ Agregações por períodos (7D, 30D, 60D, 90D)
- ✅ Suporte para dados reais (Fase 1) e simulados (Fase 2)

#### Configurações (`config.py`):
```python
PATH_DATA_SILVER = "data/silver"      # Entrada (dados limpos)
PATH_OUTPUT_GOLD = "data/gold"        # Saída (tabelas agregadas)
DATA_INICIO_GOLD = "2023-01-01"       # Data início
DATA_FIM_GOLD = "2024-12-31"          # Data fim
PERIODOS_AGREGACAO = [7, 30, 60, 90]  # Agregações em dias
N_CLIENTES_SIMULADOS = 10000          # Simulação Fase 2
```

#### Fase 1: Gold de Fundos (✓ Implementada)
**Tabelas Criadas**:
- `dim_tempo` - Dimensão temporal (7 dias até hoje)
- `dim_fundo` - Dimensão de fundos
- `fct_fundo_diario` - Fato diário por fundo
- `agg_fundo_periodo` - Agregações (7D, 30D, 60D, 90D)

**Métricas do Dashboard Gestor**:
- 📈 Fluxo Líquido = Captação - Resgate
- 📊 Total de Resgates
- 📉 Variação de Patrimônio (%)
- 👥 Variação de Cotistas (%)
- 🔄 Comparação entre fundos por nível de risco

#### Fase 2: Simulador de Clientes (🚧 Em Desenvolvimento)
**Tabelas Planejadas**:
- `dim_cliente_simulado` - 5k-20k clientes
- `fct_cliente_posicao_diaria_simulada` - Posições diárias
- `fct_cliente_risco_diario_simulada` - Score de risco e churn
- `agg_relacionamento_periodo_simulada` - Agregações

**Métricas do Dashboard Relacionamento**:
- ⚠️ Clientes em Risco
- 💰 Valor Sob Risco
- 📉 Taxa de Churn Estimada
- 📊 Distribuição de Risco
- 🏆 Top Clientes em Risco

---

### 4. ⚡ **`lambda_scripts/`** - Funções AWS Lambda
**Propósito**: Abstrações sem servidor para executar ingestão e limpeza na AWS

#### Arquivos:
- `lambda_ingestao_cvm.py` - Handler para ingestão
- `lambda_data_cleaning_cvm.py` - Handler para limpeza
- `requirements_cleaning_lambda.txt` - Dependências

#### Características:
- ✅ Deploy direto na AWS Lambda
- ✅ Integração com S3
- ✅ Payloads opcionais configuráveis
- ✅ Logging automático no CloudWatch

#### Handler 1: Ingestão Lambda
```json
{
  "timeout": 600,
  "bucket_bronze": "s3-asset-sirius-bucket-bronze",
  "prefixo_bronze": "cvm/raw"
}
```

#### Handler 2: Data Cleaning Lambda
```json
{
  "bucket_bronze": "s3-asset-sirius-bucket-bronze",
  "bucket_silver": "s3-asset-sirius-bucket-silver",
  "prefixo_bronze": "cvm/raw",
  "prefixo_silver": "cvm/clean"
}
```

---

## 🔄 Fluxo End-to-End

```
┌─────────────────────────────────────────────────────────────────┐
│                    ASSET SIRIUS ANALYTICS                       │
└─────────────────────────────────────────────────────────────────┘

1. INGESTÃO (Bronze)
   ├─ Fonte: CVM API
   ├─ Módulo: ingestao_cvm/
   ├─ Saída: S3 Bronze ou dados_cvm/ local
   └─ Freq: Diária/Sob demanda

2. LIMPEZA (Silver)
   ├─ Entrada: S3 Bronze ou dados_cvm/
   ├─ Módulo: data_cleaning/
   ├─ Processamento: Validação, transformação, padronização
   ├─ Saída: S3 Silver ou dados_tgt/ local
   └─ Freq: Diária após ingestão

3. ANALYTICS (Gold)
   ├─ Entrada: S3 Silver ou data/silver/
   ├─ Módulo: gold_layer/
   ├─ Processamento: 
   │  ├─ Fase 1: Dimensões & Fatos de Fundos ✓
   │  └─ Fase 2: Simulação de Clientes 🚧
   ├─ Saída: data/gold/ ou banco de dados
   └─ Freq: Diária após limpeza

4. DASHBOARDS
   ├─ Dashboard Gestor (Fundos)
   │  ├─ Fluxo, Patrimônio, Cotistas
   │  └─ Comparação inter-fundos
   ├─ Dashboard Relacionamento (Clientes)
   │  ├─ Risco, Churn, Carteira
   │  └─ Segmentação de clientes
   └─ Fonte: Tabelas Gold
```

---

## 🚀 Como Executar

### Local - Execução Completa
```bash
# 1. Ingestão
cd apps/ingestao_cvm
python ingestao_dados_main.py

# 2. Limpeza
cd ../data_cleaning
python data_cleaning_main.py

# 3. Gold Layer
cd ../gold_layer
python gold_main.py
```

### AWS - Execução com Lambda
```
1. Deploy lambda_ingestao_cvm.py → AWS Lambda
2. Deploy lambda_data_cleaning_cvm.py → AWS Lambda
3. Configurar triggers (SNS, EventBridge, manual)
4. Executar em sequência ou disparo automático
```

---

## 📦 Dependências Principais

### Common
- `pandas` - Manipulação de dados
- `numpy` - Operações numéricas
- `requests` - Requisições HTTP

### AWS
- `boto3` - SDK AWS
- `python-dateutil` - Manipulação de datas

### Gold Layer
- `sqlalchemy` - ORM e operações SQL
- `pyarrow` - Formato Parquet

---

## 📊 Estrutura de Dados - Exemplos de Tabelas

### Dimensões (Gold)
| Tabela | Colunas | Fonte |
|--------|---------|-------|
| `dim_tempo` | data_id, data, mes, trimestre, ano, semana | Gerada |
| `dim_fundo` | fundo_id, nome_fundo, segmento, nivel_risco | CVM |

### Fatos (Gold)
| Tabela | Colunas | Agregação |
|--------|---------|-----------|
| `fct_fundo_diario` | fundo_id, data_id, patrimonio, cotistas, fluxo | Diária |
| `agg_fundo_periodo` | fundo_id, periodo, patrimonio_medio, fluxo_total | 7/30/60/90 dias |

---

## 🔐 Permissões AWS Necessárias

### IAM Policy - Ingestão
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Action": ["s3:PutObject"], "Resource": "arn:aws:s3:::s3-asset-sirius-bucket-bronze/*"},
    {"Effect": "Allow", "Action": ["logs:*"], "Resource": "arn:aws:logs:*"}
  ]
}
```

### IAM Policy - Data Cleaning
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Action": ["s3:GetObject", "s3:ListBucket"], "Resource": "arn:aws:s3:::s3-asset-sirius-bucket-bronze*"},
    {"Effect": "Allow", "Action": ["s3:PutObject"], "Resource": "arn:aws:s3:::s3-asset-sirius-bucket-silver/*"}
  ]
}
```

---

## 📝 Notas Importantes

- **Dados CVM**: Fonte oficial de dados de fundos de investimento
- **Fase 1 ✓**: Produção - dados reais de fundos
- **Fase 2 🚧**: Desenvolvimento - simulação de clientes para testes
- **Escalabilidade**: Arquitetura preparada para crescimento em volume
- **Automação**: Lambda permite execução sem intervenção manual

---

## 👤 Próximos Passos

1. ✅ Documentar infraestrutura existente
2. ⏳ Completar Fase 2 (Simulador de Clientes)
3. ⏳ Implementar orquestração com Airflow/EventBridge
4. ⏳ Deploy de dashboards (Tableau/PowerBI)
5. ⏳ Testes de performance e escalabilidade

---

**Última Atualização**: 19/04/2026  
**Status**: 🟢 Em Operação (Fase 1 + Lambda ready)

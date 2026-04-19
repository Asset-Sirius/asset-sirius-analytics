# 🏗️ Arquitetura Técnica

## Sistema de Camadas (Medallion Architecture)

```
┌──────────────────────────────────────────────────────────────────┐
│                     ASSET SIRIUS ANALYTICS                       │
│                      (Medallion Pattern)                         │
└──────────────────────────────────────────────────────────────────┘

                         ┌──────────────┐
                         │  CVM Server  │
                         │ (Dados Brutos)│
                         └──────┬───────┘
                                │
                    ┌───────────▼──────────────┐
                    │   BRONZE LAYER (Raw)    │
                    │  ├─ S3 Bronze ou Local  │
                    │  ├─ Formato: CSV        │
                    │  └─ Sem transformação   │
                    └───────────┬──────────────┘
                                │
                    ┌───────────▼──────────────┐
                    │  INGESTAO_CVM MODULE    │
                    │  ingestao_dados_main.py │
                    │  (Downloader)           │
                    └───────────┬──────────────┘
                                │
                    ┌───────────▼──────────────┐
                    │   SILVER LAYER (Clean)  │
                    │  ├─ S3 Silver ou Local  │
                    │  ├─ Validado            │
                    │  ├─ Normalizado         │
                    │  └─ Sem duplicatas      │
                    └───────────┬──────────────┘
                                │
                    ┌───────────▼──────────────┐
                    │ DATA_CLEANING MODULE    │
                    │ data_cleaning_main.py   │
                    │ (Transformer)           │
                    └───────────┬──────────────┘
                                │
                    ┌───────────▼──────────────┐
                    │    GOLD LAYER (Facts)   │
                    │  ├─ S3 Gold ou DB       │
                    │  ├─ Dimensões           │
                    │  ├─ Fatos               │
                    │  └─ Agregações          │
                    └───────────┬──────────────┘
                                │
        ┌───────────────────────┼───────────────────────┐
        │                       │                       │
    ┌───▼────────┐      ┌──────▼────────┐      ┌──────▼─────┐
    │ Dashboard  │      │   Analytics   │      │ ML Models  │
    │  Gestor    │      │   (Reports)   │      │ (Scores)   │
    │  (Fundos)  │      │               │      │ (Churn)    │
    └────────────┘      └───────────────┘      └────────────┘
```

---

## 🗂️ Modelos de Dados - Gold Layer

### Dimensões (Tabelas Pequenas)

#### `dim_tempo`
```
┌────────────────────────────┐
│      dim_tempo             │
├────────────────────────────┤
│ data_id          INT (PK)  │ ← 2023-01-01 = 20230101
│ data             DATE      │ ← Data calendar
│ mes              INT       │ ← 1-12
│ trimestre        INT       │ ← 1-4
│ ano              INT       │ ← 2023, 2024, ...
│ semana           INT       │ ← 1-52
│ dia_semana       INT       │ ← 1-7
│ eh_fds           BOOL      │ ← True/False
│ eh_feriado       BOOL      │ ← True/False
└────────────────────────────┘
```

#### `dim_fundo`
```
┌─────────────────────────────────┐
│        dim_fundo                │
├─────────────────────────────────┤
│ fundo_id              INT (PK)  │ ← Chave primária
│ codigo_cvm            STR       │ ← CVM123456
│ nome_fundo            STR       │ ← "Fundo XYZ"
│ segmento              STR       │ ← "Renda Fixa", etc
│ nivel_risco           STR       │ ← "Baixo", "Médio", "Alto"
│ tipo_gestor           STR       │ ← "Pessoa Física", etc
│ data_constituicao     DATE      │ ← Quando foi criado
│ data_atualizacao      DATE      │ ← Atualizado em
└─────────────────────────────────┘
```

### Fatos (Tabelas Grandes)

#### `fct_fundo_diario`
```
┌──────────────────────────────────────┐
│       fct_fundo_diario               │
├──────────────────────────────────────┤
│ fundo_id              INT (FK)       │ → dim_fundo
│ data_id               INT (FK)       │ → dim_tempo
│ patrimonio_liquido    FLOAT          │ ← Em R$ (quantidade)
│ total_cotistas        INT            │ ← Nº de investidores
│ valor_cota            FLOAT          │ ← Preço da cota
│ captacao_dia          FLOAT          │ ← Entradas (R$)
│ resgate_dia           FLOAT          │ ← Saídas (R$)
│ fluxo_liquido         FLOAT          │ ← Captação - Resgate
│ rentabilidade_dia     FLOAT          │ ← Retorno (%)
│ data_processamento    DATETIME       │ ← Quando processado
└──────────────────────────────────────┘
```

#### `agg_fundo_periodo`
```
┌──────────────────────────────────────────┐
│       agg_fundo_periodo                  │
├──────────────────────────────────────────┤
│ fundo_id              INT (FK)           │ → dim_fundo
│ data_inicio           DATE               │ ← Período começa
│ data_fim              DATE               │ ← Período termina
│ periodo_dias          INT                │ ← 7, 30, 60 ou 90
│ patrimonio_medio      FLOAT              │ ← Média do período
│ patrimonio_max        FLOAT              │ ← Máximo do período
│ patrimonio_min        FLOAT              │ ← Mínimo do período
│ captacao_total        FLOAT              │ ← Soma entradas
│ resgate_total         FLOAT              │ ← Soma saídas
│ fluxo_total           FLOAT              │ ← Total período
│ cotistas_inicio       INT                │ ← No começo
│ cotistas_fim          INT                │ ← No final
│ rentabilidade_media   FLOAT              │ ← Média %
│ data_processamento    DATETIME           │ ← Quando processado
└──────────────────────────────────────────┘
```

---

## 🔧 Stack Tecnológico

### Linguagens & Runtime
- **Python 3.9+** - Linguagem principal
- **Pandas** - Manipulação DataFrames
- **NumPy** - Operações numéricas
- **SQLAlchemy** - ORM/ SQL queries

### Armazenamento
- **S3** (AWS) - Buckets Bronze/Silver/Gold
- **Parquet** - Formato eficiente Gold (opcional)
- **CSV** - Formato intermediário Bronze/Silver
- **Local FS** - Desenvolvimento local

### Orquestração
- **AWS Lambda** - Serverless execution
- **CloudWatch** - Logging e monitoramento
- **EventBridge** - Agendamento (futuro)
- **Manual** - Execução local

---

## 📋 Fluxo de Dados por Componente

### Componente 1: Ingestão
```
┌─────────────┐
│ CVM Server  │
└──────┬──────┘
       │ requests.get()
       │ urllib.urlopen()
       ▼
┌─────────────────────────────┐
│ ingestao_cvm/               │
│ - Download CSV              │
│ - Validação básica          │
│ - Armazena localmente       │
└──────┬──────────────────────┘
       │
       ▼
┌──────────────────────┐
│ S3 Bronze            │
│ cvm/raw/2024-04-19/ │
└──────────────────────┘
   OR
┌──────────────────────┐
│ Local: dados_cvm/    │
│ *.csv files          │
└──────────────────────┘
```

### Componente 2: Limpeza
```
┌──────────────────────┐
│ S3 Bronze ou Local   │
│ dados_cvm/*.csv      │
└──────┬───────────────┘
       │ pd.read_csv()
       ▼
┌─────────────────────────────┐
│ data_cleaning/              │
│ - Remover duplicatas        │
│ - Preencher nulls           │
│ - Validar tipos             │
│ - Padronizar formatos       │
└──────┬──────────────────────┘
       │
       ▼
┌──────────────────────┐
│ S3 Silver            │
│ cvm/clean/2024-04-19│
└──────────────────────┘
   OR
┌──────────────────────┐
│ Local: dados_tgt/    │
│ *.csv files          │
└──────────────────────┘
```

### Componente 3: Gold Layer
```
┌──────────────────────┐
│ S3 Silver ou Local   │
│ dados_tgt/*.csv      │
└──────┬───────────────┘
       │ pd.read_csv()
       ▼
┌────────────────────────────┐
│ gold_layer/                │
│ - Parse dados Silver       │
│ - Criar dimensões         │
│   dim_tempo               │
│   dim_fundo              │
│ - Criar fatos            │
│   fct_fundo_diario       │
│   agg_fundo_periodo      │
│ - Validações (validador) │
└──────┬───────────────────┘
       │
       ▼
┌───────────────────────┐
│ data/gold/            │
│ ├─ dim_tempo.parquet  │
│ ├─ dim_fundo.parquet  │
│ ├─ fct_fundo_diario   │
│ │  .parquet           │
│ └─ agg_fundo_periodo  │
│    .parquet           │
└───────────────────────┘
```

---

## ⏱️ Tempos de Execução Típicos

| Etapa | Dados Vol. | Tempo Esperado | Notas |
|-------|-----------|----------------------|-------|
| **Ingestão** | 100 MB | 5-10 min | Depende CVM server |
| **Limpeza** | 100 MB | 2-5 min | I/O + validação |
| **Gold** | Fase 1 | 1-3 min | Dimensões pequenas |
| **Gold** | Fase 2 * | 5-10 min | Simulação clientes |
| **Total** | | 13-28 min | End-to-end |

*Fase 2 ainda não em produção

---

## 🔐 Segurança & IAM

### Bronze Layer (Ingestão)
```json
{
  "Effect": "Allow",
  "Principal": {"Service": "lambda.amazonaws.com"},
  "Action": [
    "s3:PutObject",
    "s3:ListBucket"
  ],
  "Resource": [
    "arn:aws:s3:::s3-asset-sirius-bucket-bronze/*",
    "arn:aws:s3:::s3-asset-sirius-bucket-bronze"
  ]
}
```

### Silver Layer (Limpeza)
```json
{
  "Effect": "Allow",
  "Principal": {"Service": "lambda.amazonaws.com"},
  "Action": [
    "s3:GetObject",
    "s3:ListBucket",
    "s3:PutObject"
  ],
  "Resource": [
    "arn:aws:s3:::s3-asset-sirius-bucket-bronze/*",
    "arn:aws:s3:::s3-asset-sirius-bucket-silver/*",
    "arn:aws:s3:::s3-asset-sirius-bucket-bronze",
    "arn:aws:s3:::s3-asset-sirius-bucket-silver"
  ]
}
```

---

## 📈 Escalabilidade

### Volume Atual (Fase 1)
- ~50-100 fundos
- ~500 MB/mês dados
- Execução: 15-30 min

### Projeção (Fase 2)
- +10k clientes simulados
- +2 GB/mês simulados
- Execução: 45-60 min

### Recomendações Futuro
- Particionamento por data (S3)
- Compressão Parquet (Bronze/Silver)
- Lambda timeout: 900s (15 min max)
- RDS/Redshift para Gold (escala)

---

## 🔄 Dependências Entre Módulos

```
ingestao_cvm/
├─ Precisa: requests, pandas
├─ Produz: dados_cvm/ CSV
└─ Consumido por: data_cleaning/

data_cleaning/
├─ Requer: dados_cvm/ existir
├─ Precisa: pandas, pathlib
├─ Produz: dados_tgt/ CSV limpo
└─ Consumido por: gold_layer/

gold_layer/
├─ Requer: dados_tgt/ existir
├─ Precisa: pandas, sqlalchemy, numpy
├─ Config: config.py (DATA_INICIO, PERIODOS)
├─ Valida: validador.py
└─ Produz: data/gold/ Parquet + tabelas
```

---

## 🚨 Pontos Críticos

### Sem Redundância
- ❌ Sem backup automático S3
- ❌ Sem retry logic em downloads
- ❌ Sem recovery de falhas parciais

### Performance
- ⚠️ Sem índices em Parquet
- ⚠️ Sem cache intermediário
- ⚠️ Sem partition pruning

### Monitoramento
- ⚠️ Apenas logs CloudWatch
- ⚠️ Sem alertas de falha
- ⚠️ Sem métricas de qualidade

---

**Última Atualização**: 19/04/2026

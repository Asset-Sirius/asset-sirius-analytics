# Lambdas de Ingestao e Data Cleaning (CVM)

Este diretorio contem duas funcoes AWS Lambda para substituir a execucao local:

- `lambda_ingestao_cvm.lambda_handler`: baixa dados da CVM e grava CSVs no bucket bronze.
- `lambda_data_cleaning_cvm.lambda_handler`: le CSVs do bronze, aplica limpeza e grava no bucket silver.

## Buckets padrao

- Bronze: `s3-asset-sirius-bucket-bronze`
- Silver: `s3-asset-sirius-bucket-silver`

## Handler 1: Ingestao

Arquivo: `lambda_ingestao_cvm.py`

### Payload opcional

```json
{
  "timeout": 600,
  "bucket_bronze": "s3-asset-sirius-bucket-bronze",
  "prefixo_bronze": "cvm/raw"
}
```

### Permissoes IAM minimas

- `s3:PutObject` no bucket bronze.
- `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents`.

## Handler 2: Data Cleaning

Arquivo: `lambda_data_cleaning_cvm.py`

### Payload opcional

```json
{
  "bucket_bronze": "s3-asset-sirius-bucket-bronze",
  "bucket_silver": "s3-asset-sirius-bucket-silver",
  "prefixo_bronze": "cvm/raw",
  "prefixo_silver": "cvm/clean"
}
```

### Permissoes IAM minimas

- `s3:GetObject` e `s3:ListBucket` no bucket bronze.
- `s3:PutObject` no bucket silver.
- `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents`.

## Dependencias

- `boto3` ja esta disponivel no runtime AWS Lambda Python.
- `pandas` precisa estar no pacote da funcao ou em uma Lambda Layer.

Sugestao de `requirements.txt` para a lambda de cleaning:

```txt
pandas==2.2.3
```

## Handler 3: Criacao da Camada Gold

Arquivo: `lambda_gold_layer.py`

Baseado em: `planejamento_gold_simulacao.md`

Cria as tabelas da camada Gold (Fase 1 - Dados Reais CVM):
- `dim_tempo`: dimensão de tempo
- `dim_fundo`: dimensão de fundos
- `fct_fundo_diario`: fato fundo diário
- `agg_fundo_periodo`: agregações por período (7D, 30D, 60D, 90D)

### Payload opcional

```json
{
  "bucket_silver": "s3-asset-sirius-bucket-silver",
  "bucket_gold": "s3-asset-sirius-bucket-gold",
  "prefixo_silver": "cvm/clean",
  "prefixo_gold": "cvm/gold",
  "data_inicio": "2023-01-01",
  "data_fim": "2024-12-31",
  "periodos_agregacao": [7, 30, 60, 90],
  "modo": "s3"
}
```

### Modo de Execucao

- `"modo": "s3"`: Le dados do silver em S3, salva gold em S3 (padrao)
- `"modo": "local"`: Le dados de `dados_tgt/`, salva gold em `data/gold/`

### Permissoes IAM minimas

- `s3:GetObject` e `s3:ListBucket` no bucket silver.
- `s3:PutObject` no bucket gold.
- `logs:CreateLogGroup`, `logs:CreateLogStream`, `logs:PutLogEvents`.

### Buckets padrao

- Silver: `s3-asset-sirius-bucket-silver`
- Gold: `s3-asset-sirius-bucket-gold`

### Dependencias

- `boto3` ja esta disponivel no runtime AWS Lambda Python.
- `pandas==2.2.3`
- `numpy==1.24.3`
- `pyarrow==12.0.1` (para salvar Parquet)

Sugestao de `requirements.txt`:

```txt
pandas==2.2.3
numpy==1.24.3
pyarrow==12.0.1
```

### Resposta

Status 200 (sucesso):
```json
{
  "status": "success",
  "modo": "s3",
  "data_inicio": "2023-01-01",
  "data_fim": "2024-12-31",
  "periodos_agregacao": [7, 30, 60, 90],
  "tabelas_processadas": [
    {
      "nome": "dim_tempo",
      "quantidade_registros": 730,
      "tamanho_mb": 0.05
    },
    {
      "nome": "dim_fundo",
      "quantidade_registros": 5234,
      "tamanho_mb": 1.23
    },
    {
      "nome": "fct_fundo_diario",
      "quantidade_registros": 2500000,
      "tamanho_mb": 45.67
    },
    {
      "nome": "agg_fundo_periodo",
      "quantidade_registros": 80000,
      "tamanho_mb": 3.45
    }
  ],
  "erros": []
}
```

### Teste Local

```bash
cd apps/lambda_scripts
python lambda_gold_layer.py
```

---

## Ordem de execucao recomendada

1. **Executar `lambda_ingestao_cvm`**
   Baixa dados da CVM → Bronze

2. **Executar `lambda_data_cleaning_cvm`**
   Limpa dados → Silver

3. **Executar `lambda_gold_layer`**
   Cria tabelas gold → Gold

Voce pode orquestrar com:
- **AWS Step Functions**: Sequência de execuções
- **EventBridge Scheduler**: Agendamento diário/horário
- **AWS Glue**: ETL com paralelismo
- **Local**: Executar scripts Python conforme necessário
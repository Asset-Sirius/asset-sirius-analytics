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

## Ordem de execucao recomendada

1. Executar `lambda_ingestao_cvm`.
2. Executar `lambda_data_cleaning_cvm`.

Voce pode orquestrar com AWS Step Functions, EventBridge Scheduler ou duas regras separadas.
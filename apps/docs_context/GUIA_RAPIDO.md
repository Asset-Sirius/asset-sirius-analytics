# ⚡ Guia Rápido de Execução

## 🎯 Sumário Executivo

**Asset Sirius Analytics** é um pipeline ETL de 3 camadas que:
1. **Ingestão**: Baixa dados de fundos do CVM
2. **Limpeza**: Transforma em dados confiáveis
3. **Analytics**: Cria tabelas para 2 dashboards

---

## 📋 Verificação Rápida

### Arquivos Críticos
```
✅ ingestao_cvm/ingestao_dados_main.py      → Começa aqui
✅ data_cleaning/data_cleaning_main.py      → Depois isso
✅ gold_layer/gold_main.py                  → Por fim isso
```

### Estrutura de Saída Esperada
```
dados_cvm/                    ← Saída da ingestão
dados_tgt/                    ← Saída da limpeza (= Silver do Gold)
  └─ *.csv (limpos)
data/
  └─ gold/                    ← Saída do analytics (parquet)
     ├─ dim_tempo.parquet
     ├─ dim_fundo.parquet
     ├─ fct_fundo_diario.parquet
     └─ agg_fundo_periodo.parquet
```

---

## 🚀 Executar Local (3 Passos)

### Passo 1: Ingestão
```bash
cd apps/ingestao_cvm
python ingestao_dados_main.py
# ⏳ Aguarde download dos dados CVM (~5-10 min)
# ✅ Verifique: dados_cvm/ criado com arquivos CSV
```

### Passo 2: Limpeza
```bash
cd ../data_cleaning
python data_cleaning_main.py
# 🧹 Limpa e valida dados
# ✅ Verifique: dados_tgt/ criado com arquivos limpos
```

### Passo 3: Analytics (Gold)
```bash
cd ../gold_layer
python gold_main.py
# ✨ Cria dimensões e fatos
# ✅ Verifique: data/gold/ com tabelas Parquet
```

---

## ☁️ Executar na AWS (Lambda)

### Pré-requisitos
- ✅ Buckets S3 criados:
  - `s3-asset-sirius-bucket-bronze` (ingestão)
  - `s3-asset-sirius-bucket-silver` (limpeza)
- ✅ IAM roles configuradas com permissões S3
- ✅ Python 3.9+ no Lambda

### Deployment
```bash
# 1. Package e deploy
cd lambda_scripts
zip -r lambda_ingestao.zip lambda_ingestao_cvm.py
zip -r lambda_cleaning.zip lambda_data_cleaning_cvm.py

# 2. Upload no AWS Console ou CLI
aws lambda update-function-code \
  --function-name ingestao-cvm \
  --zip-file fileb://lambda_ingestao.zip
```

### Invocação
```bash
# Ingestão
aws lambda invoke \
  --function-name ingestao-cvm \
  --payload '{"timeout": 600}' \
  response.json

# Limpeza (após ingestão completar)
aws lambda invoke \
  --function-name data-cleaning-cvm \
  --payload '{}' \
  response.json
```

---

## 🐛 Troubleshooting Comum

| Problema | Solução |
|----------|---------|
| `Diretório de entrada não encontrado` | Execute `ingestao_dados_main.py` primeiro |
| `ModuleNotFoundError: pandas` | `pip install -r requirements.txt` |
| `S3: Access Denied` | Verifique IAM permissions e nomes de bucket |
| `Timeout Lambda` | Aumente timeout em config ou payload |
| `Dados Silver vazios` | Verifique se Bronze tem arquivos CSV |

---

## 🔍 Monitorar Execução

### Local
```bash
# Ver logs em tempo real
tail -f [diretorio]/logs.txt

# Ver estrutura de saída
ls -la dados_cvm/
ls -la dados_tgt/
ls -la data/gold/
```

### AWS Lambda
```bash
# Ver logs CloudWatch
aws logs tail /aws/lambda/ingestao-cvm --follow

# Verificar S3
aws s3 ls s3://s3-asset-sirius-bucket-bronze/cvm/raw/
aws s3 ls s3://s3-asset-sirius-bucket-silver/cvm/clean/
```

---

## 📊 Dashboards Criados

### Dashboard 1: Gestor de Fundos
**Dados**: Fundos reais CVM  
**Métricas**:
- 💰 Fluxo Líquido
- 📈 Patrimônio Líquido
- 👥 Evolução de Cotistas
- 🔄 Comparação inter-fundos

### Dashboard 2: Relacionamento (Beta)
**Dados**: Clientes simulados (Fase 2 - em dev)  
**Métricas**:
- ⚠️ Score de Risco
- 📉 Taxa de Churn
- 💼 Segmentação
- 🏆 Top Clientes

---

## 🔄 Agendamento (Próximo)

### Recomendado: AWS EventBridge
```
Diariamente 08:00 → Lambda Ingestão
Diariamente 09:00 → Lambda Limpeza
Diariamente 10:00 → Gold Layer (local ou Lambda)
```

---

## 📞 Referência Rápida

**Configurações principais** (gold_layer/config.py):
```python
DATA_INICIO_GOLD = "2023-01-01"       # Início análise
DATA_FIM_GOLD = "2024-12-31"          # Fim análise
PERIODOS_AGREGACAO = [7, 30, 60, 90]  # Janelas de tempo
N_CLIENTES_SIMULADOS = 10000          # Para Fase 2
```

**Buckets S3 padrão**:
- Bronze: `s3-asset-sirius-bucket-bronze` (raw)
- Silver: `s3-asset-sirius-bucket-silver` (cleaned)
- Gold: (banco de dados ou S3 conforme config)

---

**Última Atualização**: 19/04/2026

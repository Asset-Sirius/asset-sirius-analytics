# 📑 Índice de Documentação

**Pasta**: `apps/docs_context/`  
**Criado**: 19 de Abril de 2026  
**Status**: 🟢 Completo

---

## 📚 Documentos Disponíveis

### 1️⃣ **CONTEXTO_GERAL.md**
**Para**: Entendimento do projeto inteiro  
**Inclui**:
- 🎯 Visão geral do projeto
- 📊 Estrutura de pastas (4 apps principais)
- 🔄 Fluxo end-to-end
- 📦 Dependências
- 🔐 Permissões AWS

**Quando ler**: Primeira vez explorando o projeto

**Tempo de leitura**: 10-15 minutos

---

### 2️⃣ **GUIA_RAPIDO.md**
**Para**: Executar o projeto rapidamente  
**Inclui**:
- ⚡ 3 passos para rodar localmente
- ☁️ Instruções AWS Lambda
- 🐛 Troubleshooting comum
- 🔍 Como monitorar execução
- 📊 Descrição dos dashboards

**Quando ler**: Pronto para colocar em prática

**Tempo de leitura**: 5-8 minutos

---

### 3️⃣ **TESTE_LOCAL_PASSO_A_PASSO.md** ⭐ NOVO
**Para**: Teste completo local com commands prontos  
**Inclui**:
- ✅ Pré-requisitos (Python, pip, venv)
- 🔽 Passo 1: Ingestão (com outputs esperados)
- 🧹 Passo 2: Limpeza
- ✨ Passo 3: Gold Layer
- 🔍 Passo 4: Validação
- 🐛 Troubleshooting com soluções
- 📊 Dicas profissionais

**Quando ler**: Quando quer rodar localmente agora

**Tempo de execução**: 30-45 minutos (completo)

---

### 4️⃣ **MIGRACAO_LOCAL.md** ⭐ NOVO
**Para**: Entender as mudanças para 100% local (sem S3)  
**Inclui**:
- 📝 Mudanças em gold_main.py
- 📝 Mudanças em gold_functions.py
- 🔄 Fluxo atualizado
- ✅ Checklist de verificação
- 🚀 Como executar agora
- 📊 Output esperado

**Quando ler**: Se quer saber o que foi alterado ou validar que está local

**Tempo de leitura**: 5-10 minutos

---

### 5️⃣ **MUDANCA_SILVER_LOCAL.md** ⭐ NOVO
**Para**: Entender a integração dados_tgt → gold  
**Inclui**:
- 📊 Antes vs. Depois do fluxo
- 🔧 Mudanças efetivadas (config.py)
- 📁 Estrutura final
- 🚀 Como executar o novo fluxo
- ✅ Verificação
- 📈 Benefícios

**Quando ler**: Entender como gold lê diretamente de dados_tgt/

**Tempo de leitura**: 5 minutos

---

### 6️⃣ **ERRO_CSV_TOKENIZACAO.md** ⭐ NOVO
**Para**: Resolver erros de leitura CSV  
**Inclui**:
- 🐛 Causa: "Expected 1 fields, saw 3"
- ✅ Solução: Parâmetros robustos (encoding, on_bad_lines, engine)
- 🔧 Como diagnosticar arquivo
- 🧹 Como limpar CSV problemático
- 📋 Checklist de solução
- 📞 Referência rápida

**Quando ler**: Se receber erro de tokenização/parsing CSV

**Tempo de leitura**: 5 minutos

---

### 7️⃣ **ADAPTACAO_DINAMICA_COLUNAS.md** ⭐ NOVO
**Para**: Entender como Gold se adapta às colunas do data_cleaning  
**Inclui**:
- 🔍 Novo método `_encontrar_coluna()` flexível
- 📊 Como detecta automaticamente
- 🎯 Variações suportadas por coluna
- 📋 Case-insensitive e opcionais
- 🚀 4 cenários de adaptação
- 📈 Benefícios (antes vs depois)

**Quando ler**: Entender flexibilidade do Gold com distintos nomes de coluna

**Tempo de leitura**: 10 minutos

---

### 4️⃣ **ARQUITETURA_TECNICA.md**
**Para**: Detalhes técnicos e design  
**Inclui**:
- 🏗️ Diagrama Medallion Architecture
- 🗂️ Modelos de dados (Dimensões & Fatos)
- 🔧 Stack tecnológico
- ⏱️ Tempos de execução
- 🔐 Configurações IAM detalhadas
- 📈 Escalabilidade

**Quando ler**: Desenvolvimento, debugging, otimização

**Tempo de leitura**: 15-20 minutos

---

## 🎯 Escolha por Caso de Uso

### 📌 Sou novo no projeto
→ Leia: **CONTEXTO_GERAL.md** + **GUIA_RAPIDO.md**

### ⚡ Quero testar localmente AGORA
→ Leia: **TESTE_LOCAL_PASSO_A_PASSO.md** ⭐

### 🚀 Preciso rodar agora (resumido)
→ Leia: **GUIA_RAPIDO.md**

### 🔧 Preciso entender o design
→ Leia: **ARQUITETURA_TECNICA.md**

### 🐛 Algo quebrou
→ Leia: **TESTE_LOCAL_PASSO_A_PASSO.md** (Troubleshooting)

### 🏗️ Vou modificar o código
→ Leia: **ARQUITETURA_TECNICA.md** + **CONTEXTO_GERAL.md**

### ☁️ Vou fazer deploy AWS
→ Leia: **GUIA_RAPIDO.md** (AWS section) + **ARQUITETURA_TECNICA.md** (Security)

---

## 📂 Estrutura do Projeto

```
apps/
├── ingestao_cvm/                    ← App 1: Download CVM
│   ├── ingestao_dados_main.py
│   ├── ingestao_dados_functions.py
│   └── requirements.txt
│
├── data_cleaning/                   ← App 2: Limpeza
│   ├── data_cleaning_main.py
│   ├── data_cleaning_functions.py
│   └── requirements.txt
│
├── gold_layer/                      ← App 3: Analytics
│   ├── gold_main.py
│   ├── gold_functions.py
│   ├── config.py                    ⭐ Altere aqui para configs
│   ├── validador.py
│   ├── requirements.txt
│   └── README.md
│
├── lambda_scripts/                  ← App 4: AWS Lambda
│   ├── lambda_ingestao_cvm.py
│   ├── lambda_data_cleaning_cvm.py
│   └── requirements_cleaning_lambda.txt
│
└── docs_context/                    ← VOCÊ ESTÁ AQUI
    ├── INDICE.md                    (Este arquivo)
    ├── CONTEXTO_GERAL.md            (Visão geral)
    ├── GUIA_RAPIDO.md               (Como executar)
    └── ARQUITETURA_TECNICA.md       (Design técnico)
```

---

## ⚙️ Configurações Principais

**Arquivo**: `gold_layer/config.py`

```python
# Data range
DATA_INICIO_GOLD = "2023-01-01"
DATA_FIM_GOLD = "2024-12-31"

# Agregações (em dias)
PERIODOS_AGREGACAO = [7, 30, 60, 90]

# Simulação (Fase 2)
N_CLIENTES_SIMULADOS = 10000

# Caminhos
PATH_DATA_SILVER = "data/silver"        # Entrada
PATH_OUTPUT_GOLD = "data/gold"          # Saída
```

---

## 📊 Tabelas Geradas

### Dimensões (Lookup)
- `dim_tempo` - 7 dias até hoje
- `dim_fundo` - Fundos CVM

### Fatos (Métricas)
- `fct_fundo_diario` - Valor diário
- `agg_fundo_periodo` - Agregações 7/30/60/90D

### Fase 2 (Em Desenvolvimento)
- `dim_cliente_simulado`
- `fct_cliente_posicao_diaria_simulada`
- `fct_cliente_risco_diario_simulada`
- `agg_relacionamento_periodo_simulada`

---

## 🚀 Comando Rápido (Local)

```bash
# 1. Ingestão
cd apps/ingestao_cvm && python ingestao_dados_main.py

# 2. Limpeza
cd ../data_cleaning && python data_cleaning_main.py

# 3. Gold
cd ../gold_layer && python gold_main.py

# ✅ Resultado em: data/gold/
```

---

## 🔗 Referência Rápida de Buckets S3

| Layer | Bucket | Prefixo | Tipo |
|-------|--------|---------|------|
| **Bronze** | s3-asset-sirius-bucket-bronze | cvm/raw | Raw CSV |
| **Silver** | s3-asset-sirius-bucket-silver | cvm/clean | Cleaned CSV |
| **Gold** | s3-asset-sirius-bucket-gold * | - | Parquet |

*Gold layer pode usar banco de dados em vez de S3

---

## 📝 Checklist de Implantação

### Local Development
- [ ] Python 3.9+ instalado
- [ ] `pip install -r requirements.txt` (cada app)
- [ ] Estrutura de pastas: `dados_cvm/`, `dados_tgt/`, `data/gold/`
- [ ] Executar em sequência: ingestão → limpeza → gold

### AWS Deployment
- [ ] Buckets S3 criados
- [ ] IAM roles com permissões S3
- [ ] Lambda functions configuradas
- [ ] Triggers EventBridge (opcional)
- [ ] CloudWatch logs monitorados

### Data Quality
- [ ] CSV Bronze validados
- [ ] Sem duplicatas em Silver
- [ ] Dimensões preenchidas (dim_tempo, dim_fundo)
- [ ] Fatos com valores esperados

---

## 🆘 Suporte Rápido

| Pergunta | Resposta Rápida |
|----------|-----------------|
| Por onde começo? | CONTEXTO_GERAL.md |
| Como executo? | GUIA_RAPIDO.md |
| Como funciona? | ARQUITETURA_TECNICA.md |
| Quebrou algo | GUIA_RAPIDO.md → Troubleshooting |
| Vou modificar código | ARQUITETURA_TECNICA.md |

---

## 📞 Contatos & Recursos

**Projeto**: Asset Sirius Analytics  
**Tipo**: Pipeline ETL - Análise de Fundos CVM  
**Versão**: 1.0 (Fase 1 ✓ + Fase 2 🚧)  
**Última Atualização**: 19/04/2026

**Arquivos principais**:
- [CONTEXTO_GERAL.md](./CONTEXTO_GERAL.md) ← Comece aqui
- [GUIA_RAPIDO.md](./GUIA_RAPIDO.md)
- [TESTE_LOCAL_PASSO_A_PASSO.md](./TESTE_LOCAL_PASSO_A_PASSO.md) ⭐ Teste Local
- [MIGRACAO_LOCAL.md](./MIGRACAO_LOCAL.md) ⭐ 100% Local
- [MUDANCA_SILVER_LOCAL.md](./MUDANCA_SILVER_LOCAL.md) ⭐ dados_tgt → Gold
- [ERRO_CSV_TOKENIZACAO.md](./ERRO_CSV_TOKENIZACAO.md) ⭐ Solução de Erro
- [ADAPTACAO_DINAMICA_COLUNAS.md](./ADAPTACAO_DINAMICA_COLUNAS.md) ⭐ Flexibilidade Gold
- [ARQUITETURA_TECNICA.md](./ARQUITETURA_TECNICA.md)

---

## 🔄 Próximas Etapas

1. ⏳ Completar Fase 2 (Simulador de Clientes)
2. ⏳ Implementar Airflow/EventBridge para orquestração
3. ⏳ Deploy Dashboards (Tableau/PowerBI)
4. ⏳ Testes de performance
5. ⏳ Monitoring em produção

---

**Bem-vindo ao Asset Sirius Analytics!** 🎉

Comece pelo [CONTEXTO_GERAL.md](./CONTEXTO_GERAL.md) e explore!

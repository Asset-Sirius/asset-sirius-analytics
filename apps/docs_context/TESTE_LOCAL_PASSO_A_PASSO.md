# 🧪 Guia Prático - Teste Local Completo

**Data**: 19 de Abril de 2026  
**Objetivo**: Executar o pipeline ETL local do início ao fim  
**Tempo Estimado**: 30-45 minutos

---

## ✅ Pré-Requisitos

Antes de começar, verifique:

```bash
# 1. Python 3.9+ instalado
python --version
# Esperado: Python 3.9.x ou superior

# 2. Git (para clonar, opcional)
git --version

# 3. Espaço em disco (mínimo 1 GB)
# Dados será criados em: dados_cvm/, dados_tgt/, data/gold/
```

**Se faltar Python**:
- Download: https://www.python.org/downloads/
- Windows: Marque "Add Python to PATH" na instalação

---

## 📋 Passo 1: Preparar Ambiente

### 1.1 - Abra PowerShell ou CMD
```powershell
# Windows: Win + R → cmd (ou PowerShell)
cd d:\SPTech\asset-sirius-analytics\apps
```

### 1.2 - Crie um Virtual Environment (recomendado)
```powershell
# Criar venv
python -m venv venv

# Ativar venv
.\venv\Scripts\Activate.ps1
# Se usar CMD: venv\Scripts\activate.bat

# Resultado esperado: (venv) deve aparecer no prompt
```

**Por que Virtual Environment?**
- Isola dependências do projeto
- Evita conflitos com pacotes globais
- Reutilizável

### 1.3 - Atualize pip
```powershell
python -m pip install --upgrade pip
```

---

## 🔽 Passo 2: Ingestão (Download CVM)

### 2.1 - Instale dependências
```powershell
cd apps\ingestao_cvm
pip install -r requirements.txt
```

**Esperado**: Você verá linhas como:
```
Successfully installed pandas-1.5.3 numpy-1.24.3 requests-2.31.0
```

### 2.2 - Execute a ingestão
```powershell
python ingestao_dados_main.py
```

**O que vai acontecer**:
```
Origem dos dados: d:\SPTech\asset-sirius-analytics\dados_cvm
Destino dos dados: d:\SPTech\asset-sirius-analytics\dados_tgt
[...download em progresso...]
✅ Download completo!
```

**⏳ Tempo**: 5-15 minutos (depende velocidade internet)

### 2.3 - Verifique os dados baixados
```powershell
# Volte para a raiz
cd ..\..

# Liste arquivos baixados
ls dados_cvm\

# Esperado: múltiplos arquivos .csv
# Exemplo: registro_fundo.csv, inf_diario_fi_202401.csv, etc
```

**✅ Sucesso se**:
- Pasta `dados_cvm/` foi criada
- Contém arquivos `.csv`
- Arquivos não vazios (tamanho > 0 bytes)

---

## 🧹 Passo 3: Limpeza (Data Cleaning)

### 3.1 - Instale dependências
```powershell
cd apps\data_cleaning
pip install -r requirements.txt
```

### 3.2 - Execute limpeza
```powershell
python data_cleaning_main.py
```

**O que vai acontecer**:
```
Origem dos dados: d:\SPTech\asset-sirius-analytics\dados_cvm
Destino dos dados: d:\SPTech\asset-sirius-analytics\dados_tgt
Processando: registro_fundo.csv
Validando: inf_diario_fi_202401.csv
[...limpeza em progresso...]
✅ Limpeza completa!
```

**⏳ Tempo**: 2-5 minutos

### 3.3 - Verifique os dados limpos
```powershell
cd ..\..
ls dados_tgt\

# Esperado: mesmos arquivos, mas limpos/validados
```

**✅ Sucesso se**:
- Pasta `dados_tgt/` foi criada
- Contém mesmos `.csv` que `dados_cvm/`
- Tamanho ligeiramente menor (removidas duplicatas)

---

## ✨ Passo 4: Gold Layer (Analytics)

### 4.1 - Instale dependências
```powershell
cd apps\gold_layer
pip install -r requirements.txt
```

**Pode levar mais tempo** pois instala `sqlalchemy`, `pyarrow`, etc.

### 4.2 - (Opcional) Ajuste configurações
```powershell
# Abra com editor (VSCode, Notepad++)
notepad config.py
```

**Configurações padrão são boas**, mas pode customizar:
```python
DATA_INICIO_GOLD = "2023-01-01"       # Ajuste período se quiser
DATA_FIM_GOLD = "2024-12-31"
PERIODOS_AGREGACAO = [7, 30, 60, 90]  # Agregações
```

### 4.3 - Execute gold layer
```powershell
python gold_main.py
```

**O que vai acontecer**:
```
Lendo dados Silver...
Criando dimensão tempo...
Criando dimensão fundos...
Criando fatos diários...
Criando agregações...
[...processamento...]
✅ Gold Layer completo!
```

**⏳ Tempo**: 2-5 minutos

### 4.4 - Verifique os dados Gold
```powershell
cd ..\..
ls data\gold\

# Esperado: arquivos .parquet
# Exemplo: dim_tempo.parquet, dim_fundo.parquet, etc
```

**✅ Sucesso se**:
- Pasta `data/gold/` foi criada
- Contém arquivos `.parquet`:
  - `dim_tempo.parquet`
  - `dim_fundo.parquet`
  - `fct_fundo_diario.parquet`
  - `agg_fundo_periodo.parquet`

---

## 🔍 Passo 5: Validação e Inspeção

### 5.1 - Verificar estrutura final
```powershell
# Resumo da estrutura criada
ls -la dados_cvm/ | head -20     # Bronze (brutos)
ls -la dados_tgt/ | head -20     # Silver (limpos) = entrada do Gold
ls -la data\gold\ | head -20     # Gold (parquet)
```

### 5.2 - Inspecionar um arquivo Parquet (Python)
```powershell
# Abra Python
python
```

```python
import pandas as pd

# Ler dimensão tempo
df_tempo = pd.read_parquet('data/gold/dim_tempo.parquet')
print("dim_tempo shape:", df_tempo.shape)
print(df_tempo.head())

# Ler dimensão fundo
df_fundo = pd.read_parquet('data/gold/dim_fundo.parquet')
print("\ndim_fundo shape:", df_fundo.shape)
print(df_fundo.head())

# Ler fato diário
df_diario = pd.read_parquet('data/gold/fct_fundo_diario.parquet')
print("\nfct_fundo_diario shape:", df_diario.shape)
print(df_diario.head())

# Sair
exit()
```

**Esperado**:
```
dim_tempo shape: (730, 8)           # ~2 anos de dados
dim_tempo sample:
  data_id        data  mes  trimestre  ano  semana
0 20230101  2023-01-01    1         1 2023      1

dim_fundo shape: (150, 8)           # ~150 fundos CVM
fct_fundo_diario shape: (25000, 10)  # Muitas linhas de fatos
```

### 5.3 - Consultas SQL (Opcional - mais avançado)
```python
import pandas as pd

# Ler tabelas
tempo = pd.read_parquet('data/gold/dim_tempo.parquet')
fundo = pd.read_parquet('data/gold/dim_fundo.parquet')
diario = pd.read_parquet('data/gold/fct_fundo_diario.parquet')
periodo = pd.read_parquet('data/gold/agg_fundo_periodo.parquet')

# Exemplo: Fundos por segmento
print(diario.merge(fundo, on='fundo_id')
      .groupby('segmento')['patrimonio_liquido']
      .agg(['count', 'mean', 'sum'])
      .round(2))

# Exemplo: Fluxo por período
print(periodo.groupby('periodo_dias')['fluxo_total'].describe())
```

---

## 🐛 Troubleshooting Comum

### ❌ Erro: "ModuleNotFoundError: pandas"
**Causa**: Dependências não instaladas  
**Solução**:
```powershell
# Certifique-se de estar no venv
.\venv\Scripts\Activate.ps1

# Reinstale
pip install -r requirements.txt -force-reinstall
```

### ❌ Erro: "Diretório de entrada não encontrado"
**Causa**: Pulou a ingestão  
**Solução**:
```powershell
# Volte e execute ingestão primeiro
cd apps\ingestao_cvm
python ingestao_dados_main.py
```

### ❌ Erro: "Connection timeout" (ingestão)
**Causa**: CVM server lento ou offline  
**Solução**:
```powershell
# Tente novamente mais tarde
# Ou ajuste timeout em ingestao_dados_main.py:
TIMEOUT_SEGUNDOS = 900  # Aumentar para 15 min
```

### ❌ Erro: "Permission denied" ao criar pastas
**Causa**: Provavelmente antivírus bloqueando  
**Solução**:
```powershell
# Tente rodar como Administrador
# Ou mude permissões: icacls "d:\SPTech" /grant:r %username%:F /t
```

### ❌ Erro: "Out of memory"
**Causa**: Seus dados são muito grandes  
**Solução**:
```powershell
# Feche outros programas
# Ou processe em chunks (editar código)
```

---

## ✅ Checklist de Sucesso

- [ ] Python 3.9+ instalado
- [ ] venv criado e ativado
- [ ] Pip atualizado
- [ ] `ingestao_dados_main.py` executado sem erros
- [ ] `dados_cvm/` criado com arquivos CSV
- [ ] `data_cleaning_main.py` executado sem erros
- [ ] `dados_tgt/` criado com arquivos CSV limpos
- [ ] `gold_main.py` executado sem erros
- [ ] `data/gold/` criado com arquivos Parquet
- [ ] Arquivos Parquet podem ser lidos com pandas

---

## 📊 Próximas Etapas (Após sucesso)

### 1. Visualizar com SQL/BI
```python
# Conectar a banco de dados e criar dashboards
# Recomendado: DuckDB (leve), SQLite, ou Pandas
```

### 2. Testar Performance
```powershell
# Medir tempo de cada etapa
Measure-Command { python gold_main.py }
```

### 3. Escalar para AWS Lambda
```powershell
# Próximo passo: deploy Lambda
# Ver: GUIA_RAPIDO.md (seção AWS)
```

### 4. Automatizar
```powershell
# Criar task scheduler (Windows) ou cron (Linux)
# Para rodar diariamente
```

---

## 🎯 Dicas Profissionais

### Reutilizar venv em execuções futuras
```powershell
# Não precisa criar de novo:
.\venv\Scripts\Activate.ps1
cd apps\gold_layer
python gold_main.py
```

### Monitorar consumo de memória
```powershell
# Em outro terminal:
Get-Process python | Select-Object ProcessName, WorkingSet

# No Linux/WSL:
watch -n 1 'ps aux | grep python'
```

### Fazer backup dos dados
```powershell
# Após sucesso, salvar dados:
Copy-Item -Path "dados_cvm" -Destination "backup_$(Get-Date -f 'yyyyMMdd')" -Recurse
Copy-Item -Path "data/gold" -Destination "backup_$(Get-Date -f 'yyyyMMdd')\gold" -Recurse
```

### Debug avançado
```python
# Adiciar isto em gold_main.py para logs detalhados:
import logging
logging.basicConfig(level=logging.DEBUG)
```

---

## 📞 Referência Rápida

| Comando | Propósito |
|---------|-----------|
| `.\venv\Scripts\Activate.ps1` | Ativar ambiente virtual |
| `pip install -r requirements.txt` | Instalar dependências |
| `python ingestao_dados_main.py` | Baixar dados CVM |
| `python data_cleaning_main.py` | Limpar dados |
| `python gold_main.py` | Criar analytics |
| `pip list` | Ver pacotes instalados |
| `pip freeze > requirements_novo.txt` | Exportar dependências |

---

## 🚀 Resumo do Fluxo

```
┌──────────────────────────────────────┐
│ 1. SETUP (5 min)                     │
│ ├─ Python install                    │
│ ├─ venv create + activate            │
│ └─ pip upgrade                       │
├──────────────────────────────────────┤
│ 2. INGESTÃO (10-15 min)             │
│ ├─ pip install requirements          │
│ ├─ python ingestao_main.py           │
│ └─ Verifique dados_cvm/              │
├──────────────────────────────────────┤
│ 3. LIMPEZA (3-5 min)                │
│ ├─ pip install requirements          │
│ ├─ python data_cleaning_main.py      │
│ └─ Verifique dados_tgt/              │
├──────────────────────────────────────┤
│ 4. GOLD (3-5 min)                   │
│ ├─ pip install requirements          │
│ ├─ python gold_main.py               │
│ └─ Verifique data/gold/              │
├──────────────────────────────────────┤
│ 5. VALIDAÇÃO (5 min)                │
│ ├─ Verificar arquivos .parquet       │
│ ├─ Ler com pandas                    │
│ └─ Inspecionar dados                 │
└──────────────────────────────────────┘
   TOTAL: ~30-45 minutos para primeira run
```

---

**Boa sorte! Qualquer dúvida, consulte [GUIA_RAPIDO.md](./GUIA_RAPIDO.md)** 🚀

Última Atualização: 19/04/2026

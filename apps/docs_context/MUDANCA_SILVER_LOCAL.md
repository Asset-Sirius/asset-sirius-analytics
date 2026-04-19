# 🔄 Mudança: Gold Lê do dados_tgt/

**Data**: 19 de Abril de 2026  
**Mudança**: Gold Layer agora lê dados de `dados_tgt/` (saída data_cleaning)  
**Status**: ✅ Implementado

---

## 📊 Mudança de Fluxo

### Antes ❌
```
Ingestão → dados_cvm/ ✓
Limpeza → dados_tgt/ ✓
Gold → data/silver/ (esperado mas não existia)
      → Erro: dados não encontrados
```

### Agora ✅
```
Ingestão → dados_cvm/ ✓
Limpeza → dados_tgt/ ✓
Gold → dados_tgt/ (lê daqui)
    → data/gold/ (salva aqui em parquet)
```

---

## 🔧 Alterações Efetivadas

### **config.py** - Path Silver
```python
# ANTES
PATH_DATA_SILVER = PATH_PROJECT / "data" / "silver"

# DEPOIS ✅
PATH_DATA_SILVER = PATH_PROJECT / "dados_tgt"  # Saída data_cleaning
```

**Impacto**:
- ✅ Gold lê de `dados_tgt/` (saída do data_cleaning)
- ✅ Sem necessidade de copiar/mover arquivos
- ✅ Pipeline direto: ingestão → limpeza → gold

---

## 📁 Estrutura Final (Após Alteração)

```
apps/ (raiz do workspace)
├── ingestao_cvm/
│   ├── ingestao_dados_main.py     → Gera: dados_cvm/*.csv
│   └── requirements.txt
│
├── data_cleaning/
│   ├── data_cleaning_main.py      → Lê: dados_cvm/*.csv
│   └── requirements.txt            → Gera: dados_tgt/*.csv
│
├── gold_layer/
│   ├── gold_main.py               → Lê: dados_tgt/*.csv (MUDANÇA!)
│   ├── config.py                  → PATH_DATA_SILVER = "dados_tgt"
│   └── requirements.txt            → Gera: data/gold/*.parquet
│
└── dados_cvm/                       ← Bronze
    ├── inf_diario_fi_202301.csv
    ├── inf_diario_fi_202302.csv
    └── ...

dados_tgt/                          ← Silver (agora entrada do Gold!)
├── inf_diario_fi_*_clean.csv
├── registro_fundo_clean.csv
└── registro_classe_clean.csv

data/
└── gold/                           ← Gold (saída final)
    ├── dim_tempo.parquet
    ├── dim_fundo.parquet
    ├── fct_fundo_diario.parquet
    ├── agg_fundo_periodo.parquet
    └── dim_cliente_simulado.parquet
```

---

## 🚀 Executar (Novo Fluxo)

```powershell
# 1. Ingestão
cd apps\ingestao_cvm
python ingestao_dados_main.py
# ✓ Cria: dados_cvm/*.csv

# 2. Limpeza
cd ..\data_cleaning
python data_cleaning_main.py
# ✓ Cria: dados_tgt/*.csv (limpos)

# 3. Gold (lê de dados_tgt!)
cd ..\gold_layer
python gold_main.py
# ✓ Lê de: dados_tgt/*.csv
# ✓ Cria: data/gold/*.parquet
```

---

## ✅ Verificação

### Verificar que Gold está lendo de dados_tgt
```bash
# Conferir que config.py aponta certo
cd apps/gold_layer
python -c "from config import PATH_DATA_SILVER; print(f'Silver: {PATH_DATA_SILVER}')"
# Output: Silver: d:\SPTech\asset-sirius-analytics\dados_tgt

# Verificar arquivos
ls ../../dados_tgt/
# inf_diario_fi_*_clean.csv
# registro_fundo_clean.csv
# registro_classe_clean.csv
```

---

## 📝 Arquivos Modificados

| Arquivo | Mudança |
|---------|---------|
| `gold_layer/config.py` | `PATH_DATA_SILVER = "dados_tgt"` ✅ |
| `docs_context/GUIA_RAPIDO.md` | Estrutura de saída atualizada ✅ |
| `docs_context/MIGRACAO_LOCAL.md` | Fluxo atualizado ✅ |
| `docs_context/TESTE_LOCAL_PASSO_A_PASSO.md` | Validação atualizada ✅ |

---

## 🎯 Benefícios

✅ **Sem duplicação de dados**: Não precisa copiar para `data/silver/`  
✅ **Fluxo direto**: Saída limpeza = entrada gold  
✅ **Menos I/O**: Menos leitura/escrita de arquivos  
✅ **Mais eficiente**: Processo atomizado  
✅ **Fácil manutenção**: Um único caminho de saída (`dados_tgt/`)

---

## 🔗 Próximas Melhorias

- [ ] Adicionar validação se `dados_tgt/` existe
- [ ] Mensagem clara no output mostrando origem
- [ ] (Futuro) Usar symlinks ou hard links para poupar espaço
- [ ] (Futuro) Implementar backup automático

---

**Status**: 🟢 Implementado e Testado  
**Última Atualização**: 19/04/2026

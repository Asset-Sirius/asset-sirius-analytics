# asset-sirius-analytics

Repositório para ingestão e tratamento de dados de Fundos de Investimento (CVM), com foco em geração de base limpa para análises.

## Visão geral

O projeto possui dois fluxos principais:

- **Ingestão**: baixa dados da CVM para o diretório `dados_cvm`.
- **Data Cleaning**: aplica limpeza e filtros, gerando arquivos no diretório `dados_tgt`.

## Estrutura principal

- `apps/ingestao_cvm/`
	- `ingestao_dados_main.py`: ponto de entrada da ingestão.
	- `ingestao_dados_functions.py`: funções de download, extração e normalização de arquivos.
- `apps/data_cleaning/`
	- `data_cleaning_main.py`: ponto de entrada da limpeza.
	- `data_cleaning_functions.py`: funções de limpeza, filtros cadastrais e tratamento dos informes diários.
- `dados_cvm/`: saída da ingestão (arquivos brutos da CVM).
- `dados_tgt/`: saída da limpeza (arquivos tratados).

## Pré-requisitos

- Python 3.10+
- Bibliotecas Python:
	- `pandas`
	- `certifi` (opcional, recomendado para SSL)

Exemplo de instalação:

```bash
pip install pandas certifi
```

## Como executar

No diretório raiz do projeto:

### 1) Ingestão

```bash
python3 apps/ingestao_cvm/ingestao_dados_main.py
```

Esse processo:

- limpa o diretório `dados_cvm`;
- baixa os arquivos relevantes da CVM;
- descompacta ZIPs;
- mantém apenas arquivos `.csv`.

### 2) Data Cleaning

```bash
python3 apps/data_cleaning/data_cleaning_main.py
```

Esse processo:

- lê os CSVs de `dados_cvm`;
- executa limpeza básica e filtros de negócio;
- grava os arquivos tratados em `dados_tgt`.

## Arquivos gerados

Exemplos de saídas tratadas:

- `registro_fundo_clean.csv`
- `registro_classe_clean.csv`
- `registro_subclasse_clean.csv`
- `inf_diario_fi_YYYYMM_clean.csv`

## Observações

- `dados_cvm` e `dados_tgt` são diretórios de dados gerados localmente e estão ignorados no versionamento via `.gitignore`.
- Pastas `__pycache__` dos módulos de ingestão e limpeza também são ignoradas.

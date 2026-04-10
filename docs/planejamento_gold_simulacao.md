# Planejamento - Tabelas Gold e Simulacao de Dados

## 1) Objetivo

Definir a camada gold para alimentar os dois dashboards do prototipo:
- Dashboard do Gestor (fluxo, patrimonio, cotistas, comparacao entre fundos)
- Dashboard de Relacionamento (risco de cliente, churn, carteira)

E definir a estrategia de simulacao de dados para cobrir metricas nao disponiveis na base CVM atual.

## 2) Diagnostico das Camadas Atuais

## 2.1 Bronze e Silver existentes

Entradas CVM (bronze):
- inf_diario_fi_YYYYMM.csv
- registro_fundo.csv
- registro_classe.csv
- registro_subclasse.csv

Saidas tratadas (silver):
- inf_diario_fi_YYYYMM_clean.csv
- registro_fundo_clean.csv
- registro_classe_clean.csv
- registro_subclasse_clean.csv

Campos criticos disponiveis no informe diario (silver):
- CNPJ_FUNDO_CLASSE
- DT_COMPTC
- VL_PATRIM_LIQ
- CAPTC_DIA
- RESG_DIA
- NR_COTST

Conclusao: o Dashboard do Gestor e amplamente coberto por dados reais da CVM.

## 2.2 Lacunas para os dashboards

Lacunas para Dashboard de Relacionamento (cliente):
- Nao existe identificador de cliente
- Nao existe cadastro de cliente
- Nao existe historico de movimentacao por cliente
- Nao existe score de risco/churn por cliente
- Nao existe segmentacao comportamental por cliente

Lacunas para Dashboard do Gestor (comparacao e risco):
- Nao existe indicador de risco do fundo pronto para consumo (baixo/medio/alto)
- Necessario derivar classificacao ou regra de risco (ex.: volatilidade de fluxo, variacao de PL, concentracao de cotistas)

## 3) Modelo Gold Proposto

## 3.1 Convencoes

- Chaves tecnicas padronizadas:
  - sk_data: inteiro no formato YYYYMMDD
  - sk_fundo: hash ou surrogate key por CNPJ do fundo/classe normalizado
  - sk_cliente: surrogate key para cliente simulado
- CNPJ sempre normalizado (somente digitos)
- Particionamento recomendado por ano_mes_referencia

## 3.2 Dimensoes Gold

### dim_tempo
Granularidade: 1 linha por dia

Colunas principais:
- sk_data
- data
- ano
- mes
- dia
- trimestre
- dia_semana
- eh_fim_de_mes

Fonte:
- gerada internamente

### dim_fundo
Granularidade: 1 linha por fundo/classe

Colunas principais:
- sk_fundo
- cnpj_fundo_classe_norm
- id_registro_fundo
- id_registro_classe
- nome_fundo (denominacao social)
- classificacao
- tipo_fundo
- forma_condominio
- situacao
- publico_alvo
- gestor
- administrador
- dt_inicio
- dt_fim_vigencia (SCD2 opcional)

Fonte:
- registro_classe_clean + registro_fundo_clean (priorizar cobertura de match por CNPJ)

### dim_cliente_simulado
Granularidade: 1 linha por cliente

Colunas principais:
- sk_cliente
- id_cliente
- nome_cliente
- data_nascimento
- faixa_etaria
- perfil_risco (conservador/moderado/arrojado/agressivo)
- segmento (varejo/private/institucional)
- uf
- cidade
- renda_estimada
- patrimonio_estimado
- data_inicio_relacionamento
- canal_origem
- status_cliente (ativo/inativo)

Fonte:
- simulador

## 3.3 Fatos Gold

### fct_fundo_diario
Granularidade: 1 linha por fundo por dia

Colunas principais:
- sk_data
- sk_fundo
- cnpj_fundo_classe_norm
- captacao_dia
- resgate_dia
- fluxo_liquido_dia (captacao_dia - resgate_dia)
- patrimonio_liquido_dia
- cotistas_dia
- variacao_pl_dia_pct
- variacao_cotistas_dia_pct
- flag_base_real (sempre true)

Fonte:
- inf_diario_fi_YYYYMM_clean.csv + join com dim_fundo

Uso no dashboard:
- KPI fluxo liquido
- KPI total de resgates
- KPI variacao patrimonio
- KPI variacao cotistas
- Grafico resgates vs captacao
- Grafico evolucao patrimonio liquido
- Grafico variacao de cotistas

### agg_fundo_periodo
Granularidade: 1 linha por fundo por janela

Colunas principais:
- periodo (7D/30D/60D/90D)
- data_referencia
- sk_fundo
- captacao_periodo
- resgate_periodo
- fluxo_liquido_periodo
- var_patrimonio_periodo_pct
- var_cotistas_periodo_pct
- score_risco_fundo
- nivel_risco_fundo (baixo/medio/alto)

Fonte:
- derivada de fct_fundo_diario

Uso no dashboard:
- cards com filtros por janela
- lista comparativa entre fundos com nivel de risco

### fct_cliente_posicao_diaria_simulada
Granularidade: 1 linha por cliente por fundo por dia

Colunas principais:
- sk_data
- sk_cliente
- sk_fundo
- valor_investido
- aporte_dia
- resgate_dia
- saldo_dia
- dias_sem_movimentacao
- variacao_saldo_30d_pct
- flag_base_real (false)

Fonte:
- simulador (com calibracao por distribuicoes de fluxo de fct_fundo_diario)

Uso no dashboard:
- carteira de clientes
- top clientes por valor em risco

### fct_cliente_risco_diario_simulada
Granularidade: 1 linha por cliente por dia

Colunas principais:
- sk_data
- sk_cliente
- score_risco (0-100)
- prob_churn (0-1)
- nivel_risco (estavel/atencao/critico)
- valor_em_risco
- tendencia_risco (subindo/estavel/caindo)
- ultimo_mov_dias
- motivo_risco_dominante
- flag_base_real (false)

Fonte:
- simulador/regras

Uso no dashboard:
- clientes em risco
- valor sob risco
- taxa de churn estimada
- clientes criticos
- distribuicao por nivel de risco
- evolucao do risco medio
- tabela de carteira (score, churn, tendencia)

### agg_relacionamento_periodo_simulada
Granularidade: 1 linha por data de referencia (e opcionalmente por segmento)

Colunas principais:
- data_referencia
- segmento
- total_clientes
- clientes_em_risco
- clientes_criticos
- valor_sob_risco
- churn_estimado_pct
- risco_medio

Fonte:
- derivada de fct_cliente_risco_diario_simulada

Uso no dashboard:
- cards e series agregadas do dashboard de relacionamento

## 4) Mapeamento de Metricas do Prototipo

Dashboard do Gestor:
- Fluxo Liquido = soma(captacao_dia - resgate_dia) no periodo
- Total de Resgates = soma(resgate_dia) no periodo
- Var. Patrimonio = (PL_final - PL_inicial) / PL_inicial
- Var. Cotistas = (cotistas_final - cotistas_inicial) / cotistas_inicial
- Comparacao entre fundos = ranking por fluxo_liquido_periodo + nivel_risco_fundo

Dashboard de Relacionamento:
- Clientes em Risco = count(score_risco >= limiar_atencao)
- Valor Sob Risco = soma(valor_em_risco dos clientes em risco)
- Taxa de Churn Estimada = media(prob_churn)
- Clientes Criticos = count(nivel_risco = critico)
- Distribuicao de Risco = count por nivel_risco
- Evolucao de Risco Medio = media(score_risco) por semana
- Top clientes em risco = top N por valor_em_risco

## 5) Definicao da Simulacao de Dados

## 5.1 Entidades para simular

Obrigatorias:
- Cadastro de cliente
- Posicao de investimento por cliente e fundo
- Movimentacao diaria (aporte/resgate)
- Sinais de risco/churn

## 5.2 Estrategia de simulacao

1. Gerar base de clientes:
- Quantidade inicial recomendada: 5.000 a 20.000 clientes
- Distribuir por segmento, perfil de risco, faixa de renda e regiao

2. Alocar clientes em fundos reais da CVM:
- Usar dim_fundo como universo de fundos
- Distribuir ticket medio por perfil/segmento
- Respeitar concentracao para reproduzir carteiras realistas

3. Simular serie diaria de movimentacoes:
- Aportes e resgates por cliente/fundo
- Calibrar distribuicao com base em captacao/resgate reais de fct_fundo_diario
- Inserir sazonalidade mensal e eventos extremos de mercado

4. Simular risco e churn:
- Definir score por funcao de comportamento:
  - alta frequencia de resgates
  - queda de saldo
  - longos periodos sem movimentacao
  - volatilidade de aportes
- Converter score em probabilidade de churn
- Classificar nivel (estavel/atencao/critico)

5. Garantir consistencia:
- Soma das posicoes por fundo proxima ao PL real (com fator de escala)
- Nenhum saldo negativo
- Probabilidade de churn entre 0 e 1

## 5.3 Regras iniciais sugeridas (v1)

- score_risco = 0.35*z(resgates_30d_pct) + 0.25*z(queda_saldo_30d_pct) + 0.20*z(inatividade_dias) + 0.20*z(volatilidade_fluxo)
- prob_churn = sigmoid((score_risco - 50)/10)
- nivel_risco:
  - estavel: score < 45
  - atencao: 45 <= score < 70
  - critico: score >= 70

## 6) Plano de Implementacao

Fase 1 - Gold de Fundos (dados reais CVM):
- Entregar dim_tempo, dim_fundo, fct_fundo_diario, agg_fundo_periodo
- Validar todos os graficos e KPIs do Dashboard do Gestor

Fase 2 - Simulador de Clientes:
- Entregar dim_cliente_simulado, fct_cliente_posicao_diaria_simulada, fct_cliente_risco_diario_simulada, agg_relacionamento_periodo_simulada
- Validar Dashboard de Relacionamento com dados sinteticos

Fase 3 - Qualidade e Operacao:
- Testes de qualidade de dados
- Reprocessamento incremental diario
- Versionamento de parametros de simulacao

## 7) Criterios de Pronto

- Todas as metricas dos dois dashboards saem de tabelas gold (sem calculo no front-end)
- Filtros 7D/30D/60D/90D respondem em ate 2s no ambiente alvo
- Reconciliacao basica do Gestor:
  - fluxo_liquido_periodo = captacao_periodo - resgate_periodo
  - variacoes coerentes com serie diaria
- Simulacao reproduz distribuicoes plausiveis de:
  - ticket medio
  - frequencia de resgate
  - proporcao de clientes por nivel de risco
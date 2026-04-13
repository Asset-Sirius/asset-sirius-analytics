USE `ASSET_SIRIUS`;

INSERT INTO ASSET_SIRIUS.segmento (nome) VALUES
('Varejo'),
('Alta Renda'),
('Private');

INSERT INTO ASSET_SIRIUS.cliente 
(fk_segmento, cpf_cnpj, nome, perfil_risco, tipo_investidor, data_cadastro)
VALUES
(1, '12345678900', 'Carlos Eduardo Almeida', 'Conservador','F', NOW()),
(1, '98765432100', 'Fernanda Souza Lima', 'Moderado', 'F', NOW()),
(2, '45678901000122', 'Tech Investimentos LTDA', 'Arrojado', 'J', NOW()),
(2, '12345678000199', 'Alpha Capital Gestão', 'Arrojado', 'J', NOW()),
(1, '32165498711', 'Ricardo Mendes Costa', 'Moderado', 'F', NOW()),
(3, '65498732122', 'Juliana Pereira Rocha', 'Conservador', 'F', NOW()),
(3, '78912345000155', 'Family Office Orion', 'Moderado', 'J', NOW());

-- Inserts para gerente
INSERT INTO ASSET_SIRIUS.gerente (fk_segmento, nome, email) VALUES
(1, 'Roberto Ferreira', 'roberto.ferreira@assetsirius.com'),
(2, 'Ana Paula Mello', 'ana.mello@assetsirius.com'),
(3, 'Ricardo Lemos', 'ricardo.lemos@assetsirius.com');

-- Inserts para cliente_gerente
INSERT INTO ASSET_SIRIUS.cliente_gerente (cliente_id_cliente, gerente_id_gerente, data_inicio, data_fim) VALUES
(1, 1, '2023-01-15', NULL),
(2, 1, '2023-02-10', NULL),
(3, 2, '2023-03-20', NULL),
(4, 2, '2023-04-05', NULL),
(5, 1, '2023-05-12', NULL),
(6, 3, '2023-06-18', NULL),
(7, 3, '2023-07-22', NULL);

-- Inserts para gestor_fundo
INSERT INTO ASSET_SIRIUS.gestor_fundo (nome, email) VALUES
('SIRIUS GESTAO', 'gestao@assetsirius.com'),
('TERCEIROS GESTAO', 'contato@terceirosgestao.com');

-- ===============================================
-- 1. FUNDOS (IDs Sequenciais previstos: 1, 2, 3)
-- ===============================================
INSERT INTO ASSET_SIRIUS.fundo (cnpj, denominacao_social, gestor, cnpj_gestor, administrador, cnpj_administrador, tipo_fundo, data_constituicao, data_registro, codigo_cvm, data_cancelamento, situacao, data_inicio_situacao, is_proprio) VALUES
('11111111000111', 'SIRIUS RENDA FIXA FIC RF', 'SIRIUS GESTAO', '22222222000122', 'BEM DTVM', '33333333000133', 'RENDA FIXA', '2020-01-01', '2020-01-05', 123456, NULL, 'NORMAL', '2020-01-05', 'S'),
('44444444000144', 'SIRIUS MULTIMERCADO FIM', 'SIRIUS GESTAO', '22222222000122', 'BEM DTVM', '33333333000133', 'MULTIMERCADO', '2020-02-01', '2020-02-05', 654321, NULL, 'NORMAL', '2020-02-05', 'S'),
('55555555000155', 'TERCEIROS ACOES FIA', 'TERCEIROS GESTAO', '66666666000166', 'INTRAG', '77777777000177', 'ACOES', '2021-03-01', '2021-03-05', 987654, NULL, 'NORMAL', '2021-03-05', 'N');

-- Inserts para usuario
INSERT INTO ASSET_SIRIUS.usuario (fk_gerente, fk_gestor_fundo, email, senha, is_adm) VALUES
(1, 1, 'admin@assetsirius.com', 'admin123', 'S'),
(2, 1, 'ana.mello@assetsirius.com', 'senha123', 'N');

-- ===============================================
-- 2. CLASSES (IDs previstos: 1, 2, 3, 4)
-- Fundo 1 (RF) tem Classe 1 e Classe 2
-- Fundo 2 (Multi) tem Classe 3
-- Fundo 3 (Acoes) tem Classe 4
-- ===============================================
INSERT INTO ASSET_SIRIUS.classe (fk_registro_fundo, cnpj, id_registro_fundo, tipo_classe, denominacao_social, classificacao, auditor, custodiante, controlador, classificacao_anbima, indicador_desempenho, is_proprio) VALUES
(1, '11111111000111', 1, 'RENDA FIXA', 'SIRIUS RENDA FIXA SIMPLES', 'RF', 'KPMG', 'BRADESCO', 'BRADESCO', 'Renda Fixa Duração Livre', 'CDI', 'S'),
(1, '11111111000111', 1, 'RENDA FIXA', 'SIRIUS RENDA FIXA MASTER', 'RF', 'KPMG', 'BRADESCO', 'BRADESCO', 'Renda Fixa Institucional', 'CDI', 'S'),
(2, '44444444000144', 2, 'MULTIMERCADO', 'SIRIUS MULTIMERCADO GERAL', 'MM', 'KPMG', 'BRADESCO', 'BRADESCO', 'Multimercado Macro', 'CDI', 'S'),
(3, '55555555000155', 3, 'ACOES', 'TERCEIROS ACOES FIA CLASSE A', 'ACOES', 'PWC', 'ITAU', 'ITAU', 'Ações Livre', 'IBOV', 'N');

-- ===============================================
-- 3. SUBCLASSES (IDs previstos: 1, 2, 3, 4, 5)
-- Classe 1 tem Subclasse 1 e 2
-- Classe 2 tem Subclasse 3
-- Classe 3 tem Subclasse 4
-- Classe 4 tem Subclasse 5
-- ===============================================
INSERT INTO ASSET_SIRIUS.subclasse (id_registro_classe, codigo_cvm, denominacao_social, situacao, publico_alvo, previdenciario, is_proprio) VALUES
(1, 123001, 'SIRIUS RF SIMPLES VAREJO', 'NORMAL', 'PÚBLICO GERAL', 'N', 'S'),
(1, 123002, 'SIRIUS RF SIMPLES ALTA RENDA', 'NORMAL', 'INVESTIDORES QUALIFICADOS', 'N', 'S'),
(2, 123003, 'SIRIUS RF MASTER INSTITUCIONAL', 'NORMAL', 'INVESTIDORES PROFISSIONAIS', 'N', 'S'),
(3, 654001, 'SIRIUS MULTIMERCADO FIM SUBCLASSE UNICA', 'NORMAL', 'INVESTIDORES QUALIFICADOS', 'N', 'S'),
(4, 987001, 'TERCEIROS ACOES SUBCLASSE PADRAO', 'NORMAL', 'PÚBLICO GERAL', 'N', 'N');

-- Inserts para informe_diario
INSERT INTO ASSET_SIRIUS.informe_diario (data_competencia, fk_subclasse, fk_registro_classe, fundo_id_registro_fundo, valor_patrimonio_liquido, quantidade_cotista, valor_cota, captacao, resgate, data_insercao, data_modificacao) VALUES
('2024-01-01', 1, 1, 1, 5000000.00, 25, 10.50, 250000.00, 50000.00, NOW(), NOW()),
('2024-01-01', 2, 1, 1, 5000000.00, 25, 10.55, 250000.00, 50000.00, NOW(), NOW()),
('2024-01-01', 3, 2, 1, 25000000.00, 5, 15.00, 10000000.00, 0.00, NOW(), NOW()),
('2024-01-01', 4, 3, 2, 5000000.00, 20, 25.40, 200000.00, 0.00, NOW(), NOW()),
('2024-01-01', 5, 4, 3, 2000000.00, 100, 15.20, 10000.00, 5000.00, NOW(), NOW());

-- Inserts para fundo_cotista
INSERT INTO ASSET_SIRIUS.fundo_cotista (fk_registro_classe, fk_subclasse, fk_cliente, data_inicio, quantidade_cotas) VALUES
(1, 1, 1, '2023-01-20', 1000.000),
(1, 1, 2, '2023-02-15', 500.000),
(1, 2, 3, '2023-03-25', 2000.000),
(2, 3, 4, '2023-04-10', 3000.000),
(3, 4, 5, '2023-05-15', 400.000),
(4, 5, 6, '2023-06-20', 1500.000),
(3, 4, 7, '2023-07-25', 5000.000);

-- Inserts para parametro_alerta (PARA TODAS AS SUBCLASSES - Como solicitado!)
INSERT INTO ASSET_SIRIUS.parametro_alerta (fk_registro_classe, fk_subclasse, nome, valor) VALUES
(1, 1, 'ALTA_RESGATE_PORCENTAGEM', '0.20'),
(1, 2, 'ALTA_RESGATE_PORCENTAGEM', '0.20'),
(2, 3, 'ALTA_RESGATE_PORCENTAGEM', '0.20'),
(3, 4, 'ALTA_RESGATE_PORCENTAGEM', '0.20'),
(4, 5, 'ALTA_RESGATE_PORCENTAGEM', '0.20');

-- Inserts para aplicacao_resgate
INSERT INTO ASSET_SIRIUS.aplicacao_resgate (fk_cliente, fk_classe, fk_subclasse, natureza_operacao, valor, data) VALUES
(1, 1, 1, 'A', 10500.00, '2024-01-01 10:00:00'),
(2, 1, 1, 'R', 5000.00, '2024-01-02 11:30:00'),
(3, 1, 2, 'A', 20000.00, '2024-01-01 10:00:00'),
(4, 2, 3, 'R', 100000.00, '2024-01-02 11:30:00'),
(5, 3, 4, 'A', 50000.00, '2024-01-01 14:15:00'),
(6, 4, 5, 'R', 10000.00, '2024-01-02 15:45:00');

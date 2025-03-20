-- 1. Quantos chamados foram abertos no dia 01/04/2023?
-- Resposta: 1.903 chamados foram abertos no dia 01/04/2023.
SELECT 
    COUNT(*) 
FROM 
    `datario.adm_central_atendimento_1746.chamado` 
WHERE 
  data_inicio >= "2023-04-01 00:00:00" 
AND 
  data_inicio < "2023-04-02 00:00:00";

-- 2. Qual o tipo de chamado que teve mais ocorrências nesse dia?
-- Resposta: O tipo de chamado com mais ocorrências foi "Estacionamento Irregular", com 373 chamados abertos.
SELECT
  tipo, 
  COUNT(*) AS total_chamados
FROM 
  `datario.adm_central_atendimento_1746.chamado`
WHERE 
  data_inicio >= '2023-04-01 00:00:00' 
AND 
  data_inicio < '2023-04-02 00:00:00'
GROUP BY 
  tipo
ORDER BY 
  total_chamados DESC;

-- 3. Quais os nomes dos 3 bairros que mais tiveram chamados abertos nesse dia?
-- Resposta: Os três bairros com mais chamados foram:
-- 1. Campo Grande - 124 chamados
-- 2. Tijuca - 96 chamados
-- 3. Barra da Tijuca - 60 chamados
-- Nota: 131 chamados não possuem id_bairro definido.
SELECT 
  b.nome AS nome_bairro, 
  COUNT(*) AS total_chamados
FROM `datario.adm_central_atendimento_1746.chamado` AS c
LEFT JOIN `datario.dados_mestres.bairro` AS b
  ON c.id_bairro = b.id_bairro
WHERE c.data_inicio >= '2023-04-01 00:00:00' 
AND c.data_inicio < '2023-04-02 00:00:00'
GROUP BY nome_bairro
ORDER BY total_chamados DESC;

-- 4. Qual o nome da subprefeitura com mais chamados abertos nesse dia?
-- Resposta: A subprefeitura com mais chamados foi a Zona Norte, com 534 chamados.
-- Nota: 131 chamados não possuem id_bairro, portanto não estão associados a uma subprefeitura.
SELECT 
  b.subprefeitura AS nome_subprefeitura, 
  COUNT(*) AS total_chamados
FROM 
  `datario.adm_central_atendimento_1746.chamado` AS c
LEFT JOIN 
  `datario.dados_mestres.bairro` AS b
  ON 
    c.id_bairro = b.id_bairro
WHERE 
  c.data_inicio >= '2023-04-01 00:00:00' 
AND 
  c.data_inicio < '2023-04-02 00:00:00'
GROUP BY 
  nome_subprefeitura
ORDER BY 
  total_chamados DESC;

-- 5. Existe algum chamado aberto nesse dia que não foi associado a um bairro ou subprefeitura na tabela de bairros?
-- Resposta: Sim, 131 chamados possuem id_bairro nulo, o que significa que não estão associados a nenhum bairro ou subprefeitura.
-- Possível explicação: Isso pode ocorrer porque o atendente não preencheu corretamente o campo bairro ao registrar o chamado.
SELECT 
  COUNT(*) AS total_chamados_sem_bairro
FROM `datario.adm_central_atendimento_1746.chamado`
WHERE 
  data_inicio >= '2023-04-01 00:00:00' 
AND 
  data_inicio < '2023-04-02 00:00:00'
AND 
  id_bairro IS NULL;

-- 6. Quantos chamados com o subtipo "Perturbação do sossego" foram abertos desde 01/01/2022 até 31/12/2023 (incluindo extremidades)?
-- Resposta: 66051 chamados foram abertos nesta data
SELECT 
  COUNT(*) AS total_chamados
FROM `datario.adm_central_atendimento_1746.chamado`
WHERE 
  tipo = "Perturbação do sossego"
AND 
  data_inicio BETWEEN '2022-01-01' AND '2023-12-31'

-- 7. Selecione os chamados com esse subtipo que foram abertos durante os eventos contidos na tabela de eventos (Reveillon, Carnaval e Rock in Rio).
SELECT 
    c.id_chamado,
    c.data_inicio AS data_chamado,
    e.evento
FROM `datario.adm_central_atendimento_1746.chamado` AS c
JOIN `datario.turismo_fluxo_visitantes.rede_hoteleira_ocupacao_eventos` AS e
    ON c.data_inicio BETWEEN e.data_inicial AND e.data_final
WHERE c.tipo = "Perturbação do sossego"
AND e.data_inicial IS NOT NULL
AND e.data_final IS NOT NULL
ORDER BY c.data_inicio ASC;

-- 8. Quantos chamados desse subtipo foram abertos em cada evento?
-- Resposta: 
-- Rock in Rio - 633 chamados
-- Carnaval - 355 chamados
-- Réveillon - 162 chamados
SELECT 
    e.evento,
    -- EXTRACT(YEAR FROM e.data_inicial) AS ano_evento,
    COUNT(c.id_chamado) AS total_chamados
FROM `datario.adm_central_atendimento_1746.chamado` AS c
JOIN `datario.turismo_fluxo_visitantes.rede_hoteleira_ocupacao_eventos` AS e
    ON c.data_inicio BETWEEN e.data_inicial AND e.data_final
WHERE c.tipo = "Perturbação do sossego"
AND e.data_inicial IS NOT NULL
AND e.data_final IS NOT NULL
GROUP BY e.evento
ORDER BY total_chamados DESC;

-- 9. Qual evento teve a maior média diária de chamados abertos desse subtipo?
-- Resposta: Rock in Rio com média de 96,5 chamados por dia
SELECT 
    e.evento,
    COUNT(c.id_chamado) AS total_chamados,
    DATE_DIFF(e.data_final, e.data_inicial, DAY) + 1 AS duracao_evento_dias,
    COUNT(c.id_chamado) / (DATE_DIFF(e.data_final, e.data_inicial, DAY) + 1) AS media_diaria_chamados
FROM `datario.adm_central_atendimento_1746.chamado` AS c
JOIN `datario.turismo_fluxo_visitantes.rede_hoteleira_ocupacao_eventos` AS e
    ON c.data_inicio BETWEEN e.data_inicial AND e.data_final
WHERE c.tipo = "Perturbação do sossego"
AND e.data_inicial IS NOT NULL
AND e.data_final IS NOT NULL
AND c.data_inicio BETWEEN '2022-02-01' AND '2024-01-01'
GROUP BY e.evento, e.data_inicial, e.data_final
ORDER BY media_diaria_chamados DESC;

-- 10. Compare as médias diárias de chamados abertos desse subtipo durante os eventos específicos (Reveillon, Carnaval e Rock in Rio) e a média diária de chamados abertos desse subtipo considerando todo o período de 01/01/2022 até 31/12/2023.
WITH eventos_chamados AS (
    SELECT 
        e.evento,
        COUNT(c.id_chamado) AS total_chamados,
        DATE_DIFF(e.data_final, e.data_inicial, DAY) + 1 AS duracao_evento_dias
    FROM `datario.adm_central_atendimento_1746.chamado` AS c
    JOIN `datario.turismo_fluxo_visitantes.rede_hoteleira_ocupacao_eventos` AS e
        ON c.data_inicio BETWEEN e.data_inicial AND e.data_final
    WHERE c.tipo = "Perturbação do sossego"
        AND e.data_inicial IS NOT NULL
        AND e.data_final IS NOT NULL
        AND c.data_inicio BETWEEN '2022-01-01' AND '2023-12-31'
    GROUP BY e.evento, e.data_inicial, e.data_final
), 

media_geral AS (
    SELECT COUNT(*) / 730 AS media_diaria_geral
    FROM `datario.adm_central_atendimento_1746.chamado`
    WHERE tipo = "Perturbação do sossego"
    AND data_inicio BETWEEN '2022-01-01' AND '2023-12-31'
)

SELECT 
    e.evento,
    SUM(e.total_chamados) AS total_chamados_evento,
    SUM(e.duracao_evento_dias) AS total_dias_evento,
    SUM(e.total_chamados) / SUM(e.duracao_evento_dias) AS media_diaria_evento,
    mg.media_diaria_geral,
    (SUM(e.total_chamados) / SUM(e.duracao_evento_dias)) / mg.media_diaria_geral AS fator_variacao,
    ((SUM(e.total_chamados) / SUM(e.duracao_evento_dias)) / mg.media_diaria_geral - 1) * 100 AS variacao_percentual
FROM eventos_chamados e
CROSS JOIN media_geral mg
GROUP BY e.evento, mg.media_diaria_geral
ORDER BY variacao_percentual DESC;

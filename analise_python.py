import basedosdados as bd
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "desafiojuniordatascientist-5c2a920c3038.json"

# Definição do projeto de faturamento
billing_project_id = "desafiojuniordatascientist"

# 1. Quantos chamados foram abertos no dia 01/04/2023?
query_1 = """
SELECT COUNT(*) AS total_chamados
FROM `datario.adm_central_atendimento_1746.chamado`
WHERE data_inicio >= '2023-04-01 00:00:00' 
AND data_inicio < '2023-04-02 00:00:00'
"""
df_1 = bd.read_sql(query_1, billing_project_id=billing_project_id)
total_chamados = df_1.iloc[0, 0]
print(f"\n1. Total de chamados abertos em 01/04/2023: {total_chamados}")

# 2. Qual o tipo de chamado que teve mais ocorrências nesse dia?
query_2 = """
SELECT tipo, COUNT(*) AS total_chamados
FROM `datario.adm_central_atendimento_1746.chamado`
WHERE data_inicio >= '2023-04-01 00:00:00' 
AND data_inicio < '2023-04-02 00:00:00'
GROUP BY tipo
ORDER BY total_chamados DESC
LIMIT 1
"""
df_2 = bd.read_sql(query_2, billing_project_id=billing_project_id)
tipo_mais_frequente, qtd_tipo = df_2.iloc[0]
print(f"\n2. Tipo de chamado mais frequente: {tipo_mais_frequente} ({qtd_tipo} chamados)")

# 3. Quais os 3 bairros com mais chamados?
query_3 = """
SELECT 
  b.nome AS nome_bairro, 
  COUNT(c.id_bairro) AS total_chamados
FROM `datario.adm_central_atendimento_1746.chamado` AS c
JOIN `datario.dados_mestres.bairro` AS b
  ON c.id_bairro = b.id_bairro
WHERE c.data_inicio >= '2023-04-01 00:00:00' 
AND c.data_inicio < '2023-04-02 00:00:00'
GROUP BY b.nome
ORDER BY total_chamados DESC
LIMIT 3
"""
df_3 = bd.read_sql(query_3, billing_project_id=billing_project_id)
print("\n3. Top 3 bairros com mais chamados:")
print(df_3.to_string(index=False))

# 4. Subprefeitura com mais chamados
query_4 = """
SELECT b.subprefeitura AS nome_subprefeitura, COUNT(*) AS total_chamados
FROM `datario.adm_central_atendimento_1746.chamado` AS c
LEFT JOIN `datario.dados_mestres.bairro` AS b ON c.id_bairro = b.id_bairro
WHERE c.data_inicio >= '2023-04-01 00:00:00' 
AND c.data_inicio < '2023-04-02 00:00:00'
GROUP BY nome_subprefeitura
ORDER BY total_chamados DESC
LIMIT 1
"""
df_4 = bd.read_sql(query_4, billing_project_id=billing_project_id)
nome_subprefeitura, qtd_subprefeitura = df_4.iloc[0]
print(f"\n4. Subprefeitura com mais chamados: {nome_subprefeitura} ({qtd_subprefeitura} chamados)")

# 5. Quantidade de chamados sem bairro associado
query_5 = """
SELECT COUNT(*) AS total_chamados_sem_bairro
FROM `datario.adm_central_atendimento_1746.chamado`
WHERE data_inicio >= '2023-04-01 00:00:00' 
AND data_inicio < '2023-04-02 00:00:00'
AND id_bairro IS NULL
"""
df_5 = bd.read_sql(query_5, billing_project_id=billing_project_id)
total_sem_bairro = df_5.iloc[0, 0]
print(f"\n5. Chamados sem bairro associado: {total_sem_bairro}")

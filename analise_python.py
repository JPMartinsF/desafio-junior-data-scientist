import basedosdados as bd
from datetime import date
import os
import pandas as pd
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "desafiojuniordatascientist-5c2a920c3038.json"

# Definição do projeto de faturamento
billing_project_id = "desafiojuniordatascientist"

# 1. Quantos chamados foram abertos no dia 01/04/2023?
query_chamados = """
SELECT id_chamado, data_inicio, id_bairro, tipo
FROM `datario.adm_central_atendimento_1746.chamado`
WHERE data_inicio >= '2023-04-01 00:00:00' 
AND data_inicio < '2023-04-02 00:00:00'
"""
df_chamados = bd.read_sql(query_chamados, billing_project_id=billing_project_id)
total_chamados = df_chamados.shape[0]
print(f"\n1. Total de chamados abertos em 01/04/2023: {total_chamados}")

# 2. Qual o tipo de chamado que teve mais ocorrências nesse dia?
df_2 = df_chamados.groupby("tipo").agg({"id_chamado": "count"}).reset_index()
df_2.sort_values("id_chamado", ascending=False, inplace=True)
tipo_mais_frequente, qtd_tipo = df_2.iloc[0]
print(f"\n2. Tipo de chamado mais frequente: {tipo_mais_frequente} ({qtd_tipo} chamados)")

# 3. Quais os 3 bairros com mais chamados?
query_bairros = """
SELECT id_bairro, nome, subprefeitura
FROM `datario.dados_mestres.bairro`
"""
df_bairros = bd.read_sql(query_bairros, billing_project_id=billing_project_id)
df_chamados_bairro = df_chamados.merge(df_bairros, on="id_bairro", how="left")
df_3 = df_chamados_bairro.groupby("nome").agg({"id_chamado": "count"}).reset_index()
df_3.sort_values("id_chamado", ascending=False, inplace=True)
print("\n3. Top 3 bairros com mais chamados:")
print(df_3.iloc[0:3].to_string(index=False))

# 4. Subprefeitura com mais chamados
df_4 = df_chamados_bairro.groupby("subprefeitura").agg({"id_chamado": "count"}).reset_index()
df_4.sort_values("id_chamado", ascending=False, inplace=True)
nome_subprefeitura, qtd_subprefeitura = df_4.iloc[0]
print(f"\n4. Subprefeitura com mais chamados: {nome_subprefeitura} ({qtd_subprefeitura} chamados)")

# 5. Quantidade de chamados sem bairro associado
df_5 = df_chamados_bairro[df_chamados_bairro["id_bairro"].isna()]
total_sem_bairro = df_5.shape[0]
print(f"\n5. Chamados sem bairro associado: {total_sem_bairro}")

# 6. Quantidade de chamados do subtipo "Perturbação do sossego" em 2022 e 2023
# "Perturbação do sossego" aparece apenas na coluna de tipos
query_6 = """
SELECT id_chamado, data_inicio, tipo, subtipo
FROM `datario.adm_central_atendimento_1746.chamado`
WHERE tipo = "Perturbação do sossego"
AND data_inicio BETWEEN '2022-01-01' AND '2023-12-31'
"""
df_chamados_22_23 = bd.read_sql(query_6, billing_project_id=billing_project_id)
df_chamados_22_23.drop(["tipo", "subtipo"], axis=1, inplace=True)
total_chamados_perturbacao = df_chamados_22_23.shape[0]
print(f"\n6. Chamados com o subtipo 'Perturbação do sossego': {total_chamados_perturbacao}")

# 7. Chamados abertos durante eventos
query_eventos = """
SELECT data_inicial, data_final, evento
FROM `datario.turismo_fluxo_visitantes.rede_hoteleira_ocupacao_eventos`
"""
df_eventos = bd.read_sql(query_eventos, billing_project_id=billing_project_id)
df_eventos = df_eventos.dropna(subset=["data_inicial", "data_final"])
df_eventos["data_inicial"] = pd.to_datetime(df_eventos["data_inicial"])
df_eventos["data_final"] = pd.to_datetime(df_eventos["data_final"])
df_merged = df_chamados_22_23.merge(df_eventos, how="cross")
df_7 = df_merged[
    (df_merged["data_inicio"] >= df_merged["data_inicial"]) &
    (df_merged["data_inicio"] <= df_merged["data_final"])
]

print(f"\n7. Chamados abertos durante os eventos do tipo 'perturbação do sossego': \n{df_7.head(5)}")

# 8. Quantidade de chamados do tipo abertos em cada evento
query_8 = """
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
"""
df_8 = bd.read_sql(query_8, billing_project_id=billing_project_id)
print(f"\n8. Quantidade de chamados abertos em cada evento: \n{df_8}")
query_8_1 = """
SELECT 
    e.evento,
    EXTRACT(YEAR FROM e.data_inicial) AS ano_evento,
    COUNT(c.id_chamado) AS total_chamados
FROM `datario.adm_central_atendimento_1746.chamado` AS c
JOIN `datario.turismo_fluxo_visitantes.rede_hoteleira_ocupacao_eventos` AS e
    ON c.data_inicio BETWEEN e.data_inicial AND e.data_final
WHERE c.tipo = "Perturbação do sossego"
AND e.data_inicial IS NOT NULL
AND e.data_final IS NOT NULL
GROUP BY e.evento, ano_evento
ORDER BY ano_evento ASC, total_chamados DESC;
"""
df_8_1 = bd.read_sql(query_8_1, billing_project_id=billing_project_id)
print(f"\nObs 8.1: Quantidade de chamados abertos por evento e ano: \n{df_8_1}")

# 9. Evento com maior média diária de chamados abertos
query_9 = """
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
"""
df_9 = bd.read_sql(query_9, billing_project_id=billing_project_id)
evento_mais_problematico = df_9.iloc[0, 0]
media_diaria_chamados = df_9.iloc[0, 3]
print(f"\n9. O evento com maior média diária de chamados abertos é: {evento_mais_problematico} com média de {media_diaria_chamados} chamados abertos por dia")

# 10. Comparação de médias diárias durante eventos e geral no período em questão
data_inicio = date(2022, 1, 1)
data_fim = date(2023, 12, 31)
quantidade_dias = (data_fim - data_inicio).days + 1
media_geral = total_chamados_perturbacao/quantidade_dias
df_10 = df_9.copy()
df_10 = df_10.groupby("evento").agg({"total_chamados": "sum", "duracao_evento_dias": "sum"}).reset_index()
df_10["media_eventos"] = df_10["total_chamados"]/df_10["duracao_evento_dias"]
df_10["media_geral"] = media_geral
df_10["fator_variacao"] = df_10["media_eventos"] / df_10["media_geral"]
df_10["variacao_percentual"] = (df_10["fator_variacao"] - 1) * 100

print(f"\n10. Comparação de médias diárias entre os eventos e a média diária no período: \n{df_10[["evento", "media_eventos", "media_geral", "fator_variacao", "variacao_percentual"]]}")

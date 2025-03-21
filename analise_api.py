import json
from collections import Counter
from datetime import datetime

import pandas as pd
import requests


def fetch_data(url, params=None):
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Erro ao obter dados de {url}: {e}")
        return None


url_feriados = "https://date.nager.at/api/v3/PublicHolidays/2024/BR"
url_clima = "https://archive-api.open-meteo.com/v1/archive"
meses_mapper = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
}


params_clima = {
    "latitude": -22.9064,
    "longitude": -43.1822,
    "start_date": "2024-01-01",
    "end_date": "2024-08-01",
    "daily": "temperature_2m_mean,weather_code",
    "timezone": "America/Sao_Paulo"
}

feriados = fetch_data(url_feriados)
print(f"\n1. O número de feriados no Brasil em 2024 é: {len(feriados)}")

meses = [int(feriado["date"].split("-")[1]) for feriado in feriados]
contador_meses = Counter(meses)
mes_mais_feriados = max(contador_meses, key=contador_meses.get)
print(f"\n2. O mês com mais feriados é {meses_mapper[mes_mais_feriados]}")

feriados_dias_uteis = 0
for feriado in feriados:
    data = datetime.strptime(feriado["date"], "%Y-%m-%d")
    if data.weekday() < 5:
        feriados_dias_uteis += 1
print(f"\n3. O número de feriados de 2024 que caem em dias úteis é {feriados_dias_uteis}")

dados_clima = fetch_data(url_clima, params=params_clima)
df_clima = pd.DataFrame(dados_clima["daily"])
df_clima["time"] = pd.to_datetime(df_clima["time"])
df_clima["mes"] = df_clima["time"].dt.month
grouped_df_clima = df_clima.groupby("mes", as_index=False)["temperature_2m_mean"].mean()
print("\n4. Temperatura média de cada mês: ")
print(grouped_df_clima.to_string(index=False))

tempo_predominante = df_clima.groupby("mes")["weather_code"].agg(lambda x: x.mode()[0]).reset_index()
with open("dados/descriptions.json", "r") as file:
    weather_code_dict = json.load(file)
weather_code_mapping = {int(code): data["day"]["description"] for code, data in weather_code_dict.items()}
tempo_predominante["weather_description"] = tempo_predominante["weather_code"].map(weather_code_mapping)
print("\n5. O tempo predominante de cada mês foi: ")
print(tempo_predominante[["mes", "weather_description"]].to_string(index=False))

df_feriados = pd.DataFrame(feriados)
df_feriados["date"] = pd.to_datetime(df_feriados["date"])
merged_df = df_clima.merge(df_feriados[["date", "localName"]], left_on="time", right_on="date")
merged_df["weather_description"] = merged_df["weather_code"].map(weather_code_mapping)
print("\n6. O tempo e temperatura média de cada feriado foi:")
print(merged_df[["localName", "date", "weather_description", "temperature_2m_mean"]].to_string(index=False))

tempos_bons = [0, 1, 2]
feriados_ruins = merged_df[~merged_df["weather_code"].isin(tempos_bons)]
print(f"\n7.Houveram {feriados_ruins.shape[0]} feriados ruins, foram eles:")
print(feriados_ruins[["localName", "date"]].to_string(index=False))

feriados_bons = merged_df[merged_df["weather_code"].isin(tempos_bons)]
melhor_feriado = feriados_bons.loc[feriados_bons["temperature_2m_mean"].idxmax()]
print(f"\n8. O feriados 'mais aproveitável' de 2024 foi o {melhor_feriado['localName']} \n   em {melhor_feriado['date']}", end="")
print(f" com temperatura de {melhor_feriado['temperature_2m_mean']}°C e clima {melhor_feriado['weather_description']}")

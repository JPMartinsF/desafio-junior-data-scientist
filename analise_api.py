import json
from collections import Counter
from datetime import datetime

import pandas as pd
import requests

url_feriados = "https://date.nager.at/api/v3/PublicHolidays/2024/BR"
url_clima = "https://archive-api.open-meteo.com/v1/archive?latitude=-22.9064&longitude=-43.1822&start_date=2024-01-01&end_date=2024-08-01&daily=temperature_2m_mean,weather_code&timezone=America%2FSao_Paulo"  # noqa: 501
meses_mapper = {
    1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
    5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
    9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
}

try:
    resposta_feriados = requests.get(url_feriados)
    resposta_clima = requests.get(url_clima)

    if (resposta_feriados.status_code == 200) and (resposta_clima.status_code == 200):
        feriados = resposta_feriados.json()
        print(f"1. O número de feriados no Brasil em 2024 é: {len(feriados)}")

        meses = [int(feriado["date"].split("-")[1]) for feriado in feriados]
        contador_meses = Counter(meses)
        mes_mais_feriados = max(contador_meses, key=contador_meses.get)
        print(f"2. O mês com mais feriados é {meses_mapper[mes_mais_feriados]}")

        feriados_dias_uteis = 0
        for feriado in feriados:
            data = datetime.strptime(feriado["date"], "%Y-%m-%d")
            if data.weekday() < 5:
                feriados_dias_uteis += 1
        print(f"3. O número de feriados de 2024 que caem em dias úteis é {feriados_dias_uteis}")

        dados_clima = resposta_clima.json()["daily"]
        df_clima = pd.DataFrame(dados_clima)
        df_clima["time"] = pd.to_datetime(df_clima["time"])
        df_clima["mes"] = df_clima["time"].dt.month
        grouped_df_clima = df_clima.groupby("mes").mean("temperature_2m_mean").reset_index()
        print("4. Temperatura média de cada mês: ")
        print(grouped_df_clima[["mes", "temperature_2m_mean"]].to_string(index=False))

        tempo_predominante = df_clima.groupby("mes")["weather_code"].agg(lambda x: x.mode()[0]).reset_index()
        with open("descriptions.json", "r") as file:
            weather_code_dict = json.load(file)
        weather_code_mapping = {int(code): data["day"]["description"] for code, data in weather_code_dict.items()}
        tempo_predominante["weather_description"] = tempo_predominante["weather_code"].map(weather_code_mapping)
        print("5. O tempo predominante de cada mês foi: ")
        print(tempo_predominante[["mes", "weather_description"]].to_string(index=False))

        df_feriados = pd.DataFrame(feriados)
        df_feriados["date"] = pd.to_datetime(df_feriados["date"])
        merged_df = df_clima.merge(df_feriados[["date", "localName"]], left_on="time", right_on="date")
        merged_df["weather_description"] = merged_df["weather_code"].map(weather_code_mapping)
        print("6. O tempo e temperatura média de cada feriado foi:")
        print(merged_df[["localName", "date", "weather_description", "temperature_2m_mean"]])

        True
    else:
        print(f"Erro na requisição: {resposta_feriados.status_code}")
except:
    print("Erro na requisição")

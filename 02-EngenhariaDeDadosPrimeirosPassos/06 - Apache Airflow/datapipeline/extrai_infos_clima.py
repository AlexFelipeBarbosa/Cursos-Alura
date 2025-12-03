## Alex Barbosa 03/12/2025
## Cursos - Alura - Airflow


import os
from os.path import join 
import pandas as pd
from datetime import datetime, timedelta 

# Intervalo de datas 
data_inicio = datetime.today()
data_fim = data_inicio + timedelta(days=7)

# Formatando as datas 
data_inicio = data_inicio.strftime('%Y-%m-%d')
data_fim = data_fim.strftime('%Y-%m-%d')

# Cidade da Previsão do Tempo
city = 'Batatais'
key = 'C88Q9RGAFDB92QGVHM7YT7UJ2'

# URL da API
URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
           f'{city}/{data_inicio}/{data_fim}?unitGroup=metric&include=days&key={key}&contentType=csv')

# Recebendo os dados
dados = pd.read_csv(URL)
#print(dados.head())

# Salvando o arquivo CSV
file_path = f'/home/alex/Documentos/datapipeline/semana={data_inicio}/'
os.mkdir(file_path)

dados.to_csv(file_path + 'dados_brutos.csv')
dados[['datetime','tempmin', 'temp', 'tempmax','humidity']].to_csv(file_path + 'temperaturas.csv')
dados[['datetime', 'description', 'icon']].to_csv(file_path + 'condicoes.csv')
# Databricks notebook source
import requests
import pandas as pd

# URL de la API
url = "https://swapi.dev/api/people"

# Hacer la solicitud GET
response = requests.get(url)
data = response.json()

# Convertir los resultados a un DataFrame de pandas
df = pd.DataFrame(data['results'])

# Mostrar el DataFrame
display(df)

# COMMAND ----------

import requests
import pandas as pd

# URL de la API
url = "https://swapi.dev/api/people"
all_results = []

while url:
    # Hacer la solicitud GET
    response = requests.get(url)
    data = response.json()

    all_results.extend(data['results'])
    url = data['next']

# Convertir los resultados a un DataFrame de pandas
df = pd.DataFrame(all_results)

# Mostrar el DataFrame
display(df)

# COMMAND ----------

import requests
import pandas as pd

# Diccionario para almacenar DataFrames
dataframes = {}

# API URL de la API
url_base = "https://swapi.dev/api/"

# GET available endpoints
response = requests.get(url_base)
endpoints = response.json()

# function to get all data from a endpoint
def get_all_data(url):
    all_results = []

    while url:
        # Hacer la solicitud GET
        response = requests.get(url)
        data = response.json()
        all_results.extend(data['results'])
        url = data['next']
    return all_results

for category, url in endpoints.items():
    print(f"Retrieving {category} from: {url}")
    data = get_all_data(url)  
    if data:  # Check if data is not null
        df = pd.DataFrame(data)
        dataframes[category] = df
        print(f"DataFrame '{category}' retrieved with {len(df)} records.\n")

# COMMAND ----------

import shutil

for category, df in dataframes.items():
    print(f" {category}: {df.shape} filas, {df.shape[1]} columnas")
    # display(df.head())  # Solo en Databricks/Jupyter para visualizar
    path = f"/tmp/swapi_{category}.parquet"
    download_path = f"/databricks/driver/swapi_{category}.parquet"
    df.to_parquet(path, engine="pyarrow", index=False)
    print(f" Stored {category} in {path}")

    shutil.move(path, download_path)
    print("File ready to download")

    download_link = f"/files/{download_path.split('/')[-1]}"
    print(f"🔗 Descarga aquí: https://community.cloud.databricks.com{download_link}")

# COMMAND ----------

display(dataframes["people"])

# Databricks notebook source
# MAGIC %md
# MAGIC <h2> Tutorial based on the book: Pyhton para Todos, Raúl González Duque </h2>

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

# arrays
print(endpoints)
endpoints_keys_array = []
print(type(endpoints.keys()))
for key in endpoints.keys(): endpoints_keys_array.append(key)
print (endpoints_keys_array)
print (endpoints_keys_array[0])



# COMMAND ----------

# tuples
endpoints_values_tuple = ()
for key in endpoints.values(): endpoints_values_tuple = endpoints_values_tuple + (key,)
print(type(endpoints_values_tuple))
print (endpoints_values_tuple)

# COMMAND ----------

# dictionaries

d = {'Love Actually ': "Richard Curtis",
"Kill Bill": "Tarantino",
"Amélie": "Jean-Pierre Jeunet"}
print(type(d))

print(type(endpoints))
print(endpoints)

print(endpoints.keys())
print(endpoints.values())



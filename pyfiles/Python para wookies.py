# Databricks notebook source
# MAGIC %md
# MAGIC <h2> Tutorial based on the book: Pyhton para Todos, Raúl González Duque </h2>
# MAGIC http://mundogeek.net/tutorial-python/

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



# COMMAND ----------

display(dataframes['people'])

# COMMAND ----------

# loops

print (dataframes.keys())
# display (dataframes['people'])
print (len(dataframes['people']))

print (type(dataframes['people']))
print (type(dataframes['people'].iloc[0]))
print (dataframes['people'].iloc[0].to_dict().keys())


# COMMAND ----------

i = 0
max_people = len(dataframes['people'])
luke_height = 0

while i < 10:
    row = dataframes['people'].iloc[i]
    if (row['name'] == 'Luke Skywalker'):
        luke_height = row['height']
    print(f"The character {row['name']} is {row['height']} cm tall and weighs {row['mass']} kg")
    i=i+1

# COMMAND ----------

def safe_cast_string_to_int(str_to_cast):
    try:
        if str_to_cast is None:
            raise ValueError(f"Variable cannot be cast into integer")
        return int(str_to_cast)
    except (ValueError, TypeError) as e:
        print (f"----------- Custom Error: {e}")
        return None

def print_taller_characters(row, base_height):
    height = safe_cast_string_to_int(row['height'])
    if ( height is not None and height > int (base_height)):
        print(f"{row['name']} is taller than Luke. He is: {row['height']} cm tall")


for row in dataframes['people'].iloc:
    print_taller_characters(row, luke_height)

# COMMAND ----------

print(type(dataframes['people']))

df_spark = spark.createDataFrame(dataframes['people'])

df_spark.write.saveAsTable("mi_tabla_sql")

df_resultado = spark.sql("SELECT * FROM mi_tabla_sql")
display(df_resultado)

# COMMAND ----------

# List Comprehension
def return_taller_characters(row, base_height):
    height = safe_cast_string_to_int(row[1])
    if ( height is not None and height > int (base_height)):
        return True
    return False

people_list = dataframes['people'].values.tolist()
print(type(people_list))

tall_people_list = [p for p in people_list if return_taller_characters(p, 180)]
display(tall_people_list)

# COMMAND ----------

# Generators I
tall_people_generator = (p for p in people_list if return_taller_characters(p, 180))

print(type(tall_people_generator))
print(next(tall_people_generator))

print('---------------------')
for n in tall_people_generator:
    print (n)

# COMMAND ----------

# Generators II
def tall_people_generator_bis(people_list, base_height):
    for row in people_list:
        if return_taller_characters(row, base_height):  # Si cumple con la condición
            yield row  # Devuelve la persona que cumple la condición
        
tall_people_generator_bis_impl = tall_people_generator_bis(people_list, 180)

print(next(tall_people_generator_bis_impl))

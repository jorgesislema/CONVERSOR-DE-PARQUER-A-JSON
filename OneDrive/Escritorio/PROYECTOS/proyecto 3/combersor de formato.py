import pandas as pd



# Leer archivo Parquet con pandas
df = pd.read_parquet('F:\\PRUEBA\\Clase-03.parquet')

# Mostrar las primeras 5 filas
print(df.head())

# Asegúrate de que la ruta al archivo es correcta
parquet_file = 'F:\\PRUEBA\\Clase-03.parquet'
json_file = 'F:\\PRUEBA\\Clase-03.json'

# Cargar los datos
df_parquet = pd.read_parquet(parquet_file)
df_json = pd.read_json(json_file)

# Obtener las columnas en común
common_columns = df_parquet.columns.intersection(df_json.columns)

# Comparar solo las columnas comunes
are_equal = df_parquet[common_columns].equals(df_json[common_columns])
print("Los archivos tienen el mismo contenido en las columnas comunes." if are_equal else "Los archivos tienen diferencias en las columnas comunes.")

# Desglosar la columna 'fields' del JSON en columnas separadas
df_json_expanded = df_json['fields'].apply(pd.Series)

# Alinear columnas de los DataFrames
common_columns = df_parquet.columns.intersection(df_json_expanded.columns)

# Comparar solo las columnas comunes
are_equal = df_parquet[common_columns].equals(df_json_expanded[common_columns])
print("Los archivos tienen el mismo contenido en las columnas comunes." if are_equal else "Los archivos tienen diferencias en las columnas comunes.")

# Filtrar el diccionario de tipos para que solo contenga columnas que existen en df_json_expanded
filtered_dtypes = {col: df_parquet.dtypes[col] for col in common_columns if col in df_parquet.columns}

# Convertir los tipos de datos del JSON al mismo tipo que el Parquet
df_json_expanded = df_json_expanded.astype(filtered_dtypes, errors='ignore')

# Comparar después de alinear las columnas
are_equal = df_parquet[common_columns].equals(df_json_expanded[common_columns])
print("Los archivos tienen el mismo contenido en las columnas comunes." if are_equal else "Los archivos tienen diferencias en las columnas comunes.")

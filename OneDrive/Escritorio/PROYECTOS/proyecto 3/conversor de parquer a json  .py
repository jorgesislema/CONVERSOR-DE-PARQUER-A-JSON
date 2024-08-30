import os
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import fastavro
import sys

def convert_to_parquet(input_file, output_file):
    # Verificar si el archivo es JSON para manejarlo adecuadamente
    if input_file.endswith('.json'):
        try:
            # Cargar el JSON
            with open(input_file, 'r') as file:
                json_data = json.load(file)

            # Convertir el JSON a un DataFrame
            df = pd.json_normalize(json_data)

            # Verificar si alguna columna contiene JSON anidado y desanidarlo
            for column in df.columns:
                if df[column].dtype == object and isinstance(df[column].iloc[0], str):
                    try:
                        # Intentar desanidar si es un JSON en formato de cadena
                        nested_df = pd.json_normalize(df[column].apply(json.loads))
                        df = df.drop(columns=[column]).join(nested_df, rsuffix=f'_{column}')
                    except (ValueError, TypeError):
                        pass  # Si no es JSON, continuar
            
            # Convertir columnas problemáticas a string para evitar errores en la conversión
            for column in df.columns:
                if df[column].dtype == object:
                    df[column] = df[column].astype(str)

        except ValueError as e:
            print(f"Error al procesar el archivo JSON: {e}")
            sys.exit(1)
    else:
        df = pd.read_csv(input_file)

    # Convertir el DataFrame a Parquet
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_file)

def convert_to_csv(input_file, output_file):
    # (Lógica similar a la función anterior para procesar JSON)
    pass

def convert_to_json(input_file, output_file):
    df = pd.read_parquet(input_file)
    df.to_json(output_file, orient='records', lines=True)

def convert_to_avro(input_file, output_file):
    df = pd.read_csv(input_file)
    schema = {
        "type": "record",
        "name": "Sample",
        "fields": [{"name": col, "type": "string"} for col in df.columns]
    }
    records = df.to_dict(orient='records')
    with open(output_file, 'wb') as out:
        fastavro.writer(out, schema, records)

def convert_to_excel(input_file, output_file):
    df = pd.read_csv(input_file)
    df.to_excel(output_file, index=False)

def get_file_size(file_path):
    return os.path.getsize(file_path)

def main():
    input_file = input("Ingresa la ubicación del archivo: ")
    output_format = input("Ingresa el formato al cual deseas convertir (CSV, JSON, Avro, Parquet, Excel): ").lower()
    output_dir = input("Ingresa la ubicación de almacenamiento: ")

    if not os.path.exists(input_file):
        print("El archivo de entrada no existe.")
        sys.exit(1)
    
    if not os.path.exists(output_dir):
        print("El directorio de salida no existe.")
        sys.exit(1)
    
    base_name = os.path.splitext(os.path.basename(input_file))[0]
    output_file = os.path.join(output_dir, f"{base_name}.{output_format}")
    
    initial_size = get_file_size(input_file)
    
    try:
        if output_format == "parquet":
            convert_to_parquet(input_file, output_file)
        elif output_format == "csv":
            convert_to_csv(input_file, output_file)
        elif output_format == "json":
            convert_to_json(input_file, output_file)
        elif output_format == "avro":
            convert_to_avro(input_file, output_file)
        elif output_format == "excel":
            convert_to_excel(input_file, output_file)
        else:
            print("Formato no soportado. Los formatos soportados son: CSV, JSON, Avro, Parquet, Excel.")
            sys.exit(1)
    except Exception as e:
        print(f"Error durante la conversión: {e}")
        sys.exit(1)
    
    final_size = get_file_size(output_file)

    print(f"Conversión completada. Archivo guardado en: {output_file}")
 

if __name__ == "__main__":
    main()


# Función para comparar dos DataFrames y calcular similitudes
def compare_dataframes(df1, df2):
    comparison_results = {}

    # Comparación de columnas
    columns_match = df1.columns.equals(df2.columns)
    common_columns = df1.columns.intersection(df2.columns).size
    total_columns = df1.columns.size
    column_similarity_percentage = (common_columns / total_columns) * 100
    comparison_results['columns_match'] = columns_match
    comparison_results['column_similarity_percentage'] = column_similarity_percentage

    # Comparación de filas
    rows_match = df1.shape[0] == df2.shape[0]
    row_similarity_percentage = (min(df1.shape[0], df2.shape[0]) / max(df1.shape[0], df2.shape[0])) * 100
    comparison_results['rows_match'] = rows_match
    comparison_results['row_similarity_percentage'] = row_similarity_percentage

    # Comparación de datos
    data_match = df1.equals(df2)
    matching_rows = (df1 == df2).all(axis=1).sum()
    total_rows = df1.shape[0]
    data_similarity_percentage = (matching_rows / total_rows) * 100
    comparison_results['data_match'] = data_match
    comparison_results['data_similarity_percentage'] = data_similarity_percentage

    return comparison_results

# Ruta de archivos
parquet_file_path = 'F:\\PRUEBA\\Clase-03.parquet'
json_file_path = 'F:\\PRUEBA\\Nueva carpeta\\Clase-03.json'

# Leer archivo Parquet
df_parquet = pd.read_parquet(parquet_file_path)

# Guardar como JSON
df_parquet.to_json(json_file_path, orient='records', lines=True)

# Leer archivo JSON
df_json = pd.read_json(json_file_path, orient='records', lines=True)

# Obtener tamaños de archivo en disco
parquet_size = os.path.getsize(parquet_file_path)
json_size = os.path.getsize(json_file_path)

# Comparar DataFrames
comparison = compare_dataframes(df_parquet, df_json)

# Imprimir resultados
print(f"Tamaño del archivo Parquet antes de la conversión: {parquet_size} bytes")
print(f"Tamaño del archivo JSON después de la conversión: {json_size} bytes")
print(f"Coincidencia de columnas: {comparison['columns_match']}")
print(f"Porcentaje de similitud en columnas: {comparison['column_similarity_percentage']:.2f}%")
print(f"Coincidencia de filas: {comparison['rows_match']}")
print(f"Porcentaje de similitud en filas: {comparison['row_similarity_percentage']:.2f}%")
print(f"Coincidencia de datos: {comparison['data_match']}")
print(f"Porcentaje de similitud en datos: {comparison['data_similarity_percentage']:.2f}%")


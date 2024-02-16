import functions_framework
import json
import pandas as pd 
import numpy as np 
from google.cloud import bigquery
from google.cloud import storage


# Importar modulo processing
#from processing import procesar, hash_category

# Instatiate clients
cloud_storage = storage.Client()
bq = bigquery.Client()

project_id = 'dms-pfh'


def archivo_df_business(file_uri):
    # Crear una instancia del cliente de Google Cloud Storage
    client = storage.Client()

    # Nombre del archivo en el bucket
    nombre_archivo = 'df_mascara.csv'

    # Nombre del bucket
    nombre_bucket = 'yelp_dms'
    # Ruta completa en el bucket
    ruta_archivo = f'gs://{nombre_bucket}/{nombre_archivo}'
    
    # Filtrado segun nombre archivo para su respectivo ETL
    df_bussines = pd.read_pickle(file_uri)
    df_bussines =df_bussines.loc[:,~df_bussines.columns.duplicated()].copy()
    print("pase la mascara de juan de columnas")
    df_bussines.drop(columns=["is_open",'attributes', 'hours','postal_code'], inplace=True)
    categorias_deseadas = ["restaurant", "bar", "pub", "cafe", 'bakery', 'coffee shop', 'gym', 'gas station']

    # Eliminar filas con valores NaN en la columna 'categories'
    df_bussines = df_bussines.fillna("No Data")
    # Filtrar el DataFrame para mantener solo las filas que contienen al menos una de las categorías deseadas
    df_bussines = df_bussines[df_bussines['categories'].str.contains('|'.join(categorias_deseadas), case=False)]
    df_mascara= df_bussines["business_id"]
    

    # Guardar el DataFrame como un archivo CSV
    df_mascara.to_csv(nombre_archivo, index=False)
    # Obtener el bucket
    bucket = client.bucket(nombre_bucket)
    # Subir el archivo al bucket
    blob = bucket.blob(nombre_archivo)
    blob.upload_from_filename(nombre_archivo)
    print(f'DataFrame guardado en {ruta_archivo}')


    return df_bussines

def archivo_df_tip(file_uri):
    df_tip = pd.read_json(file_uri, lines=True)
    df_tip.drop(columns="compliment_count", inplace=True)
    for columna in df_tip.columns:
        if df_tip[columna].dtype == 'object':
            # Si la columna es de texto, llenar los valores nulos con "No data" y eliminar espacios en blanco y saltos de línea
            df_tip[columna] = df_tip[columna].fillna("No data").str.strip()
        elif df_tip[columna].dtype in ['int64', 'float64']:
            # Si la columna es numérica, llenar los valores nulos con 0
            df_tip[columna] = df_tip[columna].fillna(0)
    return df_tip

def archivo_df_user(file_uri):
    df_user001 = pd.read_json(file_uri, lines=True)
    df_user001.drop(columns=['elite','friends','compliment_hot','compliment_more', 'compliment_profile', 'compliment_cute','compliment_list', 'compliment_note', 'compliment_plain',
        'compliment_cool', 'compliment_funny', 'compliment_writer','compliment_photos'], inplace=True)
    for columna in df_user001.columns:
        if df_user001[columna].dtype == 'object':
            # Si la columna es de texto, llenar los valores nulos con "No data" y eliminar espacios en blanco y saltos de línea
            df_user001[columna] = df_user001[columna].fillna("No data").str.strip()
        elif df_user001[columna].dtype in ['int64', 'float64']:
            # Si la columna es numérica, llenar los valores nulos con 0
            df_user001[columna] = df_user001[columna].fillna(0)
    print("completado etl archivo parquet")
    return df_user001

def archivo_df_review(file_uri):
    df_review = pd.read_json(file_uri, lines=True)
    df_mascara=pd.read_csv("gs://yelp_dms/df_mascara.csv")
    print("carga de mascara completada")
    df_review['business_id'] = df_review['business_id'].astype(str)
    df_mascara['business_id'] = df_mascara['business_id'].astype(str)

    df_review = pd.merge(df_review, df_mascara, on='business_id', how='inner')
    print("filtracion completa en archivo REVIEW")
    return df_review

# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def load_df(cloud_event):
    data = cloud_event.data
    event_id = cloud_event["id"]
    event_type = cloud_event["type"]
    print(f'Cloud event ID: {event_id}')
    print(f'Event type: {event_type}')
    bucket_name = data["bucket"]
    name = data["name"]
    print(f'Bucket: {bucket_name}')
    print(f'File name: {name}')
    # Create URI
    file_uri = f'gs://{bucket_name}/{name}'
    print(f'URI: {file_uri}')
    # Obtener el objeto del bucket
    bucket = cloud_storage.bucket(bucket_name)
    # Obtener el blob (objeto) del archivo JSON en el bucket
    blob = bucket.blob(name)
    # Attempt to create DataFrame with URI
    # Attempt to process DataFrame
    print('Transformando dataframe...')
    try:
        if "business" in file_uri:
            df_procesado = archivo_df_business(file_uri)
            table_name = "yelp-sites"
            print("dataframe cargado") 
        if "tip" in file_uri:
            df_procesado = archivo_df_tip(file_uri)
            table_name = "yelp-user-tips"
            print("dataframe cargado")
        if "user" in file_uri:
            df_procesado = archivo_df_user(file_uri)
            table_name = "yelp-user-info"
            print("dataframe cargado")
        if "review-002" in file_uri:
            df_procesado=archivo_df_review(file_uri)
            table_name="yelp-user-review"
            print("dataframe cargado review")
        print('DataFrame Procesado')
        print('Nuevas dimensiones:', df_procesado.shape)
    except Exception as e:
        # Cuando no procesa la información
        print('DataFrame no pudo ser procesado: Error - ', e)
        return None
    # Insert to table
    table_id = f'dms-pfh.yelp_data.{table_name}'
    # Create job configurations
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND'
        # source_format='NEWLINE_DELIMITED_JSON'
    )
    print('Job configuration created.')
    # Requesting the job
    load_job = bq.load_table_from_dataframe(
        df_procesado,
        table_id,
        location='us-central1',
        job_config=job_config
    )
    load_job.result()
    print('Job finished')
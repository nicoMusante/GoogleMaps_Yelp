from googleapiclient.http import MediaIoBaseDownload
from google.cloud import storage
from google.cloud import bigquery
import io
import os

import time
import datetime

from google.api_core.exceptions import NotFound

def list_drive_files_and_upload(drive_service, folder_id, bucket_name, start_time, destination_path_in_bucket=""):
    print("Entro a list_drive_files_and_upload()")
    # Si destination_path_in_bucket está vacío, obtiene el nombre de la carpeta raíz
    if not destination_path_in_bucket:
        root_folder_name = get_drive_folder_name(drive_service, folder_id)
        destination_path_in_bucket = root_folder_name

    results = drive_service.files().list(
        q=f"'{folder_id}' in parents and trashed=false",
        pageSize=1000,
        fields="nextPageToken, files(id, name, mimeType)").execute()
    items = results.get('files', [])

    for item in items:
        gcs_destination_path = os.path.join(destination_path_in_bucket, item['name'])
        if item['mimeType'] == 'application/vnd.google-apps.folder':
            list_drive_files_and_upload(drive_service, item['id'], bucket_name, start_time, gcs_destination_path)
        else:
            if check_remaining_time(start_time):
                if item['name'] not in check_uploaded_files_in_bigquery(folder_id):
                    file_handle = download_file_from_drive(drive_service, item['id'], item['name'])
                    upload_blob_from_memory(bucket_name, file_handle, gcs_destination_path)
                    log_uploaded_file(item['name'], folder_id, bucket_name)
                else:
                    print(f"El archivo {item['name']} ya ha sido cargado.")



def download_file_from_drive(drive_service, file_id, file_name):
    request = drive_service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()
    fh.seek(0)
    return fh
    
def upload_blob_from_memory(bucket_name, source_file_handle, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(destination_blob_name)
    blob.upload_from_file(source_file_handle)
    print(f"Archivo cargado con exito en: {destination_blob_name}.")

def get_drive_folder_name(drive_service, folder_id):
    folder = drive_service.files().get(fileId=folder_id, fields='name').execute()
    return folder.get('name')




def check_remaining_time(start_time):
    MAX_EXECUTION_TIME_SECONDS = 540
    current_time = time.time()
    elapsed_time = current_time - start_time
    remaining_time = MAX_EXECUTION_TIME_SECONDS - elapsed_time
    print(f"tiempo restante para que se pare la funcion: {remaining_time}")
    return remaining_time > 300  #aseguramos que haya al menos 90 segundos de tiempo restante

def log_uploaded_file(file_name, folder_id, bucket_name):
    client = bigquery.Client()
    dataset_id = 'archivos_buckets'
    table_id = 'archivos_maps'

    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)

    rows_to_insert = [(file_name, folder_id, datetime.datetime.now())]
    client.insert_rows(table, rows_to_insert)

def check_uploaded_files_in_bigquery(folder_id):
    # Realiza la consulta en BigQuery para obtener la lista de archivos ya cargados en el folder
    # Retorna la lista de archivos ya cargados
    client = bigquery.Client()
    dataset_id = 'archivos_buckets'
    table_id = 'archivos_maps'

    query = f"""
        SELECT file_name 
        FROM `{dataset_id}.{table_id}`
        WHERE folder_id = '{folder_id}'
    """

    try:
        query_job = client.query(query)
        results = query_job.result()

        uploaded_files = [row['file_name'] for row in results]
        return uploaded_files
    except NotFound as e:
        # Maneja la excepción si la tabla no existe
        print(f"La tabla {dataset_id}.{table_id} no existe. Creando la tabla...")
        create_bigquery_table(client, dataset_id, table_id)
        return []
    except Exception as e:
        # Maneja otras excepciones que puedan ocurrir
        print(f"Error inesperado: {e}")
        return []

def create_bigquery_table(client, dataset_id, table_id):
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    # Define el esquema de la tabla
    schema = [
        bigquery.SchemaField("file_name", "STRING"),
        bigquery.SchemaField("folder_id", "STRING"),
        bigquery.SchemaField("timestamp", "TIMESTAMP"),
    ]

    # Configura las opciones de la tabla
    table = bigquery.Table(table_ref, schema=schema)

    # Crea la tabla
    table = client.create_table(table)

    print(f"Tabla {dataset_id}.{table_id} creada con éxito.")
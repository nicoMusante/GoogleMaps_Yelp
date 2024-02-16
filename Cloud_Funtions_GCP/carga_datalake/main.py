import functions_framework

import functions_framework
from drive_modules import list_drive_files_and_upload
from googleapiclient.discovery import build
from google.oauth2 import service_account
import time
import datetime


from drive_modules import list_drive_files_and_upload, check_uploaded_files_in_bigquery #importamos las dependencias de drive_modules.py

@functions_framework.http
def cargar_datalake_maps(request):
    print("Entro a la funcion cargar_datalake_maps()")
    start_time = time.time()    
    print(f"La funcion arranco en: {start_time}")

    bucket_name = "datalake-maps"
    drive_service = build('drive', 'v3')

    folder_id_metadata = "1WuqmWb_PWUFMAHbfHT8jLH4hVQftHujt"
    folder_id_reviews = "1hf6PZPmh5mwC6FjajuiOBGStWxo9WY9t"

    try:        

        #cargamos primero la carpeta de metadata y luego la de reviews de Google_Maps debido a la dependencia de datos (que las cloud funcions funcionen correctamente)
        list_drive_files_and_upload(drive_service, folder_id_metadata, bucket_name, start_time)
        print("CARPETA DE METADATA CARGADA CORRECTAMENTE, AHORA VAMOS A CARGAR LA DE REVIEWS...")
        list_drive_files_and_upload(drive_service, folder_id_reviews, bucket_name, start_time)

    except Exception as e:  
        print(e)
        return f'No se pudo encontrar una carpeta de google drive con el id "{folder_id_reviews}" o el id "{folder_id_metadata}" o algo inesperado ocurrio', 400

    return 'Archivos cargados exitosamente a Cloud Storage'
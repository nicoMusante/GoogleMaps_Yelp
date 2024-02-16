import functions_framework
import warnings
warnings.filterwarnings("ignore")

from google.cloud import bigquery

import pandas as pd 
import numpy as np

# Instatiate clients
# cloud_storage = storage.Client()
bigquery_client = bigquery.Client()

project_id = 'dminds-414506'
dataset = 'maps_data'


# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def load_df(cloud_event):

  data = cloud_event.data
  event_id = cloud_event["id"]
  event_type = cloud_event["type"]
  print(f'Cloud event ID: {event_id}')
  print(f'Event type: {event_type}')

  # Getting bucket name and file name
  bucket_name = data["bucket"]
  name = data["name"]
  print(f'Bucket: {bucket_name}')
  print(f'File name: {name}')

  # URI
  file_uri = f'gs://{bucket_name}/{name}'
  print(f'New file URI: {file_uri}')

  # Attempt to create DataFrame with URI
  print('Attempting to create DataFrame')
  try:
    if 'review' in file_uri:
      dtype = False
      
      # Dividir el path utilizando el carácter "/"
      parts = file_uri.split("/")

      # Buscar la parte que contiene "reviews-" seguido del nombre del estado
      state_part = next(part for part in parts if part.startswith("review-"))

      # Extraer el nombre del estado eliminando "reviews-" del comienzo
      state_name = state_part[len("review-"):]

      print("Nombre del estado:", state_name)
      #lista de estados de la costa este de US para solo cargar estos en BigQuery
      states_east=["Connecticut","Delaware","Florida","Georgia","Illinois","Indiana","Kentucky","Maryland","Massachusetts","Michigan","New_Hampshire","New_Jersey","New_York","North_Carolina","Ohio","Pennsylvania","Rhode_Island","South_Carolina","Tennessee","Vermont","Virginia","West_Virginia","Wisconsin"]
      if (state_name not in states_east) :
        print("Data frame no cargado debido a que el estado {state_name} no pertenece a la costa este de US")
        return None
    else:
      dtype = None
    
    df_uri = pd.read_json(
      file_uri,
      lines=True,
      dtype=dtype
    )
    # If DataFrame is loaded, the print a success message
    print(f'Succeed: DataFrame loaded from {file_uri}')
    print(f'Resulting shape: {df_uri.shape}')
    print(f'DataFrame Columns: {df_uri.columns}')
  except Exception as e:
    # Otherwise, catch the error
    print('Error: Could not read from URI. -', e)
    return None


  # Attempt to process DataFrame
  print('Transformando dataframe...')
  try:
    if 'metadata' in file_uri:

      from processing import process_sites
      df_procesado, df_categorias = process_sites(df_uri)

      print('Dimensiones despues de filtrar y procesar:', df_procesado.shape)

      # tabla destino sites
      table_name = 'google-sites'
      
      is_sites_metadata = True

    else:

      from processing import process_reviews
      df_procesado_nf = process_reviews(df_uri, state_name)

      print('Haciendo consulta para filtrar')
      # Filtrado en tiempo real
      query = f"SELECT gmap_id FROM `dminds-414506.maps_data.google-sites`;"
      query_job = bigquery_client.query(query)
      results = query_job.result()
      # Construir el DataFrame manualmente
      df_gmap_ids = pd.DataFrame(data=[row.get('gmap_id') for row in results],
                        columns=[field.name for field in results.schema])
      print('Consulta exitosa')
      print('Verificacion tipo obtenido:', type(df_gmap_ids))
      print('Verificacion Columnas obtenida:', df_gmap_ids.columns)
      print('Dimensiones del resultado obtenido:', df_gmap_ids.shape)
      print('Overview de datos del resultado obtenido:', df_gmap_ids.loc[0])
      print('Overview de tipo de datos del resultado obtenido:', type(df_gmap_ids.loc[0]))
                        
      df_procesado = df_procesado_nf.merge(df_gmap_ids, on='gmap_id', how='inner')
      print('Dimensiones despues de filtrar y procesar:', df_procesado.shape)

      if df_procesado.shape[0] == 0: # 0 rows
        print('Empty table after processing -> Not ingesting.')
        return None

      # Tabla destino reviews
      table_name = 'user-reviews-east'

      is_sites_metadata = False

  except Exception as e:
    # Cuando no procesa la información
    print('DataFrame no pudo ser procesado: Error - ', e)
    return None
    

  # Insert to table
  table_id = f'{project_id}.{dataset}.{table_name}'
  try:
    if is_sites_metadata:
      from insert import ingest_sites_from_dataframe
      ingest_sites_from_dataframe(
        df_procesado, df_categorias, 
        table_id, 
        client=bigquery_client)
    else:
      from insert import ingest_reviews_from_dataframe
      ingest_reviews_from_dataframe(
        df_procesado, 
        table_id, 
        client=bigquery_client)
      
  except Exception as e:
    print('¡¡!! Error during the ingestion:', e)

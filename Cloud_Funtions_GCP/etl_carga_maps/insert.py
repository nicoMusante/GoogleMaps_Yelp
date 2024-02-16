from google.cloud import bigquery
from google.cloud import storage

def ingest_sites_from_dataframe(
  dataframe,
  df_categorias,
  table_id,
  client:bigquery.Client):

  # Instantiate bigquery client
  bq_client = bigquery.Client()

  # Create job configurations
  job_config = bigquery.LoadJobConfig(
    autodetect=True,
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_APPEND'
    # source_format='NEWLINE_DELIMITED_JSON'
  )
  print('Job configuration created.')

  # Requesting the job
  load_job = bq_client.load_table_from_dataframe(
    dataframe,
    table_id,
    location='US',
    job_config=job_config
  )
  load_job.result()
  print(f'Job finished. Dataframe has been written to "{table_id}"')

  # Requesting second job for categories
  # Particular jobConfig
  job_config_2= bigquery.LoadJobConfig(
    autodetect=True,
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_EMPTY'
  )

  table_id_cat = 'dminds-414506.maps_data.site-categories'
  try:
    load_job2 = bq_client.load_table_from_dataframe(
      df_categorias,
      table_id_cat,
      location='US',
      job_config=job_config_2
    )
    load_job2.result()
    print('Categories dim: OK')
  except:
    print('Categories dim: Already created.')

def ingest_reviews_from_dataframe(
  dataframe,
  table_id,
  client:bigquery.Client):

  # Create job configurations
  job_config = bigquery.LoadJobConfig(
    autodetect=True,
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_APPEND'
  )
  print('Job configuration created.')

  # Requesting the job
  load_job = client.load_table_from_dataframe(
    dataframe,
    table_id,
    location='US',
    job_config=job_config
  )
  load_job.result()
  print(f'Job finished. Dataframe has been written to "{table_id}"')
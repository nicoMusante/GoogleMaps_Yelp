import numpy as np 
import pandas as pd


# Función para aplicar el pipeline al dataframe ----
def procesar(
  dataFrame: pd.DataFrame,
  categorias_filtrado:list):

  # Trabajar en una copia
  copy = dataFrame.copy()

  # Filtrar el DataFrame para las categorías permitidas
  df_filtrado = (
      copy[copy['category'].apply(lambda x: isinstance(x, list) and any(cat in categorias_filtrado for cat in x))]
  )

  # Drop duplicated gmap_id
  df_filtrado.drop_duplicates(subset='gmap_id', inplace=True)

  # Reset index para que sea unico
  df_filtrado.reset_index(drop=True, inplace=True)

  # Limpieza general: Eliminar columnas price, state y hours
  df_filtrado.drop(columns=['price','state', 'hours'], inplace=True)

  # Reemplazar nulos por texto legible
  df_filtrado['description'] = df_filtrado['description'].fillna('No description')
  df_filtrado['address'] = df_filtrado['address'].fillna('No address')

  # Crear category df
  df_categories = pd.DataFrame(
      {'category_id': [i for i in range(len(categorias_filtrado))],
      'category': categorias_filtrado}
  )

  return df_filtrado, df_categories


# Función para darle valor numérico a las categorías ----
def hash_category(cat_list, df_cats):
  key_dict = {
  cat: id for (cat, id) in zip(df_cats.category.tolist(), df_cats.category_id.tolist())
  }
  
  if 'Restaurant' in cat_list:
      return key_dict['Restaurant']
  elif 'Bar' in cat_list:
      return key_dict['Bar']
  elif 'Pub' in cat_list:
      return key_dict['Pub']
  elif 'Cafe' in cat_list:
      return key_dict['Cafe']
  elif 'Bakery' in cat_list:
      return key_dict['Bakery']
  elif 'Coffee shop' in cat_list:
      return key_dict['Coffee shop']
  elif 'Gym' in cat_list:
      return key_dict['Gym']
  elif 'Gas station' in cat_list:
      return key_dict['Gas station']
  else:
      return 0

# COMPLETE PIPELINE FOR REVIEWS
def process_sites(df:pd.DataFrame):
  # categorías a usar
  categories = [
    'No Category','Restaurant', 'Bar',
    'Pub', 'Cafe', 'Bakery', 'Coffee shop',
    'Gym', 'Gas station'
  ]
  # llamar funcion procesar():
  df_procesado, df_categorias = procesar(df, categories)

  # Crear hash de categorias
  df_procesado['category_id'] = df_procesado['category'].apply(hash_category, df_cats=df_categorias)
  df_procesado = df_procesado.drop(columns='category')

  # Reordenar columnas
  cols = ['gmap_id', 'name', 'address', 'description', 'category_id', 'avg_rating',
          'num_of_reviews', 'latitude', 'longitude', 'relative_results', 'url' ]
  df_procesado = df_procesado[cols]

  print('DataFrame Procesado')
  print('Nuevas dimensiones:', df_procesado.shape)

  return df_procesado, df_categorias


# ETL pipeline for reviews
def process_reviews(
  df:pd.DataFrame,
  state):

  df_copy = df.copy()

  #eliminado de columnas PICS y RESP
  df_copy.drop(columns=["pics", "resp"], inplace=True)
  print('columnas innecesarias eliminadas')

  # Convertir la columna 'time' a formato de fecha y hora
  df_copy['time'] = pd.to_datetime(df_copy['time'], unit='ms')
  df_copy['time'] = df_copy['time'].map(lambda date: date.date(), na_action='ignore')
  print('Cambio de formato a datetime')

  # Transformar e imputra columna de comentarios
  df_copy['text'] = (
    df_copy['text']
    .map(lambda text: text.strip() if type(text) == str else 'No text')
    .apply(lambda text: text.replace('\n', ' '))
  )
  print('Comentarios normalizados')

  # Eliminar duplicados basados en las columnas 'gmap_id' y 'user_id'
  df_copy.drop_duplicates(subset=['gmap_id', 'user_id'], inplace=True)
  print('Eliminados registros duplicados')

  df_copy['state'] = state
  
  return df_copy
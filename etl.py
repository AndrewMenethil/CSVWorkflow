import pandas as pd
from sqlalchemy import create_engine

def extract(file_path): 
    """Функция для извлечения данных"""
    df = pd.read_csv(file_path) # Читаем нужный файл с данными
    return df

def transform(df): 
    """Функция для очистки и преобразования данных"""
    df = df.drop(columns=['keywords', 'status','overview','tagline', 'adult','backdrop_path','homepage', 'imdb_id','poster_path']) # Удаляем лишние столбцы 
    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce') # Преобразуем столбец 'release_date' в формат datetime                 
    return df

def load(df, db_url):
    """Функция для загрузки данных в PostgreSQL"""
    engine = create_engine(db_url)
    df.to_sql("movies", engine, if_exists='replace',index=False)

def run_etl():
    """Функция запуска программы"""
    file_path = 'TMDB_movie_dataset.csv'
    db_url = 'postgresql://postgres:mypassword@localhost:5432/postgres'
    df = extract(file_path)
    df = transform(df)
    load(df, db_url)
    print("ETL процесс завершён успешно!")
run_etl()
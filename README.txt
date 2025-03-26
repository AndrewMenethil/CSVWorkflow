<div align="center">
<h1>CSV Workflow</h1>
</div>

**Данный проект представляет собой полный ETL-процесс по обработке данных о фильмах из базы TMDB. Мы загружаем, очищаем, храним и анализируем данные, а также визуализируем их с помощью Power BI.**

---

## Технологический стек:
- Источник данных: Датасет с платформы Kaggle
- **Python**: Для обработки и преобразования данных (с использованием Pandas и SQLAlchemy)
- **VS Code**: Для написания кода и выполнения скриптов
- **Jupyter Notebook**: Для быстрого исследования данных и проверки колонок
- **PostgreSQL**: Для хранения обработанных данных
- **DBeaver**: Для управления базой данных и выполнения SQL-запросов
- **SQL**: Для создания представлений (Data Warehouse)
- **Git Bash**: Для выполнения команд Git и работы с репозиторием через терминал
- **Power BI**: Для визуализации данных и создания дашбордов
- **draw.io** и **sqldbm.com**: Для диаграммирования и моделирования

---

## План проекта:
1. [Получение данных](#1-получение-данных)
2. [Извлечение, подготовка и загрузка данных (ETL)](#2-извлечение-подготовка-и-загрузка-данных-etl)
3. [Создание витрины данных](#3-создание-витрины-данных)
4. [Визуализация с Power BI](#4-визуализация-с-power-bi)
5. [Документирование](#5-документирование)


![Архитектура проекта](ProjectArchitecture.png)

---

## <a id="1-получение-данных"></a> 1. Получение данных

С платформы Kaggle мной был взят Dataset [Full TMDB Movies Dataset 2024 (1M Movies)](https://www.kaggle.com/datasets/asaniczka/tmdb-movies-dataset-2023-930k-movies) в формате `.csv`. Этот набор данных содержит коллекцию из 1 000 000 фильмов из базы данных TMDB.

---

## <a id="2-извлечение-подготовка-и-загрузка-данных-etl"></a> 2. Извлечение, подготовка и загрузка данных (ETL)

Для начала в VS Code мной были импортированы соответствующие библиотеки:
```python
import pandas as pd
from sqlalchemy import create_engine
```
После чего я приступил к написанию ETL процесса. Параллельно в Jupiter Notebook я смотрел на структуру таблицы и определял какие данные могут мне пригодиться, а какие не имеют значения для проекта.

**E - extract**
```python
def extract(file_path): 
    """Функция для извлечения данных"""
    df = pd.read_csv(file_path) # Читаем нужный файл с данными
    return df
```

**T - transform**
```python
def transform(df): 
    """Функция для очистки и преобразования данных"""
    df = df.drop(columns=['keywords', 'status','overview','tagline', 'adult','backdrop_path','homepage', 'imdb_id','poster_path']) # Удалем лишние столбцы 
    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce') # Преобразуем столбец 'release_date' в формат datetime                 
    return df
```

**L - load**
```python
def load(df, db_url):
    """Функция для загрузки данных в PostgreSQL"""
    engine = create_engine(db_url)
    df.to_sql("movies", engine, if_exists='replace',index=False)
```

В конце была добавлена функция объединяющая весь предыдущий код и запускающая программу:
```python
def run_etl():
    """Функция запуска программы"""
    file_path = 'TMDB_movie_dataset.csv'
    db_url = 'postgresql://postgres:mypassword@localhost:5432/postgres'
    df = extract(file_path)
    df = transform(df)
    load(df, db_url)
    print("ETL процесс завершён успешно!")
run_etl()
```

## <a id="3-создание-витрины-данных"></a> 3. Создание витрины данных

После того как данные были загруженны в PostgreSQL. В БД появилась таблица `movies`. Данные в ней уже были полезны, но таблица оставалась достаточно большой и для планируемой аналитики лучше создать отдельную витрину данных. 

Сначала была создана новая таблица `movies_summary`: 
```SQL
-- Создаём витрину данных путём создания новой таблицы
DROP TABLE IF EXISTS movies_summary; -- Удаляем таблицу, если она уже существует
CREATE TABLE movies_summary (        -- Создаём таблицу "movies_summary" с нужными полями
    id           SERIAL PRIMARY KEY,
    title        varchar(1000) NOT NULL,
    release_year integer,
    genres       varchar(2500),
    avg_rating   decimal(3,1),
    revenue      bigint
);
```

После чего она была заполнена данными из таблицы `movies`:
```SQL
-- Заполняем новую таблицу агрегированными данными из таблицы "movies"
INSERT INTO movies_summary (title, release_year, genres, avg_rating, revenue)
SELECT
    title,
    EXTRACT(YEAR FROM release_date)::integer AS release_year,
    genres,
    CASE
        WHEN ROUND(AVG(vote_average)::numeric, 1) = 0 THEN NULL 
        ELSE ROUND(AVG(vote_average)::numeric, 1)
    END AS avg_rating,
    CASE 
        WHEN revenue = 0 THEN NULL 
        ELSE revenue
    END AS revenue
FROM movies
WHERE 
    title IS NOT NULL 
    AND EXTRACT(YEAR FROM release_date)::integer >= 1980
GROUP BY 
    title, 
    EXTRACT(YEAR FROM release_date), 
    genres, revenue
```
![Логическая модель данных](LogicalDataModel.png)

## <a id="4-визуализация-с-power-bi"></a> 4. Визуализация с Power BI

В Power BI я выполнил подключение к PostgreSQL и таблице `movies_summary`. После этого создал несколько дашбордов:

![Средний рейтинг фильмов по годам](MovieRating.png)

![Сумма общей выручки по годам](AmountRevenue.png)

## <a id="5-документирование"></a> 5. Документирование

Создал файл `README.md` где описал процессы создания своего pet-проекта и загрузил с помощью Git файлы проекта в репозиторий Github.

```Git
# 1. Инициализация локального репозитория
git init

# 2. Добавление файлов в индекс
git add .

# 3. Создание коммита
git commit -m "Initial commit"

# 4. Подключение удаленного репозитория
git remote add origin https://github.com/AndrewMenethil/CSVWorkflow.git

# 5. Загрузка кода на GitHub
git push -u origin master
```

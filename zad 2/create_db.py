import os
import time
import zipfile
import requests
from pathlib import Path
import psycopg2

from dotenv import load_dotenv
load_dotenv()

DATA_DIR = Path("data_cache")
ZIP_PATH = DATA_DIR / "ml-32m.zip"
EXTRACT_DIR = DATA_DIR / "ml-32m"
DATASET_URL = "https://files.grouplens.org/datasets/movielens/ml-32m.zip"

SQL_DIR = Path("sql")

def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def wait_for_postgres(max_retries=30, delay=2):
    for attempt in range(max_retries):
        try:
            conn = get_connection()
            conn.close()
            print("Connected to Postgres.")
            return
        except Exception as e:
            print(f"Waiting for Postgres... ({attempt + 1}/{max_retries})")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Postgres.")


def execute_sql_file(conn, filepath):
    print(f"Running SQL file: {filepath}")
    with open(filepath, "r", encoding="utf-8") as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def ensure_dataset():
    DATA_DIR.mkdir(exist_ok=True)

    if not ZIP_PATH.exists():
        print("Downloading MovieLens dataset...")
        response = requests.get(DATASET_URL, stream=True, timeout=120)
        response.raise_for_status()

        with open(ZIP_PATH, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)

    if not EXTRACT_DIR.exists():
        print("Extracting MovieLens dataset...")
        with zipfile.ZipFile(ZIP_PATH, "r") as zf:
            zf.extractall(DATA_DIR)


def copy_csv_to_table(conn, table_name, csv_path, columns):
    print(f"Loading {csv_path.name} into {table_name}")
    with conn.cursor() as cur:
        with open(csv_path, "r", encoding="utf-8") as f:
            next(f)  # skip header
            cur.copy_expert(
                f"COPY {table_name} ({columns}) FROM STDIN WITH CSV",
                f
            )
    conn.commit()


def load_raw_data(conn):
    copy_csv_to_table(
        conn,
        "raw.movies",
        EXTRACT_DIR / "movies.csv",
        "movie_id, title, genres"
    )
    copy_csv_to_table(
        conn,
        "raw.ratings",
        EXTRACT_DIR / "ratings.csv",
        "user_id, movie_id, rating, rating_timestamp"
    )
    copy_csv_to_table(
        conn,
        "raw.tags",
        EXTRACT_DIR / "tags.csv",
        "user_id, movie_id, tag, tag_timestamp"
    )
    copy_csv_to_table(
        conn,
        "raw.links",
        EXTRACT_DIR / "links.csv",
        "movie_id, imdb_id, tmdb_id"
    )


def main():
    wait_for_postgres()
    ensure_dataset()

    conn = get_connection()
    try:
        execute_sql_file(conn, SQL_DIR / "01_create_schemas.sql")
        execute_sql_file(conn, SQL_DIR / "02_create_raw_tables.sql")
        load_raw_data(conn)
        execute_sql_file(conn, SQL_DIR / "03_create_clean_tables.sql")
        execute_sql_file(conn, SQL_DIR / "04_create_gold_views.sql")
        execute_sql_file(conn, SQL_DIR / "05_quality_checks.sql")
        print("Pipeline finished successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
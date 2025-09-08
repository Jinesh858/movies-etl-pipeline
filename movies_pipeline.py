import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import argparse
import logging
import re
from tabulate import tabulate

# ----------------- CONFIG -----------------
DB_CONFIG = {
    "host": "localhost",
    "dbname": "moviesdb",
    "user": "postgres",
    "password": "Jinesh@858",  # change this
    "port": 5432
}

# ----------------- LOGGING -----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)

# ----------------- UTIL FUNCTIONS -----------------
def convert_runtime(runtime_str):
    """Convert '2h 22m' → 142 (minutes)"""
    hours = minutes = 0
    if isinstance(runtime_str, str):
        h_match = re.search(r"(\d+)h", runtime_str)
        m_match = re.search(r"(\d+)m", runtime_str)
        if h_match:
            hours = int(h_match.group(1))
        if m_match:
            minutes = int(m_match.group(1))
    return hours * 60 + minutes

# ----------------- DATABASE HANDLER -----------------
class MoviesDBHandler:
    def __init__(self, config):
        self.conn = psycopg2.connect(**config)
        self.cur = self.conn.cursor()

    def create_table(self):
        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS movies (
                rank INT PRIMARY KEY,
                title TEXT,
                release_year INT,
                runtime_minutes INT,
                rated TEXT,
                rating FLOAT
            );
        """)
        self.conn.commit()
        logging.info("Table 'movies' created or already exists.")

    def insert_movies(self, df):
        # Cast columns to Python-native types
        df = df.astype({
            "rank": "int",
            "title": "string",
            "release_year": "Int64",
            "runtime_minutes": "Int64",
            "rated": "string",
            "rating": "float"
        })

        # Convert DataFrame rows to tuples with None instead of NaN
        records = [tuple(map(lambda x: None if pd.isna(x) else x, row)) for row in df.to_numpy()]

        query = """
            INSERT INTO movies (rank, title, release_year, runtime_minutes, rated, rating)
            VALUES %s
            ON CONFLICT (rank) DO NOTHING;
        """
        execute_values(self.cur, query, records)
        self.conn.commit()
        logging.info(f"Inserted {len(df)} rows into 'movies' table.")

    def run_analysis(self):
        queries = {
            "Total rows": "SELECT COUNT(*) FROM movies;",
            "Top 10 highest-rated movies":
                "SELECT title, rating FROM movies ORDER BY rating DESC LIMIT 10;",
            "Average rating per decade":
                "SELECT (release_year/10)*10 AS decade, AVG(rating) AS avg_rating FROM movies GROUP BY decade ORDER BY decade;",
            "Longest 5 movies":
                "SELECT title, runtime_minutes FROM movies ORDER BY runtime_minutes DESC LIMIT 5;"
        }

        for desc, sql in queries.items():
            logging.info(f"\n--- {desc} ---")
            self.cur.execute(sql)
            rows = self.cur.fetchall()
            col_names = [desc[0] for desc in self.cur.description]
            print(tabulate(rows, headers=col_names, tablefmt="psql"))

    def run_custom(self, sql):
        try:
            self.cur.execute(sql)
            rows = self.cur.fetchall()
            col_names = [desc[0] for desc in self.cur.description]
            print("\nCustom Query Result:")
            print(tabulate(rows, headers=col_names, tablefmt="psql"))
        except Exception as e:
            logging.error(f"Error running custom SQL: {e}")
            self.conn.rollback()

    def close(self):
        self.cur.close()
        self.conn.close()

# ----------------- PIPELINE RUNNER -----------------
class PipelineRunner:
    def __init__(self, db_handler):
        self.db_handler = db_handler

    def run(self, file_path):
        logging.info(f"Reading CSV file: {file_path}")
        df = pd.read_csv(file_path)

        # Transform step
        df = df.rename(columns={
            "Rank": "rank",
            "Title": "title",
            "Release": "release_year",
            "Runtime": "runtime",
            "Rated": "rated",
            "Ratings": "rating"
        })
        df["release_year"] = pd.to_numeric(df["release_year"], errors="coerce")
        df["runtime_minutes"] = df["runtime"].apply(convert_runtime)
        df = df[["rank", "title", "release_year", "runtime_minutes", "rated", "rating"]]

        logging.info("Data transformed. Sample:")
        logging.info(df.head())

        # Load step
        self.db_handler.create_table()
        self.db_handler.insert_movies(df)

# ----------------- MAIN -----------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Movies Data ETL Pipeline")
    parser.add_argument("--file", type=str, help="Path to CSV file")
    parser.add_argument("--analyze", action="store_true", help="Run analysis queries")
    parser.add_argument("--custom", type=str, help="Run custom SQL query")
    args = parser.parse_args()

    db_handler = MoviesDBHandler(DB_CONFIG)

    if args.file:
        pipeline = PipelineRunner(db_handler)
        pipeline.run(file_path=args.file)
        logging.info("Pipeline completed successfully ✅")

    if args.analyze:
        db_handler.run_analysis()

    if args.custom:
        db_handler.run_custom(args.custom)

    db_handler.close()

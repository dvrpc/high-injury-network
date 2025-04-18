import os
import psycopg2
import requests
from dotenv import load_dotenv
import geopandas as gpd
from pyproj import CRS
import pandas as pd
from urllib.parse import urlparse
from sqlalchemy import create_engine, text
import re

load_dotenv()

host = os.getenv("HOST")
database = os.getenv("DB")
user = os.getenv("USER")
password = os.getenv("PASSWORD")
port = os.getenv("PORT")

def create_database(dbname):
    """
    Creates a PostgreSQL database with the given name.
    """
    pgconn = psycopg2.connect(
        host=host, port=port, database=database, user=user, password=password
    )
    pgconn.autocommit = True
    cur = pgconn.cursor()

    cur.execute(f"DROP DATABASE IF EXISTS {dbname};")    
    cur.execute(f"CREATE DATABASE {dbname};")
    print(f"Creating Database {dbname}...\n")
    cur.close()
    pgconn.close()


def create_schemas(dbname, schemas):
    """
    Creates PostgreSQL schemas with the given names in the database.
    """
    cmpconn = psycopg2.connect(
        host=host, port=port, database=dbname, user=user, password=password
    )
    cur = cmpconn.cursor()
    cmpconn.autocommit = True

    for schema in schemas:
        cur.execute(f"SELECT 1 FROM pg_namespace WHERE nspname='{schema}'")
        schema_exists = bool(cur.rowcount)
        if not schema_exists:
            cur.execute(f"CREATE SCHEMA {schema};")
            print(f"Creating schema {schema}...\n")
    cur.close()
    cmpconn.close()


def create_postgis_extension(dbname):
    """
    Creates the POSTGIS extension in the database.
    """
    cmpconn = psycopg2.connect(
        host=host, port=port, database=dbname, user=user, password=password
    )
    cur = cmpconn.cursor()
    cmpconn.autocommit = True

    cur.execute("SELECT 1 FROM pg_extension WHERE extname='postgis'")
    postgis_extension_exists = bool(cur.rowcount)

    if not postgis_extension_exists:
        cur.execute("CREATE EXTENSION POSTGIS;")
        print(f"POSTGIS added...\n")
    cur.close()
    cmpconn.close()


def execute_analysis(dbname, sql, windowsize=0.5, window_increment=0.01, gap=0.47343, crashcount=2, start_year=2018):
    """
    Executes the analysis sql.  Messy but working....
    """
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")

    with open(sql, 'r') as sql_file:
        sql_contents = sql_file.read()

    transaction_blocks = re.split(r'COMMIT;\s*\n', sql_contents)

    with engine.connect() as connection:
        try:
            for transaction_block in transaction_blocks:
                transaction_block = transaction_block.strip()
                if not transaction_block:
                    continue
                try:
                    comment_match = re.search(r'/\*([\s\S]*?)\*/', transaction_block)
                    if comment_match:
                        comment = comment_match.group(1).strip()
                        print(f"\nSQL: {comment}\n")
                    connection.execute(text(transaction_block), parameters=dict(windowsize=windowsize,window_increment=window_increment,gap=gap,crashcount=crashcount,start_year=start_year))
                except Exception as e:
                    print("Error message:", str(e))
                    break
                try:
                    connection.execute(text('commit;'))
                except Exception as e:
                    print("Error during commit:", str(e))
                    break
        except:
            raise
        
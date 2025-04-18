import os
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy import text
from dotenv import load_dotenv
import geopandas as gpd

load_dotenv()

host = os.getenv("HOST")
database = os.getenv("DB")
user = os.getenv("USER")
password = os.getenv("PASSWORD")
port = os.getenv("PORT")

def create_geojson(dbname, path):
    """
    Creates shapefiles of output tables
    """
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")
    
    connection = engine.connect()

    query = text(f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'output' AND table_name LIKE '%rhin%'")

    tables = connection.execute(query)
    table_names = [table[0] for table in tables]

    for table_name in table_names:
        if 'crashes' in table_name:
            gdf = gpd.read_postgis(f"SELECT * FROM output.{table_name} where geom is not null", engine)
        elif 'rhin' in table_name:
            gdf = gpd.read_postgis(f"SELECT * FROM output.{table_name}", engine)
        else:
            continue
        output_file = os.path.join(path, f"{table_name}.geojson") 
        gdf.to_file(output_file)
        print(f"Created {output_file} in output folder...\n")
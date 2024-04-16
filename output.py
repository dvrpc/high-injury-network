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

    query = text(f"SELECT table_name FROM information_schema.tables WHERE table_schema = 'output' AND table_name LIKE '%gis%'")

    tables = connection.execute(query)
    table_names = [table[0] for table in tables]

    for table_name in table_names:
        gdf = gpd.read_postgis(f"SELECT * FROM output.{table_name} where st_geometrytype(geom) != 'ST_Point'", engine)
        gdf = gdf.dissolve('hin_id')
        output_file = os.path.join(path, f"{table_name}.geojson") 
        gdf.to_file(output_file, geometry="geometry")
        print(f"Created {output_file} in output folder...\n")
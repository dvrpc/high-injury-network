import os
import psycopg2
import requests
import math
from dotenv import load_dotenv
import geopandas as gpd
from pyproj import CRS
import pandas as pd
from urllib.parse import urlparse
from sqlalchemy import create_engine
import fiona
from shapely.geometry import LineString

load_dotenv()

host = os.getenv("HOST")
database = os.getenv("DB")
user = os.getenv("USER")
password = os.getenv("PASSWORD")
port = os.getenv("PORT")

portal = {
    "username": os.getenv("PORTAL_USERNAME"),
    "password": os.getenv("PORTAL_PASSWORD"),
    "client": os.getenv("PORTAL_CLIENT"),
    "referer": os.getenv("PORTAL_URL"),
    "expiration": int(os.getenv("PORTAL_EXPIRATION")),
    "f": os.getenv("PORTAL_F")
}


def dvrpc_data(dbname, target_schema, url_key, url, target_crs):
    """
    Loads the DVRPC data from feature services into database.
    """
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")

    if url.startswith("https://arcgis.dvrpc.org"):
        response = requests.post("https://arcgis.dvrpc.org/dvrpc/sharing/rest/generateToken", data=portal)
        response_obj = response.json()
        token = response_obj.get("token")
    else:
        token = None

    parsed_url = urlparse(url)
    path_parts = parsed_url.path.split("/")
    table_name = path_parts[-4].lower()
    base_url = url.split('?')[0]

    count_url = f"{base_url}?where=1=1&returnCountOnly=true"
    if token:
        count_url += f"&token={token}"
    count_url += "&f=json"

    count_response = requests.get(count_url)
    total_features = count_response.json().get("count")

    gdf_list = []

    total_chunks = math.ceil(total_features / 2000) # 2000 default esri record limit on feature services

    print(f"Loading {table_name}...")

    for chunk in range(total_chunks):
        offset = chunk * 2000
        query_url = f"{url}&resultOffset={offset}&resultRecordCount=2000"
        if token:
            query_url += f"&token={token}"
        response = requests.get(query_url)
        data = response.json()
        if "f=json" in url: # deals with the bringing the m values in from lrs (can't with f=geojson)
            if 'geometry' in data['features'][0]:
                for feature in data['features']:
                    attributes = feature['attributes']
                    df = pd.DataFrame(attributes, index=[0])
                    df['geometry'] = LineString(feature['geometry']['paths'][0])
                    chunk_gdf = gpd.GeoDataFrame(df, crs=target_crs, geometry='geometry')
                    gdf_list.append(chunk_gdf)
            else: # handle non geometry feature service
                df = pd.DataFrame([feature['attributes'] for feature in data['features']])
                gdf_list.append(df)
        else:
            chunk_gdf = gpd.GeoDataFrame.from_features(data['features'])
            gdf_list.append(chunk_gdf)

    gdf = pd.concat(gdf_list, ignore_index=True)

    if 'geometry' not in gdf.columns:
        # no geometry service
        gdf.columns = map(str.lower, gdf.columns)
        gdf.to_sql(url_key.lower(), engine, schema=target_schema, if_exists='replace', index=False)
    else:
        # geometries
        gdf.columns = map(str.lower, gdf.columns)
        gdf.crs = target_crs
        gdf.to_postgis(url_key.lower(), engine, schema=target_schema, if_exists='replace', index=False)


def csv_tables(dbname, target_schema, source_path):
    """
    Loads the csv into database.
    """
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{dbname}")

    for filename in os.listdir(source_path):
        if filename.endswith(".csv"):
            df = pd.read_csv(os.path.join(source_path, filename))
            table_name = os.path.splitext(os.path.basename(filename))[0]

            print(f"Loading {table_name}.csv...\n")
            df.to_sql(table_name, con=engine, schema=target_schema, if_exists='replace', index=False)
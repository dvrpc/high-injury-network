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
    response_data = count_response.json()
    
    # Check if the response contains an error
    if "error" in response_data:
        print(f"Error from API: {response_data['error']['message']}")
        return  # Skip this service if there's an error
        
    total_features = response_data.get("count")
    
    # Add a check for None
    if total_features is None:
        print(f"Could not get count from {url_key} - service may not exist or format has changed")
        return  # Skip this service if count is None

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
        
        # First, check if we have valid data
        if not data.get('features'):
            continue
            
        if "f=json" in url:  # deals with bringing the m values in from lrs
            if 'geometry' in data['features'][0]:
                for feature in data['features']:
                    attributes = feature['attributes']
                    df = pd.DataFrame(attributes, index=[0])
                    
                    # Extract coordinates with M values from paths
                    paths = feature['geometry']['paths'][0]
                    
                    # Create a regular LineString with just X and Y coordinates
                    xy_coords = [(p[0], p[1]) for p in paths]
                    line_geom = LineString(xy_coords)
                    df['geometry'] = line_geom
                    
                    # Store the M values as a separate attribute for later processing
                    df['m_values'] = str([p[2] for p in paths])
                    
                    chunk_gdf = gpd.GeoDataFrame(df, crs=target_crs, geometry='geometry')
                    gdf_list.append(chunk_gdf)
            else:  # handle non geometry feature service
                df = pd.DataFrame([feature['attributes'] for feature in data['features']])
                gdf_list.append(df)
        else:
            # For GeoJSON format, handle the case where 'geometry' exists in properties
            try:
                # Pre-process to remove conflicting 'geometry' property
                for feature in data['features']:
                    if 'properties' in feature and 'geometry' in feature['properties']:
                        # Store it as 'geom_text' if you need to keep it
                        if feature['properties']['geometry'] is not None:
                            feature['properties']['geom_text'] = feature['properties']['geometry']
                        # Delete the conflicting property
                        del feature['properties']['geometry']
                
                # Now use GeoPandas to create the GeoDataFrame
                chunk_gdf = gpd.GeoDataFrame.from_features(data['features'], crs=target_crs)
                
                # Debug info
                if not chunk_gdf.empty:
                    geom_types = chunk_gdf.geometry.apply(lambda g: type(g).__name__ if g is not None else "None").value_counts()
                
                gdf_list.append(chunk_gdf)
            except Exception as e:
                print(f"Error creating GeoDataFrame from features for {url_key}: {e}")
                
                # Try an alternative approach
                try:
                    features_list = []
                    geometries = []
                    
                    for feature in data['features']:
                        # Get properties without the conflicting 'geometry' field
                        props = {k: v for k, v in feature.get('properties', {}).items() if k != 'geometry'}
                        
                        # Extract the actual geometry (rms has an weird extra geometry text field)
                        geom = None
                        if feature.get('geometry') and feature['geometry'].get('coordinates'):
                            geom_type = feature['geometry'].get('type')
                            coords = feature['geometry'].get('coordinates')
                            
                            try:
                                if geom_type == 'Point':
                                    geom = Point(coords)
                                elif geom_type == 'LineString':
                                    geom = LineString(coords)
                                elif geom_type == 'Polygon':
                                    geom = Polygon(coords)
                                elif geom_type == 'MultiPoint':
                                    geom = MultiPoint(coords)
                                elif geom_type == 'MultiLineString':
                                    geom = MultiLineString(coords)
                                elif geom_type == 'MultiPolygon':
                                    geom = MultiPolygon(coords)
                                else:
                                    print(f"Unsupported geometry type: {geom_type}")
                            except Exception as geo_err:
                                print(f"Error creating {geom_type}: {geo_err}")
                        
                        features_list.append(props)
                        geometries.append(geom)
                    
                    # Create the GeoDataFrame manually
                    chunk_gdf = gpd.GeoDataFrame(features_list, geometry=geometries, crs=target_crs)
                    gdf_list.append(chunk_gdf)
                except Exception as e2:
                    print(f"Alternative approach also failed: {e2}")
                    # Fall back to non-geometry dataframe
                    try:
                        properties_list = []
                        for feature in data['features']:
                            props = feature.get('properties', {}).copy()
                            if 'geometry' in props:
                                del props['geometry']
                            properties_list.append(props)
                        
                        df = pd.DataFrame(properties_list)
                        gdf_list.append(df)
                        print(f"Fell back to non-geometry dataframe for {url_key} (chunk {chunk})")
                    except Exception as e3:
                        print(f"All attempts failed for {url_key} (chunk {chunk}): {e3}")

    if not gdf_list:
        return

    gdf = pd.concat(gdf_list, ignore_index=True)
    
    if 'geometry' not in gdf.columns:
        # no geometry service
        gdf.columns = map(str.lower, gdf.columns)
        gdf.to_sql(url_key.lower(), engine, schema=target_schema, if_exists='replace', index=False)
    else:
        # Check for valid geometries
        valid_geoms = gdf['geometry'].apply(lambda g: g is not None and not g.is_empty if g is not None else False)
        
        if valid_geoms.sum() == 0:
            print("WARNING: No valid geometries found!")
            # Fall back to regular SQL if no valid geometries
            if 'geometry' in gdf.columns:
                gdf = gdf.drop(columns=['geometry'])
            gdf.columns = map(str.lower, gdf.columns)
            gdf.to_sql(url_key.lower(), engine, schema=target_schema, if_exists='replace', index=False)
        else:
            # Filter out rows with invalid geometries if any
            if valid_geoms.sum() < len(gdf):
                gdf = gdf[valid_geoms]
            
            # geometries
            gdf.columns = map(str.lower, gdf.columns)
            gdf.crs = target_crs
            try:
                gdf.to_postgis(url_key.lower(), engine, schema=target_schema, if_exists='replace', index=False)
            except ValueError as e:
                if "No valid geometries in the data" in str(e):
                    print("Error when saving to PostGIS. Falling back to regular SQL table.")
                    # Drop geometry column if it exists
                    if 'geometry' in gdf.columns:
                        gdf = gdf.drop(columns=['geometry'])
                    gdf.to_sql(url_key.lower(), engine, schema=target_schema, if_exists='replace', index=False)
                else:
                    raise


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
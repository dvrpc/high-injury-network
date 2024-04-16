import db
import output
import load
import json
import os
from dotenv import load_dotenv
import time

start_time = time.time()

load_dotenv()

dbname = r"hin"
schemas = ["input", "output"]
source_path = "./source"
out_path = "./output"
dvrpc_gis_sources = "source/dvrpc_data_sources.json"

windowsize = 0.5 
window_increment = 0.01 
gap = 0.47343 
crashcount = 2

db.create_database(dbname)

db.create_schemas(dbname, schemas)

db.create_postgis_extension(dbname)

load.csv_tables(dbname, 'input', source_path)

with open(dvrpc_gis_sources, 'r') as config_file:
    urls_config = json.load(config_file)
urls = urls_config['urls']
for url_key, url_value in urls.items():
    load.dvrpc_data(dbname, 'input', url_key, url_value, 'EPSG:26918')

db.execute_analysis(dbname, './sql/analysis.sql', windowsize, window_increment, gap, crashcount)
db.execute_analysis(dbname, './sql/output.sql')

output.create_geojson(dbname, out_path)

end_time = time.time()
duration = end_time - start_time
print("Script duration: {:.2f} seconds".format(duration))
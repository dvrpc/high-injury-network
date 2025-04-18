# Regional High Injury Network (RHIN)
Data driven approach to identify road segments with KSI and Bicycle/Pedestrian crash trends over a 5 year time period.

### Methodology
This project uses a sliding window approach to identify segments with high KSI and bike/ped crashes.  Each window is 0.5 miles long with a 0.01 mile sliding increment along along a route.
Windows with 2 or more KSI or bike/ped crash locations are selected for the HIN.  Nearby windows who also meet the 2 or more KSI or bike/ped criteria are aggregated into larger segments if they are overlapping or within 2,500ft or 0.47343 miles.  The sliding window and crash threshold attributes can be adjusted using these variables in [run.py](run.py) (sliding window is dependant on **mile** units only)

```
windowsize = 0.5 
window_increment = 0.01 
gap = 0.47343 
crashcount = 2
start_year = 2018
```

**Disclaimer:**
We utilize linear referencing measures from both DOT datasets to associate the crash locations to each road network.  PennDOT's crash data has incomplete road referencing data especially for local road crashes.  Because of this, a spatial snapping method is used to snap these crashes to their closest (within 10 meters) road network.  There will be some crashes that aren't included in this analysis because they don't include any type of location information.

### Output 
KSI and Bike/Ped high injury network geojson files for each state in your output folder.

### Requirements
- PostgreSQL w/ PostGIS
- Python 3.x
- DVRPC ArcGIS Portal credentials to pull GIS data from ArcGIS Server
- [.csv](./source/nj_lrs_access.csv) created with ArcGIS Overlay Route Events tool using LRS table and Limited Access table

### Run
1. Clone the repo
    ``` cmd
    git clone https://github.com/dvrpc/high-injury-network.git
    ```
2. Create a Python virtual environment with dependencies

    Working in the repo directory from your terminal:
    ```
    cd \high-injury-network
    ```
    - create new venv
    ```cmd
    python -m venv venv
    ```
    - activate venv
    ```
    .\venv\scripts\activate
    ```
    - install requirements
    ```
    pip install -r requirements.txt
    ```
    - copy .env_sample and rename to .env
    ```
    copy .env_sample .env
    ```
    - edit .env environmental variables in VSCode and provide PostgreSQL/ArcGIS Portal credentials

3. Edit variables in `run.py` for sliding window preferences or anything else you want adjusted

4. Start the process
    ```
    python run.py
    ```
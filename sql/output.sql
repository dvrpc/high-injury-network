/*
Creating output tables with geometry for each HIN
 */
 -- nj join crashes back to hin segments
 BEGIN;
 CREATE VIEW
    output.nj_crash_hinid as   
WITH
    nj_hin AS (
        SELECT
            hin_id,
            sri,
            window_from,
            window_to,
            'bp' AS TYPE
        FROM
            output.nj_bp_hin bp
        UNION
        SELECT
            hin_id,
            sri,
            window_from,
            window_to,
            'ksi' AS TYPE
        FROM
            output.nj_ksi_hin ksi
    )
SELECT
    c.*,
    h.hin_id,
TYPE
FROM
    INPUT.nj_crashes c
    JOIN nj_hin h ON c.sri_std_rte_identifier = h.sri
    AND c.milepost::numeric BETWEEN h.window_from AND h.window_to;
COMMIT;
 -- pa join crashes back to hin segments
BEGIN;
CREATE VIEW
    output.pa_crash_hinid AS
WITH
    pa_hin AS (
        SELECT
            hin_id,
            id,
            window_from,
            window_to,
            'bp' AS
        TYPE
        FROM
            output.pa_bp_hin bp
        UNION
        SELECT
            hin_id,
            id,
            window_from,
            window_to,
            'ksi' AS
        TYPE
        FROM
            output.pa_ksi_hin ksi
    )
SELECT
    c.*,
    h.hin_id,
TYPE
FROM
    INPUT.pa_crashes c
    JOIN pa_hin h ON c.id = h.id
    AND c.cumulative_offset BETWEEN h.window_from AND h.window_to;
COMMIT;
-- rhin pa crash experience
CREATE TABLE output.pa_rhin AS
WITH person_summary AS (
    SELECT 
        p.crn,
        COUNT(CASE WHEN p.inj_severity = '0' THEN 1 ELSE NULL END) AS not_injured_count
    FROM 
        input.crash_pa_person p
    GROUP BY 
        p.crn        
)
SELECT 
    rg.hin_id,
    rg.type,
    rg.road_type,
    'pa' AS state,
    rg.threshold AS hin_threshold_count,
    -- Total crash count
    COUNT(DISTINCT t.crn) AS total_crash_count,
    -- Crashes by year
    SUM(CASE WHEN c.crash_year = 2018 THEN 1 ELSE 0 END) AS crashes_2018,
    SUM(CASE WHEN c.crash_year = 2019 THEN 1 ELSE 0 END) AS crashes_2019,
    SUM(CASE WHEN c.crash_year = 2020 THEN 1 ELSE 0 END) AS crashes_2020,
    SUM(CASE WHEN c.crash_year = 2021 THEN 1 ELSE 0 END) AS crashes_2021,
    SUM(CASE WHEN c.crash_year = 2022 THEN 1 ELSE 0 END) AS crashes_2022,
    -- Collision types
    SUM(CASE WHEN c.collision_type = '0' THEN 1 ELSE 0 END) AS non_collision,
    SUM(CASE WHEN c.collision_type = '1' THEN 1 ELSE 0 END) AS rear_end,
    SUM(CASE WHEN c.collision_type = '2' THEN 1 ELSE 0 END) AS head_on,
    SUM(CASE WHEN c.collision_type = '3' THEN 1 ELSE 0 END) AS backing,
    SUM(CASE WHEN c.collision_type = '4' THEN 1 ELSE 0 END) AS angle,
    SUM(CASE WHEN c.collision_type = '5' THEN 1 ELSE 0 END) AS sideswipe_same_dir,
    SUM(CASE WHEN c.collision_type = '6' THEN 1 ELSE 0 END) AS sideswipe_opp_dir,
    SUM(CASE WHEN c.collision_type = '7' THEN 1 ELSE 0 END) AS hit_fixed_object,
    SUM(CASE WHEN c.collision_type = '8' THEN 1 ELSE 0 END) AS hit_non_motorist,
    SUM(CASE WHEN c.collision_type = '9' THEN 1 ELSE 0 END) AS other_expired,
    SUM(CASE WHEN c.collision_type = '98' THEN 1 ELSE 0 END) AS collision_other,
    SUM(CASE WHEN c.collision_type = '99' THEN 1 ELSE 0 END) AS collision_unknown,
    -- Illumination
    SUM(CASE WHEN c.illumination = '1' THEN 1 ELSE 0 END) AS daylight,
    SUM(CASE WHEN c.illumination = '2' THEN 1 ELSE 0 END) AS dark_no_streetlights,
    SUM(CASE WHEN c.illumination = '3' THEN 1 ELSE 0 END) AS dark_streetlights,
    SUM(CASE WHEN c.illumination = '4' THEN 1 ELSE 0 END) AS dusk,
    SUM(CASE WHEN c.illumination = '5' THEN 1 ELSE 0 END) AS dawn,
    SUM(CASE WHEN c.illumination = '6' THEN 1 ELSE 0 END) AS dark_unknown_lighting,
    SUM(CASE WHEN c.illumination = '8' THEN 1 ELSE 0 END) AS light_other,
    SUM(CASE WHEN c.illumination = '9' THEN 1 ELSE 0 END) AS light_unknown,
    -- Road Surface Condition
    SUM(CASE WHEN c.road_condition = '01' THEN 1 ELSE 0 END) AS surface_dry,
    SUM(CASE WHEN c.road_condition = '02' THEN 1 ELSE 0 END) AS surface_ice_frost,
    SUM(CASE WHEN c.road_condition = '03' THEN 1 ELSE 0 END) AS surface_mud_dirt_gravel,
    SUM(CASE WHEN c.road_condition = '04' THEN 1 ELSE 0 END) AS surface_oil,
    SUM(CASE WHEN c.road_condition = '05' THEN 1 ELSE 0 END) AS surface_sand,
    SUM(CASE WHEN c.road_condition = '06' THEN 1 ELSE 0 END) AS surface_slush,
    SUM(CASE WHEN c.road_condition = '07' THEN 1 ELSE 0 END) AS surface_snow,
    SUM(CASE WHEN c.road_condition = '08' THEN 1 ELSE 0 END) AS surface_water,
    SUM(CASE WHEN c.road_condition = '09' THEN 1 ELSE 0 END) AS surface_wet,
    SUM(CASE WHEN c.road_condition = '22' THEN 1 ELSE 0 END) AS surface_mud_sand_dirt_oil_expired,
    SUM(CASE WHEN c.road_condition = '98' THEN 1 ELSE 0 END) AS surface_other,
    SUM(CASE WHEN c.road_condition = '99' THEN 1 ELSE 0 END) AS surface_unknown,
    -- Weather
    SUM(CASE WHEN c.weather1 = '01' THEN 1 ELSE 0 END) AS weather_blowing_sand_soil_dirt,
    SUM(CASE WHEN c.weather1 = '02' THEN 1 ELSE 0 END) AS weather_blowing_snow,
    SUM(CASE WHEN c.weather1 = '03' THEN 1 ELSE 0 END) AS weather_clear,
    SUM(CASE WHEN c.weather1 = '04' THEN 1 ELSE 0 END) AS weather_cloudy,
    SUM(CASE WHEN c.weather1 = '05' THEN 1 ELSE 0 END) AS weather_fog_smog_smoke,
    SUM(CASE WHEN c.weather1 = '06' THEN 1 ELSE 0 END) AS weather_freezing_rain_drizzle,
    SUM(CASE WHEN c.weather1 = '07' THEN 1 ELSE 0 END) AS weather_rain,
    SUM(CASE WHEN c.weather1 = '08' THEN 1 ELSE 0 END) AS weather_severe_crosswinds,
    SUM(CASE WHEN c.weather1 = '09' THEN 1 ELSE 0 END) AS weather_sleet_hail,
    SUM(CASE WHEN c.weather1 = '10' THEN 1 ELSE 0 END) AS weather_snow,
    SUM(CASE WHEN c.weather1 = '98' THEN 1 ELSE 0 END) AS weather_other,
    SUM(CASE WHEN c.weather1 = '99' THEN 1 ELSE 0 END) AS weather_unknown,
    -- Injury summaries
    SUM(c.fatal_count) AS fatal_count,
    SUM(c.susp_serious_inj_count) AS major_injury_count,
    SUM(c.susp_minor_inj_Count) AS minor_injury_count,
    SUM(ps.not_injured_count) AS no_injury_count,
    -- Pedestrian and Bicyclist counts
    SUM(c.ped_death_count) AS ped_fatal_count,
    SUM(c.bicycle_death_count) AS bike_fatal_count,
    rg.geometry as geom
FROM 
    (
        SELECT 
            hin_id, 
            id, 
            window_from, 
            window_to, 
            road_type, 
            'bp' AS type, 
            bp_count AS threshold, 
            geometry 
        FROM 
            output.pa_bp_hin bp
        UNION
        SELECT 
            hin_id, 
            id, 
            window_from, 
            window_to, 
            road_type, 
            'ksi' AS type, 
            ksi_count AS threshold, 
            geometry 
        FROM 
            output.pa_ksi_hin ksi
    ) rg
    JOIN output.pa_crash_hinid t ON t.hin_id = rg.hin_id AND t.type = rg.type
    -- Join to your crash table to get all the required fields
    JOIN input.crash_pennsylvania c ON c.crn = t.crn
    LEFT JOIN person_summary ps ON ps.crn = c.crn
GROUP BY 
    rg.hin_id,
    rg.road_type,
    rg.type,
    rg.threshold,
    rg.geometry
ORDER BY 
    hin_id,
    type;
COMMIT;
-- nj rhin crash experience	
BEGIN;
CREATE TABLE
 output.nj_rhin AS
WITH 
-- Pre-calculate injury counts per crash
injury_counts AS (
    WITH occupant_counts AS (
        SELECT 
            casenumber,
            COUNT(CASE WHEN physical_condition = '01' THEN 1 END) AS fatal_count,
            COUNT(CASE WHEN physical_condition = '02' THEN 1 END) AS major_injury_count,
            COUNT(CASE WHEN physical_condition = '03' THEN 1 END) AS moderate_injury_count
        FROM 
            input.crash_nj_occupant
        GROUP BY 
            casenumber
    ),
    pedestrian_counts AS (
        SELECT 
            casenumber,
            COUNT(CASE WHEN physical_condition = '01' AND (is_bycyclist IS NULL OR is_bycyclist != 'Y') THEN 1 END) AS ped_fatal_count,
            COUNT(CASE WHEN physical_condition = '01' AND is_bycyclist = 'Y' THEN 1 END) AS bike_fatal_count,
            COUNT(CASE WHEN physical_condition = '02' THEN 1 END) AS major_injury_count,
            COUNT(CASE WHEN physical_condition = '03' THEN 1 END) AS moderate_injury_count
        FROM 
            input.crash_nj_pedestrian
        GROUP BY 
            casenumber
    )
    SELECT 
        COALESCE(o.casenumber, p.casenumber) AS casenumber,
        COALESCE(o.fatal_count, 0) AS occupant_fatal_count,
        COALESCE(o.major_injury_count, 0) AS occupant_major_injury_count,
        COALESCE(o.moderate_injury_count, 0) AS occupant_moderate_injury_count,
        COALESCE(p.ped_fatal_count, 0) AS ped_fatal_count,
        COALESCE(p.bike_fatal_count, 0) AS bike_fatal_count,
        COALESCE(p.major_injury_count, 0) AS ped_major_injury_count,
        COALESCE(p.moderate_injury_count, 0) AS ped_moderate_injury_count,
        -- Flag for existence of major injuries (for compatibility with original query)
        CASE WHEN COALESCE(o.major_injury_count, 0) + COALESCE(p.major_injury_count, 0) > 0 THEN 1 ELSE 0 END AS has_major_injury,
        -- Flag for existence of moderate injuries (for compatibility with original query)
        CASE WHEN COALESCE(o.moderate_injury_count, 0) + COALESCE(p.moderate_injury_count, 0) > 0 THEN 1 ELSE 0 END AS has_moderate_injury
    FROM 
        occupant_counts o
    FULL OUTER JOIN 
        pedestrian_counts p ON o.casenumber = p.casenumber
),
-- Combine HIN data from both sources
hin_data AS (
    SELECT 
        hin_id, 
        sri, 
        window_from, 
        window_to, 
        'bp' AS type, 
        bp_count AS threshold, 
        geometry 
    FROM 
        output.nj_bp_hin bp
    UNION
    SELECT 
        hin_id, 
        sri, 
        window_from, 
        window_to, 
        'ksi' AS type, 
        ksi_count AS threshold, 
        geometry 
    FROM 
        output.nj_ksi_hin ksi
)
SELECT 
    rg.hin_id,
    rg.type,
    'nj' AS state,
    rg.threshold,
    -- Total crash count
    COUNT(DISTINCT t.casenumber) AS total_crash_count,
    SUM(CASE WHEN right(c.crash_date,4) = '2017' THEN 1 ELSE 0 END) AS crashes_2017,
    SUM(CASE WHEN right(c.crash_date,4) = '2018' THEN 1 ELSE 0 END) AS crashes_2018,
    SUM(CASE WHEN right(c.crash_date,4) = '2019' THEN 1 ELSE 0 END) AS crashes_2019,
    SUM(CASE WHEN right(c.crash_date,4) = '2020' THEN 1 ELSE 0 END) AS crashes_2020,
    SUM(CASE WHEN right(c.crash_date,4) = '2021' THEN 1 ELSE 0 END) AS crashes_2021,
    -- Collision Type
    SUM(CASE WHEN c.crash_type_code = '01' THEN 1 ELSE 0 END) AS same_direction_rear_end,
    SUM(CASE WHEN c.crash_type_code = '02' THEN 1 ELSE 0 END) AS same_direction_sideswipe,
    SUM(CASE WHEN c.crash_type_code = '03' THEN 1 ELSE 0 END) AS angle,
    SUM(CASE WHEN c.crash_type_code IN ('04', '05') THEN 1 ELSE 0 END) AS head_on,
    SUM(CASE WHEN c.crash_type_code = '06' THEN 1 ELSE 0 END) AS parked_vehicle,
    SUM(CASE WHEN c.crash_type_code = '07' THEN 1 ELSE 0 END) AS left_turn_u_turn,
    SUM(CASE WHEN c.crash_type_code = '08' THEN 1 ELSE 0 END) AS backing,
    SUM(CASE WHEN c.crash_type_code = '09' THEN 1 ELSE 0 END) AS encroachment,
    SUM(CASE WHEN c.crash_type_code = '10' THEN 1 ELSE 0 END) AS overturned,
    SUM(CASE WHEN c.crash_type_code = '11' THEN 1 ELSE 0 END) AS fixed_object,
    SUM(CASE WHEN c.crash_type_code = '12' THEN 1 ELSE 0 END) AS animal,
    SUM(CASE WHEN c.crash_type_code = '13' THEN 1 ELSE 0 END) AS pedestrian,
    SUM(CASE WHEN c.crash_type_code = '14' THEN 1 ELSE 0 END) AS pedalcycle,
    SUM(CASE WHEN c.crash_type_code = '15' THEN 1 ELSE 0 END) AS non_fixed_object,
    SUM(CASE WHEN c.crash_type_code = '16' THEN 1 ELSE 0 END) AS railcar_vehicle,
    SUM(CASE WHEN c.crash_type_code = '00' THEN 1 ELSE 0 END) AS unknown,
    SUM(CASE WHEN c.crash_type_code = '99' THEN 1 ELSE 0 END) AS other,
    -- Light Conditions
    SUM(CASE WHEN c.light_condition = '01' THEN 1 ELSE 0 END) AS light_day,
    SUM(CASE WHEN c.light_condition = '03' THEN 1 ELSE 0 END) AS light_dusk,
    SUM(CASE WHEN c.light_condition IN ('04', '05', '06', '07') THEN 1 ELSE 0 END) AS light_night,
    SUM(CASE WHEN c.light_condition = '02' THEN 1 ELSE 0 END) AS light_dawn,
    SUM(CASE WHEN c.light_condition = '00' THEN 1 ELSE 0 END) AS light_unknown,
    SUM(CASE WHEN c.light_condition = '99' THEN 1 ELSE 0 END) AS light_other,
    -- Surface Conditions
    SUM(CASE WHEN c.surface_condition = '01' THEN 1 ELSE 0 END) AS surface_dry,
    SUM(CASE WHEN c.surface_condition IN ('02', '06') THEN 1 ELSE 0 END) AS surface_wet,
    SUM(CASE WHEN c.surface_condition = '03' THEN 1 ELSE 0 END) AS surface_snow,
    SUM(CASE WHEN c.surface_condition = '04' THEN 1 ELSE 0 END) AS surface_ice,
    SUM(CASE WHEN c.surface_condition = '00' THEN 1 ELSE 0 END) AS surface_unknown,
    SUM(CASE WHEN c.surface_condition IN ('05', '07', '08', '99') THEN 1 ELSE 0 END) AS surface_other,
    -- Weather Conditions (Environmental Condition)
    SUM(CASE WHEN c.environmental_condition = '00' THEN 1 ELSE 0 END) AS weather_unknown,
    SUM(CASE WHEN c.environmental_condition = '01' THEN 1 ELSE 0 END) AS weather_clear,
    SUM(CASE WHEN c.environmental_condition = '02' THEN 1 ELSE 0 END) AS weather_rain,
    SUM(CASE WHEN c.environmental_condition = '03' THEN 1 ELSE 0 END) AS weather_snow,
    SUM(CASE WHEN c.environmental_condition = '04' THEN 1 ELSE 0 END) AS weather_fog_smog_smoke,
    SUM(CASE WHEN c.environmental_condition = '05' THEN 1 ELSE 0 END) AS weather_overcast,
    SUM(CASE WHEN c.environmental_condition = '06' THEN 1 ELSE 0 END) AS weather_sleet_hail,
    SUM(CASE WHEN c.environmental_condition = '07' THEN 1 ELSE 0 END) AS weather_freezing_rain,
    SUM(CASE WHEN c.environmental_condition = '08' THEN 1 ELSE 0 END) AS weather_blowing_snow,
    SUM(CASE WHEN c.environmental_condition = '09' THEN 1 ELSE 0 END) AS weather_blowing_sand_dirt,
    SUM(CASE WHEN c.environmental_condition = '10' THEN 1 ELSE 0 END) AS weather_severe_crosswinds,
    -- Injury summaries - now using pre-calculated counts
    SUM(c.total_killed::numeric) AS fatal_count,
    -- Use the count of crashes with major injuries
    COUNT(DISTINCT CASE WHEN ic.has_major_injury = 1 THEN c.casenumber ELSE NULL END) AS major_injury_count,
    -- Use the count of crashes with moderate injuries
    COUNT(DISTINCT CASE WHEN ic.has_moderate_injury = 1 THEN c.casenumber ELSE NULL END) AS moderate_injury_crashes,
    SUM(CASE WHEN c.severity = 'P' THEN 1 ELSE 0 END) AS no_injury_count,
    -- Pedestrian and Bicyclist counts - now using pre-calculated counts
    SUM(c.pedestrians_killed::numeric) AS ped_fatal_count,
    -- Count of crashes with bicycle fatalities
    COUNT(DISTINCT CASE WHEN ic.bike_fatal_count > 0 THEN c.casenumber ELSE NULL END) AS bike_fatal_count,
    rg.geometry as geom
FROM 
    hin_data rg
    JOIN output.nj_crash_hinid t ON t.hin_id = rg.hin_id AND t.type = rg.type
    JOIN input.nj_crashes c ON c.casenumber = t.casenumber
    LEFT JOIN injury_counts ic ON c.casenumber = ic.casenumber
GROUP BY 
    rg.hin_id,
    rg.type,
    rg.threshold,
    rg.geometry
ORDER BY 
    rg.hin_id,
    rg.type;
COMMIT;    
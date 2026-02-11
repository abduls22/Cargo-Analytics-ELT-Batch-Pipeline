WITH geopoints AS (
    SELECT 
        *,
        ST_GEOGPOINT(-90.0, 35.0) AS memphis_geopoint,
        ST_GEOGPOINT(-85.7, 38.2) AS louisville_geopoint,
        ST_GEOGPOINT(-84.6, 39.0) AS cincinnati_geopoint,
        ST_GEOGPOINT(-150.0, 61.2) AS anchorage_geopoint
    FROM {{ ref('stg_cargo_flights') }}
),

distances AS (
    SELECT
        *,
        ST_DISTANCE(flight_geopoint, memphis_geopoint) / 1000 AS dist_to_memphis,
        ST_DISTANCE(flight_geopoint, louisville_geopoint) / 1000 AS dist_to_louisville,
        ST_DISTANCE(flight_geopoint, cincinnati_geopoint) / 1000 AS dist_to_cincinnati,
        ST_DISTANCE(flight_geopoint, anchorage_geopoint) / 1000 AS dist_to_anchorage
    FROM geopoints
),

flights AS (
    SELECT
        *,
        LEAST(dist_to_memphis, dist_to_louisville, dist_to_cincinnati, dist_to_anchorage) AS dist_to_nearest_hub_km
    FROM distances
)
 

SELECT
    *,

    CASE 
        WHEN speed_kmh > 850 THEN 'High Speed (Tailwinds)'
        WHEN speed_kmh BETWEEN 700 AND 850 THEN 'Standard Cruise'
        ELSE 'Reduced Speed/Approaching'
    END AS flight_velocity_status,

    CASE
        WHEN on_ground = TRUE THEN 'On Ground'
        WHEN baro_altitude >= 7315 THEN 'Cruising'
        WHEN baro_altitude BETWEEN 3048 AND 7315 AND vertical_rate > 0 THEN 'Climbing'
        WHEN baro_altitude BETWEEN 3048 AND 7315 AND vertical_rate < 0 THEN 'Descending'    
        WHEN baro_altitude BETWEEN 3048 AND 7315 AND vertical_rate = 0 THEN 'Transition'
        WHEN baro_altitude < 3048 THEN 'Terminal Operations'
        ELSE 'Undetermined'
    END AS flight_phase,

    CASE
        WHEN dist_to_nearest_hub_km = dist_to_memphis THEN 'Memphis SuperHub (FDX)'
        WHEN dist_to_nearest_hub_km = dist_to_louisville THEN 'Worldport Louisville (UPS)'
        WHEN dist_to_nearest_hub_km = dist_to_cincinnati THEN 'Cincinnati Hub (DHL)'
        WHEN dist_to_nearest_hub_km = dist_to_anchorage THEN 'Anchorage Gateway'
        ELSE NULL
    END AS nearest_hub,
    
    CASE
        WHEN dist_to_nearest_hub_km >= 250 THEN 'In Transit'
        WHEN dist_to_nearest_hub_km BETWEEN 50 AND 250 THEN 'Near Hub'
        WHEN dist_to_nearest_hub_km < 50 THEN 'Arrival/Departure'
        ELSE NULL
    END AS hub_proximity,

    ROUND(AVG(speed_kmh) OVER (PARTITION BY carrier_code), 2) AS average_fleet_speed,
    ROUND(speed_kmh - AVG(speed_kmh) OVER (PARTITION BY carrier_code), 2) AS fleet_speed_deviation,

    processed_at = MAX(processed_at) OVER () AS is_latest_sync
FROM flights

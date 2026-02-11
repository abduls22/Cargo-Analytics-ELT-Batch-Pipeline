{{
    config(
        materialized='incremental',
        unique_key=['icao_24', 'last_contact']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('opensky_raw', 'flight_records') }}
    WHERE time_position IS NOT NULL

    {% if is_incremental() %}
      AND time_position > (SELECT MAX(time_position) FROM {{ this }})
    {% endif %}
)

SELECT
    icao_24,
    TRIM(callsign) AS callsign,
    SUBSTR(TRIM(callsign), 1, 3) AS carrier_code,
    origin_country,
    time_position,
    last_contact,
    longitude,
    latitude,
    baro_altitude,
    on_ground,
    velocity,
    ROUND((velocity * 3.6), 2) AS speed_kmh,
    vertical_rate,
    ST_GEOGPOINT(longitude, latitude) AS flight_geopoint, 
    {{ dbt.current_timestamp() }} AS processed_at
FROM source
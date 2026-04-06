WITH base AS (

    SELECT
        machine_id,
        timestamp,
        temperature,
        vibration,
        pressure
    FROM {{ ref('stg_machine_readings') }}

)

SELECT
    machine_id,
    timestamp,
    temperature,
    vibration,
    pressure,

    -- Rolling metrics 
    AVG(temperature) OVER (
        PARTITION BY machine_id
        ORDER BY timestamp
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    ) AS rolling_temp_avg,

    AVG(vibration) OVER (
        PARTITION BY machine_id
        ORDER BY timestamp
        ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
    ) AS rolling_vibration_avg

FROM base

WITH metrics AS (

    SELECT *
    FROM {{ ref('int_machine_metrics') }}

)

SELECT
    machine_id,
    timestamp,
    temperature,
    rolling_temp_avg,

    -- 🔥 Anomaly logic
    CASE
        WHEN temperature > rolling_temp_avg * 1.5 THEN TRUE
        ELSE FALSE
    END AS is_anomaly,

    CASE
        WHEN temperature > 90 THEN 'HIGH_TEMP'
        WHEN temperature > rolling_temp_avg * 1.5 THEN 'SPIKE'
        ELSE 'NORMAL'
    END AS anomaly_type

FROM metrics

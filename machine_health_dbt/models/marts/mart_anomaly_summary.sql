SELECT
    machine_id,
    DATE(timestamp) AS day,
    COUNT(*) FILTER (WHERE is_anomaly) AS anomaly_count
FROM {{ ref('mart_anomalies') }}
GROUP BY 1, 2

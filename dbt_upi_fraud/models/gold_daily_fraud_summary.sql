{{ config(materialized='table') }}

WITH txns AS (
    SELECT
        DATE_TRUNC('DAY', event_ts) AS txn_date,
        COUNT(DISTINCT txn_id)      AS total_txns,
        ROUND(SUM(amount), 2)       AS total_volume_inr
    FROM {{ source('raw_upi', 'RAW_UPI_TRANSACTIONS') }}
    GROUP BY 1
),
alerts AS (
    SELECT
        DATE_TRUNC('DAY', detected_at) AS alert_date,
        COUNT(*)                       AS total_fraud_alerts,
        ROUND(SUM(total_amount), 2)    AS flagged_volume_inr,
        COUNT_IF(fraud_reason = 'VELOCITY_BURST')     AS velocity_alerts,
        COUNT_IF(fraud_reason = 'HIGH_VALUE_ANOMALY') AS high_value_alerts,
        COUNT_IF(fraud_reason = 'IMPOSSIBLE_TRAVEL')  AS travel_alerts
    FROM {{ source('raw_upi', 'RAW_FRAUD_ALERTS') }}
    GROUP BY 1
)

SELECT
    t.txn_date,
    t.total_txns,
    t.total_volume_inr,
    COALESCE(a.total_fraud_alerts, 0) AS total_fraud_alerts,
    COALESCE(a.flagged_volume_inr, 0) AS flagged_volume_inr,
    COALESCE(a.velocity_alerts, 0)    AS velocity_alerts,
    COALESCE(a.high_value_alerts, 0)  AS high_value_alerts,
    COALESCE(a.travel_alerts, 0)      AS travel_alerts
FROM txns t
LEFT JOIN alerts a ON t.txn_date = a.alert_date
ORDER BY t.txn_date DESC
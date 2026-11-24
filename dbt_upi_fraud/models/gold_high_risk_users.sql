{{ config(materialized='table') }}

SELECT
    user_id,
    COUNT(*)                         AS total_alerts,
    ROUND(SUM(total_amount), 2)      AS total_flagged_inr,
    MAX(max_amount)                  AS max_single_txn_inr,
    ARRAY_AGG(DISTINCT fraud_reason) AS triggered_rules,
    MAX(detected_at)                 AS last_flagged_at
FROM {{ source('raw_upi', 'RAW_FRAUD_ALERTS') }}
GROUP BY user_id
ORDER BY total_alerts DESC, total_flagged_inr DESC
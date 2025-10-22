{{ config(
    materialized = "table"
) }}

WITH source_data AS (
    SELECT
        customer_id,
        transaction_timestamp,
        _ingested_at
    FROM {{ ref('clean_sales') }}
),

deduped AS (
    SELECT
        customer_id,

        -- Capture activity windows
        MIN(transaction_timestamp) AS first_seen_at,
        MAX(transaction_timestamp) AS last_seen_at,

        -- Track ingestion for lineage (latest ingestion per customer)
        MAX(_ingested_at) AS ingested_at
    FROM source_data
    GROUP BY customer_id
),

final AS (
    SELECT
        -- Surrogate SK for warehouse best practices
        MD5(customer_id::text) AS customer_sk,

        customer_id,
        first_seen_at,
        last_seen_at,
        ingested_at,

        -- Placeholder for ML/scoring pipeline
        'TBD' AS customer_segment,

        -- Flag active customers (last seen in last 90 days)
        CASE
            WHEN last_seen_at > NOW() - INTERVAL '90 days' THEN TRUE
            ELSE FALSE
        END AS is_active,

        CURRENT_TIMESTAMP AS record_created_at
    FROM deduped
)

SELECT * FROM final

WITH base AS (
    SELECT
        customer_id,
        product_id,
        quantity,

        -- parse timestamp once
        TO_TIMESTAMP(transaction_timestamp, 'MM/DD/YYYY HH24:MI') AS parsed_ts,

        -- Extract clean datetime parts from the parsed timestamp
        CAST(TO_TIMESTAMP(transaction_timestamp, 'MM/DD/YYYY HH24:MI') AS DATE) AS sale_date,
        CAST(TO_TIMESTAMP(transaction_timestamp, 'MM/DD/YYYY HH24:MI') AS TIME) AS sale_time,
        EXTRACT(YEAR FROM TO_TIMESTAMP(transaction_timestamp, 'MM/DD/YYYY HH24:MI')) AS sale_year,
        EXTRACT(MONTH FROM TO_TIMESTAMP(transaction_timestamp, 'MM/DD/YYYY HH24:MI')) AS sale_month,
        EXTRACT(WEEK FROM TO_TIMESTAMP(transaction_timestamp, 'MM/DD/YYYY HH24:MI')) AS sale_week,

        -- cast double precision to numeric before rounding to 2 decimals
        ROUND(unit_price::numeric, 2)     AS unit_price,
        ROUND(total_amount::numeric, 2)   AS total_amount,
        ROUND(discount_percent::numeric, 2) AS discount_percent,

        -- Standardize text fields
        INITCAP(payment_method) AS payment_method,
        INITCAP(product_category) AS product_category,

        store_location,
        _ingested_at,

        -- expose parsed timestamp as the model's transaction_timestamp
        TO_TIMESTAMP(transaction_timestamp, 'MM/DD/YYYY HH24:MI') AS transaction_timestamp
    FROM {{ ref('stg_sales') }}
)

SELECT * FROM base
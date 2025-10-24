{{ config(
    materialized = "table"
) }}

WITH sales AS (
    SELECT * FROM {{ ref('clean_sales') }}
),

dim_store AS (
    SELECT store_id, raw_store_location
    FROM {{ ref('clean_stores') }}
),

dim_customer AS (
    SELECT customer_sk, customer_id
    FROM {{ ref('dim_customers') }}
),

dim_product AS (
    SELECT product_sk, product_category, unit_price
    FROM {{ ref('dim_products') }}
)

SELECT
    -- Fact table surrogate key
    MD5(CONCAT(s.customer_id::text, '|', s.product_category, '|', s.parsed_ts::text)) AS sales_sk,

    -- Join keys
    dc.customer_sk,
    dp.product_sk,
    ds.store_id AS store_sk,

    -- Base metrics
    s.quantity,
    s.unit_price,
    s.total_amount,
    s.discount_percent,

    -- Date breakdowns
    s.sale_date,
    s.sale_time,
    s.sale_year,
    s.sale_month,
    s.sale_week,

    -- Payment and category
    UPPER(s.payment_method) AS payment_method,
    UPPER(s.product_category) AS product_category,

    -- TS and lineage
    s.parsed_ts AS transaction_timestamp,
    s._ingested_at AS _fact_ingested_at,
    CURRENT_TIMESTAMP AS _fact_created_at

FROM sales s
LEFT JOIN dim_customer dc
    ON s.customer_id = dc.customer_id
LEFT JOIN dim_product dp
    ON UPPER(s.product_category) = dp.product_category
    AND s.unit_price = dp.unit_price
LEFT JOIN dim_store ds
    ON s.store_location = ds.raw_store_location

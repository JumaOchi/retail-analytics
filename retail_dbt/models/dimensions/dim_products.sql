{{ config(
    materialized = "table"
) }}

WITH base AS (
    SELECT
        -- Keep raw product ID for lineage but not as grain
        product_id AS raw_product_id,
        UPPER(product_category) AS product_category_norm,
        unit_price,
        discount_percent
    FROM {{ ref('clean_sales') }}
    WHERE product_category IS NOT NULL
      AND unit_price IS NOT NULL
),

aggregated AS (
    SELECT
        -- Surrogate key grain: category + price
        MD5(CONCAT(product_category_norm, '|', unit_price::text)) AS product_sk,

        MIN(raw_product_id) AS sample_raw_product_id,  -- just for lineage reference
        product_category_norm AS product_category,
        unit_price,

        -- Compute average discount at product grain
        ROUND(AVG(discount_percent)::numeric, 2) AS avg_discount_percent,

        CURRENT_TIMESTAMP AS _created_at
    FROM base
    GROUP BY product_category_norm, unit_price
)

SELECT * FROM aggregated

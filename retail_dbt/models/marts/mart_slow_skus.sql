{{ config(
    materialized = "table"
) }}

WITH base AS (
    SELECT
        dp.product_sk,
        dp.product_category,
        dp.unit_price,
        dp.avg_discount_percent,
        COUNT(DISTINCT fs.customer_sk) AS distinct_customers,
        SUM(fs.quantity) AS total_quantity_sold,
        SUM(fs.total_amount) AS total_revenue,
        MAX(fs.sale_date) AS last_sold_date
    FROM {{ ref('fact_sales') }} fs
    LEFT JOIN {{ ref('dim_products') }} dp
        ON fs.product_sk = dp.product_sk
    GROUP BY dp.product_sk, dp.product_category, dp.unit_price, dp.avg_discount_percent
),

ranked AS (
    SELECT
        *,
        NTILE(4) OVER (ORDER BY total_quantity_sold ASC) AS quantity_quartile,
        NTILE(4) OVER (ORDER BY total_revenue ASC) AS revenue_quartile
    FROM base
),

flagged AS (
    SELECT
        product_sk,
        product_category,
        unit_price,
        avg_discount_percent,
        distinct_customers,
        total_quantity_sold,
        total_revenue,
        last_sold_date,
        
        -- Mark bottom 25% by both quantity and revenue as slow movers
        CASE 
            WHEN quantity_quartile = 1 AND revenue_quartile = 1 THEN TRUE
            ELSE FALSE
        END AS is_slow_mover,

        CURRENT_TIMESTAMP AS _mart_created_at
    FROM ranked
)

SELECT * FROM flagged

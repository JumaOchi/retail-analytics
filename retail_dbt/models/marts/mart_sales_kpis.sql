{{ config(
    materialized = "table"
) }}

WITH base AS (
    SELECT
        fs.sale_date,
        fs.sale_year,
        fs.sale_month,
        fs.sale_week,
        dp.product_category,
        COUNT(DISTINCT fs.sales_sk) AS total_transactions,
        SUM(fs.total_amount) AS total_revenue,
        SUM(fs.discount_percent) AS total_discount_percent,
        SUM(fs.quantity) AS total_quantity,
        AVG(fs.unit_price) AS avg_unit_price
    FROM {{ ref('fact_sales') }} fs
    LEFT JOIN {{ ref('dim_products') }} dp
        ON fs.product_sk = dp.product_sk
    GROUP BY
        fs.sale_date, fs.sale_year, fs.sale_month, fs.sale_week, dp.product_category
),

agg AS (
    SELECT
        sale_year,
        sale_month,
        COUNT(DISTINCT sale_date) AS active_days,
        SUM(total_transactions) AS total_transactions,
        SUM(total_revenue) AS total_revenue,
        ROUND(SUM(total_revenue) / NULLIF(SUM(total_transactions), 0), 2) AS avg_order_value,
        ROUND(SUM(total_discount_percent)::numeric / NULLIF(SUM(total_transactions), 0), 2) AS avg_discount_percent,
        SUM(total_quantity) AS total_units_sold
    FROM base
    GROUP BY sale_year, sale_month
),

ranked_categories AS (
    SELECT
        sale_year,
        sale_month,
        product_category,
        SUM(total_revenue) AS category_revenue,
        RANK() OVER (PARTITION BY sale_year, sale_month ORDER BY SUM(total_revenue) DESC) AS category_rank
    FROM base
    GROUP BY sale_year, sale_month, product_category
),

top_categories AS (
    SELECT
        sale_year,
        sale_month,
        product_category AS top_category,
        category_revenue AS top_category_revenue
    FROM ranked_categories
    WHERE category_rank = 1
)

SELECT
    a.sale_year,
    a.sale_month,
    a.active_days,
    a.total_transactions,
    a.total_revenue,
    a.avg_order_value,
    a.avg_discount_percent,
    a.total_units_sold,
    t.top_category,
    t.top_category_revenue,
    CURRENT_TIMESTAMP AS _mart_created_at
FROM agg a
LEFT JOIN top_categories t
    ON a.sale_year = t.sale_year
   AND a.sale_month = t.sale_month

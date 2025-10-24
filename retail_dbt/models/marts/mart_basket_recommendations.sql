{{ config(
    materialized = "table"
) }}

WITH base AS (
    SELECT
        customer_sk,
        product_sk,
        product_category,
        sale_date
    FROM {{ ref('fact_sales') }}
    WHERE product_sk IS NOT NULL
      AND customer_sk IS NOT NULL
),

baskets AS (
    -- Pair products bought by same customer on same date
    SELECT
        a.product_sk AS product_a,
        b.product_sk AS product_b,
        a.sale_date
    FROM base a
    JOIN base b
      ON a.customer_sk = b.customer_sk
     AND a.sale_date = b.sale_date
     AND a.product_sk <> b.product_sk
),

cooccurrence AS (
    SELECT
        product_a,
        product_b,
        COUNT(*) AS cooccurrence_count,
        MAX(sale_date) AS last_seen_date
    FROM baskets
    GROUP BY product_a, product_b
),

basket_stats AS (
    SELECT
        COUNT(DISTINCT customer_sk || '-' || sale_date) AS total_baskets
    FROM base
),

final AS (
    SELECT
        c.product_a,
        c.product_b,
        c.cooccurrence_count,
        ROUND(c.cooccurrence_count * 100.0 / b.total_baskets, 2) AS support,
        c.last_seen_date,
        CURRENT_TIMESTAMP AS _generated_at
    FROM cooccurrence c
    CROSS JOIN basket_stats b
)

SELECT * FROM final

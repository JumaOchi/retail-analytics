{{ config(
    materialized = "table"
) }}

WITH sales AS (
    SELECT
        customer_sk,
        product_category,
        SUM(total_amount) AS total_spent,
        COUNT(*) AS total_txn
    FROM {{ ref('fact_sales') }}
    WHERE product_category IS NOT NULL
    GROUP BY customer_sk, product_category
),

-- Calculate overall totals per customer to get % contribution by category
customer_totals AS (
    SELECT
        customer_sk,
        SUM(total_spent) AS customer_total_spent
    FROM sales
    GROUP BY customer_sk
),

-- Derive preference strength per customer-category
customer_pref AS (
    SELECT
        s.customer_sk,
        s.product_category,
        s.total_spent,
        s.total_txn,
        ct.customer_total_spent,
        ROUND( (s.total_spent / ct.customer_total_spent) * 100, 2 ) AS pct_of_spend
    FROM sales s
    JOIN customer_totals ct
      ON s.customer_sk = ct.customer_sk
),

-- Identify each customer’s top category by spend
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY customer_sk ORDER BY total_spent DESC) AS rank_in_cust
    FROM customer_pref
),

top_category_pref AS (
    SELECT
        customer_sk,
        product_category AS top_category,
        total_spent AS top_category_spend,
        pct_of_spend,
        total_txn
    FROM ranked
    WHERE rank_in_cust = 1
),

-- Now aggregate to see how these categories perform overall
category_summary AS (
    SELECT
        top_category,
        COUNT(DISTINCT customer_sk) AS customers_count,
        SUM(top_category_spend) AS total_revenue,
        ROUND(AVG(pct_of_spend), 2) AS avg_share_of_wallet,
        ROUND(AVG(total_txn), 2) AS avg_txn_per_cust
    FROM top_category_pref
    GROUP BY top_category
),

-- Create readable "segment labels"
labeled AS (
    SELECT
        top_category AS segment_label,
        customers_count,
        total_revenue,
        avg_share_of_wallet,
        avg_txn_per_cust,
        CASE
            WHEN total_revenue > (SELECT AVG(total_revenue) FROM category_summary)
                THEN ' High-Value Category'
            ELSE ' Regular Category'
        END AS category_tier,
        CURRENT_TIMESTAMP AS _created_at
    FROM category_summary
)

SELECT * FROM labeled

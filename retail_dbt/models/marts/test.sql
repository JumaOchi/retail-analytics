WITH txn_items AS (
    SELECT
        customer_sk,
        transaction_timestamp,
        COUNT(DISTINCT product_sk) AS product_count
    FROM {{ ref('fact_sales') }}
    GROUP BY customer_sk, transaction_timestamp
),

summary AS (
    SELECT
        customer_sk,
        COUNT(*) AS total_txns,
        SUM(CASE WHEN product_count > 1 THEN 1 ELSE 0 END) AS multi_item_txns
    FROM txn_items
    GROUP BY customer_sk
)

SELECT
    customer_sk,
    total_txns,
    multi_item_txns,
    ROUND(multi_item_txns::numeric / NULLIF(total_txns, 0), 3) AS pct_multi_item_txns
FROM summary
ORDER BY total_txns DESC

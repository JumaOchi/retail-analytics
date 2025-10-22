-- ...existing code...
WITH source AS (
    SELECT
        customerid,
        productid,
        quantity,
        price,
        transactiondate,
        paymentmethod,
        storelocation,
        productcategory,
        discountapplied AS discountapplied_raw,
        totalamount,
        CURRENT_TIMESTAMP AS _ingested_at
    FROM {{ source('raw', 'transactions_raw') }}
)

SELECT
    customerid                       AS customer_id,
    productid                        AS product_id,
    CAST(quantity AS INTEGER)        AS quantity,

    -- normalize price to double precision
    (
      -- try native numeric value first
      CASE
        WHEN price IS NULL THEN NULL
        WHEN (price::text) ~ '^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?$'
          THEN price::double precision
        ELSE
          -- try common cleaning strategies:
          -- 1) replace malformed exponent like "4.6-05" -> "4.6e-05"
          -- 2) remove non-numeric characters (except e/E + - .)
          -- only cast if result matches numeric pattern
          CASE
            WHEN REGEXP_REPLACE(price::text, '([0-9]+(\.[0-9]+)?)\-([0-9]+)$', '\1e-\3') 
                 ~ '^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?$'
              THEN REGEXP_REPLACE(price::text, '([0-9]+(\.[0-9]+)?)\-([0-9]+)$', '\1e-\3')::double precision
            WHEN REGEXP_REPLACE(price::text, '[^0-9eE.+-]', '', 'g')
                 ~ '^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?$'
              THEN REGEXP_REPLACE(price::text, '[^0-9eE.+-]', '', 'g')::double precision
            ELSE NULL
          END
      END
    ) AS unit_price,

    -- normalize totalamount to double precision (same logic)
    (
      CASE
        WHEN totalamount IS NULL THEN NULL
        WHEN (totalamount::text) ~ '^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?$'
          THEN totalamount::double precision
        ELSE
          CASE
            WHEN REGEXP_REPLACE(totalamount::text, '([0-9]+(\.[0-9]+)?)\-([0-9]+)$', '\1e-\3')
                 ~ '^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?$'
              THEN REGEXP_REPLACE(totalamount::text, '([0-9]+(\.[0-9]+)?)\-([0-9]+)$', '\1e-\3')::double precision
            WHEN REGEXP_REPLACE(totalamount::text, '[^0-9eE.+-]', '', 'g')
                 ~ '^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?$'
              THEN REGEXP_REPLACE(totalamount::text, '[^0-9eE.+-]', '', 'g')::double precision
            ELSE NULL
          END
      END
    ) AS total_amount,

    paymentmethod                    AS payment_method,
    productcategory                  AS product_category,
    storelocation                    AS store_location,

    -- discount: strip percent and normalize to double precision
    (
      CASE
        WHEN discountapplied_raw IS NULL THEN NULL
        WHEN (discountapplied_raw::text) ~ '^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?$'
          THEN discountapplied_raw::double precision
        ELSE
          -- handle forms like "12%", "12.5%", "4.6-05%"
          CASE
            WHEN REGEXP_REPLACE(discountapplied_raw::text, '([0-9]+(\.[0-9]+)?)\-([0-9]+)%?$', '\1e-\3') 
                 ~ '^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?$'
              THEN REGEXP_REPLACE(discountapplied_raw::text, '([0-9]+(\.[0-9]+)?)\-([0-9]+)%?$', '\1e-\3')::double precision
            WHEN REGEXP_REPLACE(discountapplied_raw::text, '[^0-9eE.+-]', '', 'g')
                 ~ '^[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?$'
              THEN REGEXP_REPLACE(discountapplied_raw::text, '[^0-9eE.+-]', '', 'g')::double precision
            ELSE NULL
          END
      END
    ) AS discount_percent,

    transactiondate                  AS transaction_timestamp,
    _ingested_at
FROM source
-- ...existing code...
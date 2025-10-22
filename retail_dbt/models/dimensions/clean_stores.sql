{{ config(materialized="view") }}

WITH base AS (
    SELECT
        store_location AS raw_store_location
        -- Include any necessary primary/foreign keys from 'clean_sales' here 
        -- e.g., store_pk_id, sale_id
    FROM {{ ref('clean_sales') }}
    WHERE store_location IS NOT NULL
),

parsed AS (
    SELECT
        raw_store_location,

        -- 1. Extract ZIPCODE (last 5 digits) - This should still be fine
        TRIM(REGEXP_SUBSTR(raw_store_location, '\d{5}$')) AS zipcode,

        -- 2. Extract STATE (2 capital letters just before ZIP) - MODIFIED
        -- Pattern: (Non-capturing group for delimiter: ", " OR " ") 
        --          followed by (Capture Group 1: State Code [A-Z]{2}) 
        --          followed by (space and ZIP).
        -- Use the 'i' flag for case-insensitivity just in case, though your data is uppercase.
        -- We're now asking for the second capture group (2) because the delimiter is the first one.
        TRIM(REGEXP_SUBSTR(raw_store_location, '(?:,\s|\s)([A-Z]{2})\s\d{5}$', 1, 1, 'i', 1)) AS state_code,

        -- 3. Extract ADDRESS (everything before state and zip) - MODIFIED for consistency
        -- Pattern to remove: one or more separators (space or comma/space) followed by State, space, and ZIP at the end.
        TRIM(
            REGEXP_REPLACE(
                raw_store_location,
                '(\s|,\s)[A-Z]{2}\s\d{5}$', 
                '', 
                'i' -- Use 'i' flag for case-insensitivity on removal as well
            ),
            ' ,.' -- Final trim to remove any trailing commas, spaces, or periods
        ) AS address
    FROM base
)

SELECT
    -- If you want a stable store_id across runs, use a stable source PK, not MD5(raw_store_location)
    -- If you must use MD5 for this, it's fine for this view/model.
    MD5(TRIM(raw_store_location)) AS store_id, 
    raw_store_location,
    NULLIF(address, '') AS address,
    NULLIF(state_code, '') AS state_code,
    zipcode,
    CURRENT_TIMESTAMP AS _cleaned_at
FROM parsed